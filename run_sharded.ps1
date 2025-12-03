Param(
    [int]$Workers = 4,          # 起動するワーカー数
    [int]$PerWorkerConcurrency = 4,  # 各ワーカーの FETCH_CONCURRENCY 相当
    [string]$DbPath = "data/companies.db"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path $DbPath)) {
    Write-Error "SQLite DB が見つかりません: $DbPath"
    exit 1
}

if (-not (Test-Path "logs")) {
    New-Item -ItemType Directory -Path "logs" | Out-Null
}

# python 実行ファイルを決定（仮想環境があれば優先）
$python = Join-Path ".venv" "Scripts\python.exe"
if (-not (Test-Path $python)) {
    $python = "python"
}

# SQLite から max(id) を取得
$code = @"
import sqlite3
conn = sqlite3.connect(r"$DbPath")
cur = conn.cursor()
cur.execute("select max(id) from companies")
row = cur.fetchone()
print(row[0] or 0)
"@

try {
    $maxIdStr = & $python -c $code
} catch {
    Write-Error "max(id) 取得に失敗しました。python/SQLite 利用可否を確認してください。`n$($_.Exception.Message)"
    exit 1
}

if (-not $maxIdStr) {
    Write-Error "max(id) が取得できませんでした。DB に companies テーブルが存在するか確認してください。"
    exit 1
}

try {
    $maxId = [int]$maxIdStr
} catch {
    Write-Error "max(id) の変換に失敗しました: '$maxIdStr'"
    exit 1
}

if ($maxId -lt 1) {
    Write-Error "companies テーブルに有効な id がありません。"
    exit 1
}

$minId = 1
$range = [int][Math]::Ceiling( ($maxId - $minId + 1.0) / $Workers )

Write-Host "workers=$Workers, per_worker_concurrency=$PerWorkerConcurrency, id_range=$minId..$maxId (chunk=$range)"

for ($i = 0; $i -lt $Workers; $i++) {
    $start = $minId + $i * $range
    if ($start -gt $maxId) { break }
    $end = [Math]::Min($start + $range - 1, $maxId)

    $workerId = "w$($i + 1)"
    $logPath = "logs/app-$i.log"

    Write-Host "shard $i (WORKER_ID=$workerId): $start..$end -> $logPath"

    $env:WORKER_ID = $workerId
    $env:ID_MIN = $start
    $env:ID_MAX = $end
    $env:FETCH_CONCURRENCY = $PerWorkerConcurrency

    # 非同期で main.py を起動（stdout/stderr を同一ログにリダイレクト）
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $python
    $psi.Arguments = "main.py"
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.WorkingDirectory = (Get-Location).Path

    $proc = New-Object System.Diagnostics.Process
    $proc.StartInfo = $psi
    $null = $proc.Start()

    # ログ書き込みを別スレッドで非同期に実施
    Start-Job -ScriptBlock {
        param($pId, $logFile)
        $p = [System.Diagnostics.Process]::GetProcessById($pId)
        $logDir = [System.IO.Path]::GetDirectoryName($logFile)
        if (-not [string]::IsNullOrEmpty($logDir) -and -not (Test-Path $logDir)) {
            New-Item -ItemType Directory -Path $logDir | Out-Null
        }
        $fs = [System.IO.File]::Create($logFile)
        $sw = New-Object System.IO.StreamWriter($fs, [System.Text.Encoding]::UTF8)
        try {
            while (-not $p.HasExited) {
                while (-not $p.StandardOutput.EndOfStream) {
                    $sw.WriteLine($p.StandardOutput.ReadLine())
                }
                while (-not $p.StandardError.EndOfStream) {
                    $sw.WriteLine($p.StandardError.ReadLine())
                }
                $sw.Flush()
                Start-Sleep -Seconds 1
            }
            while (-not $p.StandardOutput.EndOfStream) {
                $sw.WriteLine($p.StandardOutput.ReadLine())
            }
            while (-not $p.StandardError.EndOfStream) {
                $sw.WriteLine($p.StandardError.ReadLine())
            }
            $sw.Flush()
        } finally {
            $sw.Dispose()
            $fs.Dispose()
        }
    } -ArgumentList $proc.Id, (Resolve-Path $logPath).Path | Out-Null
}

Write-Host "全ワーカーを起動しました。ログは logs/app-*.log を参照してください。"

