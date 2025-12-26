#!/usr/bin/env bash
set -e
N=${1:-4}        # 起動するプロセス数（まず4から）
PER=${2:-4}      # 各プロセス内の並列（合計= N*PER）
MIN=1
MAX=$(sqlite3 data/companies.db 'select max(id) from companies;')
R=$(( (MAX - MIN + 1 + N -1)/N ))
mkdir -p logs
for i in $(seq 0 $((N-1))); do
  S=$(( MIN + i*R ))
  E=$(( S + R - 1 ))
  if [ "$S" -gt "$MAX" ]; then
    break
  fi
  if [ "$E" -gt "$MAX" ]; then
    E="$MAX"
  fi
  echo "shard $i: $S..$E"
  WORKER_ID="w$((i+1))" ID_MIN=$S ID_MAX=$E FETCH_CONCURRENCY=$PER \
    nohup bash -c ": > logs/app-$i.log; python main.py >> logs/app-$i.log 2>&1" &
done
echo "起動完了。進捗は logs/app-*.log を参照。停止は: pkill -f 'python main.py'"
