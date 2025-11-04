#!/usr/bin/env bash
set -e
N=${1:-4}        # 起動するプロセス数（まず4から）
PER=${2:-4}      # 各プロセス内の並列（合計= N*PER）
MIN=1
MAX=$(sqlite3 data/companies.db 'select max(id) from companies;')
R=$(( (MAX - MIN + 1 + N -1)/N ))
for i in $(seq 0 $((N-1))); do
  S=$(( MIN + i*R ))
  E=$(( S + R - 1 ))
  echo "shard $i: $S..$E"
  ID_MIN=$S ID_MAX=$E CONCURRENCY=$PER \
    nohup bash -c ": > logs/app-$i.log; python main.py >> logs/app-$i.log 2>&1" &
done
echo "起動完了。進捗は logs/app-*.log を参照。停止は: pkill -f 'python main.py'"
