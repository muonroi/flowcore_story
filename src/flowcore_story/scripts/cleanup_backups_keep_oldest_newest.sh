#!/usr/bin/env bash
set -euo pipefail

backup_dir="/home/storyflow-backups"

if [[ ! -d "$backup_dir" ]]; then
  exit 0
fi

mapfile -d '' records < <(
  find "$backup_dir" -maxdepth 1 -mindepth 1 -type d -printf '%T@ %p\0' | sort -z -n
)

count=${#records[@]}
if (( count <= 2 )); then
  exit 0
fi

oldest_path=${records[0]#* }
newest_path=${records[count-1]#* }

for ((i=1; i<count-1; i++)); do
  path=${records[i]#* }
  if [[ "$path" != "$oldest_path" && "$path" != "$newest_path" ]]; then
    rm -rf -- "$path"
  fi
done
