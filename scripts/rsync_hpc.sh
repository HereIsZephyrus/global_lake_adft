#!/bin/bash
# 从 HPC 拉取 parquet 数据到本地。
# 代码推送用 git push hpc main，与本脚本分离。
#
#   --pull        拉取全量 parquet
#   --pull-gt10   拉取 gt10 数据
#   --pull-all    拉取全部
set -e

REMOTE_USER="guxh01"
REMOTE_HOST="111.172.12.146"
REMOTE_PORT="4351"
REMOTE_PASS="${HPC_PASSWORD:-Guxh_Extreme2024_01}"
REMOTE_PATH="/data/users/guxh01/2026_tcb/lake"
LOCAL_PROJECT="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL_DATA="$LOCAL_PROJECT/data/parquet"
REMOTE_DATA="$REMOTE_PATH/lake_data"
LOCAL_DATA_GT10="$LOCAL_PROJECT/data/parquet_gt10"
REMOTE_DATA_GT10="$REMOTE_PATH/lake_data_gt10"
REMOTE_ARCHIVE="$REMOTE_PATH/data_archive_20260426"

RSYNC_SSH=(-e "sshpass -p $REMOTE_PASS ssh -p $REMOTE_PORT -o StrictHostKeyChecking=no -o PubkeyAuthentication=no -o PreferredAuthentications=password -o ConnectTimeout=30")

pull_parquet() {
    local remote_data="${1:-$REMOTE_DATA}"
    local local_data="${2:-$LOCAL_DATA}"
    echo ">>> 拉取 HPC parquet: $remote_data → $local_data"
    mkdir -p "$local_data"
    sshpass -p "$REMOTE_PASS" rsync -avz --progress \
        "${RSYNC_SSH[@]}" \
        "$REMOTE_USER@$REMOTE_HOST:$remote_data/" "$local_data/"
    echo "完成"
}

case "${1:-}" in
    --pull)        pull_parquet "$REMOTE_DATA" "$LOCAL_DATA" ;;
    --pull-gt10)   pull_parquet "$REMOTE_DATA_GT10" "$LOCAL_DATA_GT10" ;;
    --pull-all)    pull_parquet "$REMOTE_DATA" "$LOCAL_DATA"
                   pull_parquet "$REMOTE_DATA_GT10" "$LOCAL_DATA_GT10" ;;
    *)             echo "Usage: $0 --pull | --pull-gt10 | --pull-all" >&2
                   exit 1 ;;
esac
