#!/bin/bash
# 本地 ↔ HPC 双向同步：
#   --push    推送代码到 HPC
#   --pull    拉取 parquet 数据到本地
#   默认      双向上传代码、下拉数据
set -e

REMOTE_USER="guxh01"
REMOTE_HOST="111.172.12.146"
REMOTE_PORT="4351"
REMOTE_PASS="${HPC_PASSWORD:-Guxh_Extreme2024_01}"
REMOTE_PATH="/data/users/guxh01/2026_tcb/lake"
LOCAL_PROJECT="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL_DATA="$LOCAL_PROJECT/data/parquet"
REMOTE_DATA="$REMOTE_PATH/lake_data"
REMOTE_PROJECT="$REMOTE_PATH/global_lake_adft"
REMOTE_DATA_GT10="$REMOTE_PATH/lake_data_gt10"
LOCAL_DATA_GT10="$LOCAL_PROJECT/data/parquet_gt10"
REMOTE_ARCHIVE="$REMOTE_PATH/data_archive_20260426"

SSH_CMD=()
build_ssh_cmd() {
    SSH_CMD=(sshpass -p "$REMOTE_PASS" ssh -p "$REMOTE_PORT"
             -o StrictHostKeyChecking=no -o PubkeyAuthentication=no -o PreferredAuthentications=password
             -o ConnectTimeout=30 -o ServerAliveInterval=5)
}
build_ssh_cmd

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

pull_data() {
    pull_parquet "$REMOTE_DATA" "$LOCAL_DATA"
}

pull_data_gt10() {
    pull_parquet "$REMOTE_DATA_GT10" "$LOCAL_DATA_GT10"
}

push_code() {
    echo ">>> 推送代码 → HPC"
    sshpass -p "$REMOTE_PASS" rsync -avz --progress \
        "${RSYNC_SSH[@]}" \
        --exclude='.venv' --exclude='.git' --exclude='__pycache__' \
        --exclude='*.pyc' --exclude='data/' --exclude='logs/' \
        --exclude='.pytest_cache' --exclude='*.egg-info' \
        --exclude='node_modules' --exclude='dist/' --exclude='build/' \
        "$LOCAL_PROJECT/" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_PROJECT/"
    echo ">>> HPC git pull (fast-forward)"
    "${SSH_CMD[@]}" "$REMOTE_USER@$REMOTE_HOST" \
        "cd $REMOTE_PROJECT && git stash && git pull origin main --ff-only"
    echo "完成"
}

case "${1:-}" in
    --push)        push_code ;;
    --pull)        pull_data ;;
    --pull-gt10)   pull_data_gt10 ;;
    --pull-all)    pull_data && pull_data_gt10 ;;
    *)             push_code && pull_data ;;
esac
