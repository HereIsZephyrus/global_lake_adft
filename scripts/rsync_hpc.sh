#!/bin/bash
# 统一同步 HPC 非 Git 目录。
# 代码仓库通过 git push / git clone 管理，不走 rsync。
#
#   --push-data              本地 data/ -> HPC lake_data/
#   --pull-data              HPC lake_data/ -> 本地 data/
#   --push-lsf               本地 lsf/ -> HPC lsf/
#   --pull-lsf               HPC lsf/ -> 本地 lsf/
#   --pull-output            HPC output/ -> 本地 output/
#   --push-output            本地 output/ -> HPC output/
#   --filter <name>          仅同步 output/<name>/ 子树（仅 output 命令支持）
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$SCRIPT_DIR/../.env" ]; then
    set -a
    source "$SCRIPT_DIR/../.env"
    set +a
fi

REMOTE_USER="${HPC_USER:-guxh01}"
REMOTE_HOST="${HPC_HOST:?HPC_HOST is required. Set it in .env or export it.}"
REMOTE_PORT="${HPC_PORT:-4351}"
REMOTE_PASS="${HPC_PASSWORD:?HPC_PASSWORD is required. Set it in .env or export it.}"
REMOTE_ROOT="/data/users/guxh01/2026_tcb/lake"
LOCAL_REPO="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL_DATA="$LOCAL_REPO/data"
LOCAL_LSF="$LOCAL_REPO/lsf"
LOCAL_OUTPUT="$LOCAL_REPO/output"
REMOTE_DATA="$REMOTE_ROOT/lake_data"
REMOTE_LSF="$REMOTE_ROOT/lsf"
REMOTE_REPO="$REMOTE_ROOT/global_lake_adft"
REMOTE_OUTPUT="$REMOTE_REPO/output"

FILTER_NAME=""
ACTION=""

RSYNC_SSH=(
    -e
    "sshpass -p $REMOTE_PASS ssh -p $REMOTE_PORT -o StrictHostKeyChecking=no -o PubkeyAuthentication=no -o PreferredAuthentications=password -o ConnectTimeout=30"
)

usage() {
    cat <<'EOF'
Usage:
  bash scripts/rsync_hpc.sh --push-data
  bash scripts/rsync_hpc.sh --pull-data
  bash scripts/rsync_hpc.sh --push-lsf
  bash scripts/rsync_hpc.sh --pull-lsf
  bash scripts/rsync_hpc.sh --pull-output [--filter full|gt10|no_pwm_err]
  bash scripts/rsync_hpc.sh --push-output [--filter full|gt10|no_pwm_err]

Notes:
  - 本脚本只同步非 Git 目录：data、lsf、output。
  - HPC 代码仓库请使用 Git 同步，不要用 rsync 覆盖工作区。
  - output 默认同步整个 output/；带 --filter 时仅同步 output/<filter>/。
EOF
}

ensure_local_dir() {
    mkdir -p "$1"
}

sync_to_remote() {
    local local_dir="$1"
    local remote_dir="$2"
    local label="$3"
    echo ">>> 推送 $label: $local_dir -> $remote_dir"
    ensure_local_dir "$local_dir"
    sshpass -p "$REMOTE_PASS" rsync -avz --progress \
        "${RSYNC_SSH[@]}" \
        "$local_dir/" "$REMOTE_USER@$REMOTE_HOST:$remote_dir/"
    echo "完成"
}

sync_from_remote() {
    local remote_dir="$1"
    local local_dir="$2"
    local label="$3"
    echo ">>> 拉取 $label: $remote_dir -> $local_dir"
    ensure_local_dir "$local_dir"
    sshpass -p "$REMOTE_PASS" rsync -avz --progress \
        "${RSYNC_SSH[@]}" \
        "$REMOTE_USER@$REMOTE_HOST:$remote_dir/" "$local_dir/"
    echo "完成"
}

output_paths() {
    if [ -n "$FILTER_NAME" ]; then
        printf '%s\n%s\n' "$LOCAL_OUTPUT/$FILTER_NAME" "$REMOTE_OUTPUT/$FILTER_NAME"
        return
    fi

    printf '%s\n%s\n' "$LOCAL_OUTPUT" "$REMOTE_OUTPUT"
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --push-data|--pull-data|--push-lsf|--pull-lsf|--pull-output|--push-output)
            if [ -n "$ACTION" ]; then
                echo "Only one action can be specified per run." >&2
                usage >&2
                exit 1
            fi
            ACTION="$1"
            shift
            ;;
        --filter)
            if [ "$#" -lt 2 ]; then
                echo "--filter requires a value." >&2
                usage >&2
                exit 1
            fi
            FILTER_NAME="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if [ -z "$ACTION" ]; then
    usage >&2
    exit 1
fi

case "$ACTION" in
    --push-data)
        sync_to_remote "$LOCAL_DATA" "$REMOTE_DATA" "共享输入 data"
        ;;
    --pull-data)
        sync_from_remote "$REMOTE_DATA" "$LOCAL_DATA" "共享输入 data"
        ;;
    --push-lsf)
        sync_to_remote "$LOCAL_LSF" "$REMOTE_LSF" "LSF 脚本"
        ;;
    --pull-lsf)
        sync_from_remote "$REMOTE_LSF" "$LOCAL_LSF" "LSF 脚本"
        ;;
    --pull-output)
        mapfile -t paths < <(output_paths)
        sync_from_remote "${paths[1]}" "${paths[0]}" "输出 output"
        ;;
    --push-output)
        mapfile -t paths < <(output_paths)
        sync_to_remote "${paths[0]}" "${paths[1]}" "输出 output"
        ;;
esac
