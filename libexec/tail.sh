#!/usr/bin/env bash
set -euo pipefail

WARTT_ROOT="${WARTT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
# shellcheck disable=SC1090
source "$WARTT_ROOT/libexec/common.sh"

SINCE="30m"
MESSAGE_TYPE="all"
STATUS_FILTER="all"
MODEL_FILTER=""
FOLLOW=0
HUMAN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --since)
      [[ $# -ge 2 ]] || die "--since requires a value"
      SINCE="$2"
      shift 2
      ;;
    --message-type)
      [[ $# -ge 2 ]] || die "--message-type requires a value"
      MESSAGE_TYPE="$2"
      shift 2
      ;;
    --status)
      [[ $# -ge 2 ]] || die "--status requires a value"
      STATUS_FILTER="$2"
      shift 2
      ;;
    --model)
      [[ $# -ge 2 ]] || die "--model requires a value"
      MODEL_FILTER="$2"
      shift 2
      ;;
    --follow)
      FOLLOW=1
      shift
      ;;
    --human)
      HUMAN=1
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: tail.sh [--since <duration>] [--message-type <text|voice|all>] [--status <ok|error|timeout|partial|all>] [--model <glob>] [--human] [--follow]
EOF
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

regex_model=""
if [[ -n "$MODEL_FILTER" ]]; then
  regex_model="$(glob_to_regex "$MODEL_FILTER")"
fi

"$WARTT_ROOT/libexec/aggregate.sh" --since "$SINCE"

if [[ "$HUMAN" -eq 1 ]]; then
  source_file="$WARTT_HUMAN_FILE"
  msg_col=3
  status_col=4
  model_col=16
else
  source_file="$WARTT_SUMMARY_FILE"
  msg_col=4
  status_col=19
  model_col=18
fi

filter_cmd=(awk -F, -v msg_filter="$MESSAGE_TYPE" -v status_filter="$STATUS_FILTER" -v model_regex="$regex_model" -v msg_col="$msg_col" -v status_col="$status_col" -v model_col="$model_col" '
NR == 1 { print; next }
{
  if (msg_filter != "all" && $msg_col != msg_filter) next
  if (status_filter != "all" && $status_col != status_filter) next
  if (model_regex != "" && $model_col !~ model_regex) next
  print
}
')

if [[ "$FOLLOW" -eq 1 ]]; then
  printf "Following %s\n" "$source_file"
  "${filter_cmd[@]}" "$source_file"
  tail -n 0 -F "$source_file" | awk -F, -v msg_filter="$MESSAGE_TYPE" -v status_filter="$STATUS_FILTER" -v model_regex="$regex_model" -v msg_col="$msg_col" -v status_col="$status_col" -v model_col="$model_col" '
  {
    if (msg_filter != "all" && $msg_col != msg_filter) next
    if (status_filter != "all" && $status_col != status_filter) next
    if (model_regex != "" && $model_col !~ model_regex) next
    print
    fflush()
  }
  '
else
  "${filter_cmd[@]}" "$source_file"
fi
