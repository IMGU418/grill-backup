#!/usr/bin/env bash
# 모델 백업 작업 (.github/workflows/model-backup.yml 이 러너 여러 대로 돌린다)
# 이 저장소는 공개라 Actions 로그를 누구나 본다: 로그에는 파일 이름·주소·오류 원문을 남기지 않고 sha256 앞 8자리와 개수만 남긴다
# (실패 이유는 로비로만 보낸다).
# 로비에 아직 아무도 맡지 않은 일을 조금씩 받아(WORKER 이름으로 맡는다) → 원래 주소에서 받아 크기·sha256을 확인한 파일만
# R2(models/<sha256>)에 올리고 → 결과를 알린다. 다 하면 다시 묻고, 빈 답이면 끝낸다 (도는 동안 새로 등록한 모델도 받는다).
# 받기는 stream.py 가 한다: 여러 연결로 받으면서 바로 R2에 올린다 (디스크를 거치지 않는다), 파일 3개씩 동시에.
# 어떤 앱도 쓰지 않게 된 지 7일 지난 파일은 지운다.
#
# 환경변수: LOBBY_URL (로비 주소), BACKUP_SECRET (로비의 GRILL_BACKUP_SECRET),
#           R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET
#           R2_ENDPOINT (선택: 시험용 S3 호환 저장소), WORK_DIR (선택: 받는 곳)
#           CIVITAI_API_KEY (선택: Civitai는 로그인 없이 받을 수 없다. 이 키로 받는다)
#           WORKER (선택: 이 러너의 이름. 없으면 한 번에 모든 일을 받는다), BACKUP_MAX_MIN (선택: 새 일을 받지 않기 시작하는 분, 기본 280)
set -euo pipefail

: "${LOBBY_URL:?}" "${BACKUP_SECRET:?}" "${R2_ACCESS_KEY_ID:?}" "${R2_SECRET_ACCESS_KEY:?}" "${R2_BUCKET:?}"
EP="${R2_ENDPOINT:-https://${R2_ACCOUNT_ID:?}.r2.cloudflarestorage.com}"
export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY" AWS_DEFAULT_REGION=auto
# R2는 최신 AWS CLI가 기본으로 붙이는 체크섬 일부를 받지 않는다
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
WORK="${WORK_DIR:-$(mktemp -d)}"
mkdir -p "$WORK"
LOBBY="${LOBBY_URL%/}/api"

api() { curl -fsS --retry 3 -H "Authorization: Bearer $BACKUP_SECRET" "$@"; }
report() { api -X POST -H 'Content-Type: application/json' -d "$1" "$LOBBY/backups/report" > /dev/null; }
s3() { aws --endpoint-url "$EP" "$@"; }
free_bytes() { df -B1 --output=avail "$WORK" | tail -1 | tr -d ' '; }
# R2에 이미 있는지 (같은 크기): 있으면 다시 받거나 올리지 않는다
in_r2() { [ "$(s3 s3api head-object --bucket "$R2_BUCKET" --key "models/$1" --query ContentLength --output text 2>/dev/null)" = "$2" ]; }
cleaned=0

# 받기: Civitai 주소에만 키를 붙인다 (curl은 다른 호스트로 넘겨줄 때 Authorization을 보내지 않는다)
download() {
  local url="$1" out="$2" max="$3" host
  host=$(sed -E 's#^https://([^/:]+).*#\1#' <<< "$url" | tr 'A-Z' 'a-z')
  local auth=()
  if [ -n "${CIVITAI_API_KEY:-}" ] && [[ "$host" =~ (^|\.)civitai\.(com|red|green)$ ]]; then auth=(-H "Authorization: Bearer $CIVITAI_API_KEY"); fi
  curl -fsSL --retry 5 --retry-all-errors --max-filesize "$max" -o "$out" -A "grill-backup" "${auth[@]}" "$url"
}

HERE="$(cd "$(dirname "$0")" && pwd)"
ok=0; failed=0; pinned=0
started=$(date +%s)
while :; do
  if [ -n "${WORKER:-}" ]; then
    # 제한 시간 안에 끝내려고 일정 시간이 지나면 새 일을 받지 않는다 (맡은 일은 시간이 지나면 다른 러너가 맡는다)
    (( $(date +%s) - started < ${BACKUP_MAX_MIN:-280} * 60 )) || { echo "시간이 다 되어 새 일을 받지 않습니다"; break; }
    work=$(api "$LOBBY/backups/work?worker=$(jq -rn --arg w "$WORKER" '$w|@uri')")
  else
    work=$(api "$LOBBY/backups/work")
  fi
  [ "$(jq '[.pending, .pin // [], .verify // [], .remove] | map(length) | add' <<< "$work")" -gt 0 ] || break
  echo "할 일: 올리기 $(jq '.pending | length' <<< "$work"), 해시 재기 $(jq '.pin | length // 0' <<< "$work"), 확인 $(jq '.verify | length // 0' <<< "$work"), 지우기 $(jq '.remove | length' <<< "$work")"

  for sha in $(jq -r '.remove[]' <<< "$work"); do
    [[ "$sha" =~ ^[0-9a-f]{64}$ ]] || continue
    s3 s3 rm --quiet "s3://$R2_BUCKET/models/$sha" && { report "$(jq -nc --arg s "$sha" '{sha256:$s,state:"removed"}')" || true; } && echo "지움 ${sha:0:8}"
  done

  # 제작자 PC에서 올린 파일: sha256이 이름과 같은지 확인하고, 다르면 지운다
  while IFS= read -r v; do
    [ -n "$v" ] || continue
    sha=$(jq -r .sha256 <<< "$v"); size=$(jq -r .size <<< "$v")
    [[ "$sha" =~ ^[0-9a-f]{64}$ && "$size" =~ ^[0-9]+$ ]] || continue
    f="$WORK/$sha.verify"
    if s3 s3 cp --no-progress "s3://$R2_BUCKET/models/$sha" "$f" > /dev/null && [ "$(stat -c%s "$f")" = "$size" ] && [ "$(sha256sum "$f" | cut -d' ' -f1)" = "$sha" ]; then
      report "$(jq -nc --arg s "$sha" '{sha256:$s,state:"verified"}')" || true; echo "확인 ${sha:0:8}"
    else
      s3 s3 rm "s3://$R2_BUCKET/models/$sha" > /dev/null 2>&1 || true
      report "$(jq -nc --arg s "$sha" '{sha256:$s,state:"bad"}')" || true; echo "올린 파일이 맞지 않아 지움 ${sha:0:8}"
    fi
    rm -f "$f"
  done < <(jq -c '.verify[]?' <<< "$work")

  # 받으면서 바로 올리기 (stream.py)
  if [ "$(jq '.pending | length' <<< "$work")" -gt 0 ]; then
    res=$(jq -c '.pending' <<< "$work" | python3 "$HERE/stream.py" | tee /dev/stderr | tail -1)
    [[ "$res" =~ ^ok\ ([0-9]+)\ failed\ ([0-9]+)$ ]] && { ok=$((ok + BASH_REMATCH[1])); failed=$((failed + BASH_REMATCH[2])); }
  fi

  # 해시 없이 주소만 적힌 모델: 받아서 sha256을 재고, 그 이름으로 백업한 뒤 로비에 적는다 (그 뒤로 받는 쪽은 이 해시로 확인한다)
  while IFS= read -r job; do
    [ -n "$job" ] || continue
    size=$(jq -r .size <<< "$job"); url=$(jq -r .url <<< "$job")
    [[ "$size" =~ ^[0-9]+$ ]] || continue
    pinfail() {
      failed=$((failed + 1)); echo "실패 (해시 재기)"
      report "$(jq -nc --arg u "$url" --argjson z "$size" --arg e "$1" '{url:$u,state:"pinFailed",size:$z,error:$e}')" || true
    }
    [[ "$url" == https://* ]] || { pinfail "https 주소가 아닙니다"; continue; }
    (( size + 1024 ** 3 <= $(free_bytes) )) || { pinfail "작업 공간이 모자랍니다 (${size} bytes)"; continue; }
    f="$WORK/pin.part"
    rm -f "$f"
    if ! download "$url" "$f" "$size"; then
      rm -f "$f"; pinfail "받지 못했습니다"; continue
    fi
    got=$(stat -c%s "$f")
    if [ "$got" != "$size" ]; then rm -f "$f"; pinfail "크기가 다릅니다 ($got)"; continue; fi
    sha=$(sha256sum "$f" | cut -d' ' -f1)
    # 같은 파일이 이미 있으면(다른 앱이 해시로 쓰던 파일 등) 올리지 않고 해시만 적는다
    if in_r2 "$sha" "$size" || s3 s3 cp --quiet "$f" "s3://$R2_BUCKET/models/$sha"; then
      report "$(jq -nc --arg u "$url" --arg s "$sha" --argjson z "$size" '{url:$u,state:"pinned",sha256:$s,size:$z}')" || true
      pinned=$((pinned + 1)); echo "해시 기록·백업 ${sha:0:8}"
    else
      pinfail "R2에 올리지 못했습니다"
    fi
    rm -f "$f"
  done < <(jq -c '.pin[]?' <<< "$work")
  # 이름 없이 돌면 한 번만
  [ -n "${WORKER:-}" ] || break
done

echo "끝: 백업 $ok, 해시 기록 $pinned, 실패 $failed"
