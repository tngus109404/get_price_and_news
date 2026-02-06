#!/usr/bin/env bash
set -euo pipefail

cd ~/boostcamp_final_project
source .venv_eda/bin/activate

BASE="./data/_new/final_news"
LOGDIR="${BASE}/logs"
mkdir -p "${LOGDIR}"

# -----------------------------
# Defaults (원래대로 START~END 전체 수집)
# -----------------------------
START="2026-01-01"
END="2026-01-01"

# ✅ 최종만 남기기 옵션 (1=중간 산출물 삭제)
KEEP_ONLY_FINAL=1

# -----------------------------
# Arg parsing
# -----------------------------
MODE="full"  # full | missing | scan
SLOT_MODE="both"  # day | night | both

usage() {
  cat <<EOF
Usage:
  bash run_range_final.sh
    -> run full collection for START~END (day+night)

  bash run_range_final.sh --scan-only
    -> scan and list missing/empty/incomplete slots in START~END (no execution)

  bash run_range_final.sh --missing-only
    -> rerun only missing/empty/incomplete slots in START~END

Optional:
  --start YYYY-MM-DD     override START
  --end   YYYY-MM-DD     override END
  --slot  day|night|both choose which slot(s) to run/scan (default: both)

Examples:
  # day만
  bash run_range_final.sh --slot day --start 2026-02-05 --end 2026-02-05

  # scan-only도 day만
  bash run_range_final.sh --scan-only --slot day --start 2025-11-14 --end 2026-01-31

  # missing-only도 night만
  bash run_range_final.sh --missing-only --slot night --start 2025-11-14 --end 2026-01-31
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --missing-only)
      MODE="missing"; shift ;;
    --scan-only)
      MODE="scan"; shift ;;
    --start)
      START="$2"; shift 2 ;;
    --end)
      END="$2"; shift 2 ;;
    --slot)
      SLOT_MODE="$2"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "[ERROR] unknown arg: $1"
      usage; exit 1 ;;
  esac
done

case "$SLOT_MODE" in
  day|night|both) ;;
  *)
    echo "[ERROR] --slot must be one of: day|night|both (got: $SLOT_MODE)"
    exit 1 ;;
esac

echo "[MODE] ${MODE} | [RANGE] ${START} ~ ${END} | [SLOT] ${SLOT_MODE}"

# SLOT 리스트 결정
SLOTS=()
if [[ "$SLOT_MODE" == "both" ]]; then
  SLOTS=("day" "night")
else
  SLOTS=("$SLOT_MODE")
fi

# ------------------------------------------------------------
# helper: count data rows in CSV (excluding header)
# ------------------------------------------------------------
count_csv_rows_excluding_header() {
  python - "$1" <<'PY'
import csv, os, sys
p = sys.argv[1]
if (not os.path.exists(p)) or os.path.getsize(p) == 0:
    print(0); sys.exit(0)
with open(p, newline='', encoding='utf-8') as f:
    r = csv.reader(f)
    n = sum(1 for _ in r)
print(max(0, n - 1))
PY
}

# ------------------------------------------------------------
# helper: missing reason detector (slot 단위)
#  - "missing" 정의: 아래 중 하나면 missing 취급
#    1) 로그가 없음 (NO_LOG)
#    2) 로그에 SKIPPED 흔적 (SKIPPED)
#    3) 로그에 FINAL DONE이 없음 (INCOMPLETE)
#    4) 최종 OUT3_ART가 없음 (NO_FINAL)
#    5) 최종 OUT3_ART가 헤더만 있음 (EMPTY_FINAL)
# ------------------------------------------------------------
missing_reason_slot() {
  local LOG="$1"
  local OUT3_ART="$2"

  if [[ ! -f "$LOG" ]]; then
    echo "NO_LOG"
    return 0
  fi

  if grep -q "FINAL DONE (SKIPPED)" "$LOG" 2>/dev/null; then
    echo "SKIPPED"
    return 0
  fi
  if grep -q "\[SKIP_SLOT\]" "$LOG" 2>/dev/null; then
    echo "SKIPPED"
    return 0
  fi

  if ! grep -q "FINAL DONE" "$LOG" 2>/dev/null; then
    echo "INCOMPLETE"
    return 0
  fi

  if [[ ! -f "$OUT3_ART" ]]; then
    echo "NO_FINAL"
    return 0
  fi

  local nrows
  nrows=$(count_csv_rows_excluding_header "$OUT3_ART")
  if [[ "${nrows}" -le 0 ]]; then
    echo "EMPTY_FINAL"
    return 0
  fi

  echo ""
  return 1
}

# ------------------------------------------------------------
# helper: create empty "final" outputs (header-only)
# ------------------------------------------------------------
create_empty_finals() {
  local OUT3_ART="$1"
  local OUT4_ENT="$2"
  local OUT5_TRI="$3"

  printf "id,title,doc_url,all_text,authors,publish_date,meta_site_name,key_word,filter_status,description,named_entities,triples,article_embedding\n" > "$OUT3_ART"
  printf "entity_text,embedding\n" > "$OUT4_ENT"
  printf "triple_text,embedding\n" > "$OUT5_TRI"
}

# ------------------------------------------------------------
# helper: missing rerun 전에 슬롯 관련 파일들 정리(선택)
# ------------------------------------------------------------
cleanup_slot_outputs() {
  local OUT0="$1" OUT1="$2" OUT2_NEWS="$3" OUT2_ENT="$4" OUT2_TRI="$5"
  local OUT3_ART="$6" OUT4_ENT="$7" OUT5_TRI="$8"
  local JLOG="$9" ELOG="${10}"

  rm -f "$OUT0" "$OUT1" "$OUT2_NEWS" "$OUT2_ENT" "$OUT2_TRI" "$JLOG" "$ELOG"
  rm -f "$OUT3_ART" "$OUT4_ENT" "$OUT5_TRI"
}

# -----------------------------
# 날짜 루프 (GNU date 필요)
# -----------------------------
missing_count=0

d="$START"
while [[ "$d" < "$END" || "$d" == "$END" ]]; do
  D="$d"

  OUTDIR="${BASE}/${D}"
  mkdir -p "${OUTDIR}"

  for SLOT in "${SLOTS[@]}"; do
    if [[ "$SLOT" == "day" ]]; then
      END_TS="${D}T09:00:00+00:00"
    else
      END_TS="${D}T21:00:00+00:00"
    fi

    LOG="${LOGDIR}/${D}_${SLOT}.log"

    OUT0="${OUTDIR}/${D}_${SLOT}_news_resources.csv"
    OUT1="${OUTDIR}/${D}_${SLOT}_news_resources_judged_tf.csv"
    OUT2_NEWS="${OUTDIR}/${D}_${SLOT}_news_with_entities_triples.csv"
    OUT2_ENT="${OUTDIR}/${D}_${SLOT}_entities.csv"
    OUT2_TRI="${OUTDIR}/${D}_${SLOT}_triples.csv"
    OUT3_ART="${OUTDIR}/${D}_${SLOT}_news_with_article_embedding.csv"
    OUT4_ENT="${OUTDIR}/${D}_${SLOT}_entities_with_embedding.csv"
    OUT5_TRI="${OUTDIR}/${D}_${SLOT}_triples_with_embedding.csv"

    JLOG="${OUTDIR}/${D}_${SLOT}_judge_o4mini.jsonl"
    ELOG="${OUTDIR}/${D}_${SLOT}_extract_entities_triples.jsonl"

    # ---- scan-only 모드 ----
    if [[ "$MODE" == "scan" ]]; then
      reason="$(missing_reason_slot "$LOG" "$OUT3_ART")" || true
      if [[ -n "$reason" ]]; then
        echo "[MISSING] ${D} ${SLOT} reason=${reason} | final=${OUT3_ART}"
        missing_count=$((missing_count+1))
      fi
      continue
    fi

    # ---- missing-only 모드 ----
    if [[ "$MODE" == "missing" ]]; then
      reason="$(missing_reason_slot "$LOG" "$OUT3_ART")" || true
      if [[ -z "$reason" ]]; then
        echo "[MISSING-ONLY] already complete -> skip ${D} ${SLOT}"
        continue
      fi
      echo "[MISSING-ONLY] rerun missing slot -> ${D} ${SLOT} (reason=${reason})"
      cleanup_slot_outputs "$OUT0" "$OUT1" "$OUT2_NEWS" "$OUT2_ENT" "$OUT2_TRI" "$OUT3_ART" "$OUT4_ENT" "$OUT5_TRI" "$JLOG" "$ELOG"
    fi

    echo "==================== ${D} ${SLOT} FINAL START ====================" | tee -a "$LOG"
    echo "[INFO] outdir  -> ${OUTDIR}" | tee -a "$LOG"
    echo "[INFO] log     -> ${LOG}" | tee -a "$LOG"
    echo "[INFO] end_ts  -> ${END_TS} (UTC, hours_back=12)" | tee -a "$LOG"

    # ---- STEP 1: fetch ----
    echo "" | tee -a "$LOG"
    echo "----- [1/4] FETCH GDELT (${D} ${SLOT}) -----" | tee -a "$LOG"

    set +e
    python -u ./fetch_daily_news_gdelt.py \
      --hours_back 12 \
      --end_ts "$END_TS" \
      --out "$OUT0" \
      --sourcelang English \
      --max_per_keyword 300 \
      --dedupe_by_url 1 \
      --drop_bad_pages 1 \
      --require_text 0 \
      --sleep 5.2 \
      --abort_on_unhealthy 1 \
      --unhealthy_streak 3 \
      --verbose 1 \
      --print_fail 1 \
      --log_every 5 \
      --workers 1 \
      2>&1 | tee -a "$LOG"
    FETCH_RC=${PIPESTATUS[0]}
    set -e

    if [[ "$FETCH_RC" != "0" ]]; then
      echo "[SKIP_SLOT] fetch failed (rc=$FETCH_RC). create empty finals and continue." | tee -a "$LOG"
      create_empty_finals "$OUT3_ART" "$OUT4_ENT" "$OUT5_TRI"
      echo "==================== ${D} ${SLOT} FINAL DONE (SKIPPED) ====================" | tee -a "$LOG"
      ls -lh "$OUT3_ART" "$OUT4_ENT" "$OUT5_TRI" 2>/dev/null | tee -a "$LOG"
      continue
    fi

    NROWS=$(count_csv_rows_excluding_header "$OUT0")
    echo "[INFO] fetched_rows=$NROWS" | tee -a "$LOG"

    if [[ "$NROWS" -le 0 ]]; then
      echo "[SKIP_SLOT] no news in this slot. create empty finals and continue." | tee -a "$LOG"
      create_empty_finals "$OUT3_ART" "$OUT4_ENT" "$OUT5_TRI"
      echo "==================== ${D} ${SLOT} FINAL DONE (SKIPPED) ====================" | tee -a "$LOG"
      ls -lh "$OUT3_ART" "$OUT4_ENT" "$OUT5_TRI" 2>/dev/null | tee -a "$LOG"
      continue
    fi

    # ---- STEP 2: judge ----
    echo "" | tee -a "$LOG"
    echo "----- [2/4] JUDGE T/F (o4-mini) (${D} ${SLOT}) -----" | tee -a "$LOG"
    python -u ./judge_filter_status_openai.py \
      --in_csv "$OUT0" \
      --out_csv "$OUT1" \
      --model o4-mini \
      --workers 8 \
      --timeout 60 \
      --retries 2 \
      --mode balanced \
      --use_all_text 0 \
      --max_chars 1800 \
      --progress_every 10 \
      --store 0 \
      --max_output_tokens 512 \
      --reasoning_effort low \
      --reset_filter_status 1 \
      --log_jsonl "$JLOG" \
      2>&1 | tee -a "$LOG"

    # ---- STEP 3: extract entities/triples (T only) ----
    echo "" | tee -a "$LOG"
    echo "----- [3/4] EXTRACT ENTITIES/TRIPLES (T only) (${D} ${SLOT}) -----" | tee -a "$LOG"
    python -u ./extract_entities_triples_openai.py \
      --in_csv "$OUT1" \
      --out_news_csv "$OUT2_NEWS" \
      --out_entities_csv "$OUT2_ENT" \
      --out_triples_csv "$OUT2_TRI" \
      --model o4-mini \
      --workers 8 \
      --retries 2 \
      --use_all_text 0 \
      --max_chars 2000 \
      --max_output_tokens 800 \
      --reasoning_effort low \
      --store 0 \
      --progress_every 10 \
      --log_jsonl "$ELOG" \
      2>&1 | tee -a "$LOG"

    # ---- STEP 4: embed (Titan v2) ----
    echo "" | tee -a "$LOG"
    echo "----- [4/4] EMBED TITAN v2 (${D} ${SLOT}) -----" | tee -a "$LOG"

    echo "[4-1] article512 (T only) -> $OUT3_ART" | tee -a "$LOG"
    python -u ./embed_titan_csv.py \
      --in_csv "$OUT2_NEWS" \
      --out_csv "$OUT3_ART" \
      --mode article512 \
      --region us-east-1 \
      --model_id amazon.titan-embed-text-v2:0 \
      --workers 8 \
      --retries 4 \
      --normalize 1 \
      --tf_only 1 --tf_col filter_status --t_value T \
      2>&1 | tee -a "$LOG"

    echo "[4-2] entity1024 -> $OUT4_ENT" | tee -a "$LOG"
    python -u ./embed_titan_csv.py \
      --in_csv "$OUT2_ENT" \
      --out_csv "$OUT4_ENT" \
      --mode entity1024 \
      --region us-east-1 \
      --model_id amazon.titan-embed-text-v2:0 \
      --workers 8 \
      --retries 4 \
      --normalize 1 \
      2>&1 | tee -a "$LOG"

    echo "[4-3] triple1024 -> $OUT5_TRI" | tee -a "$LOG"
    python -u ./embed_titan_csv.py \
      --in_csv "$OUT2_TRI" \
      --out_csv "$OUT5_TRI" \
      --mode triple1024 \
      --region us-east-1 \
      --model_id amazon.titan-embed-text-v2:0 \
      --workers 8 \
      --retries 4 \
      --normalize 1 \
      2>&1 | tee -a "$LOG"

    echo "==================== ${D} ${SLOT} FINAL DONE ====================" | tee -a "$LOG"
    ls -lh "$OUT3_ART" "$OUT4_ENT" "$OUT5_TRI" 2>/dev/null | tee -a "$LOG"

    # ✅✅ 슬롯별로 최종 3개만 남기기
    if [[ "$KEEP_ONLY_FINAL" == "1" ]]; then
      OUT3_ROWS=$(count_csv_rows_excluding_header "$OUT3_ART")
      if [[ -s "$OUT3_ART" && -s "$OUT4_ENT" && -s "$OUT5_TRI" && "$OUT3_ROWS" -gt 0 ]]; then
        echo "[CLEANUP] keep only final 3 csvs for ${D} ${SLOT}" | tee -a "$LOG"
        rm -f "$OUT0" "$OUT1" "$OUT2_NEWS" "$OUT2_ENT" "$OUT2_TRI" "$JLOG" "$ELOG"
      else
        echo "[CLEANUP] skipped (final outputs missing/empty) -> keep intermediates for debugging" | tee -a "$LOG"
      fi
    fi

  done

  d=$(date -I -d "$d + 1 day")
done

if [[ "$MODE" == "scan" ]]; then
  echo "[SCAN DONE] missing_slots=${missing_count} | range=${START}~${END} | slot=${SLOT_MODE}"
  echo "  - 재수집: bash run_range_final.sh --missing-only --slot ${SLOT_MODE} --start ${START} --end ${END}"
  exit 0
fi

echo "[ALL DONE] ${START} ~ ${END} | slot=${SLOT_MODE}"
