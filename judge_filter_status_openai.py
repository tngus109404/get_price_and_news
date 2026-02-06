#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import re
import time
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from openai import OpenAI

# --- 판단 기준(모드별) ---
SYSTEM_STRICT = """You are a binary classifier for commodity-futures relevance.
Return EXACTLY ONE LETTER: T or F.

T = The article is clearly relevant to grain/oilseed futures markets and price drivers for corn, wheat, soybean, rice, sorghum.
It must mention at least one target commodity (corn/wheat/soybean/rice/sorghum) OR a clearly grain-specific market context (USDA WASDE, crop yield, planting/harvest, exports, inventories, grain futures/CBOT, basis, ethanol/biofuel demand for those crops, feed demand).
F = Otherwise (unrelated topics, generic finance not tied to those commodities, unrelated disasters, lifestyle, politics without crop-market linkage, etc).

No explanations. Output only T or F.
"""

SYSTEM_BALANCED = """You are a binary classifier for commodity-futures relevance.
Return EXACTLY ONE LETTER: T or F.

T = The article is relevant to supply/demand/inventory/price drivers for corn, wheat, soybean, rice, sorghum (directly OR via clearly connected agricultural factors).
Examples: crop conditions, weather impacting crops, harvest/planting, yields, exports/imports, logistics that affect grain flows, policy affecting grain trade, USDA/NASS/WASDE, biofuel/ethanol impacting corn/soy demand, feed demand, futures/CBOT commentary for grains.
F = Otherwise.

No explanations. Output only T or F.
"""

SYSTEM_LENIENT = """You are a binary classifier for commodity-futures relevance.
Return EXACTLY ONE LETTER: T or F.

T = The article is about agriculture/food supply chains and plausibly impacts grain/oilseed markets (corn/wheat/soybean/rice/sorghum), even if indirect.
F = Otherwise.

No explanations. Output only T or F.
"""

TL = threading.local()

def get_client(timeout: float, base_url: str | None):
    if getattr(TL, "client", None) is None:
        kwargs = {}
        if base_url:
            kwargs["base_url"] = base_url
        TL.client = OpenAI(**kwargs)
        TL.timeout = timeout
    return TL.client

def build_input(row: dict, use_all_text: bool, max_chars: int) -> str:
    def s(x):
        if x is None:
            return ""
        return str(x).strip()

    title = s(row.get("title"))
    desc  = s(row.get("description"))
    all_text = s(row.get("all_text"))
    url = s(row.get("doc_url") or row.get("url"))
    kw = s(row.get("key_word"))

    parts = []
    if kw:
        parts.append(f"keyword: {kw}")
    if url:
        parts.append(f"url: {url}")
    if title:
        parts.append(f"title: {title}")
    if desc:
        parts.append(f"description: {desc}")
    if use_all_text and all_text:
        parts.append(f"all_text: {all_text}")

    text = "\n".join(parts).strip()
    if not text:
        text = "(empty)"
    return text[:max_chars]

LABEL_RE = re.compile(r"\b([TF])\b", re.IGNORECASE)

def parse_label(text: str) -> str:
    if not text:
        return "F"
    m = LABEL_RE.search(text.strip().upper())
    if m:
        return m.group(1).upper()
    t = text.strip().upper()
    if t and t[0] in ("T", "F"):
        return t[0]
    return "F"

def normalize_tf_value(x) -> str:
    """
    입력 파일에 filter_status가 boolean/숫자/문자열로 섞여 들어와도
    최종적으로 'T' or 'F' 문자열로만 정규화.
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    if not s:
        return ""
    u = s.upper()
    if u in ("T", "TRUE", "1"):
        return "T"
    if u in ("F", "FALSE", "0"):
        return "F"
    # 그 외는 그대로 두되(예: E) 이번 스크립트는 T/F만 쓰니까 빈칸 처리
    return ""

def _get_field(obj, name, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)

def extract_output_text(resp) -> str:
    out_text = getattr(resp, "output_text", None)
    if isinstance(out_text, str) and out_text.strip():
        return out_text.strip()

    output = _get_field(resp, "output", []) or []
    texts = []

    for item in output:
        itype = _get_field(item, "type", "")
        if itype == "message":
            content = _get_field(item, "content", []) or []
            for c in content:
                ctype = _get_field(c, "type", "")
                if ctype in ("output_text", "text"):
                    t = _get_field(c, "text", None) or _get_field(c, "value", None)
                    if t:
                        texts.append(str(t))
        if itype in ("output_text", "text"):
            t = _get_field(item, "text", None) or _get_field(item, "value", None)
            if t:
                texts.append(str(t))

    return "\n".join(texts).strip()

def is_reasoning_model(model: str) -> bool:
    m = (model or "").strip().lower()
    return m.startswith("o")

def get_incomplete_reason(resp) -> str:
    inc = _get_field(resp, "incomplete_details", None)
    reason = _get_field(inc, "reason", None)
    return str(reason) if reason else ""

def call_judge(
    model: str,
    system: str,
    user: str,
    timeout: float,
    retries: int,
    store: bool,
    base_url: str | None,
    max_output_tokens: int,
    reasoning_effort: str,
):
    last_err = None
    cur_max = int(max_output_tokens)

    for attempt in range(retries + 1):
        try:
            client = get_client(timeout=timeout, base_url=base_url)

            req = {
                "model": model,
                "input": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "max_output_tokens": cur_max,
                "store": store,
            }

            if is_reasoning_model(model):
                req["reasoning"] = {"effort": reasoning_effort}
            # temperature는 o*에서 미지원일 수 있어 넣지 않음

            resp = client.responses.create(**req)
            out = extract_output_text(resp)

            status = _get_field(resp, "status", "")
            if (not out) and (status == "incomplete"):
                reason = get_incomplete_reason(resp)
                if reason == "max_output_tokens":
                    cur_max = min(cur_max * 4, 8192)
                    continue

            lab = parse_label(out)
            return lab, out.strip() if out else ""

        except Exception as e:
            last_err = str(e)
            time.sleep(min(2.0 * (attempt + 1), 10.0))

    return "F", f"[error] {last_err}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", required=True)
    ap.add_argument("--out_csv", required=True)
    ap.add_argument("--model", default="o4-mini")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--timeout", type=float, default=60.0)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--mode", choices=["strict", "balanced", "lenient"], default="balanced")
    ap.add_argument("--use_all_text", type=int, default=0)
    ap.add_argument("--max_chars", type=int, default=1800)
    ap.add_argument("--progress_every", type=int, default=10)
    ap.add_argument("--store", type=int, default=0)
    ap.add_argument("--base_url", default="")
    ap.add_argument("--log_jsonl", default="")

    ap.add_argument("--max_output_tokens", type=int, default=512)
    ap.add_argument("--reasoning_effort", choices=["low", "medium", "high"], default="low")

    # ✅ 추가: 기존 filter_status 무시하고 새로 판정할지
    ap.add_argument("--reset_filter_status", type=int, default=0,
                    help="1이면 기존 filter_status 컬럼이 있어도 무시하고 다시 판정")

    args = ap.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY env var is not set.")

    os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
    if args.log_jsonl:
        os.makedirs(os.path.dirname(args.log_jsonl) or ".", exist_ok=True)

    df = pd.read_csv(args.in_csv)

    # ✅✅ (수정) filter_status를 처음부터 끝까지 "string" dtype으로 강제
    if args.reset_filter_status or ("filter_status" not in df.columns):
        df["filter_status"] = pd.Series([""] * len(df), dtype="string")
    else:
        df["filter_status"] = df["filter_status"].apply(normalize_tf_value).astype("string")

    if args.mode == "strict":
        system = SYSTEM_STRICT
    elif args.mode == "lenient":
        system = SYSTEM_LENIENT
    else:
        system = SYSTEM_BALANCED

    use_all_text = bool(args.use_all_text)
    store = bool(args.store)
    base_url = args.base_url.strip() or None

    total = len(df)
    results = [None] * total

    def task(i: int):
        row = df.iloc[i].to_dict()
        user = build_input(row, use_all_text=use_all_text, max_chars=args.max_chars)
        lab, raw = call_judge(
            model=args.model,
            system=system,
            user=user,
            timeout=args.timeout,
            retries=args.retries,
            store=store,
            base_url=base_url,
            max_output_tokens=int(args.max_output_tokens),
            reasoning_effort=str(args.reasoning_effort),
        )
        return i, lab, raw

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(task, i) for i in range(total)]
        done = 0
        for fut in as_completed(futs):
            i, lab, raw = fut.result()
            results[i] = (lab, raw)
            done += 1
            if args.progress_every and (done % args.progress_every == 0 or done == total):
                print(f"[progress] {done}/{total}")

    raw_logs = []
    for i, (lab, raw) in enumerate(results):
        # ✅ 여기서도 무조건 'T'/'F'만 넣는다 (문자열)
        df.at[i, "filter_status"] = "T" if lab == "T" else "F"
        if args.log_jsonl:
            raw_logs.append({
                "row_index": i,
                "label": "T" if lab == "T" else "F",
                "raw": raw,
                "doc_url": str(df.iloc[i].get("doc_url", "")),
                "title": str(df.iloc[i].get("title", "")),
            })

    print("\n[label counts]")
    print(df["filter_status"].value_counts(dropna=False).to_string())

    # ✅✅ (수정) 저장 직전에 다시 한 번 string 고정(방어)
    df["filter_status"] = df["filter_status"].astype("string")

    df.to_csv(args.out_csv, index=False)
    print(f"\n[WROTE] {args.out_csv}")
    print(f"[elapsed] {time.time()-t0:.1f}s")

    if args.log_jsonl:
        with open(args.log_jsonl, "w", encoding="utf-8") as f:
            for x in raw_logs:
                f.write(json.dumps(x, ensure_ascii=False) + "\n")
        print(f"[LOG] {args.log_jsonl}")

if __name__ == "__main__":
    main()
