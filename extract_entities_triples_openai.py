import argparse
import os
import re
import json
import time
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import pandas as pd
from openai import OpenAI

TL = threading.local()

def compute_mdhash_id(content: str, prefix: str = "") -> str:
    return prefix + hashlib.md5(content.encode("utf-8")).hexdigest()

def get_client(base_url: str | None):
    if getattr(TL, "client", None) is None:
        kwargs = {}
        if base_url:
            kwargs["base_url"] = base_url
        TL.client = OpenAI(**kwargs)
    return TL.client

# o4-mini 같은 reasoning 모델에서 JSON만 안정적으로 받기 위한 프롬프트
SYSTEM = """You are an information extraction agent for news about commodities and related markets.
Return STRICT JSON only (no markdown, no explanations).

Schema:
{
  "entities": ["..."],                 // list of short noun phrases, deduplicated as best as possible
  "triples": [["S","R","O"], ...]      // list of triples (subject, relation, object) as strings
}

Rules:
- Extract from the provided text fields (title/description and optionally all_text).
- Entities: keep meaningful noun phrases (organizations, places, commodities, agencies like USDA/NASS, contracts like CBOT futures, events like drought, etc).
- Triples: factual relations stated or strongly implied in the text (avoid hallucination).
- Keep strings concise; no empty strings.
- If nothing can be extracted, return {"entities": [], "triples": []}.
"""

def build_user_payload(row: dict, use_all_text: bool, max_chars: int) -> str:
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

JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)

def safe_json_load(s: str):
    if not s:
        return None
    s = s.strip()

    # 혹시 ```json ... ``` 형태면 내부만 추출
    m = JSON_FENCE_RE.search(s)
    if m:
        s = m.group(1).strip()

    # 가장 바깥 JSON 객체 영역만 대충 잘라내기
    if "{" in s and "}" in s:
        s2 = s[s.find("{"):s.rfind("}")+1].strip()
    else:
        s2 = s

    try:
        return json.loads(s2)
    except Exception:
        return None

def normalize_entities(entities: list[str]) -> list[str]:
    out = []
    seen = set()
    for e in entities or []:
        if e is None:
            continue
        t = str(e).strip()
        if not t:
            continue
        # 너무 긴 것 방지
        if len(t) > 120:
            t = t[:120].strip()
        k = t.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return out

def normalize_triples(triples: list) -> list[list[str]]:
    out = []
    seen = set()
    for tri in triples or []:
        if not isinstance(tri, (list, tuple)) or len(tri) != 3:
            continue
        s, r, o = (str(tri[0]).strip(), str(tri[1]).strip(), str(tri[2]).strip())
        if not s or not r or not o:
            continue
        # 너무 긴 것 방지
        if len(s) > 160: s = s[:160].strip()
        if len(r) > 80:  r = r[:80].strip()
        if len(o) > 160: o = o[:160].strip()
        key = (s.lower(), r.lower(), o.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append([s, r, o])
    return out

def call_extract(model: str, user_text: str, retries: int, store: bool, base_url: str | None, max_output_tokens: int, reasoning_effort: str):
    last_err = None
    for attempt in range(retries + 1):
        try:
            client = get_client(base_url=base_url)

            # Responses API
            resp = client.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": SYSTEM},
                    {"role": "user", "content": user_text},
                ],
                # o4-mini는 temperature 미지원 가능성이 있어 아예 안 넣음
                max_output_tokens=max_output_tokens,
                store=store,
                reasoning={"effort": reasoning_effort},
                text={"format": {"type": "text"}},
            )

            out_text = getattr(resp, "output_text", "") or ""
            j = safe_json_load(out_text)

            if not isinstance(j, dict):
                return {"entities": [], "triples": []}, out_text.strip() or "(no_text)"

            ents = normalize_entities(j.get("entities", []))
            tris = normalize_triples(j.get("triples", []))
            return {"entities": ents, "triples": tris}, out_text.strip()

        except Exception as e:
            last_err = str(e)
            time.sleep(min(2.0 * (attempt + 1), 8.0))

    return {"entities": [], "triples": []}, f"[error] {last_err}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", required=True)
    ap.add_argument("--out_news_csv", required=True, help="기사 CSV에 named_entities/triples 컬럼 채워서 저장")
    ap.add_argument("--out_entities_csv", required=True)
    ap.add_argument("--out_triples_csv", required=True)
    ap.add_argument("--model", default="o4-mini")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--timeout", type=float, default=60.0)  # 현재는 내부 httpx timeout을 직접 제어하진 않음
    ap.add_argument("--mode_tf_col", default="filter_status", help="T/F 컬럼명")
    ap.add_argument("--t_value", default="T")
    ap.add_argument("--use_all_text", type=int, default=0)
    ap.add_argument("--max_chars", type=int, default=2000)
    ap.add_argument("--max_output_tokens", type=int, default=800)
    ap.add_argument("--reasoning_effort", choices=["low","medium","high"], default="low")
    ap.add_argument("--store", type=int, default=0)
    ap.add_argument("--base_url", default="", help="비우면 기본 https://api.openai.com/v1")
    ap.add_argument("--progress_every", type=int, default=10)
    ap.add_argument("--log_jsonl", default="", help="원하면 row별 raw결과 저장")
    args = ap.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY env var is not set.")

    os.makedirs(os.path.dirname(args.out_news_csv) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(args.out_entities_csv) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(args.out_triples_csv) or ".", exist_ok=True)
    if args.log_jsonl:
        os.makedirs(os.path.dirname(args.log_jsonl) or ".", exist_ok=True)

    df = pd.read_csv(args.in_csv)

    # 틸다 컬럼명과 맞추기: named_entities, triples (T인 경우에만)
    if "named_entities" not in df.columns:
        df["named_entities"] = ""
    if "triples" not in df.columns:
        df["triples"] = ""

    df["named_entities"] = df["named_entities"].astype("string")
    df["triples"] = df["triples"].astype("string")

    use_all_text = bool(args.use_all_text)
    store = bool(args.store)
    base_url = args.base_url.strip() or None

    tf_col = args.mode_tf_col
    if tf_col not in df.columns:
        raise RuntimeError(f"'{tf_col}' column not found in input CSV.")

    # 대상 인덱스: filter_status == 'T'
    target_idx = [i for i in range(len(df)) if str(df.at[i, tf_col]).strip() == args.t_value]
    total = len(target_idx)
    print(f"[INFO] total_rows={len(df)} | target(T)={total} | use_all_text={use_all_text}")

    results = {}  # idx -> {"entities":..., "triples":...}
    raw_map = {}  # idx -> raw text

    def task(idx: int):
        row = df.iloc[idx].to_dict()
        user_text = build_user_payload(row, use_all_text=use_all_text, max_chars=args.max_chars)
        data, raw = call_extract(
            model=args.model,
            user_text=user_text,
            retries=args.retries,
            store=store,
            base_url=base_url,
            max_output_tokens=args.max_output_tokens,
            reasoning_effort=args.reasoning_effort,
        )
        return idx, data, raw

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(task, idx) for idx in target_idx]
        done = 0
        for fut in as_completed(futs):
            idx, data, raw = fut.result()
            results[idx] = data
            raw_map[idx] = raw
            done += 1
            if args.progress_every and (done % args.progress_every == 0 or done == total):
                print(f"[progress] {done}/{total}")

    # 기사 CSV 컬럼 채우기 + 전역 unique 테이블 만들기
    ent_rows = []
    tri_rows = []

    log_f = open(args.log_jsonl, "w", encoding="utf-8") if args.log_jsonl else None

    for idx in target_idx:
        data = results.get(idx, {"entities": [], "triples": []})
        ents = data["entities"]
        tris = data["triples"]

        # 기사 row에 저장 (JSON 문자열)
        df.at[idx, "named_entities"] = json.dumps(ents, ensure_ascii=False)
        df.at[idx, "triples"] = json.dumps(tris, ensure_ascii=False)

        # 전역 테이블 수집
        for e in ents:
            e2 = e.strip()
            if not e2:
                continue
            ent_rows.append({
                "hash_id": compute_mdhash_id(e2, prefix="entity-"),
                "entity_text": e2,
            })

        for tri in tris:
            tri_text = str(list(tri)).strip()  # 틸다 샘플처럼 "['S','R','O']" 형태
            tri_rows.append({
                "hash_id": compute_mdhash_id(tri_text, prefix="triple-"),
                "triple_text": tri_text,
            })

        if log_f:
            row = df.iloc[idx]
            log_f.write(json.dumps({
                "row_index": int(idx),
                "doc_url": str(row.get("doc_url","")),
                "title": str(row.get("title","")),
                "entities_n": len(ents),
                "triples_n": len(tris),
                "raw": raw_map.get(idx, ""),
            }, ensure_ascii=False) + "\n")

    if log_f:
        log_f.close()

    # 전역 unique
    ent_df = pd.DataFrame(ent_rows).drop_duplicates(subset=["hash_id"])
    tri_df = pd.DataFrame(tri_rows).drop_duplicates(subset=["hash_id"])

    print("\n[INFO] extracted uniques")
    print(f"  entities_unique: {len(ent_df)}")
    print(f"  triples_unique : {len(tri_df)}")

    df.to_csv(args.out_news_csv, index=False)
    ent_df.to_csv(args.out_entities_csv, index=False)
    tri_df.to_csv(args.out_triples_csv, index=False)

    print(f"\n[WROTE] news    : {args.out_news_csv}")
    print(f"[WROTE] entities: {args.out_entities_csv}")
    print(f"[WROTE] triples : {args.out_triples_csv}")
    if args.log_jsonl:
        print(f"[LOG] {args.log_jsonl}")
    print(f"[elapsed] {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
