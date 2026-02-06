import argparse
import json
import os
import time
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pandas as pd
from pandas.errors import EmptyDataError
from tqdm import tqdm


def bedrock_client(region: str):
    return boto3.client("bedrock-runtime", region_name=region)


def invoke_titan_embed(
    client,
    model_id: str,
    text: str,
    dims: int,
    normalize: bool = True,
    retries: int = 4,
    backoff: float = 0.7,
) -> List[float]:
    body = {"inputText": text, "dimensions": dims, "normalize": normalize}

    last_err = None
    for a in range(retries + 1):
        try:
            resp = client.invoke_model(
                modelId=model_id,
                contentType="application/json",
                accept="application/json",
                body=json.dumps(body).encode("utf-8"),
            )
            out = json.loads(resp["body"].read())
            emb = out.get("embedding") or out.get("vector") or out.get("embeddings")
            if emb is None:
                raise RuntimeError(f"No embedding key in response: keys={list(out.keys())}")
            return emb
        except Exception as e:
            last_err = e
            time.sleep(min(backoff * (2 ** a), 8.0))

    raise RuntimeError(f"invoke_titan_embed failed: {last_err}")


def build_text(row: Dict[str, Any], mode: str) -> str:
    if mode == "article512":
        t = str(row.get("title", "") or "").strip()
        d = str(row.get("description", "") or "").strip()
        return f"{t}\n\n{d}".strip()
    elif mode == "entity1024":
        return str(row.get("entity_text", "") or "").strip()
    elif mode == "triple1024":
        return str(row.get("triple_text", "") or "").strip()
    else:
        raise ValueError("unknown mode")


def norm_tf(x: Any) -> str:
    """filter_status 값이 섞여 들어와도 최종 비교를 위해 대문자 문자열로 정규화"""
    if x is None:
        return ""
    try:
        if isinstance(x, float) and pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip().upper()


def _default_columns_for_mode(mode: str, out_col: str) -> List[str]:
    if mode == "entity1024":
        return ["entity_text", out_col]
    if mode == "triple1024":
        return ["triple_text", out_col]
    # article512: 최소한 title/description + out_col
    return ["title", "description", out_col]


def _write_empty_header_only(out_csv: str, mode: str, out_col: str) -> None:
    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    cols = _default_columns_for_mode(mode, out_col)
    pd.DataFrame(columns=cols).to_csv(out_csv, index=False)
    print(f"[WROTE] {out_csv} (empty header only)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", required=True)
    ap.add_argument("--out_csv", required=True)
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--model_id", default="amazon.titan-embed-text-v2:0")
    ap.add_argument("--mode", choices=["article512", "entity1024", "triple1024"], required=True)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--retries", type=int, default=4)
    ap.add_argument("--normalize", type=int, default=1)
    ap.add_argument("--limit", type=int, default=0, help="테스트용. 0이면 전체")
    ap.add_argument("--sleep", type=float, default=0.0, help="요청 간 강제 sleep(초)")

    # ✅ NEW: T only 임베딩 옵션
    ap.add_argument("--tf_only", type=int, default=0,
                    help="1이면 tf_col==t_value 인 행만 임베딩 생성 (그 외는 빈칸 유지)")
    ap.add_argument("--tf_col", default="filter_status",
                    help="T/F 판정 컬럼명 (기본 filter_status)")
    ap.add_argument("--t_value", default="T",
                    help="T로 간주할 값 (기본 'T')")

    args = ap.parse_args()

    dims = 512 if args.mode == "article512" else 1024
    normalize = bool(args.normalize)

    out_col = "embedding" if args.mode != "article512" else "article_embedding"

    # ✅ 입력이 아예 비었거나 헤더가 없는 경우(EmptyDataError) 방어
    try:
        if (not os.path.exists(args.in_csv)) or (os.path.getsize(args.in_csv) == 0):
            print(f"[SKIP] input csv missing/empty: {args.in_csv}")
            _write_empty_header_only(args.out_csv, args.mode, out_col)
            return
        df = pd.read_csv(args.in_csv)
    except EmptyDataError:
        print(f"[SKIP] input csv has no columns (EmptyDataError): {args.in_csv}")
        _write_empty_header_only(args.out_csv, args.mode, out_col)
        return

    if args.limit and args.limit > 0:
        df = df.head(args.limit).copy()

    # ✅ 0행이면 그냥 out_col 컬럼만 맞춰서 저장하고 종료
    if len(df) == 0:
        if out_col not in df.columns:
            df[out_col] = ""
        df.to_csv(args.out_csv, index=False)
        print(f"[WROTE] {args.out_csv} rows=0 col={out_col}")
        return

    if out_col not in df.columns:
        df[out_col] = ""
    else:
        df[out_col] = ""

    # ✅ T-only 마스크 만들기
    tf_only = bool(args.tf_only)
    selected_mask = [True] * len(df)
    if tf_only:
        if args.tf_col not in df.columns:
            raise RuntimeError(f"--tf_only=1 인데 '{args.tf_col}' 컬럼이 입력 CSV에 없습니다.")
        tv = str(args.t_value).strip().upper()
        selected_mask = [norm_tf(df.at[i, args.tf_col]) == tv for i in range(len(df))]

    client = bedrock_client(args.region)

    # ✅ 임베딩을 실제로 만들 인덱스만 futures로 생성
    target_indices = []
    for i in range(len(df)):
        if not selected_mask[i]:
            continue
        row = df.iloc[i].to_dict()
        text = build_text(row, args.mode)
        if not text:
            continue
        target_indices.append(i)

    def task(i: int):
        row = df.iloc[i].to_dict()
        text = build_text(row, args.mode)
        if not text:
            return i, None
        emb = invoke_titan_embed(
            client=client,
            model_id=args.model_id,
            text=text,
            dims=dims,
            normalize=normalize,
            retries=args.retries,
        )
        if args.sleep and args.sleep > 0:
            time.sleep(args.sleep)
        return i, emb

    futures = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for i in target_indices:
            futures.append(ex.submit(task, i))

        for fut in tqdm(as_completed(futures), total=len(futures), desc=f"embedding({args.mode})"):
            i, emb = fut.result()
            if emb is None:
                df.at[i, out_col] = ""
            else:
                df.at[i, out_col] = json.dumps(emb)

    os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
    df.to_csv(args.out_csv, index=False)

    if tf_only:
        n_sel = sum(1 for x in selected_mask if x)
        print(f"[INFO] tf_only=1 -> selected {n_sel}/{len(df)} rows where {args.tf_col}=={args.t_value}")
    print(f"[WROTE] {args.out_csv} rows={len(df)} col={out_col}")


if __name__ == "__main__":
    main()
