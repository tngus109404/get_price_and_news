#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_daily_news_gdelt.py

- GDELT DOC 2.1 API로 특정 날짜/기간 뉴스 URL 리스트를 가져온 뒤
- 각 URL에서 본문/description/authors/publish_time/site_name(가능하면) 추출
- "틸다 호환 CSV"로 저장

Output columns (TILDA order):
[id,title,doc_url,all_text,authors,publish_date,meta_site_name,key_word,
 filter_status,description,named_entities,triples,article_embedding]

Key behaviors (TILDA-like):
(A) all_text: "preview… [+N chars]" 형태로 생성
    - preview target length: --all_text_maxlen (기본 215)
    - hard cap length: --all_text_hardcap (기본 218)
(B) meta_site_name: 도메인 기반 + 길이 cap(26)
(C) doc_url: 출력은 raw URL(쿼리 유지). dedupe는 별도 normalize_url(key)로 수행.
(D) filter_status는 이번 패치에서는 건드리지 않음(요청대로 제외)

Guardrails:
- --max_total_candidates: (dedupe 전) 전체 후보 URL 누적 상한 (0이면 제한 없음)
- --max_pages_per_keyword: 키워드당 ArtList 페이지 상한 (0이면 제한 없음)
- --max_runtime_sec: 전체 실행 시간 상한(초, 0이면 제한 없음)

New-ish:
- --drop_bad_pages: 본문 파싱이 막혀서 네비/구독/로그인/쿠키/JS 문구로 채워진 페이지는 row 자체를 제외
- --require_text / --min_text_len: 본문이 너무 짧으면 row 제외(틸다처럼 "실제 기사" 위주로)
- --keep_title_prefix: all_text 생성 시 title/desc를 앞에 붙일지 여부
- --strict_date + --date_tolerance_days: 날짜 필터를 엄격하게(±tolerance 포함)

New (time window by end timestamp):
- --hours_back + --end_ts 를 같이 쓰면,
  end_ts(UTC로 변환됨) 기준으로 [end_ts - hours_back, end_ts] 윈도우로 수집한다.
  --end_ts 가 없으면 기존처럼 now(UTC)를 end로 사용한다.

Optional:
- --site_whitelist_file: 허용 도메인(한 줄에 하나)만 남김

========================
PATCH (이번 수정사항 핵심)
1) ArtList 요청에서 sort=HybridRel 제거
2) GDELT 응답이 비정상일 때(빈 바디, {}, articles 키 누락)는 "정상 0건"으로 보지 않음
   - request_json_with_retries에서 재시도(backoff)
   - 그래도 실패하면 "__BAD_RESPONSE__"로 표시해 상위에서 "GDELT unhealthy" 판단 가능
3) "GDELT 죽음으로 인한 0개"가 연속으로 나오면 즉시 중지(abort)
   - --abort_on_unhealthy=1 (기본)
   - --unhealthy_streak=3 (기본)
4) GDELT 429(요청 제한) 대응 강화
   - 429 시 최소 5.2초 이상 sleep
   - 기본 --sleep 을 5.2로 상향(원하면 CLI로 낮출 수 있음)
========================
"""

import argparse
import csv
import html
import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import requests


# ✅ 틸다 실제 CSV 컬럼 순서
DEFAULT_OUT_COLUMNS = [
    "id",
    "title",
    "doc_url",
    "all_text",
    "authors",
    "publish_date",
    "meta_site_name",
    "key_word",
    "filter_status",
    "description",
    "named_entities",
    "triples",
    "article_embedding",
]

# ✅ 틸다 키워드에 최대한 맞춤(phrase 검색용 쌍따옴표 포함)
DEFAULT_KEYWORDS = [
    "corn and (price or demand or supply or inventory)",
    "rice and (price or demand or supply or inventory)",
    "wheat and (price or demand or supply or inventory)",
    "soybean and (price or demand or supply or inventory)",
    "united states department of agriculture",
    "national agricultural statistics service",
    "\"soybean production\"",
    "\"soybean oil\" and (production or outputs or supplies or supply or biofuel or biodiesel or demand or price)",
    "\"soy oil\" and (production or outputs or supplies or supply or biofuel or biodiesel or demand or price)",
    "sorghum and (price or demand or supply or inventory)",
]

TRACKING_KEYS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "mc_cid", "mc_eid", "ref", "ref_src", "ref_url",
    "cexp_id", "cexp_var", "_f", "cmpid", "ito", "icid",
    "guccounter", "guce_referrer", "guce_referrer_sig",
}

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

TOO_SHORT_HINT = "keyword that was too short"
STOPWORDS_FOR_REWRITE = {"of", "the", "a", "an"}
PAREN_HINT = "Parentheses may only be used around OR'd statements"

# 429 최소 대기 (GDELT가 "5초에 1회" 경고를 주기 때문에)
MIN_SLEEP_ON_429 = 5.2


# -----------------------------
# bad-parse / blocked-page detection
# -----------------------------
BAD_HEAD_PATTERNS = [
    # hard blocks / bot checks
    "access denied",
    "request blocked",
    "forbidden",
    "error 403",
    "error 404",
    "error 502",
    "just a moment",
    "checking your browser",
    "verify you are human",
    "captcha",
    "cloudflare",

    # paywall / auth gates
    "subscribe",
    "subscription",
    "sign in",
    "log in",
    "login",
    "register",
    "create an account",
    "already a subscriber",
    "manage your subscription",

    # cookie / consent overlays
    "we use cookies",
    "cookie policy",
    "accept cookies",
    "manage cookies",
    "your privacy choices",

    # JS required pages
    "enable javascript",
    "please enable javascript",
]

BAD_HEAD_COMBO_RULES = [
    (["skip to content", "subscribe"], "nav_subscribe"),
    (["skip to content", "sign in"], "nav_signin"),
    (["skip to content", "log in"], "nav_login"),
    (["we use cookies", "accept"], "cookie_banner"),
]


# -----------------------------
# logging helpers
# -----------------------------
def now_ts() -> str:
    return time.strftime("%H:%M:%S", time.localtime())


def log(msg: str, verbose: bool = True) -> None:
    if verbose:
        print(f"[{now_ts()}] {msg}", flush=True)


# -----------------------------
# basic helpers
# -----------------------------
def clean_text(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = s.replace("\x00", " ")
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def strip_www(netloc: str) -> str:
    n = (netloc or "").lower()
    return n[4:] if n.startswith("www.") else n


def domain_from_url(url: str) -> str:
    try:
        return strip_www(urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def normalize_url(u: Any) -> str:
    """
    dedupe key 용: tracking 제거 + fragment 제거 + trailing slash 정리
    """
    u = clean_text(u)
    if not u:
        return ""
    try:
        p = urlparse(u)
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()
        path = p.path or ""
        if path != "/" and path.endswith("/"):
            path = path[:-1]

        q = []
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            kl = k.lower()
            if kl.startswith("utm_") or kl in TRACKING_KEYS:
                continue
            q.append((k, v))
        query = urlencode(q, doseq=True)

        frag = ""  # drop fragment
        return urlunparse((scheme, netloc, path, p.params, query, frag))
    except Exception:
        return u


def meta_site_name_domain_only(domain: str, url_raw: str, cap: int = 26) -> str:
    """
    B) meta_site_name: 도메인 기반 + 길이 cap(26)
    """
    d = clean_text(domain).strip()
    if not d:
        d = domain_from_url(url_raw)
    d = strip_www(d.lower())
    if not d:
        return ""
    site = d[:1].upper() + d[1:]
    return site[:cap]


def looks_like_blocked_or_nav_page(text: str, *, fallback_used: bool) -> Optional[str]:
    """
    파싱 결과(text)가 '본문'이 아니라
    - 구독/로그인/쿠키/봇체크/JS요구/네비게이션 덩어리로 보이는지 휴리스틱 체크.
    return: None(정상) or reason(str)
    """
    t = clean_text(text)
    if not t:
        return "empty_text"

    head = t[:650].lower()

    # combo rules: 오탐 방지(강한 신호)
    for musts, reason in BAD_HEAD_COMBO_RULES:
        if all(m in head for m in musts):
            return reason

    # single keyword patterns: fallback/짧은 본문에서만 강하게 의심
    if (fallback_used and len(t) < 1200) or (len(t) < 700):
        for p in BAD_HEAD_PATTERNS:
            if p in head:
                return f"pattern:{p}"

    # fallback인데 길이도 짧고 네비 느낌이 강하면 제외
    if fallback_used and len(t) < 800 and ("skip to content" in head or "subscribe" in head or "sign in" in head):
        return "short_fallback_nav"

    return None


# -----------------------------
# datetime helpers
# -----------------------------
def _parse_ymd(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def ymd_range_to_gdelt_range(date_from: str, date_to: str) -> Tuple[str, str]:
    d1 = _parse_ymd(date_from)
    d2 = _parse_ymd(date_to)
    if d2 < d1:
        raise ValueError(f"date_to must be >= date_from (got {date_from} ~ {date_to})")
    y1, m1, d1v = date_from.split("-")
    y2, m2, d2v = date_to.split("-")
    start = f"{y1}{m1}{d1v}000000"
    end = f"{y2}{m2}{d2v}235959"
    return start, end


def yyyymmdd_to_range(date_str: str) -> Tuple[str, str]:
    y, m, d = date_str.split("-")
    start = f"{y}{m}{d}000000"
    end = f"{y}{m}{d}235959"
    return start, end


def parse_end_ts_to_utc(end_ts: str) -> datetime:
    """
    Parse end_ts to timezone-aware UTC datetime.
    Accepts:
      - 'YYYY-MM-DDTHH:MM:SS+09:00'
      - 'YYYY-MM-DD HH:MM:SS+09:00'
      - 'YYYY-MM-DDTHH:MM:SSZ'
      - if no timezone info -> assume UTC
    """
    s = clean_text(end_ts)
    if not s:
        raise ValueError("end_ts is empty")

    s2 = s.strip()

    # allow 'YYYY-MM-DD HH:MM:SS' -> 'YYYY-MM-DDTHH:MM:SS'
    if "T" not in s2 and re.search(r"\d{2}:\d{2}", s2):
        s2 = s2.replace(" ", "T", 1)

    # 'Z' -> '+00:00'
    if s2.endswith("Z"):
        s2 = s2[:-1] + "+00:00"

    dt = datetime.fromisoformat(s2)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def normalize_publish_datetime(value: str) -> str:
    """
    다양한 입력을 'YYYY-MM-DDTHH:MM:SS'로 정규화.
    - 14자리 seendate(YYYYMMDDhhmmss)
    - ISO 8601 with Z/offset
    - 'YYYY-MM-DD' only -> T00:00:00
    """
    s = clean_text(value)
    if not s:
        return ""

    # GDELT seendate: YYYYMMDDhhmmss
    if re.fullmatch(r"\d{14}", s):
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}T{s[8:10]}:{s[10:12]}:{s[12:14]}"

    # date only
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s + "T00:00:00"

    s2 = s.strip()

    # convert space to 'T' once if it looks like datetime
    if "T" not in s2 and re.search(r"\d{2}:\d{2}", s2):
        s2 = s2.replace(" ", "T", 1)

    # Z -> +00:00
    if s2.endswith("Z"):
        s2 = s2[:-1] + "+00:00"

    # remove fractional seconds (both before offset and without)
    s2 = re.sub(r"\.\d+(?=[+-]\d{2}:\d{2}$)", "", s2)
    s2 = re.sub(r"\.\d+", "", s2)

    # Try fromisoformat
    try:
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        pass

    # Regex fallback: YYYY-MM-DD ... HH:MM(:SS)?
    m = re.search(r"(\d{4})-(\d{2})-(\d{2}).*?(\d{2}):(\d{2})(?::(\d{2}))?", s)
    if m:
        y, mo, d, hh, mm, ss = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), (m.group(6) or "00")
        try:
            dt = datetime(int(y), int(mo), int(d), int(hh), int(mm), int(ss))
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            return ""

    return ""


def publish_datetime_from_seendate(seendate: str, fallback_date: str) -> str:
    """
    seendate: YYYYMMDDhhmmss -> YYYY-MM-DDTHH:MM:SS
    fallback_date: YYYY-MM-DD -> YYYY-MM-DDT00:00:00
    """
    s = clean_text(seendate)
    if re.fullmatch(r"\d{14}", s):
        return normalize_publish_datetime(s)
    fb = clean_text(fallback_date)
    if "T" in fb:
        return normalize_publish_datetime(fb)
    return normalize_publish_datetime(fb) or (fb + "T00:00:00")


def date_only_from_iso(iso_dt: str) -> str:
    s = clean_text(iso_dt)
    return s[:10] if len(s) >= 10 else ""


def within_date_tolerance(date_iso: str, target_ymd: str, tol_days: int) -> bool:
    d0 = date_only_from_iso(date_iso)
    if not d0:
        return False
    try:
        x = _parse_ymd(d0)
        t = _parse_ymd(target_ymd)
    except Exception:
        return False
    delta = abs((x - t).days)
    return delta <= max(0, int(tol_days))


# -----------------------------
# GDELT query helpers
# -----------------------------
def build_gdelt_query(keyword: str, sourcelang: str) -> str:
    k = clean_text(keyword)
    sl = clean_text(sourcelang)

    if not sl or sl.strip().lower() == "all":
        return k

    return f"{k} sourcelang:{sl}"


def rewrite_query_if_too_short(query: str) -> str:
    q = clean_text(query)
    toks = [t for t in re.split(r"\s+", q) if t]
    kept = []
    for t in toks:
        tl = t.lower()
        if len(tl) <= 2:
            continue
        if tl in STOPWORDS_FOR_REWRITE:
            continue
        kept.append(t)
    return " ".join(kept).strip()


def sanitize_query_if_paren_error(query: str) -> str:
    q = clean_text(query)
    q2 = q.replace("(", " ").replace(")", " ")
    q2 = re.sub(r"\s+", " ", q2).strip()
    return q2


def request_json_with_retries(
    url: str,
    params: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int,
    max_retries: int,
    base_backoff: float,
    verbose: bool,
    print_fail: bool,
    ctx: str,
    expect_articles_key: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    - 200 OK라도 빈 바디 / {} / articles 키 누락은 "정상 0건"이 아니라 비정상으로 간주 → 재시도
    - 재시도 끝까지 실패하면 "__BAD_RESPONSE__" 마커를 반환 (상위에서 'GDELT unhealthy' 판단용)
    """
    backoff = base_backoff
    last_bad_body: str = ""

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
        except Exception as e:
            if print_fail:
                log(f"[{ctx}] request failed: {type(e).__name__}: {e} -> sleep {backoff:.1f}s retry ({attempt}/{max_retries})", verbose)
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)
            continue

        if r.status_code == 429:
            # GDELT: 5초에 1회 제한 경고가 있음 → 최소 5.2초 이상 기다림
            sleep_s = max(backoff, MIN_SLEEP_ON_429)
            if print_fail:
                log(f"[{ctx}] HTTP 429 -> sleep {sleep_s:.1f}s retry ({attempt}/{max_retries})", verbose)
            time.sleep(sleep_s)
            backoff = min(backoff * 2.0, 30.0)
            continue

        if r.status_code != 200:
            if print_fail:
                body = (r.text or "")[:200].replace("\n", " ")
                log(f"[{ctx}] HTTP {r.status_code} -> stop; body='{body}'", verbose)
            return None

        body_full = r.text or ""
        if not body_full.strip():
            last_bad_body = body_full
            if print_fail:
                log(f"[{ctx}] EMPTY_BODY(200 OK) -> sleep {backoff:.1f}s retry ({attempt}/{max_retries})", verbose)
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)
            continue

        # JSON parse
        try:
            js = r.json()

            # ArtList 정상 응답은 보통 {"articles":[...]} 형태
            if expect_articles_key and isinstance(js, dict):
                if ("articles" not in js):
                    last_bad_body = body_full
                    if print_fail:
                        head = body_full[:200].replace("\n", " ")
                        log(f"[{ctx}] NO 'articles' key -> sleep {backoff:.1f}s retry ({attempt}/{max_retries}); body='{head}'", verbose)
                    time.sleep(backoff)
                    backoff = min(backoff * 2.0, 30.0)
                    continue

            return js

        except Exception:
            low = body_full.lower()

            if TOO_SHORT_HINT in low:
                if print_fail:
                    body = body_full[:200].replace("\n", " ")
                    log(f"[{ctx}] keyword too short hint detected; body='{body}'", verbose)
                return {"__KEYWORD_TOO_SHORT__": True, "__RAW_BODY__": body_full}

            if PAREN_HINT.lower() in low:
                if print_fail:
                    body = body_full[:200].replace("\n", " ")
                    log(f"[{ctx}] parentheses hint detected; body='{body}'", verbose)
                return {"__PAREN_ERROR__": True, "__RAW_BODY__": body_full}

            last_bad_body = body_full
            if print_fail:
                body = body_full[:200].replace("\n", " ")
                log(f"[{ctx}] JSON parse fail -> sleep {backoff:.1f}s retry ({attempt}/{max_retries}); body='{body}'", verbose)

            time.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)
            continue

    # 재시도 다 했는데도 비정상 응답이 계속이면 unhealthy로 표기
    return {
        "__BAD_RESPONSE__": True,
        "__RAW_BODY__": last_bad_body[:500],
    }


def gdelt_fetch_articles_for_keyword(
    keyword: str,
    startdt: str,
    enddt: str,
    max_records: int,  # 0=unlimited
    sleep_sec: float,
    timeout: int,
    sourcelang: str,
    verbose: bool,
    print_fail: bool,
    max_pages_per_keyword: int,  # 0=unlimited
    max_retries: int = 5,
) -> Tuple[List[Dict[str, Any]], str, bool]:
    """
    returns: (articles, final_query, unhealthy_flag)
      unhealthy_flag=True 인 경우는:
        - 200 OK지만 {} / articles 키 누락 / 빈바디 등이 반복되어 "__BAD_RESPONSE__"로 종료된 경우
        - 또는 HTTP 실패/타임아웃/429 폭주로 끝까지 못 받은 경우
    """
    out: List[Dict[str, Any]] = []

    base = "https://api.gdeltproject.org/api/v2/doc/doc"
    startrecord = 1
    per_page = 250
    pages = 0

    headers = {"User-Agent": USER_AGENT}

    original_query = build_gdelt_query(keyword, sourcelang)
    query = original_query

    target_txt = "unlimited" if max_records == 0 else str(max_records)
    log(f"[GDELT] keyword='{keyword}' start (target max={target_txt}) -> query='{query}'", verbose)

    unhealthy = False

    while True:
        if max_pages_per_keyword > 0 and pages >= max_pages_per_keyword:
            if print_fail:
                log(f"[GDELT:{keyword}] hit max_pages_per_keyword={max_pages_per_keyword} -> stop", verbose)
            break

        if max_records > 0:
            remaining = max_records - len(out)
            if remaining <= 0:
                break
            this_page = min(per_page, remaining)
        else:
            this_page = per_page

        params = {
            "query": query,
            "mode": "ArtList",
            "format": "json",
            "startdatetime": startdt,
            "enddatetime": enddt,
            "maxrecords": str(this_page),
            "startrecord": str(startrecord),
            # ✅ PATCH: sort=HybridRel 제거
        }

        ctx = f"GDELT:{keyword}"
        js = request_json_with_retries(
            url=base,
            params=params,
            headers=headers,
            timeout=timeout,
            max_retries=max_retries,
            base_backoff=1.9,
            verbose=verbose,
            print_fail=print_fail,
            ctx=ctx,
            expect_articles_key=True,
        )

        if not js:
            # HTTP 에러 등으로 None이면 unhealthy 가능성이 큼
            unhealthy = True
            break

        if js.get("__BAD_RESPONSE__"):
            unhealthy = True
            if print_fail:
                head = clean_text(js.get("__RAW_BODY__") or "")[:200]
                log(f"[{ctx}] BAD_RESPONSE persists -> mark unhealthy; body_head='{head}'", verbose)
            break

        if js.get("__KEYWORD_TOO_SHORT__"):
            rewritten = rewrite_query_if_too_short(query)
            if rewritten and rewritten != query:
                log(f"[GDELT] keyword too short -> rewrite query: '{query}' -> '{rewritten}'", verbose)
                query = rewritten
                continue
            break

        if js.get("__PAREN_ERROR__"):
            sanitized = sanitize_query_if_paren_error(query)
            if sanitized and sanitized != query:
                log(f"[GDELT] parentheses issue -> sanitize query: '{query}' -> '{sanitized}'", verbose)
                query = sanitized
                continue
            break

        arts = js.get("articles") or []
        if not isinstance(arts, list):
            unhealthy = True
            if print_fail:
                log(f"[{ctx}] 'articles' is not a list -> mark unhealthy", verbose)
            break

        # ✅ 정상 0개: {"articles": []} -> break (unhealthy 아님)
        if not arts:
            break

        for a in arts:
            out.append(
                {
                    "title": clean_text(a.get("title") or ""),
                    "url": clean_text(a.get("url") or ""),      # raw url for output/extraction
                    "seendate": clean_text(a.get("seendate") or ""),
                    "domain": clean_text(a.get("domain") or ""),
                }
            )

        pages += 1
        startrecord += len(arts)

        if verbose:
            log(f"[GDELT] keyword='{keyword}' fetched so far: {len(out)} (pages={pages})", verbose)

        # 페이지간 딜레이 (GDELT rate-limit 고려)
        if sleep_sec > 0:
            time.sleep(sleep_sec)

        if len(arts) < this_page:
            break

    log(f"[GDELT] keyword='{keyword}' done (got={len(out)})", verbose)
    return out, query, unhealthy


# -----------------------------
# HTML metadata extraction
# -----------------------------
_META_TAG_RE = re.compile(r"(?is)<meta\s+[^>]*?>")
_SCRIPT_LDJSON_RE = re.compile(
    r'(?is)<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
)


def _parse_tag_attrs(tag: str) -> Dict[str, str]:
    # key can include ":" "-" "."
    attrs = {}
    for k, v in re.findall(r'([a-zA-Z0-9:_\.\-]+)\s*=\s*["\'](.*?)["\']', tag, flags=re.I | re.S):
        attrs[k.lower()] = html.unescape(v).strip()
    return attrs


def _collect_jsonld_objects(jsval: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if isinstance(jsval, dict):
        out.append(jsval)
        # sometimes graph
        g = jsval.get("@graph")
        if isinstance(g, list):
            for x in g:
                if isinstance(x, dict):
                    out.append(x)
    elif isinstance(jsval, list):
        for x in jsval:
            if isinstance(x, dict):
                out.append(x)
    return out


def _is_article_type(t: Any) -> bool:
    if not t:
        return False
    if isinstance(t, str):
        tl = t.lower()
        return any(k in tl for k in ["newsarticle", "article", "reportagenewsarticle", "blogposting"])
    if isinstance(t, list):
        return any(_is_article_type(x) for x in t)
    return False


def _extract_author_names_from_jsonld(obj: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    author = obj.get("author")
    if not author:
        return names

    def add_author(a: Any):
        if isinstance(a, str):
            n = clean_text(a)
            if n:
                names.append(n)
        elif isinstance(a, dict):
            n = clean_text(a.get("name") or a.get("headline") or "")
            if n:
                names.append(n)
        elif isinstance(a, list):
            for xx in a:
                add_author(xx)

    add_author(author)

    # de-dup preserving order
    seen = set()
    uniq = []
    for n in names:
        if n not in seen:
            seen.add(n)
            uniq.append(n)
    return uniq


def _extract_published_time_from_jsonld(obj: Dict[str, Any]) -> str:
    for k in ["datePublished", "dateCreated", "dateModified", "dateUpdated"]:
        v = obj.get(k)
        if isinstance(v, str) and clean_text(v):
            return clean_text(v)
    return ""


def _extract_byline_from_text(text: str) -> str:
    # 아주 단순 byline 패턴: 초반에서만 탐색
    head = clean_text(text)[:600]
    # e.g. "By JOSH FUNK" / "By Matt Lavietes | NBC News"
    m = re.search(r"\bBy\s+([A-Z][A-Za-z\.\-\'\s]{2,80})(?:\s*\||\s*[-–—]|,|\n|$)", head)
    if not m:
        return ""
    cand = clean_text(m.group(1))
    if len(cand) < 3:
        return ""
    return cand


def extract_text_fallback(html_text: str) -> str:
    h = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\1>", " ", html_text)
    h = re.sub(r"(?is)<br\s*/?>", "\n", h)
    h = re.sub(r"(?is)</p\s*>", "\n", h)
    h = re.sub(r"(?is)<.*?>", " ", h)
    h = html.unescape(h)
    h = re.sub(r"[ \t\r\f\v]+", " ", h)
    h = re.sub(r"\n\s*\n+", "\n", h)
    return clean_text(h)


def extract_article(url: str, timeout: int) -> Dict[str, str]:
    """
    Returns dict:
      - text
      - description
      - authors
      - published_time
      - og_title (optional)
      - fallback_used ("1" or "0")
    """
    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code != 200:
            return {"text": "", "description": "", "authors": "", "published_time": "", "og_title": "", "fallback_used": "1"}
        html_text = r.text or ""
    except Exception:
        return {"text": "", "description": "", "authors": "", "published_time": "", "og_title": "", "fallback_used": "1"}

    meta_name = {}
    meta_prop = {}
    og_title = ""

    # meta tags
    for tag in _META_TAG_RE.findall(html_text):
        attrs = _parse_tag_attrs(tag)
        if not attrs:
            continue
        content = clean_text(attrs.get("content") or "")
        if not content:
            continue

        n = clean_text(attrs.get("name") or "").lower()
        p = clean_text(attrs.get("property") or "").lower()
        itemprop = clean_text(attrs.get("itemprop") or "").lower()

        if n:
            meta_name.setdefault(n, content)
        if p:
            meta_prop.setdefault(p, content)
        if itemprop:
            meta_name.setdefault(itemprop, content)

    # title candidates
    og_title = meta_prop.get("og:title", "") or meta_name.get("og:title", "")
    if not og_title:
        m = re.search(r"(?is)<title[^>]*>(.*?)</title>", html_text)
        if m:
            og_title = clean_text(m.group(1))

    # description candidates
    desc = (
        meta_prop.get("og:description", "")
        or meta_name.get("twitter:description", "")
        or meta_name.get("description", "")
        or meta_name.get("og:description", "")
    )

    # publish time candidates (page)
    published_time = (
        meta_prop.get("article:published_time", "")
        or meta_prop.get("og:published_time", "")
        or meta_name.get("pubdate", "")
        or meta_name.get("publish_date", "")
        or meta_name.get("publish-date", "")
        or meta_name.get("timestamp", "")
        or meta_name.get("date", "")
    )

    # authors candidates (page meta)
    meta_author = (
        meta_name.get("author", "")
        or meta_prop.get("article:author", "")
        or meta_name.get("article:author", "")
        or meta_name.get("byl", "")
    )

    # JSON-LD extraction
    jsonld_authors: List[str] = []
    jsonld_pubtime = ""
    for block in _SCRIPT_LDJSON_RE.findall(html_text):
        raw = block.strip()
        if not raw:
            continue
        try:
            jsval = json.loads(raw)
        except Exception:
            raw2 = raw.strip().strip("<!--").strip("-->")
            try:
                jsval = json.loads(raw2)
            except Exception:
                continue

        for obj in _collect_jsonld_objects(jsval):
            if not isinstance(obj, dict):
                continue
            if not _is_article_type(obj.get("@type")):
                continue

            if not jsonld_authors:
                jsonld_authors = _extract_author_names_from_jsonld(obj)
            if not jsonld_pubtime:
                jsonld_pubtime = _extract_published_time_from_jsonld(obj)

            if jsonld_authors and jsonld_pubtime:
                break

    # choose best authors
    authors = ""
    if jsonld_authors:
        authors = ", ".join([a for a in jsonld_authors if a])
    elif meta_author:
        authors = clean_text(meta_author)

    # byline fallback
    if not authors:
        plain = extract_text_fallback(html_text)
        authors = _extract_byline_from_text(plain)

    # choose best published_time
    if jsonld_pubtime:
        published_time = jsonld_pubtime or published_time

    # main text extraction
    text = ""
    try:
        import trafilatura  # type: ignore
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            text = trafilatura.extract(downloaded, include_comments=False, include_tables=False) or ""
        else:
            text = trafilatura.extract(html_text, include_comments=False, include_tables=False) or ""
        text = clean_text(text)
    except Exception:
        pass

    fallback_used = False
    if not text:
        fallback_used = True
        text = extract_text_fallback(html_text)

    return {
        "text": text,
        "description": clean_text(desc),
        "authors": clean_text(authors),
        "published_time": clean_text(published_time),
        "og_title": clean_text(og_title),
        "fallback_used": "1" if fallback_used else "0",
    }


# -----------------------------
# all_text (TILDA-like preview)
# -----------------------------
def make_tilda_preview(full_text: str, *, target_len: int = 215, hard_cap: int = 218) -> str:
    """
    (A) TILDA-style preview: "<prefix>… [+N chars]"
    """
    txt = clean_text(full_text)
    if not txt:
        return ""

    target_len = max(1, int(target_len))
    hard_cap = max(target_len, int(hard_cap))

    total = len(txt)

    # 초기 prefix 길이(대충 suffix 길이 고려)
    prefix_len = max(0, target_len - 18)

    # 고정점 반복(remaining 숫자 자리수 변화 보정)
    for _ in range(6):
        prefix = txt[:prefix_len].rstrip()
        remaining = max(0, total - len(prefix))
        suffix = f"… [+{remaining} chars]"
        allowed = min(target_len, hard_cap)
        new_prefix_len = max(0, allowed - len(suffix))
        if new_prefix_len == prefix_len:
            break
        prefix_len = new_prefix_len

    # 최종 조립 + hard_cap 안전장치
    prefix = txt[:prefix_len].rstrip()
    remaining = max(0, total - len(prefix))
    suffix = f"… [+{remaining} chars]"

    allowed_prefix = max(0, hard_cap - len(suffix))
    if len(prefix) > allowed_prefix:
        prefix = prefix[:allowed_prefix].rstrip()
        remaining = max(0, total - len(prefix))
        suffix = f"… [+{remaining} chars]"
        allowed_prefix = max(0, hard_cap - len(suffix))
        if len(prefix) > allowed_prefix:
            prefix = prefix[:allowed_prefix].rstrip()
            remaining = max(0, total - len(prefix))
            suffix = f"… [+{remaining} chars]"

    return f"{prefix}{suffix}"


def trunc_with_ellipsis(s: str, maxlen: int, ellipsis: str = "...") -> str:
    s = clean_text(s)
    if len(s) <= maxlen:
        return s
    if maxlen <= len(ellipsis):
        return ellipsis[:maxlen]
    return (s[: maxlen - len(ellipsis)].rstrip() + ellipsis)


# -----------------------------
# output row
# -----------------------------
@dataclass
class TildaRow:
    id: int
    title: str
    doc_url: str
    all_text: str
    authors: str
    publish_date: str
    meta_site_name: str
    key_word: str
    filter_status: str
    description: str
    named_entities: str
    triples: str
    article_embedding: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title or "",
            "doc_url": self.doc_url or "",
            "all_text": self.all_text or "",
            "authors": self.authors or "",
            "publish_date": self.publish_date or "",
            "meta_site_name": self.meta_site_name or "",
            "key_word": self.key_word or "",
            "filter_status": self.filter_status or "",
            "description": self.description or "",
            "named_entities": self.named_entities or "[]",
            "triples": self.triples or "[]",
            "article_embedding": self.article_embedding or "",
        }


def build_tilda_row(
    idx: int,
    keyword_raw: str,
    title: str,
    url_raw: str,
    domain: str,
    seendate_iso: str,
    extracted: Dict[str, str],
    *,
    all_text_target_len: int,
    all_text_hardcap: int,
    description_maxlen: int,
    keep_title_prefix: bool,
) -> TildaRow:
    doc_url = clean_text(url_raw)

    t = clean_text(title)
    if not t:
        t = clean_text(extracted.get("og_title") or "")

    pub = normalize_publish_datetime(extracted.get("published_time") or "")
    if not pub:
        pub = normalize_publish_datetime(seendate_iso) or seendate_iso

    site = meta_site_name_domain_only(domain, doc_url, cap=26)

    authors = clean_text(extracted.get("authors") or "")

    desc_src = extracted.get("description") or extracted.get("text") or ""
    description = trunc_with_ellipsis(desc_src, description_maxlen, ellipsis="...")

    body = clean_text(extracted.get("text") or "")
    if not body:
        body = clean_text(f"{t}\n\n{description}")

    if keep_title_prefix and t:
        merged = clean_text(f"{t}\n\n{body}")
    else:
        merged = body

    all_text = make_tilda_preview(
        merged,
        target_len=int(all_text_target_len),
        hard_cap=int(all_text_hardcap),
    )

    return TildaRow(
        id=idx,
        title=t,
        doc_url=doc_url,
        all_text=all_text,
        authors=authors,
        publish_date=pub,
        meta_site_name=site,
        key_word=clean_text(keyword_raw),
        filter_status="",
        description=description,
        named_entities="[]",
        triples="[]",
        article_embedding="",
    )


def write_csv(out_path: str, rows: List[TildaRow]) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=DEFAULT_OUT_COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow(r.to_dict())


# -----------------------------
# main
# -----------------------------
def load_site_whitelist(path: str) -> set:
    s = set()
    if not path:
        return s
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            x = line.strip()
            if not x or x.startswith("#"):
                continue
            if "://" in x:
                d = domain_from_url(x)
            else:
                d = strip_www(x).lower()
            if d:
                s.add(d)
    return s


def main():
    ap = argparse.ArgumentParser()

    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", default="", help="YYYY-MM-DD (single day)")
    g.add_argument("--date_from", default="", help="YYYY-MM-DD (range start)")
    g.add_argument("--hours_back", type=int, default=0, help="collect from end(UTC) back N hours (e.g., 12 or 13)")

    ap.add_argument("--date_to", default="", help="YYYY-MM-DD (range end). required when --date_from is used.")
    ap.add_argument("--out", required=True, help="output csv path")

    ap.add_argument(
        "--end_ts",
        default="",
        help="(hours_back mode) end timestamp. "
             "ISO8601 recommended, e.g. '2026-02-02T21:00:00+00:00' or '2026-02-02T21:00:00Z'. "
             "If timezone is omitted, it is treated as UTC."
    )

    ap.add_argument("--keywords_file", default="", help="optional: one keyword per line")

    ap.add_argument("--max_per_keyword", type=int, default=300)

    ap.add_argument("--max_total_candidates", type=int, default=50000,
                    help="(guardrail) stop if total candidates (pre-dedupe) reaches this. 0=unlimited")
    ap.add_argument("--max_pages_per_keyword", type=int, default=200,
                    help="(guardrail) stop per keyword after N pages. 0=unlimited")
    ap.add_argument("--max_runtime_sec", type=int, default=0,
                    help="(guardrail) stop whole run after N seconds. 0=unlimited")

    ap.add_argument("--dedupe_by_url", type=int, default=1, help="1=dedupe by normalized url across keywords")

    ap.add_argument("--all_text_maxlen", type=int, default=215, help="TILDA-like all_text preview target length (default 215)")
    ap.add_argument("--all_text_hardcap", type=int, default=218, help="TILDA-like all_text hard cap length (default 218)")

    ap.add_argument("--description_maxlen", type=int, default=260)

    # ✅ 기본을 5.2초로 (GDELT rate-limit 고려)
    ap.add_argument("--sleep", type=float, default=5.2, help="polite delay between GDELT pages (default 5.2s)")
    ap.add_argument("--http_timeout", type=int, default=20)

    ap.add_argument("--sourcelang", default="English",
                    help="GDELT language filter. e.g., English. Use ALL to disable filtering.")

    ap.add_argument("--workers", type=int, default=1)

    ap.add_argument("--verbose", type=int, default=0)
    ap.add_argument("--log_every", type=int, default=10, help="progress log every N articles during page fetch")
    ap.add_argument("--print_fail", type=int, default=0, help="1=print errors/retries")

    # parsing/quality filters
    ap.add_argument("--drop_bad_pages", type=int, default=1,
                    help="1=drop pages that look blocked/paywalled/nav-only (bad parse). 0=keep them")
    ap.add_argument("--require_text", type=int, default=0,
                    help="1=require extracted text (non-empty) and length >= min_text_len; otherwise drop row")
    ap.add_argument("--min_text_len", type=int, default=400,
                    help="min length of extracted text to keep when --require_text=1")
    ap.add_argument("--keep_title_prefix", type=int, default=0,
                    help="1=prepend title into all_text source before preview. 0=use body only (default)")

    ap.add_argument("--strict_date", type=int, default=1,
                    help="1=when --date is used, keep only rows within date±tolerance. 0=disable")
    ap.add_argument("--date_tolerance_days", type=int, default=0,
                    help="tolerance days for strict_date in --date mode (e.g., 1 means ±1 day)")

    ap.add_argument("--site_whitelist_file", default="",
                    help="optional: allowed domains (one per line). if set, keep only those domains.")

    # ✅ NEW: GDELT unhealthy면 중지 옵션
    ap.add_argument("--abort_on_unhealthy", type=int, default=1,
                    help="1이면 GDELT가 비정상 응답({}/빈바디/키누락) 반복 시 즉시 중지")
    ap.add_argument("--unhealthy_streak", type=int, default=3,
                    help="연속으로 unhealthy가 N번 나오면 abort (default 3)")

    args = ap.parse_args()
    verbose = bool(int(args.verbose))
    print_fail = bool(int(args.print_fail))

    # resolve date range
    if args.hours_back and int(args.hours_back) > 0:
        if args.end_ts:
            end_dt = parse_end_ts_to_utc(args.end_ts)
        else:
            end_dt = datetime.now(timezone.utc)

        start_dt = end_dt - timedelta(hours=int(args.hours_back))

        startdt = start_dt.strftime("%Y%m%d%H%M%S")
        enddt   = end_dt.strftime("%Y%m%d%H%M%S")

        fallback_date = end_dt.strftime("%Y-%m-%d")

        date_from = start_dt.strftime("%Y-%m-%d")
        date_to   = end_dt.strftime("%Y-%m-%d")

        if verbose:
            log(f"[WINDOW] start_utc={start_dt.isoformat()} end_utc={end_dt.isoformat()} hours_back={int(args.hours_back)}", verbose)

    elif args.date:
        date_from = args.date
        date_to = args.date
        startdt, enddt = yyyymmdd_to_range(args.date)
        fallback_date = args.date

    else:
        if not args.date_to:
            raise ValueError("--date_to is required when using --date_from")
        date_from = args.date_from
        date_to = args.date_to
        startdt, enddt = ymd_range_to_gdelt_range(date_from, date_to)
        fallback_date = date_to

    # keywords (원문 그대로 유지)
    keywords = DEFAULT_KEYWORDS
    if args.keywords_file:
        with open(args.keywords_file, "r", encoding="utf-8") as f:
            kws = [line.rstrip("\n") for line in f if line.strip()]
        if kws:
            keywords = kws

    # whitelist
    whitelist = load_site_whitelist(args.site_whitelist_file)
    if whitelist:
        log(f"[WHITELIST] enabled: {len(whitelist)} domains", verbose)

    t0 = time.time()

    def time_exceeded() -> bool:
        if int(args.max_runtime_sec) <= 0:
            return False
        return (time.time() - t0) >= int(args.max_runtime_sec)

    max_total = int(args.max_total_candidates)
    max_pages_kw = int(args.max_pages_per_keyword)
    max_per_kw = int(args.max_per_keyword)

    if args.date:
        log(f"[START] date={args.date} keywords={len(keywords)} max_per_keyword={max_per_kw} sourcelang={args.sourcelang}", verbose)
    else:
        log(f"[START] range={date_from}~{date_to} keywords={len(keywords)} max_per_keyword={max_per_kw} sourcelang={args.sourcelang}", verbose)

    log(f"[GUARD] max_total_candidates={max_total} max_pages_per_keyword={max_pages_kw} max_runtime_sec={args.max_runtime_sec}", verbose)
    log(f"[TILDA-LIKE] all_text_target={args.all_text_maxlen} hardcap={args.all_text_hardcap} drop_bad_pages={args.drop_bad_pages} require_text={args.require_text} min_text_len={args.min_text_len} keep_title_prefix={args.keep_title_prefix}", verbose)
    log(f"[GDELT] sleep_per_page={args.sleep}s abort_on_unhealthy={args.abort_on_unhealthy} unhealthy_streak={args.unhealthy_streak}", verbose)

    # 1) fetch candidates
    candidates: List[Tuple[str, Dict[str, Any]]] = []
    rewritten_notes: List[Tuple[str, str]] = []

    unhealthy_streak = 0
    max_unhealthy = max(1, int(args.unhealthy_streak))

    for i, kw in enumerate(keywords, start=1):
        if time_exceeded():
            log("[GUARD] max_runtime_sec reached during candidate fetch -> stop", verbose)
            break

        if max_total > 0 and len(candidates) >= max_total:
            log(f"[GUARD] max_total_candidates reached (candidates={len(candidates)}) -> stop", verbose)
            break

        log(f"[1/3] ({i}/{len(keywords)}) fetching list from GDELT...", verbose)

        arts, final_query, unhealthy = gdelt_fetch_articles_for_keyword(
            keyword=kw,
            startdt=startdt,
            enddt=enddt,
            max_records=max_per_kw,
            sleep_sec=float(args.sleep),
            timeout=args.http_timeout,
            sourcelang=args.sourcelang,
            verbose=verbose,
            print_fail=print_fail,
            max_pages_per_keyword=max_pages_kw,
        )

        if unhealthy:
            unhealthy_streak += 1
            log(f"[GDELT-UNHEALTHY] keyword '{kw}' unhealthy (streak={unhealthy_streak})", verbose)
            if int(args.abort_on_unhealthy) == 1 and unhealthy_streak >= max_unhealthy:
                raise RuntimeError(
                    f"GDELT appears unhealthy: {unhealthy_streak} consecutive unhealthy keyword fetches "
                    f"(likely empty body/{{}}/missing articles/rate-limit). Aborting."
                )
        else:
            unhealthy_streak = 0

        if final_query != build_gdelt_query(kw, args.sourcelang):
            rewritten_notes.append((build_gdelt_query(kw, args.sourcelang), final_query))

        for a in arts:
            candidates.append((kw, a))
            if max_total > 0 and len(candidates) >= max_total:
                break

        log(f"[GDELT] after keyword {i}/{len(keywords)}: candidates={len(candidates)}", verbose)

        if max_total > 0 and len(candidates) >= max_total:
            log(f"[GUARD] max_total_candidates reached (candidates={len(candidates)}) -> stop", verbose)
            break

    # 2) dedupe by normalized url (optional) + (옵션) whitelist filter
    seen = set()
    deduped: List[Tuple[str, Dict[str, Any]]] = []
    skipped_dupe = 0
    skipped_empty_url = 0
    skipped_whitelist = 0

    for kw, a in candidates:
        url_raw = clean_text(a.get("url") or "")
        if not url_raw:
            skipped_empty_url += 1
            continue

        # whitelist check (domain 기준)
        if whitelist:
            dom = strip_www(clean_text(a.get("domain") or "")).lower()
            if not dom:
                dom = domain_from_url(url_raw)
            if dom and (dom not in whitelist):
                skipped_whitelist += 1
                continue

        url_norm = normalize_url(url_raw)
        if not url_norm:
            skipped_empty_url += 1
            continue

        if int(args.dedupe_by_url) == 1:
            if url_norm in seen:
                skipped_dupe += 1
                continue
            seen.add(url_norm)

        a2 = dict(a)
        a2["url_raw"] = url_raw      # 출력/수집용
        a2["url_norm"] = url_norm    # dedupe key
        deduped.append((kw, a2))

    log(
        f"[2/3] candidates={len(candidates)} -> deduped={len(deduped)} "
        f"(skipped_dupe={skipped_dupe}, skipped_empty_url={skipped_empty_url}, skipped_whitelist={skipped_whitelist})",
        verbose
    )

    # 3) fetch pages
    log(f"[3/3] fetching article pages... workers={args.workers}", verbose)

    rows_out: List[Optional[TildaRow]] = [None] * len(deduped)
    fails = 0
    dropped = 0
    done = 0
    log_every = max(1, int(args.log_every))

    def _worker(job_idx: int, kw_raw: str, a: Dict[str, Any]) -> Tuple[int, Optional[TildaRow], Optional[str]]:
        url_raw = clean_text(a.get("url_raw") or a.get("url") or "")
        title = clean_text(a.get("title") or "")
        domain = clean_text(a.get("domain") or "")
        seendate = clean_text(a.get("seendate") or "")

        seendate_iso = publish_datetime_from_seendate(seendate, fallback_date=fallback_date)

        try:
            extracted = extract_article(url_raw, timeout=args.http_timeout)

            # quality filter: require_text + min length
            body = clean_text(extracted.get("text") or "")
            if int(args.require_text) == 1:
                if (not body) or (len(body) < int(args.min_text_len)):
                    return job_idx, None, f"DROP_BAD_PAGE:too_short<{int(args.min_text_len)}"

            # blocked/nav/paywall-like page drop
            fallback_used = (clean_text(extracted.get("fallback_used") or "") == "1")
            reason = looks_like_blocked_or_nav_page(body, fallback_used=fallback_used)
            if int(args.drop_bad_pages) == 1 and reason:
                return job_idx, None, f"DROP_BAD_PAGE:{reason}"

            row = build_tilda_row(
                idx=job_idx + 1,
                keyword_raw=kw_raw,
                title=title,
                url_raw=url_raw,
                domain=domain,
                seendate_iso=seendate_iso,
                extracted=extracted,
                all_text_target_len=int(args.all_text_maxlen),
                all_text_hardcap=int(args.all_text_hardcap),
                description_maxlen=int(args.description_maxlen),
                keep_title_prefix=bool(int(args.keep_title_prefix)),
            )
            return job_idx, row, None
        except Exception as e:
            extracted = {"text": "", "description": "", "authors": "", "published_time": "", "og_title": "", "fallback_used": "1"}
            row = build_tilda_row(
                idx=job_idx + 1,
                keyword_raw=kw_raw,
                title=title,
                url_raw=url_raw,
                domain=domain,
                seendate_iso=seendate_iso,
                extracted=extracted,
                all_text_target_len=int(args.all_text_maxlen),
                all_text_hardcap=int(args.all_text_hardcap),
                description_maxlen=int(args.description_maxlen),
                keep_title_prefix=bool(int(args.keep_title_prefix)),
            )
            return job_idx, row, f"{type(e).__name__}: {e}"

    if args.workers <= 1:
        for j, (kw_raw, a) in enumerate(deduped):
            if time_exceeded():
                log("[GUARD] max_runtime_sec reached during page fetch -> stop", verbose)
                break

            job_idx, row, err = _worker(j, kw_raw, a)

            if err:
                if err.startswith("DROP_BAD_PAGE:"):
                    dropped += 1
                    if print_fail:
                        log(f"[DROP] ({job_idx+1}/{len(deduped)}) url={a.get('url_raw','')} reason={err}", verbose)
                else:
                    fails += 1
                    if print_fail:
                        log(f"[FAIL] ({job_idx+1}/{len(deduped)}) url={a.get('url_raw','')} err={err}", verbose)

            rows_out[job_idx] = row
            done += 1
            if verbose and done % log_every == 0:
                log(f"[PROGRESS] {done}/{len(deduped)} done (fails={fails} dropped={dropped})", verbose)
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = []
            for j, (kw_raw, a) in enumerate(deduped):
                futures.append(ex.submit(_worker, j, kw_raw, a))

            for fut in as_completed(futures):
                if time_exceeded():
                    log("[GUARD] max_runtime_sec reached during page fetch -> stop (remaining futures will be ignored)", verbose)
                    break

                job_idx, row, err = fut.result()

                if err:
                    if err.startswith("DROP_BAD_PAGE:"):
                        dropped += 1
                        if print_fail:
                            log(f"[DROP] ({job_idx+1}/{len(deduped)}) url={deduped[job_idx][1].get('url_raw','')} reason={err}", verbose)
                    else:
                        fails += 1
                        if print_fail:
                            log(f"[FAIL] ({job_idx+1}/{len(deduped)}) url={deduped[job_idx][1].get('url_raw','')} err={err}", verbose)

                rows_out[job_idx] = row
                done += 1
                if verbose and done % log_every == 0:
                    log(f"[PROGRESS] {done}/{len(deduped)} done (fails={fails} dropped={dropped})", verbose)

    final_rows: List[TildaRow] = []
    for r in rows_out:
        if r is None:
            continue
        final_rows.append(r)

    # strict_date: --date 모드에서 publish_date가 해당 날짜(±tolerance)인 것만 유지
    if args.date and int(args.strict_date) == 1:
        before = len(final_rows)
        tol = int(args.date_tolerance_days)
        final_rows = [r for r in final_rows if within_date_tolerance(r.publish_date, args.date, tol)]
        after = len(final_rows)
        log(f"[STRICT_DATE] keep only {args.date} (±{tol}d): {before} -> {after}", verbose)

    # id 재부여
    for i, r in enumerate(final_rows, start=1):
        r.id = i

    write_csv(args.out, final_rows)
    log(f"[DONE] wrote: {args.out} (rows={len(final_rows)} fails={fails} dropped={dropped})", verbose)

    if rewritten_notes:
        log("[NOTE] some keywords were auto-rewritten/sanitized by GDELT error handling:", verbose)
        for before_q, after_q in rewritten_notes:
            log(f"  - '{before_q}' -> '{after_q}'", verbose)


if __name__ == "__main__":
    main()
