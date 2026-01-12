"""
Indeed Jobs Scraper (JobSpy)
- Scrapes Indeed via JobSpy for company groups/aliases across selected countries
- Writes one JSON per company group + summary.json
- Outputs common job fields + search/provenance metadata
- Expands to cities if country search hits ~1000 cap
- Enforces exact company-name matching; saves mismatch examples
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import logging
import math
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from jobspy import scrape_jobs


# CONFIG

@dataclass(frozen=True)
class Config:
    output_dir: str = "indeed_json"
    results_wanted: int = 1000
    hours_old: Optional[int] = None  # e.g. 168 for last 7 days

    sleep_between_searches: float = 1.25
    random_jitter_seconds: float = 0.50

    max_workers: int = 5
    max_retries: int = 3
    retry_base_seconds: float = 2.0
    retry_max_seconds: float = 20.0
    enforce_exact_company_match: bool = True
    checkpoint_every_n_requests: int = 250
    log_level: str = "INFO"


# INPUTS

INDEED_COUNTRIES: List[str] = [
    "Canada", "United States",
]

# Used when country search hits cap
INDEED_CITY_LOCATIONS: Dict[str, List[str]] = {
    "Canada": [
        "Toronto, ON",
        "Mississauga, ON",
        "Brampton, ON",
        "Ottawa, ON",
        "Hamilton, ON",
        "London, ON",
        "Kitchener, ON",
        "Waterloo, ON",
        "Montreal, QC",
        "Quebec City, QC",
        "Laval, QC",
        "Gatineau, QC",
        "Vancouver, BC",
        "Burnaby, BC",
        "Surrey, BC",
        "Richmond, BC",
        "Victoria, BC",
        "Calgary, AB",
        "Edmonton, AB",
        "Winnipeg, MB",
        "Halifax, NS",
        "Saskatoon, SK",
        "Regina, SK",
        "St. John's, NL",
    ],
    "United States": [
        "New York, NY",
        "Jersey City, NJ",
        "Philadelphia, PA",
        "Washington, DC",
        "Boston, MA",
        "Chicago, IL",
        "Atlanta, GA",
        "Miami, FL",
        "Orlando, FL",
        "Tampa, FL",
        "Dallas, TX",
        "Fort Worth, TX",
        "Houston, TX",
        "Austin, TX",
        "Denver, CO",
        "Phoenix, AZ",
        "Minneapolis, MN",
        "Seattle, WA",
        "Portland, OR",
        "San Francisco, CA",
        "San Jose, CA",
        "Los Angeles, CA",
        "San Diego, CA",
    ],
}


COMPANY_GROUPS: Dict[str, List[str]] = {
    "RBC": [
        "RBC",
        "Royal Bank of Canada",
        "RBC Dominion Securities",
        "RBC Insurance",
        "RBCx",
        "RBC Wealth Management",
        "RBC Capital Markets",
        "RBC Investor Services",
        "RBC Global Asset Management",
    ],
    "TD": [
        "TD",
        "TD Bank",
    ],
    "Scotiabank": [
        "Scotiabank",
    ],
    "Desjardins": [
        "Desjardins",
    ],
    "CIBC": [
        "CIBC",
    ],
    "BMO": [
        "BMO",
        "BMO Groupe financier",
        "BMO Financial Group",
    ],
    "National Bank of Canada": [
        "National Bank of Canada",
        "Banque Nationale du Canada",
    ],
    "Fairstone Bank": [
        "Fairstone",
    ],
    "Questrade Financial Group": [
        "Questrade Financial Group",
    ],
    "Canada Mortgage and Housing Corporation (CMHC)": [
        "Canada Mortgage and Housing Corporation (CMHC)",
        "CMHC",
    ],
    "EQ Bank | Equitable Bank": [
        "EQ Bank | Equitable Bank",
    ],
    "Vancity": [
        "Vancity",
        "Vancity Community Investment Bank",
    ],
    "ATB Financial": [
        "ATB Financial",
    ],
    "nesto": [
        "nesto",
    ],
    "Tangerine": [
        "Tangerine",
    ],
    "Meridian Credit Union": [
        "Meridian Credit Union",
    ],
    "Alterna Savings": [
        "Alterna Savings",
    ],
    "First National Financial LP": [
        "First National Financial",
    ],
    "Servus Credit Union": [
        "Servus Credit Union",
    ],
    "Coast Capital Savings": [
        "Coast Capital Savings",
    ],
    "CMLS Financial": [
        "CMLS Financial",
    ],
    "HomeEquity Bank": [
        "HomeEquity Bank",
    ],
    "DUCA Financial Services Credit Union Ltd.": [
        "DUCA Credit Union Ltd.",
    ],
    "MCAP": [
        "MCAP",
    ],
    "Sagen": [
        "Sagen",
    ],
    "Ratehub.ca": [
        "RateHub",
    ],
    "Haventree Bank": [
        "Haventree Bank",
    ],
    "Pine": [
        "Pine",
        "Pine Financial",
        "Pine Canada",
    ],
    "True North Mortgage": [
        "True North Mortgage",
    ],
    "Home Trust Company": [
        "Home Trust",
    ],
    "Canada Guaranty": [
        "Canada Guaranty",
    ],
    "RFA Bank of Canada": [
        "RFA Bank of Canada",
        "RFA Mortgages Corp.",
    ],
    "Dominion Lending Centres Inc": [
        "Dominion Lending Centres",
    ],
    "Lendesk": [
        "Lendesk",
    ],
    "M3 Financial Group": [
        "M3 Financial Services, Inc.",
    ],
    "Smith Financial Solutions": [  # not in indeed
        "Smith Financial Solutions",
    ],
    "Marathon Mortgage Corp.": [
        "Marathon Mortgage Corp.",
    ],
    "MERIX Financial": [
        "Merix Financial",
    ],
    "Newton Connectivity Systems": [
        "Newton Connectivity Systems",
    ],
    "CENTUM Mortgage Network": [
        "CENTUM MORTGAGE",
    ],
    "Filogix, A Finastra Company": [
        "Filogix",
    ],
    "Paradigm Quest": [
        "Paradigm Quest",
    ],
    "RATESDOTCA": [
        "RATESDOTCA Group Ltd.",
    ],
    "VERICO Canada": [
        "Verico",
    ],
    "Manulife Bank of Canada": [
        "Manulife",
    ],
    "Canadian Western Bank": [
        "Canadian Western Bank",
    ],
    "MCAN Home Mortgage Corporation": [
        "MCAN Mortgage",
    ],
    "The Mortgage Centre": [
        "The Mortgage Centre",
    ],
    "Bridgewater Bank": [
        "Bridgewater Bank",
    ],
    "B2B Bank": [
        "Laurentian Bank Financial Group",
    ],
}


# LOGGING

def setup_logging(level: str) -> logging.Logger:
    logger = logging.getLogger("indeed_jobspy_clean")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(fmt)
        h.setLevel(getattr(logging, level.upper(), logging.INFO))
        logger.addHandler(h)

    return logger


# HELPERS

REMOTE_RE = re.compile(r"\b(remote|work from home|wfh|telecommute|telecommuting)\b", re.IGNORECASE)
HYBRID_RE = re.compile(r"\b(hybrid)\b", re.IGNORECASE)
ONSITE_RE = re.compile(r"\b(on[- ]?site|in[- ]office|in the office)\b", re.IGNORECASE)

FULLTIME_RE = re.compile(r"\b(full[- ]?time)\b", re.IGNORECASE)
PARTTIME_RE = re.compile(r"\b(part[- ]?time)\b", re.IGNORECASE)
CONTRACT_RE = re.compile(r"\b(contract|fixed[- ]term)\b", re.IGNORECASE)
TEMP_RE = re.compile(r"\b(temporary|temp)\b", re.IGNORECASE)
INTERN_RE = re.compile(r"\b(intern|internship|co-?op)\b", re.IGNORECASE)

SALARY_RANGE_RE = re.compile(
    r"(?P<currency>\$|USD|CAD|AUD|NZD|SGD|HKD|GBP|£|EUR|€)\s*"
    r"(?P<min>[\d,]+(?:\.\d+)?)\s*[-–]\s*(?P<max>[\d,]+(?:\.\d+)?)"
    r"(?:\s*(?:per|/)\s*(?P<interval>year|yr|month|mo|week|wk|day|hour|hr))?",
    re.IGNORECASE,
)
SALARY_SINGLE_RE = re.compile(
    r"(?P<currency>\$|USD|CAD|AUD|NZD|SGD|HKD|GBP|£|EUR|€)\s*"
    r"(?P<amount>[\d,]+(?:\.\d+)?)"
    r"(?:\s*(?:per|/)\s*(?P<interval>year|yr|month|mo|week|wk|day|hour|hr))?",
    re.IGNORECASE,
)

INTERVAL_MAP = {
    "year": "yearly", "yr": "yearly",
    "month": "monthly", "mo": "monthly",
    "week": "weekly", "wk": "weekly",
    "day": "daily",
    "hour": "hourly", "hr": "hourly",
}

CURRENCY_MAP = {
    "$": None,  # infer from country
    "£": "GBP",
    "€": "EUR",
    "USD": "USD",
    "CAD": "CAD",
    "AUD": "AUD",
    "NZD": "NZD",
    "SGD": "SGD",
    "HKD": "HKD",
    "GBP": "GBP",
    "EUR": "EUR",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_missing(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    try:
        return bool(pd.isna(v))
    except Exception:
        return False


def norm(v: Any) -> Any:
    if is_missing(v):
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    return v


def normalize_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: norm(v) for k, v in d.items()}


def atomic_write_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    os.replace(tmp, path)


def safe_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_")


def split_location(location: Optional[str]) -> Dict[str, Optional[str]]:
    if not location:
        return {"city": None, "region": None, "country": None}
    loc = location.strip()
    if not loc or loc.lower() == "remote":
        return {"city": None, "region": None, "country": None}

    parts = [p.strip() for p in loc.split(",") if p.strip()]
    if len(parts) == 1:
        return {"city": parts[0], "region": None, "country": None}
    if len(parts) == 2:
        return {"city": parts[0], "region": parts[1], "country": None}
    return {"city": parts[0], "region": parts[1], "country": parts[-1]}


def stable_dedupe_key(job: Dict[str, Any]) -> str:
    pieces = [
        str(job.get("site") or ""),
        str(job.get("id") or ""),
        str(job.get("job_url") or ""),
        str(job.get("job_url_direct") or ""),
        str(job.get("title") or ""),
        str(job.get("company") or ""),
        str(job.get("location") or ""),
    ]
    raw = "||".join(pieces).encode("utf-8", errors="ignore")
    return hashlib.sha1(raw).hexdigest()


def to_clean_lower(x: Any) -> Optional[str]:
    if is_missing(x):
        return None
    s = str(x).strip()
    return s.lower() if s else None


def make_allowed_company_set(aliases: List[str]) -> Set[str]:
    out: Set[str] = set()
    for a in aliases:
        s = to_clean_lower(a)
        if s:
            out.add(s)
    return out


def company_exact_allowed(job_company: Any, allowed_company_lc: Set[str]) -> bool:
    jc = to_clean_lower(job_company)
    if not jc:
        return False
    return jc in allowed_company_lc


def infer_work_arrangement(title: str, location: str, desc: str) -> Optional[str]:
    text = f"{title}\n{location}\n{desc}".strip()
    if not text:
        return None
    if HYBRID_RE.search(text):
        return "hybrid"
    if REMOTE_RE.search(text) or (location.strip().lower() == "remote"):
        return "remote"
    if ONSITE_RE.search(text):
        return "onsite"
    return None


def infer_employment_types(title: str, desc: str, job_type_raw: Optional[str]) -> Optional[List[str]]:
    text = f"{title}\n{desc}\n{job_type_raw or ''}".lower()
    found: Set[str] = set()

    if FULLTIME_RE.search(text): found.add("fulltime")
    if PARTTIME_RE.search(text): found.add("parttime")
    if CONTRACT_RE.search(text): found.add("contract")
    if TEMP_RE.search(text): found.add("temporary")
    if INTERN_RE.search(text): found.add("internship")

    return sorted(found) if found else None


def infer_currency(country: str, symbol_or_code: Optional[str]) -> Optional[str]:
    if not symbol_or_code:
        return None
    raw = symbol_or_code.upper().strip()
    mapped = CURRENCY_MAP.get(raw, raw)
    if mapped is not None:
        return mapped

    # Infer "$" from country
    if country in ("Canada",):
        return "CAD"
    if country in ("USA", "United States", "United States of America"):
        return "USD"
    if country == "Australia":
        return "AUD"
    return None


def parse_salary_from_text(text: str, country: str) -> Tuple[Optional[float], Optional[float], Optional[str], Optional[str]]:
    if not text:
        return (None, None, None, None)

    m = SALARY_RANGE_RE.search(text)
    if m:
        cur_raw = (m.group("currency") or "").strip()
        interval_raw = (m.group("interval") or "").strip().lower() if m.group("interval") else None

        try:
            min_val = float((m.group("min") or "").replace(",", ""))
            max_val = float((m.group("max") or "").replace(",", ""))
        except Exception:
            return (None, None, None, None)

        interval = INTERVAL_MAP.get(interval_raw, None) if interval_raw else None
        currency = infer_currency(country, cur_raw)
        return (min_val, max_val, interval, currency)

    m2 = SALARY_SINGLE_RE.search(text)
    if m2:
        cur_raw = (m2.group("currency") or "").strip()
        interval_raw = (m2.group("interval") or "").strip().lower() if m2.group("interval") else None

        try:
            amt = float((m2.group("amount") or "").replace(",", ""))
        except Exception:
            return (None, None, None, None)

        interval = INTERVAL_MAP.get(interval_raw, None) if interval_raw else None
        currency = infer_currency(country, cur_raw)
        return (amt, amt, interval, currency)

    return (None, None, None, None)


def posted_days_ago(date_posted: Optional[str], scraped_at_iso: str) -> Optional[int]:
    if not date_posted:
        return None
    try:
        dp = datetime.fromisoformat(date_posted).date()
    except Exception:
        try:
            dp = datetime.strptime(date_posted, "%Y-%m-%d").date()
        except Exception:
            return None

    try:
        sa = datetime.fromisoformat(scraped_at_iso.replace("Z", "+00:00")).date()
    except Exception:
        sa = datetime.now(timezone.utc).date()

    return int((sa - dp).days)


def safe_sleep(cfg: Config) -> None:
    time.sleep(cfg.sleep_between_searches + random.random() * cfg.random_jitter_seconds)


# SCRAPE WRAPPER

def call_scrape_jobs(kwargs: Dict[str, Any]) -> pd.DataFrame:
    sig = inspect.signature(scrape_jobs)
    filtered = {k: v for k, v in kwargs.items() if k in sig.parameters}
    return scrape_jobs(**filtered)


def scrape_with_retries(
    cfg: Config,
    *,
    search_term: str,
    country_indeed: str,
    location: str,
) -> Tuple[pd.DataFrame, Optional[str]]:
    base_kwargs = {
        "site_name": ["indeed"],
        "search_term": f'"{search_term}"',  # exact phrase
        "location": location,
        "results_wanted": int(cfg.results_wanted),
        "hours_old": cfg.hours_old,
        "country_indeed": country_indeed,
        "linkedin_fetch_description": False,
    }

    last_err: Optional[str] = None
    for attempt in range(cfg.max_retries + 1):
        try:
            df = call_scrape_jobs(base_kwargs)
            if df is None or df.empty:
                return pd.DataFrame(), None
            return df, None
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            if attempt >= cfg.max_retries:
                break
            backoff = min(cfg.retry_max_seconds, cfg.retry_base_seconds * (2 ** attempt))
            time.sleep(backoff + random.random())

    return pd.DataFrame(), last_err


# FIELD SELECTION & ENRICHMENT

COMMON_FIELDS = {
    "site", "id", "title", "company", "location",
    "job_url", "job_url_direct",
    "description",
    "date_posted",
    "job_type",
    "min_amount", "max_amount", "interval", "currency",
    "is_remote",
}


def keep_common_fields(raw: Dict[str, Any]) -> Dict[str, Any]:
    out = {k: raw.get(k) for k in COMMON_FIELDS}
    return normalize_dict(out)


def enrich_common_fields(job: Dict[str, Any]) -> Dict[str, Any]:
    job = normalize_dict(job)

    title = job.get("title") or ""
    desc = job.get("description") or ""
    location = job.get("location") or ""
    country = job.get("search_country_indeed") or ""

    wa = infer_work_arrangement(title, location, desc)
    job["work_arrangement"] = wa

    if wa == "remote":
        job["is_remote"] = True
    elif wa in ("hybrid", "onsite"):
        job["is_remote"] = False
    else:
        job["is_remote"] = job["is_remote"] if isinstance(job.get("is_remote"), bool) else None

    job["employment_types"] = infer_employment_types(title, desc, job.get("job_type"))

    if any(job.get(k) is None for k in ("min_amount", "max_amount", "interval", "currency")):
        min_amt, max_amt, interval, currency = parse_salary_from_text(desc, country)
        job["min_amount"] = job.get("min_amount") if job.get("min_amount") is not None else min_amt
        job["max_amount"] = job.get("max_amount") if job.get("max_amount") is not None else max_amt
        job["interval"] = job.get("interval") if job.get("interval") is not None else interval
        job["currency"] = job.get("currency") if job.get("currency") is not None else currency

    parts = split_location(location)
    job["location_city"] = parts["city"]
    job["location_region"] = parts["region"]
    job["location_country_hint"] = parts["country"]

    job["scraped_at"] = job.get("scraped_at") or utc_now_iso()
    job["posted_days_ago"] = posted_days_ago(job.get("date_posted"), job["scraped_at"])

    job["dedupe_key"] = stable_dedupe_key(job)

    return normalize_dict(job)


# GROUP PROCESSING

@dataclass
class GroupStats:
    requests: int = 0
    rows_seen: int = 0
    added: int = 0
    deduped: int = 0
    dropped_company_mismatch: int = 0
    dropped_missing_anchor: int = 0
    errors: int = 0


def sort_jobs_newest_first(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def key(j: Dict[str, Any]) -> Tuple[str, str, str]:
        return (
            j.get("date_posted") or "",
            j.get("title") or "",
            j.get("job_url") or (j.get("job_url_direct") or ""),
        )
    return sorted(jobs, key=key, reverse=True)


def save_group(cfg: Config, logger: logging.Logger, company_group: str, jobs: List[Dict[str, Any]], stats: GroupStats) -> Dict[str, Any]:
    os.makedirs(cfg.output_dir, exist_ok=True)
    path = os.path.join(cfg.output_dir, f"{safe_filename(company_group)}.json")

    jobs_sorted = sort_jobs_newest_first(jobs)
    atomic_write_json(path, jobs_sorted)

    summary = {
        "company_group": company_group,
        "scraped_at": datetime.now().isoformat(),
        "total_jobs": len(jobs_sorted),
        "output_file": path,
        "stats": asdict(stats),
    }

    logger.info(f"[SAVED] {company_group}: {len(jobs_sorted)} -> {path}")
    return summary


def save_overall_summary(cfg: Config, logger: logging.Logger, group_summaries: List[Dict[str, Any]]) -> None:
    overall = {
        "scraped_at": datetime.now().isoformat(),
        "total_jobs_all_groups": sum(int(s.get("total_jobs", 0) or 0) for s in group_summaries),
        "company_groups": {s["company_group"]: s for s in group_summaries},
    }
    path = os.path.join(cfg.output_dir, "summary.json")
    atomic_write_json(path, overall)
    logger.info(f"[SAVED] summary.json -> {path}")


def save_mismatch_examples(cfg: Config, logger: logging.Logger, mismatch_examples: Dict[str, Dict[str, Any]]) -> None:
    if not mismatch_examples:
        return

    payload = {
        "scraped_at": datetime.now().isoformat(),
        "total_company_groups_with_mismatches": len(mismatch_examples),
        "mismatch_examples": mismatch_examples,
    }
    path = os.path.join(cfg.output_dir, "mismatch_examples.json")
    atomic_write_json(path, payload)
    logger.info(f"[SAVED] mismatch_examples.json -> {path}")


def scrape_company_group(
    cfg: Config,
    logger: logging.Logger,
    company_group: str,
    aliases: List[str],
    countries: List[str],
) -> Tuple[List[Dict[str, Any]], GroupStats, Optional[Dict[str, Any]]]:
    stats = GroupStats()
    seen: Set[str] = set()
    jobs: List[Dict[str, Any]] = []
    mismatch_example: Optional[Dict[str, Any]] = None
    search_terms = list(dict.fromkeys([a.strip() for a in aliases if a and a.strip()]))
    allowed_company_lc = make_allowed_company_set(search_terms)
    cap_threshold = max(1, min(int(cfg.results_wanted), 1000) - 1)

    def ingest_df(df: pd.DataFrame, *, search_term: str, country: str, search_location: str) -> None:
        nonlocal jobs, stats, seen, mismatch_example
        if df is None or df.empty:
            return

        df = df.copy()
        df["company_group"] = company_group
        df["company_search_term"] = search_term
        df["search_country_indeed"] = country
        df["search_location"] = search_location

        for _, row in df.iterrows():
            stats.rows_seen += 1
            raw = row.to_dict()

            if is_missing(raw.get("job_url")) and is_missing(raw.get("job_url_direct")) and is_missing(raw.get("id")):
                stats.dropped_missing_anchor += 1
                continue

            if cfg.enforce_exact_company_match and not company_exact_allowed(raw.get("company"), allowed_company_lc):
                stats.dropped_company_mismatch += 1

                if mismatch_example is None:
                    example = dict(raw)
                    example.pop("description", None)
                    example["company_group"] = company_group
                    example["company_search_term"] = search_term
                    example["search_country_indeed"] = country
                    example["search_location"] = search_location
                    example["allowed_company_names"] = sorted(search_terms)
                    mismatch_example = normalize_dict(example)

                continue

            kept = keep_common_fields(raw)

            kept["company_group"] = company_group
            kept["company_search_term"] = search_term
            kept["search_country_indeed"] = country
            kept["search_location"] = search_location

            enriched = enrich_common_fields(kept)

            key = enriched.get("dedupe_key")
            if key in seen:
                stats.deduped += 1
                continue
            seen.add(key)

            jobs.append(enriched)
            stats.added += 1

    logger.info(f"[START] {company_group} (aliases={len(search_terms)} countries={len(countries)})")

    for search_term in search_terms:
        for country in countries:
            stats.requests += 1
            df_country, err = scrape_with_retries(
                cfg,
                search_term=search_term,
                country_indeed=country,
                location=country,
            )
            if err:
                stats.errors += 1

            ingest_df(df_country, search_term=search_term, country=country, search_location=country)

            country_rows = 0 if (df_country is None) else int(len(df_country))
            looks_truncated = country_rows >= cap_threshold

            if looks_truncated:
                cities = INDEED_CITY_LOCATIONS.get(country, [])
                cities = list(dict.fromkeys([c.strip() for c in cities if c and c.strip()]))

                logger.info(
                    f"[CAP_DETECTED] {company_group} term='{search_term}' country='{country}' "
                    f"rows={country_rows} (>= {cap_threshold}); expanding to {len(cities)} cities"
                )

                for city in cities:
                    stats.requests += 1
                    df_city, err2 = scrape_with_retries(
                        cfg,
                        search_term=search_term,
                        country_indeed=country,
                        location=city,
                    )
                    if err2:
                        stats.errors += 1

                    ingest_df(df_city, search_term=search_term, country=country, search_location=city)

                    if stats.requests % cfg.checkpoint_every_n_requests == 0:
                        try:
                            save_group(cfg, logger, company_group, jobs, stats)
                        except Exception as e:
                            logger.error(f"[CHECKPOINT_FAIL] {company_group}: {type(e).__name__}: {e}")

                    safe_sleep(cfg)
            else:
                if stats.requests % cfg.checkpoint_every_n_requests == 0:
                    try:
                        save_group(cfg, logger, company_group, jobs, stats)
                    except Exception as e:
                        logger.error(f"[CHECKPOINT_FAIL] {company_group}: {type(e).__name__}: {e}")

                safe_sleep(cfg)

    logger.info(f"[DONE] {company_group}: jobs={len(jobs)} requests={stats.requests} errors={stats.errors}")
    return jobs, stats, mismatch_example


# MAIN

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Clean Indeed scraper (JobSpy) with common fields only.")
    p.add_argument("--output-dir", default=None, help="Output directory for JSON files.")
    p.add_argument("--results-wanted", type=int, default=None, help="Max results per search.")
    p.add_argument("--hours-old", type=int, default=None, help="Filter to postings within N hours (e.g. 168).")
    p.add_argument("--max-workers", type=int, default=None, help="Parallel company groups.")
    p.add_argument("--countries", default=None, help="Comma-separated list of countries (overrides built-in list).")
    p.add_argument("--no-exact-company-match", action="store_true", help="Disable exact company-name enforcement.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    cfg = Config(
        output_dir=args.output_dir or Config.output_dir,
        results_wanted=args.results_wanted if args.results_wanted is not None else Config.results_wanted,
        hours_old=args.hours_old if args.hours_old is not None else Config.hours_old,
        max_workers=args.max_workers if args.max_workers is not None else Config.max_workers,
        enforce_exact_company_match=(not args.no_exact_company_match),
        log_level=Config.log_level,
    )

    logger = setup_logging(cfg.log_level)
    os.makedirs(cfg.output_dir, exist_ok=True)

    countries = INDEED_COUNTRIES
    if args.countries:
        countries = [c.strip() for c in args.countries.split(",") if c.strip()]

    logger.info(
        f"Groups={len(COMPANY_GROUPS)} Countries={len(countries)} Workers={cfg.max_workers} "
        f"ResultsWanted={cfg.results_wanted} ExactCompanyMatch={cfg.enforce_exact_company_match}"
    )

    group_summaries: List[Dict[str, Any]] = []
    mismatch_examples: Dict[str, Dict[str, Any]] = {}

    with ThreadPoolExecutor(max_workers=cfg.max_workers) as ex:
        futures = {
            ex.submit(scrape_company_group, cfg, logger, cg, aliases, countries): cg
            for cg, aliases in COMPANY_GROUPS.items()
        }

        for fut in as_completed(futures):
            cg = futures[fut]
            try:
                jobs, stats, mismatch_example = fut.result()
                summary = save_group(cfg, logger, cg, jobs, stats)

                if mismatch_example is not None:
                    mismatch_examples[cg] = mismatch_example

            except Exception as e:
                logger.error(f"[FAIL] {cg}: {type(e).__name__}: {e}")
                summary = {
                    "company_group": cg,
                    "scraped_at": datetime.now().isoformat(),
                    "total_jobs": 0,
                    "output_file": None,
                    "stats": {"error": f"{type(e).__name__}: {e}"},
                }

            group_summaries.append(summary)
            save_overall_summary(cfg, logger, group_summaries)
            save_mismatch_examples(cfg, logger, mismatch_examples)

    logger.info("DONE")


if __name__ == "__main__":
    main()
