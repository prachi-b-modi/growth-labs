#!/usr/bin/env python3
"""
Select 100 Mixpanel users who did `Page Viewed` within a date range and
push them to a Dispatcher endpoint (no cohorts). Optionally set a profile flag.

Usage (US project):
  python3 select_target_100.py --region US --project-id 1234567

Optional flags:
  --strategy random            # pick a random 100 (default: top by activity)
  --count 50                   # pick a different size
  --no-push                    # don’t call the dispatcher (dry run / profiles only)
  --set-profiles               # also set target_100=true via Engage (off by default)
  --dispatcher-url http://localhost:8000
  --dispatcher-secret your_shared_secret
"""

from __future__ import annotations
import sys, json, time, random, argparse
from collections import Counter
from typing import Iterable, Dict, Any, List, Optional

import requests

# ----------------------
# Inline configuration
# ----------------------

# Mixpanel credentials (user supplied)
MIXPANEL_PROJECT_TOKEN = "1509fe11525d27d2a4d3427abfd88af4"
MIXPANEL_SERVICE_USER  = "Growthlab.32b28e.mp-service-account"
MIXPANEL_SERVICE_SECRET= "UHMvkL8RhwSVRm2ItzKokHdvvpqw4bSy"

# REQUIRED: numeric project id (find in Mixpanel: Project Settings → Overview)
# You can also pass --project-id 1234567 on the CLI to override this constant.
MIXPANEL_PROJECT_ID = 3808848  # <-- TODO: set this to your numeric Project ID (int)

# Defaults (override via CLI)
DEFAULT_REGION = "US"                    # or "EU"
DEFAULT_FROM   = "2025-07-01"
DEFAULT_TO     = "2025-07-26"
DEFAULT_COUNT  = 100
DEFAULT_STRAT  = "top"                   # or "random"

# Dispatcher (your FastAPI service from earlier)
DEFAULT_DISPATCHER_URL    = "http://localhost:8000"
DEFAULT_DISPATCHER_SECRET = "I3gp3MFUpDcWRcjvIMdRF-gALZvtzv5smRZR0uugu8w"

# ----------------------
# API endpoints
# ----------------------
US_EXPORT = "https://data.mixpanel.com/api/2.0/export"
EU_EXPORT = "https://data-eu.mixpanel.com/api/2.0/export"
ENGAGE    = "https://api.mixpanel.com/engage"   # Profiles (People) API


def export_events(from_date: str, to_date: str, service_user: str, service_secret: str,
                  event: str = "Page Viewed", where: Optional[str] = None,
                  region: str = "US", project_id: Optional[int] = None) -> Iterable[Dict[str, Any]]:
    """
    Stream events NDJSON from Mixpanel Raw Export API (time-bounded).
    NOTE: When authenticating with a SERVICE ACCOUNT you MUST pass project_id (numeric).
    """
    if not project_id:
        raise SystemExit("ERROR: numeric project_id is required for Export API with service accounts.")
    base = US_EXPORT if region.upper() == "US" else EU_EXPORT
    params = {
        "from_date": from_date,
        "to_date": to_date,
        "event": json.dumps([event]),
        "project_id": int(project_id),
    }
    if where:
        params["where"] = where

    with requests.get(base, params=params, auth=(service_user, service_secret),
                      stream=True, timeout=120) as r:
        if r.status_code != 200:
            raise SystemExit(f"Export error {r.status_code}: {r.text[:500]}")
        for line in r.iter_lines():
            if line:
                yield json.loads(line)


def choose_target_ids(events_iter: Iterable[Dict[str, Any]],
                      target_count: int = 100,
                      strategy: str = "top") -> List[str]:
    """
    Return list of distinct_ids (len up to target_count) based on strategy.
    """
    counts: Counter[str] = Counter()
    for ev in events_iter:
        props = ev.get("properties", {})
        did = props.get("distinct_id")
        if not did:
            continue
        counts[did] += 1

    candidates = list(counts.keys())
    if not candidates:
        raise SystemExit("No candidates found in the given date window (event name / window may be empty).")

    if len(candidates) <= target_count:
        print(f"Only {len(candidates)} candidates found; selecting all.", file=sys.stderr)
        return candidates

    if strategy == "random":
        random.shuffle(candidates)
        return candidates[:target_count]
    else:
        return [did for did, _ in counts.most_common(target_count)]


def chunks(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def profile_set_batch(project_token: str, distinct_ids: List[str], window_label: str) -> str:
    """
    Batch $set via Engage API. Mixpanel expects application/x-www-form-urlencoded with 'data' param.
    """
    payload = []
    now = int(time.time())
    for did in distinct_ids:
        payload.append({
            "$token": project_token,
            "$distinct_id": did,
            "$ignore_time": True,
            "$ip": 0,
            "$set": {"target_100": True, "target_window": window_label},
            "$time": now
        })
    resp = requests.post(
        ENGAGE,
        data={"data": json.dumps(payload)},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=60,
    )
    if resp.status_code != 200:
        raise SystemExit(f"Engage error {resp.status_code}: {resp.text[:400]}")
    return resp.text


def push_targets(distinct_ids, window, dispatcher_url, dispatcher_secret):
    payload = {
        "window": window,
        "insert": [{"distinct_id": d} for d in distinct_ids],
        "remove": []
    }
    r = requests.post(
        f"{dispatcher_url.rstrip('/')}/targets/sync",
        headers={"X-Dispatcher-Secret": dispatcher_secret},
        json=payload, timeout=60
    )
    r.raise_for_status()
    print("Sync:", r.status_code, r.text)

def push_targets_to_dispatcher(distinct_ids, window_label, dispatcher_url, dispatcher_secret, mode="replace"):
    """
    MCP dispatcher version: POST /targets/sync with header X-Dispatcher-Secret.
    We ignore `mode` here (kept for compatibility with existing call sites).
    """
    payload = {
        "window": window_label,
        "insert": [{"distinct_id": d} for d in distinct_ids],
        "remove": []   # add items here if you want to de-target some users
    }
    r = requests.post(
        f"{dispatcher_url.rstrip('/')}/targets/sync",
        headers={"X-Dispatcher-Secret": dispatcher_secret, "Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )
    r.raise_for_status()
    print("Sync:", r.status_code, r.text)


def main():
    ap = argparse.ArgumentParser(description="Select N users from Mixpanel and push to Dispatcher (no cohorts).")
    ap.add_argument("--region", default=DEFAULT_REGION, choices=["US", "EU"])
    ap.add_argument("--project-id", type=int, default=MIXPANEL_PROJECT_ID,
                    help="Numeric Mixpanel Project ID (overrides inline constant).")
    ap.add_argument("--from-date", default=DEFAULT_FROM)
    ap.add_argument("--to-date", default=DEFAULT_TO)
    ap.add_argument("--event", default="Page Viewed")
    ap.add_argument("--count", type=int, default=DEFAULT_COUNT)
    ap.add_argument("--strategy", choices=["top", "random"], default=DEFAULT_STRAT)
    ap.add_argument("--dispatcher-url", default=DEFAULT_DISPATCHER_URL)
    ap.add_argument("--dispatcher-secret", default=DEFAULT_DISPATCHER_SECRET)
    ap.add_argument("--no-push", action="store_true", help="Do not call the dispatcher (dry run / profiles only).")
    ap.add_argument("--set-profiles", action="store_true", help="Also set target_100=true on profiles (optional).")
    args = ap.parse_args()

    if not args.project_id or int(args.project_id) <= 0:
        raise SystemExit("Please set MIXPANEL_PROJECT_ID in the file (numeric) or pass --project-id 1234567.")

    window_label = f"{args.from_date}..{args.to_date}"

    print(f"[1/3] Exporting '{args.event}' from {args.from_date} to {args.to_date} …", file=sys.stderr)
    ev_iter = export_events(args.from_date, args.to_date,
                            MIXPANEL_SERVICE_USER, MIXPANEL_SERVICE_SECRET,
                            event=args.event, region=args.region, project_id=int(args.project_id))

    print(f"[2/3] Choosing {args.count} users by strategy={args.strategy} …", file=sys.stderr)
    target_ids = choose_target_ids(ev_iter, target_count=args.count, strategy=args.strategy)
    print(f"Selected {len(target_ids)} users.", file=sys.stderr)

    if args.set_profiles:
        print(f"[opt] Setting profiles (target_100=true, target_window={window_label}) …", file=sys.stderr)
        for batch in chunks(target_ids, 50):
            res = profile_set_batch(MIXPANEL_PROJECT_TOKEN, batch, window_label)
            print(res)

    if not args.no_push:
        print(f"[3/3] Pushing {len(target_ids)} users to Dispatcher at {args.dispatcher_url} …", file=sys.stderr)
        push_targets(target_ids, WINDOW_LABEL, args.dispatcher_url, args.dispatcher_secret)
    else:
        print("[skip] --no-push set: not calling Dispatcher.", file=sys.stderr)

    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
