#!/usr/bin/env python3
"""
select_target_100.py
--------------------
Export Mixpanel events, select a target set of users, and push them to the MCP dispatcher.

Usage:
  python3 select_target_100.py \
    --region US \
    --project-id 3808848 \
    --dispatcher-url http://localhost:8000 \
    --dispatcher-secret 'YOUR_DISPATCHER_SECRET'

Options:
  --from-date 2025-07-01
  --to-date   2025-07-26
  --event "Page Viewed"
  --count 100
  --strategy top|random
"""

from __future__ import annotations
import sys
import json
import argparse
import os
from typing import Iterable, Dict, Any, List, Optional
from collections import Counter

import requests

# ----------------------
# Inline configuration
# ----------------------

# Mixpanel credentials (Service Account for API access)
# TIP: for security, prefer exporting these as env vars and reading via os.getenv.
MIXPANEL_SERVICE_USER   = os.getenv("MIXPANEL_SERVICE_USER", "growthlab-mp-service-account")
MIXPANEL_SERVICE_SECRET = os.getenv("MIXPANEL_SERVICE_SECRET", "UHMvkL8RhwSVRm2ItzKokHdvvpqw4bSy")

# Numeric project ID is REQUIRED when authenticating with a service account
# You can override this with --project-id on the CLI
MIXPANEL_PROJECT_ID = 3808848  # <-- set your numeric project id or pass --project-id 1234567

# Defaults (override via CLI)
DEFAULT_REGION    = "US"               # or "EU"
DEFAULT_FROM_DATE = "2025-07-01"
DEFAULT_TO_DATE   = "2025-07-26"
DEFAULT_EVENT     = "Page Viewed"
DEFAULT_COUNT     = 100
DEFAULT_STRATEGY  = "top"              # or "random"

# MCP Dispatcher defaults
DEFAULT_DISPATCHER_URL    = "http://localhost:8000"
DEFAULT_DISPATCHER_SECRET = "changeme"

# ----------------------
# Mixpanel Export API endpoints
# ----------------------
US_EXPORT = "https://data.mixpanel.com/api/2.0/export"
EU_EXPORT = "https://data-eu.mixpanel.com/api/2.0/export"


def export_events(
    from_date: str,
    to_date: str,
    service_user: str,
    service_secret: str,
    event: str = DEFAULT_EVENT,
    where: Optional[str] = None,
    region: str = DEFAULT_REGION,
    project_id: Optional[int] = None,
) -> Iterable[Dict[str, Any]]:
    """
    Stream events NDJSON from Mixpanel Raw Export API.
    Service-account auth requires numeric project_id.
    """
    if not project_id or int(project_id) <= 0:
        raise SystemExit("ERROR: numeric --project-id is required with service account auth.")

    base = US_EXPORT if region.upper() == "US" else EU_EXPORT
    params = {
        "from_date": from_date,
        "to_date": to_date,
        "event": json.dumps([event]),
        "project_id": int(project_id),
    }
    if where:
        params["where"] = where

    with requests.get(base, params=params, auth=(service_user, service_secret), stream=True, timeout=120) as r:
        if r.status_code != 200:
            # Show a helpful error snippet
            snippet = r.text[:500] if r.text else str(r.status_code)
            raise SystemExit(f"Export API error {r.status_code}: {snippet}")
        for line in r.iter_lines():
            if line:
                yield json.loads(line)


def choose_target_ids(
    events_iter: Iterable[Dict[str, Any]],
    target_count: int = DEFAULT_COUNT,
    strategy: str = DEFAULT_STRATEGY,
) -> List[str]:
    """
    Build a frequency map of distinct_id and select target_count users.
    """
    counts: Counter[str] = Counter()
    for ev in events_iter:
        props = ev.get("properties", {})
        did = props.get("distinct_id")
        if did:
            counts[did] += 1

    if not counts:
        raise SystemExit("No events found. Check event name and date window.")

    candidates = list(counts.keys())

    if len(candidates) <= target_count:
        print(f"Only {len(candidates)} candidates found; selecting all.", file=sys.stderr)
        return candidates

    if strategy == "random":
        import random
        random.shuffle(candidates)
        return candidates[:target_count]
    else:
        # top by activity
        return [did for did, _ in counts.most_common(target_count)]


def push_targets(
    distinct_ids: List[str],
    window_label: str,
    dispatcher_url: str,
    dispatcher_secret: str,
) -> None:
    """
    Push target users to the MCP dispatcher:
      POST /targets/sync
      Header: X-Dispatcher-Secret
      Body: { window, insert: [{distinct_id}], remove: [] }
    """
    url = f"{dispatcher_url.rstrip('/')}/targets/sync"
    payload = {
        "window": window_label,
        "insert": [{"distinct_id": d} for d in distinct_ids],
        "remove": []
    }
    try:
        r = requests.post(
            url,
            headers={
                "X-Dispatcher-Secret": dispatcher_secret,
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60,
        )
        r.raise_for_status()
    except requests.HTTPError as e:
        # Surface server’s error body to help debugging
        body = e.response.text[:400] if e.response is not None else str(e)
        raise SystemExit(f"Dispatcher error {e.response.status_code if e.response else ''}: {body}")
    except Exception as e:
        raise SystemExit(f"Dispatcher request failed: {e}")
    else:
        print(f"Sync: {r.status_code} {r.text}")


def main():
    ap = argparse.ArgumentParser(description="Select N Mixpanel users and push to MCP dispatcher.")
    ap.add_argument("--region", default=DEFAULT_REGION, choices=["US", "EU"])
    ap.add_argument("--project-id", type=int, default=MIXPANEL_PROJECT_ID, help="Numeric Mixpanel Project ID.")
    ap.add_argument("--from-date", default=DEFAULT_FROM_DATE)
    ap.add_argument("--to-date", default=DEFAULT_TO_DATE)
    ap.add_argument("--event", default=DEFAULT_EVENT)
    ap.add_argument("--count", type=int, default=DEFAULT_COUNT)
    ap.add_argument("--strategy", choices=["top", "random"], default=DEFAULT_STRATEGY)
    ap.add_argument("--dispatcher-url", default=DEFAULT_DISPATCHER_URL)
    ap.add_argument("--dispatcher-secret", default=DEFAULT_DISPATCHER_SECRET)
    args = ap.parse_args()

    if not args.project_id or int(args.project_id) <= 0:
        raise SystemExit("Please pass --project-id <NUMERIC_ID> for service account auth.")

    window_label = f"{args.from_date}..{args.to_date}"

    print(f"[1/3] Exporting '{args.event}' from {args.from_date} to {args.to_date} …", file=sys.stderr)
    ev_iter = export_events(
        from_date=args.from_date,
        to_date=args.to_date,
        service_user=MIXPANEL_SERVICE_USER,
        service_secret=MIXPANEL_SERVICE_SECRET,
        event=args.event,
        region=args.region,
        project_id=args.project_id,
    )

    print(f"[2/3] Choosing {args.count} users by strategy={args.strategy} …", file=sys.stderr)
    target_ids = choose_target_ids(ev_iter, target_count=args.count, strategy=args.strategy)
    print(f"Selected {len(target_ids)} users.", file=sys.stderr)

    print(f"[3/3] Pushing {len(target_ids)} users to Dispatcher at {args.dispatcher_url} …", file=sys.stderr)
    push_targets(
        distinct_ids=target_ids,
        window_label=window_label,
        dispatcher_url=args.dispatcher_url,
        dispatcher_secret=args.dispatcher_secret,
    )

    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
