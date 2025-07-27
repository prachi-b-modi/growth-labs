
import sys, json, time, random, argparse
from collections import Counter
from typing import Iterable, Dict, Any, List
import requests

# ----------------------
# Inline configuration
# ----------------------

# Mixpanel credentials
MIXPANEL_PROJECT_TOKEN = "1509fe11525d27d2a4d3427abfd88af4"
MIXPANEL_SERVICE_USER  = "Growthlab.32b28e.mp-service-account"
MIXPANEL_SERVICE_SECRET= "UHMvkL8RhwSVRm2ItzKokHdvvpqw4bSy"

# REQUIRED: numeric project id (find in Mixpanel: Project Settings → Overview)
# You can also pass --project-id 1234567 on the CLI to override this constant.
MIXPANEL_PROJECT_ID = 3808848  # <-- TODO: set this to your numeric Project ID

# Region: "US" or "EU" (EU uses different export host)
REGION = "US"

# Time window
FROM_DATE = "2025-07-01"
TO_DATE   = "2025-07-26"

# Selection
TARGET_COUNT = 100
SELECTION_STRATEGY = "top"   # or "random"

# ----------------------
# End inline config
# ----------------------

US_EXPORT = "https://data.mixpanel.com/api/2.0/export"
EU_EXPORT = "https://data-eu.mixpanel.com/api/2.0/export"
ENGAGE = "https://api.mixpanel.com/engage"  # Profiles (People) API

def export_events(from_date: str, to_date: str, service_user: str, service_secret: str,
                  event: str = "Page Viewed", where: str | None = None,
                  region: str = "US", project_id: int | None = None) -> Iterable[Dict[str, Any]]:
    """Stream events NDJSON from Mixpanel Export API (time-bounded)."""
    base = US_EXPORT if region.upper() == "US" else EU_EXPORT
    params = {
        "from_date": from_date,
        "to_date": to_date,
        "event": json.dumps([event]),
    }
    if where:
        params["where"] = where
    if project_id:
        params["project_id"] = int(project_id)
    else:
        raise SystemExit("ERROR: MIXPANEL_PROJECT_ID (numeric) is required for service-account export.")
    with requests.get(base, params=params, auth=(service_user, service_secret), stream=True, timeout=90) as r:
        if r.status_code != 200:
            raise SystemExit(f"Export error {r.status_code}: {r.text[:400]}")
        for line in r.iter_lines():
            if line:
                yield json.loads(line)

def choose_target_ids(events_iter: Iterable[Dict[str, Any]], target_count: int = 100, strategy: str = "top") -> List[str]:
    """Return list of distinct_ids (len == target_count) based on strategy."""
    counts: Counter[str] = Counter()
    for ev in events_iter:
        props = ev.get("properties", {})
        did = props.get("distinct_id")
        if not did:
            continue
        counts[did] += 1

    candidates = list(counts.keys())
    if len(candidates) == 0:
        raise SystemExit("No candidates found in the given date window. Check event name/date range.")
    if len(candidates) < target_count:
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

def profile_set_batch(project_token: str, distinct_ids: List[str], window_label: str):
    """Send a batch $set to Profiles via Engage API."""
    payload = []
    now = int(time.time())
    for did in distinct_ids:
        body = {
            "$token": project_token,
            "$distinct_id": did,
            "$ignore_time": True,
            "$ip": 0,
            "$set": {"target_100": True, "target_window": window_label},
            "$time": now
        }
        payload.append(body)
    resp = requests.post(
        ENGAGE,
        data={"data": json.dumps(payload)},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=60,
    )
    if resp.status_code != 200:
        raise SystemExit(f"Engage error {resp.status_code}: {resp.text[:400]}")
    return resp.text

def main():
    parser = argparse.ArgumentParser(description="Select 100 target users and flag profiles in Mixpanel.")
    parser.add_argument("--region", default=REGION, choices=["US","EU"])
    parser.add_argument("--project-id", type=int, help="Numeric Mixpanel Project ID (overrides inline constant).")
    parser.add_argument("--strategy", default=SELECTION_STRATEGY, choices=["top","random"])
    parser.add_argument("--from-date", default=FROM_DATE)
    parser.add_argument("--to-date", default=TO_DATE)
    parser.add_argument("--count", type=int, default=TARGET_COUNT)
    parser.add_argument("--event", default="Page Viewed")
    args = parser.parse_args()

    project_id = args.project_id if args.project_id else MIXPANEL_PROJECT_ID
    if not project_id or int(project_id) <= 0:
        raise SystemExit("Please set MIXPANEL_PROJECT_ID in the file (numeric) or pass --project-id 1234567")

    window_label = f"{args.from_date}..{args.to_date}"
    print(f"[1/3] Exporting '{args.event}' from {args.from_date} to {args.to_date} …", file=sys.stderr)
    ev_iter = export_events(args.from_date, args.to_date,
                            MIXPANEL_SERVICE_USER, MIXPANEL_SERVICE_SECRET,
                            event=args.event, region=args.region, project_id=int(project_id))

    print(f"[2/3] Choosing {args.count} users by strategy={args.strategy} …", file=sys.stderr)
    target_ids = choose_target_ids(ev_iter, target_count=args.count, strategy=args.strategy)
    print(f"Selected {len(target_ids)} users.", file=sys.stderr)

    print(f"[3/3] Flagging profiles in Mixpanel (target_100=true) …", file=sys.stderr)
    for batch in chunks(target_ids, 50):
        res = profile_set_batch(MIXPANEL_PROJECT_TOKEN, batch, window_label)
        print(res)  # Mixpanel usually returns '1' per request when accepted

    print("Done.", file=sys.stderr)

if __name__ == "__main__":
    main()

