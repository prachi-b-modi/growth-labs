import os, sys, json, time, base64, random, argparse
from collections import Counter
from datetime import datetime
import requests

US_EXPORT = "https://data.mixpanel.com/api/2.0/export"
EU_EXPORT = "https://data-eu.mixpanel.com/api/2.0/export"
ENGAGE = "https://api.mixpanel.com/engage"  # People (Profiles) API

def env(name, default=None, required=False):
    v = os.getenv(name, default)
    if required and not v:
        print(f"Missing env: {name}", file=sys.stderr); sys.exit(2)
    return v

def export_events(from_date, to_date, service_user, service_secret, event="Page Viewed", where=None, region="US"):
    """Stream events NDJSON from Mixpanel Export API (time-bounded)."""
    url = US_EXPORT if region.upper()=="US" else EU_EXPORT
    params = {
        "from_date": from_date,
        "to_date": to_date,
        "event": json.dumps([event]),
    }
    if where:
        params["where"] = where
    auth = (service_user, service_secret)
    with requests.get(url, params=params, auth=auth, stream=True, timeout=60) as r:
        r.raise_for_status()
        for line in r.iter_lines():
            if not line: continue
            yield json.loads(line)

def choose_target_ids(events_iter, target_count=100, strategy="top"):
    """Return list of distinct_ids (len == target_count) based on strategy."""
    counts = Counter()
    seen = set()
    for ev in events_iter:
        # Export API returns distinct_id under properties.distinct_id
        props = ev.get("properties", {})
        did = props.get("distinct_id")
        if not did: 
            continue
        counts[did] += 1
        seen.add(did)
    candidates = list(seen)
    if len(candidates) < target_count:
        print(f"Only {len(candidates)} candidates found; selecting all.", file=sys.stderr)
        return candidates

    if strategy == "random":
        random.shuffle(candidates)
        return candidates[:target_count]
    else:
        # top by activity
        return [did for did, _ in counts.most_common(target_count)]

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def profile_set_batch(project_token, distinct_ids, extra=None):
    """Send a batch $set to Profiles via Engage API."""
    payload = []
    now = int(time.time())
    for did in distinct_ids:
        body = {
            "$token": project_token,
            "$distinct_id": did,
            "$ignore_time": True,
            "$ip": 0,
            "$set": {"target_100": True, "target_window": extra or ""},
            "$time": now
        }
        payload.append(body)
    # Engage expects application/x-www-form-urlencoded with "data" param (JSON array)
    resp = requests.post(
        ENGAGE,
        data={"data": json.dumps(payload)},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.text

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="US", choices=["US","EU"])
    args = parser.parse_args()

    token = env("MIXPANEL_PROJECT_TOKEN", required=True)
    su = env("MIXPANEL_SERVICE_USER", required=True)
    ss = env("MIXPANEL_SERVICE_SECRET", required=True)
    from_date = env("FROM_DATE", required=True)
    to_date = env("TO_DATE", required=True)
    target_count = int(env("TARGET_COUNT", "100"))
    strategy = env("SELECTION_STRATEGY", "top")
    window_label = f"{from_date}..{to_date}"

    print(f"[1/3] Exporting 'Page Viewed' from {from_date} to {to_date} …", file=sys.stderr)
    ev_iter = export_events(from_date, to_date, su, ss, region=args.region)

    print(f"[2/3] Choosing {target_count} users by strategy={strategy} …", file=sys.stderr)
    target_ids = choose_target_ids(ev_iter, target_count=target_count, strategy=strategy)
    print(f"Selected {len(target_ids)} users.", file=sys.stderr)

    print(f"[3/3] Flagging profiles in Mixpanel (target_100=true) …", file=sys.stderr)
    for batch in chunks(target_ids, 50):
        res = profile_set_batch(token, batch, extra=window_label)
        # Mixpanel returns '1' or a small JSON; print for visibility
        print(res)

    # (Optional) clear previous flags for non-selected users:
    #   - Query your last target cohort via Cohort Members API (if enabled) or store last run locally,
    #   - send $unset for those no longer in target.
    print("Done.", file=sys.stderr)

if __name__ == "__main__":
    main()
