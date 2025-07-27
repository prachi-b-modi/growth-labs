import requests
import json

def fetch_snapshot_results(snapshot_id):
    url = "https://api.brightdata.com/datasets/v3/trigger/result"
    headers = {
        "Authorization": "Bearer 4561152efcf7312d5da5ff4669b15cb439866b4a8e0e24a429e93032403261d1",
        "Content-Type": "application/json",
    }
    params = {
        "snapshot_id": snapshot_id,
        "format": "json"
    }
    
    response = requests.get(url, headers=headers, params=params)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"Results: {json.dumps(result, indent=2)}")
    else:
        print(f"Error: {response.text}")

# Try both snapshots
fetch_snapshot_results("s_mdkveuky13x61cbsku")
print("\n" + "="*50 + "\n")
fetch_snapshot_results("s_mdkveuvl1augvhzi6b") 