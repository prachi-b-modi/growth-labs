import requests
import json
import time

def fetch_snapshot_results(snapshot_id):
    """Try different possible Bright Data API endpoints for fetching results."""
    
    api_token = "4561152efcf7312d5da5ff4669b15cb439866b4a8e0e24a429e93032403261d1"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    
    # Try different possible endpoints
    endpoints_to_try = [
        f"https://api.brightdata.com/datasets/v3/trigger/{snapshot_id}",
        f"https://api.brightdata.com/datasets/v3/trigger/result/{snapshot_id}",
        f"https://api.brightdata.com/datasets/v3/trigger/result?snapshot_id={snapshot_id}",
        f"https://api.brightdata.com/datasets/v3/trigger/status/{snapshot_id}",
        f"https://api.brightdata.com/datasets/v3/trigger/{snapshot_id}/result",
    ]
    
    for i, endpoint in enumerate(endpoints_to_try, 1):
        print(f"\nTrying endpoint {i}: {endpoint}")
        
        try:
            response = requests.get(endpoint, headers=headers, timeout=30)
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Success! Response keys: {list(result.keys())}")
                print(f"Response preview: {json.dumps(result, indent=2)[:500]}...")
                return result
            elif response.status_code == 404:
                print("❌ 404 - Not found")
            else:
                print(f"❌ Error: {response.text[:200]}...")
                
        except Exception as e:
            print(f"❌ Exception: {e}")
    
    print(f"\n❌ All endpoints failed for snapshot {snapshot_id}")
    return None

def check_snapshot_status(snapshot_id):
    """Check if snapshot is ready by trying to fetch it."""
    print(f"\n🔍 Checking status for snapshot: {snapshot_id}")
    
    result = fetch_snapshot_results(snapshot_id)
    
    if result:
        print(f"✅ Snapshot {snapshot_id} is ready!")
        return True
    else:
        print(f"⏳ Snapshot {snapshot_id} is still processing...")
        return False

if __name__ == "__main__":
    print("=== Checking Bright Data Snapshots ===")
    
    # Your snapshot IDs
    snapshots = [
        "s_mdkveuky13x61cbsku",  # Devpost complaints problems issues
        "s_mdkveuvl1augvhzi6b"   # site:reddit.com Devpost sucks bad experience
    ]
    
    for snapshot_id in snapshots:
        check_snapshot_status(snapshot_id)
        print("-" * 60)
    
    print("\n💡 If all snapshots are still processing, try again in 10-15 minutes.")
    print("�� The snapshots will be ready when they return actual data instead of 404 errors.") 