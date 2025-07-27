import requests
import json

def download_snapshot_results(snapshot_id):
    """Download results from a ready snapshot using the correct endpoint."""
    
    api_token = "4561152efcf7312d5da5ff4669b15cb439866b4a8e0e24a429e93032403261d1"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    
    # Try different possible download endpoints
    endpoints_to_try = [
        f"https://api.brightdata.com/datasets/v3/trigger/{snapshot_id}/download",
        f"https://api.brightdata.com/datasets/v3/trigger/download/{snapshot_id}",
        f"https://api.brightdata.com/datasets/v3/trigger/{snapshot_id}/data",
        f"https://api.brightdata.com/datasets/v3/trigger/{snapshot_id}/json",
        f"https://api.brightdata.com/datasets/v3/trigger/{snapshot_id}/results",
    ]
    
    for i, endpoint in enumerate(endpoints_to_try, 1):
        print(f"  Trying endpoint {i}: {endpoint}")
        
        try:
            response = requests.get(endpoint, headers=headers, timeout=30)
            print(f"    Status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    result = response.json()
                    print(f"    ✅ Success! Found data")
                    return result
                except:
                    print(f"    ✅ Success! (Not JSON)")
                    return response.text
            elif response.status_code == 404:
                print(f"    ❌ 404 - Not found")
            else:
                print(f"    ❌ Error: {response.text[:100]}...")
                
        except Exception as e:
            print(f"    ❌ Exception: {e}")
    
    return None

def fetch_ready_snapshots():
    """Fetch results from the ready snapshots."""
    
    # Your ready snapshots from the dashboard
    snapshots = [
        {
            "id": "s_mdkveuvl1augvhzi",
            "query": "site:reddit.com Devpost sucks bad experience"
        },
        {
            "id": "s_mdkveuky13x61ct", 
            "query": "Devpost complaints problems issues"
        }
    ]
    
    all_results = []
    
    for snapshot in snapshots:
        snapshot_id = snapshot["id"]
        query = snapshot["query"]
        
        print(f"\n🔍 Downloading results for: {query}")
        print(f"   Snapshot ID: {snapshot_id}")
        
        result = download_snapshot_results(snapshot_id)
        
        if result:
            print(f"   ✅ Downloaded successfully!")
            all_results.append({
                "snapshot_id": snapshot_id,
                "query": query,
                "data": result
            })
        else:
            print(f"   ❌ Failed to download")
    
    return all_results

if __name__ == "__main__":
    print("=== Downloading Ready Devpost Negative Feedback ===")
    
    results = fetch_ready_snapshots()
    
    if results:
        # Save raw results
        with open("devpost_raw_results.json", "w") as f:
            json.dump(results, f, indent=2)
        
        print(f"\n📁 Raw results saved to: devpost_raw_results.json")
        
        # Try to extract and analyze the data
        print(f"\n📊 Analyzing results...")
        
        for result in results:
            print(f"\nSnapshot: {result['snapshot_id']}")
            print(f"Query: {result['query']}")
            
            data = result['data']
            if isinstance(data, dict):
                print(f"Keys: {list(data.keys())}")
                if 'data' in data:
                    print(f"Data items: {len(data['data'])}")
            elif isinstance(data, list):
                print(f"List items: {len(data)}")
            else:
                print(f"Data type: {type(data)}")
                print(f"Data preview: {str(data)[:200]}...")
    else:
        print(f"\n❌ No results downloaded")
        print(f"💡 You may need to use the Bright Data dashboard to download the results manually") 