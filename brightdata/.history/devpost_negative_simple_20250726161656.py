import requests
import json

def create_negative_snapshots():
    """Create 2 snapshots for negative Devpost feedback."""
    
    api_token = "4561152efcf7312d5da5ff4669b15cb439866b4a8e0e24a429e93032403261d1"
    dataset_id = "gd_lvz8ah06191smkebj4"
    
    # Just 2 focused queries for negative feedback
    negative_queries = [
        "Devpost complaints problems issues",
        "site:reddit.com Devpost sucks bad experience"
    ]
    
    url = "https://api.brightdata.com/datasets/v3/trigger"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    
    snapshots = []
    
    for query in negative_queries:
        params = {
            "dataset_id": dataset_id,
            "include_errors": "true",
            "type": "discover_new",
            "discover_by": "keyword",
        }
        
        data = [
            {
                "keyword": query,
                "date": "Past year",
                "sort_by": "Relevance",
                "num_of_posts": 50
            }
        ]
        
        try:
            print(f"Creating snapshot for: {query}")
            response = requests.post(url, headers=headers, params=params, json=data, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                snapshot_id = result.get("snapshot_id")
                if snapshot_id:
                    snapshots.append({
                        "query": query,
                        "snapshot_id": snapshot_id
                    })
                    print(f"  ✅ Created snapshot: {snapshot_id}")
            else:
                print(f"  ❌ Error: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"  ❌ Error creating snapshot: {e}")
    
    print(f"\n🎉 Created {len(snapshots)} snapshots for negative feedback research")
    print("\nSnapshot IDs:")
    for item in snapshots:
        print(f"  �� {item['query']}: {item['snapshot_id']}")
    
    return snapshots

if __name__ == "__main__":
    print("=== Devpost Negative Feedback Research (Simple) ===")
    print("Creating 2 snapshots for negative feedback search...\n")
    
    snapshots = create_negative_snapshots()
    
    print(f"\n✅ Done! Created {len(snapshots)} snapshots.")
    print("📊 These snapshots will gather negative feedback about Devpost.")
    print("⏰ They'll be ready in 15-30 minutes.")
    print("�� You can check them later to see what people are complaining about.") 