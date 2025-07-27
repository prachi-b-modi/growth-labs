import requests
import json
import time

def search_devpost_negative_feedback():
    """Direct search for negative Devpost feedback using Bright Data API."""
    
    api_token = "4561152efcf7312d5da5ff4669b15cb439866b4a8e0e24a429e93032403261d1"
    dataset_id = "gd_lvz8ah06191smkebj4"
    
    # Search queries specifically for negative feedback
    negative_queries = [
        "Devpost complaints",
        "Devpost problems",
        "Devpost issues",
        "Devpost sucks",
        "Devpost bad experience",
        "Devpost hackathon problems",
        "Devpost disqualified",
        "Devpost unfair",
        "Devpost broken",
        "Devpost scam",
        "site:reddit.com Devpost complaints",
        "site:reddit.com Devpost problems",
        "Devpost hackathon rules unfair",
        "Devpost prize not received",
        "Devpost support bad"
    ]
    
    url = "https://api.brightdata.com/datasets/v3/trigger"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    
    all_snapshots = []
    
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
                "num_of_posts": 20
            }
        ]
        
        try:
            print(f"Creating snapshot for: {query}")
            response = requests.post(url, headers=headers, params=params, json=data, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                snapshot_id = result.get("snapshot_id")
                if snapshot_id:
                    all_snapshots.append({
                        "query": query,
                        "snapshot_id": snapshot_id
                    })
                    print(f"  Created snapshot: {snapshot_id}")
            else:
                print(f"  Error: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"  Error creating snapshot: {e}")
        
        time.sleep(2)  # Small delay between requests
    
    print(f"\nCreated {len(all_snapshots)} snapshots for negative feedback research")
    print("Snapshot IDs:")
    for item in all_snapshots:
        print(f"  {item['query']}: {item['snapshot_id']}")
    
    return all_snapshots

def fetch_negative_results(snapshots, wait_minutes=30):
    """Fetch results from snapshots after they're ready."""
    
    api_token = "4561152efcf7312d5da5ff4669b15cb439866b4a8e0e24a429e93032403261d1"
    results_url = "https://api.brightdata.com/datasets/v3/trigger/result"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    
    print(f"\nWaiting {wait_minutes} minutes for snapshots to be ready...")
    time.sleep(wait_minutes * 60)
    
    negative_feedback = []
    
    for snapshot in snapshots:
        snapshot_id = snapshot["snapshot_id"]
        query = snapshot["query"]
        
        print(f"\nFetching results for: {query}")
        
        params = {
            "snapshot_id": snapshot_id,
            "format": "json"
        }
        
        try:
            response = requests.get(results_url, headers=headers, params=params, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                
                # Extract data
                data_items = []
                if "data" in result:
                    data_items = result["data"]
                elif "results" in result:
                    data_items = result["results"]
                elif isinstance(result, list):
                    data_items = result
                
                print(f"  Found {len(data_items)} results")
                
                for item in data_items:
                    if isinstance(item, dict):
                        text = item.get("text", "") or item.get("snippet", "")
                        if text:
                            # Simple negative sentiment detection
                            negative_words = ["bad", "terrible", "awful", "horrible", "sucks", "problem", "issue", "complaint", "unfair", "broken", "scam", "disqualified", "unfair"]
                            if any(word in text.lower() for word in negative_words):
                                negative_feedback.append({
                                    "query": query,
                                    "url": item.get("url", ""),
                                    "title": item.get("title", ""),
                                    "text": text[:500],  # First 500 chars
                                    "source": "reddit" if "reddit.com" in item.get("url", "") else "other"
                                })
            else:
                print(f"  Error fetching: {response.status_code}")
                
        except Exception as e:
            print(f"  Error: {e}")
    
    return negative_feedback

if __name__ == "__main__":
    print("=== Devpost Negative Feedback Research ===")
    print("Creating snapshots for negative feedback search...")
    
    snapshots = search_devpost_negative_feedback()
    
    print("\n=== Fetching Results ===")
    negative_results = fetch_negative_results(snapshots, wait_minutes=20)
    
    print(f"\n=== Found {len(negative_results)} Negative Feedback Items ===")
    for i, item in enumerate(negative_results, 1):
        print(f"\n{i}. Query: {item['query']}")
        print(f"   Source: {item['source']}")
        print(f"   URL: {item['url']}")
        print(f"   Text: {item['text'][:200]}...")
        print("-" * 80)
    
    # Save to file
    with open("devpost_negative_feedback.json", "w") as f:
        json.dump(negative_results, f, indent=2)
    
    print(f"\nResults saved to: devpost_negative_feedback.json") 