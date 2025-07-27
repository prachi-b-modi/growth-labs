import requests
import json

def fetch_ready_snapshots():
    """Fetch results from the ready snapshots."""
    
    api_token = "4561152efcf7312d5da5ff4669b15cb439866b4a8e0e24a429e93032403261d1"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    
    # Your ready snapshots
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
    
    all_negative_feedback = []
    
    for snapshot in snapshots:
        snapshot_id = snapshot["id"]
        query = snapshot["query"]
        
        print(f"\n🔍 Fetching results for: {query}")
        print(f"   Snapshot ID: {snapshot_id}")
        
        # Try to fetch results
        endpoint = f"https://api.brightdata.com/datasets/v3/trigger/result?snapshot_id={snapshot_id}&format=json"
        
        try:
            response = requests.get(endpoint, headers=headers, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                data = result.get('data', [])
                
                print(f"   ✅ Found {len(data)} results")
                
                for i, item in enumerate(data, 1):
                    title = item.get('title', 'No title')
                    url = item.get('url', 'No URL')
                    text = item.get('text', '') or item.get('snippet', '')
                    
                    # Check for negative sentiment
                    negative_words = ["bad", "terrible", "awful", "horrible", "sucks", "problem", "issue", "complaint", "unfair", "broken", "scam", "disqualified", "unfair", "hate", "worst"]
                    is_negative = any(word in text.lower() for word in negative_words)
                    
                    feedback_item = {
                        "query": query,
                        "title": title,
                        "url": url,
                        "text": text[:500],  # First 500 chars
                        "is_negative": is_negative,
                        "source": "reddit" if "reddit.com" in url else "other"
                    }
                    
                    all_negative_feedback.append(feedback_item)
                    
                    print(f"     {i}. {title}")
                    print(f"        URL: {url}")
                    print(f"        Negative: {'Yes' if is_negative else 'No'}")
                    print(f"        Text: {text[:100]}...")
                    print()
                    
            else:
                print(f"   ❌ Error: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"   ❌ Exception: {e}")
    
    return all_negative_feedback

if __name__ == "__main__":
    print("=== Fetching Ready Devpost Negative Feedback ===")
    
    results = fetch_ready_snapshots()
    
    print(f"\n🎉 Total results: {len(results)}")
    
    # Save to file
    with open("devpost_negative_feedback_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"📁 Results saved to: devpost_negative_feedback_results.json")
    
    # Show summary
    negative_count = sum(1 for item in results if item["is_negative"])
    reddit_count = sum(1 for item in results if item["source"] == "reddit")
    
    print(f"\n📊 Summary:")
    print(f"   Total feedback items: {len(results)}")
    print(f"   Negative feedback: {negative_count}")
    print(f"   From Reddit: {reddit_count}") 