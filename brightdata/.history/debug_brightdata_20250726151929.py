import requests
import json

def test_brightdata_api():
    url = "https://api.brightdata.com/datasets/v3/trigger"
    headers = {
        "Authorization": "Bearer 4561152efcf7312d5da5ff4669b15cb439866b4a8e0e24a429e93032403261d1",
        "Content-Type": "application/json",
    }
    params = {
        "dataset_id": "gd_lvz8ah06191smkebj4",
        "include_errors": "true",
        "type": "discover_new",
        "discover_by": "keyword",
    }
    data = [
        {"keyword":"Devpost reviews","date":"Past year","sort_by":"Relevance","num_of_posts":5},
    ]

    print("Testing Bright Data API...")
    print(f"URL: {url}")
    print(f"Headers: {headers}")
    print(f"Params: {params}")
    print(f"Data: {json.dumps(data, indent=2)}")

    try:
        response = requests.post(url, headers=headers, params=params, json=data, timeout=60)
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"Response: {json.dumps(result, indent=2)}")
            
            if "data" in result:
                print(f"Found {len(result['data'])} data items")
                for i, item in enumerate(result['data'][:3]):  # Show first 3 items
                    print(f"Item {i+1}: {json.dumps(item, indent=2)}")
            else:
                print("No 'data' key in response")
                print(f"Available keys: {list(result.keys())}")
        else:
            print(f"Error Response: {response.text}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_brightdata_api() 