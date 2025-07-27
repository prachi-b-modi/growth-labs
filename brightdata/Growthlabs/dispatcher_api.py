"""
Dispatcher (Bright Data API version)
- Receives a target list from select_target_100.py
- For each user, uses Bright Data API directly to:
    1) search the web for Devpost reviews (Reddit + Devpost)
    2) scrape the result pages
    3) compute a naive sentiment (positive/negative/neutral)
    4) store results in SQLite
- Exposes:
    GET /inbox  -> recent runs + per-user harvested URLs and sentiment
    POST /targets/sync (auth via X-Dispatcher-Secret)

Prereqs
- Bright Data API token:
    export API_TOKEN=brd_xxx
- Env for this app:
    export DISPATCHER_SECRET='your-shared-secret'
    export DATABASE_URL='sqlite:///./agents.db'   # default used if unset
"""

import asyncio
import os
import re
import time
import requests
import json
from datetime import datetime
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime, ForeignKey
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# --- DB setup ---------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./agents.db")
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False)
Base = declarative_base()

class Run(Base):
    __tablename__ = "runs"
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    kind = Column(String(64))          # e.g., 'targets_sync'
    metric = Column(String(256))       # e.g., 'window:2025-07-01..2025-07-26'
    status = Column(String(32), default="queued")
    note = Column(Text, default="")
    results = relationship("Review", back_populates="run", cascade="all, delete")

class Review(Base):
    __tablename__ = "reviews"
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("runs.id"))
    user_distinct_id = Column(String(128))
    source = Column(String(64))        # 'reddit' | 'devpost' | 'other'
    query = Column(String(512))
    url = Column(Text)
    title = Column(Text)
    snippet = Column(Text)
    excerpt = Column(Text)
    sentiment = Column(String(16))     # positive|negative|neutral
    created_at = Column(DateTime, default=datetime.utcnow)
    run = relationship("Run", back_populates="results")

Base.metadata.create_all(engine)

# --- Bright Data API Client -------------------------------------------------
BRAND = "Devpost"
REGION = os.getenv("REGION", "US").lower()

SEARCH_QUERIES = [
    f"{BRAND} reviews",
    f"site:reddit.com {BRAND}",
    f"site:devpost.com discussions {BRAND}",
    f"{BRAND} hackathon feedback",
]

class BrightDataAPI:
    """Direct Bright Data API client for web scraping."""
    def __init__(self):
        self.api_token = os.getenv("API_TOKEN")
        if not self.api_token:
            raise RuntimeError("API_TOKEN is required (Bright Data API token).")
        
        self.dataset_id = os.getenv("BRIGHTDATA_DATASET_ID", "gd_lvz8ah06191smkebj4")
        self.base_url = "https://api.brightdata.com/datasets/v3/trigger"
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

    def create_search_snapshots(self, keyword: str, num_posts: int = 100) -> List[Dict[str, Any]]:
        """
        Create search snapshots using Bright Data API.
        Returns list of snapshot info for later retrieval.
        """
        params = {
            "dataset_id": self.dataset_id,
            "include_errors": "true",
            "type": "discover_new",
            "discover_by": "keyword",
        }
        
        data = [
            {
                "keyword": keyword,
                "date": "Past year",
                "sort_by": "Relevance",
                "num_of_posts": 5
            }
        ]
        
        try:
            print(f"Creating Bright Data snapshot for keyword: {keyword}")
            response = requests.post(
                self.base_url, 
                headers=self.headers, 
                params=params, 
                json=data,
                timeout=120
            )
            
            print(f"Bright Data API status code: {response.status_code}")
            
            if response.status_code != 200:
                print(f"Bright Data API error response: {response.text}")
                return []
            
            result = response.json()
            print(f"Bright Data API response: {json.dumps(result, indent=2)}")
            
            # Get the snapshot ID
            snapshot_id = result.get("snapshot_id")
            if not snapshot_id:
                print("No snapshot_id in response")
                return []
            
            print(f"Created snapshot_id: {snapshot_id}")
            
            # Return snapshot info for later retrieval
            return [{
                "snapshot_id": snapshot_id,
                "keyword": keyword,
                "status": "created",
                "url": f"https://api.brightdata.com/datasets/v3/trigger/result?snapshot_id={snapshot_id}",
                "title": f"Bright Data Snapshot: {snapshot_id}",
                "text": f"Snapshot created for keyword: {keyword}. Results will be available later.",
                "snippet": f"Snapshot ID: {snapshot_id} - Processing...",
                "source": "brightdata"
            }]
            
        except requests.exceptions.RequestException as e:
            print(f"Bright Data API request error: {e}")
            return []
        except Exception as e:
            print(f"Bright Data API unexpected error: {e}")
            return []

    def _classify_source(self, url: str) -> str:
        """Classify the source of the content based on URL."""
        if "reddit.com" in url:
            return "reddit"
        elif "devpost.com" in url:
            return "devpost"
        else:
            return "other"

# --- Naive sentiment --------------------------------------------------------
POS = {"good", "great", "love", "helpful", "useful", "amazing", "excellent", "positive", "recommend", "win", "best"}
NEG = {"bad", "hate", "awful", "worst", "bug", "spam", "scam", "negative", "problem", "issue", "slow", "confusing"}

def simple_sentiment(text: str) -> str:
    """Naive keyword-based sentiment analysis."""
    if not text:
        return "neutral"
    text_lower = text.lower()
    pos_count = sum(1 for word in POS if word in text_lower)
    neg_count = sum(1 for word in NEG if word in text_lower)
    if pos_count > neg_count:
        return "positive"
    elif neg_count > pos_count:
        return "negative"
    return "neutral"

# --- FastAPI app ------------------------------------------------------------
app = FastAPI(title="Growthlabs Dispatcher (Bright Data API)")

class TargetItem(BaseModel):
    distinct_id: str

class SyncBody(BaseModel):
    window: str
    insert: List[TargetItem] = []
    remove: List[TargetItem] = []

async def harvest_for_user(bd_api: BrightDataAPI, db, distinct_id: str, run: Run):
    """Harvest reviews for a single user using Bright Data API."""
    try:
        for query in SEARCH_QUERIES:
            print(f"Creating snapshot for: {query}")
            
            # Create Bright Data snapshots (async processing)
            snapshot_results = bd_api.create_search_snapshots(query, num_posts=20)
            
            for result in snapshot_results:
                if not result.get("snapshot_id"):
                    continue
                
                # Store snapshot info in database
                review = Review(
                    run_id=run.id,
                    user_distinct_id=distinct_id,
                    source=result.get("source", "brightdata"),
                    query=query,
                    url=result.get("url", ""),
                    title=result.get("title", ""),
                    snippet=result.get("snippet", ""),
                    excerpt=result.get("text", ""),
                    sentiment="neutral"  # Placeholder until we get actual results
                )
                db.add(review)
            
            # Small delay between queries
            await asyncio.sleep(1)
        
        db.commit()
        print(f"Created {len(snapshot_results)} snapshots for user {distinct_id}")
        
    except Exception as e:
        print(f"Error creating snapshots for user {distinct_id}: {e}")
        db.rollback()

@app.post("/targets/sync")
async def sync_targets(req: Request, body: SyncBody, background: BackgroundTasks):
    """Sync target users and trigger harvesting."""
    secret = req.headers.get("X-Dispatcher-Secret")
    expected_secret = os.getenv("DISPATCHER_SECRET", "I3gp3MFUpDcWRcjvIMdRF-gALZvtzv5smRZR0uugu8w")
    
    if secret != expected_secret:
        raise HTTPException(status_code=401, detail="Invalid secret")
    
    # Create a run record
    with SessionLocal() as db:
        run = Run(
            kind="targets_sync",
            metric=f"window:{body.window}",
            status="running",
            note=f"Processing {len(body.insert)} users"
        )
        db.add(run)
        db.commit()
        db.refresh(run)
    
    async def process():
        try:
            bd_api = BrightDataAPI()
            with SessionLocal() as db:
                for item in body.insert:
                    await harvest_for_user(bd_api, db, item.distinct_id, run)
                
                # Update run status
                run.status = "success"
                run.note = f"Completed harvesting for {len(body.insert)} users"
                db.add(run)
                db.commit()
                
        except Exception as e:
            with SessionLocal() as db:
                run.status = "error"
                run.note = f"Error: {str(e)}"
                db.add(run)
                db.commit()
    
    background.add_task(process)
    return {"ok": True, "run_id": run.id}

@app.get("/inbox")
def inbox(limit: int = 50):
    """Get recent runs and their results."""
    with SessionLocal() as db:
        runs = db.query(Run).order_by(Run.id.desc()).limit(limit).all()
        result = []
        
        for run in runs:
            run_data = {
                "id": run.id,
                "created_at": run.created_at.isoformat(),
                "kind": run.kind,
                "metric": run.metric,
                "status": run.status,
                "note": run.note,
                "items": []
            }
            
            # Get reviews for this run
            reviews = db.query(Review).filter(Review.run_id == run.id).all()
            for review in reviews:
                review_data = {
                    "user_distinct_id": review.user_distinct_id,
                    "source": review.source,
                    "query": review.query,
                    "url": review.url,
                    "title": review.title,
                    "snippet": review.snippet,
                    "sentiment": review.sentiment,
                    "created_at": review.created_at.isoformat()
                }
                run_data["items"].append(review_data)
            
            result.append(run_data)
        
        return result

@app.get("/fetch-snapshots")
async def fetch_snapshots():
    """Manually fetch results from pending snapshots."""
    with SessionLocal() as db:
        # Get all reviews that are from brightdata snapshots
        reviews = db.query(Review).filter(
            Review.source == "brightdata",
            Review.snippet.like("%Processing...%")
        ).all()
        
        bd_api = BrightDataAPI()
        updated_count = 0
        
        for review in reviews:
            # Extract snapshot_id from snippet
            snippet = review.snippet
            if "Snapshot ID:" in snippet:
                snapshot_id = snippet.split("Snapshot ID:")[1].split(" -")[0].strip()
                
                # Try to fetch results
                results = bd_api._fetch_snapshot_results(snapshot_id)
                if results:
                    # Update the review with actual results
                    result = results[0]  # Take first result
                    review.url = result.get("url", review.url)
                    review.title = result.get("title", review.title)
                    review.snippet = result.get("snippet", review.snippet)
                    review.excerpt = result.get("text", review.excerpt)
                    review.sentiment = simple_sentiment(result.get("text", ""))
                    review.source = result.get("source", review.source)
                    updated_count += 1
        
        db.commit()
        return {"updated": updated_count, "total": len(reviews)}

@app.get("/health")
async def health():
    """Health check endpoint."""
    try:
        bd_api = BrightDataAPI()
        return {"status": "healthy", "brightdata_configured": True}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.get("/debug/search")
async def debug_search(keyword: str = "Devpost reviews"):
    """Debug endpoint to test Bright Data API search."""
    try:
        bd_api = BrightDataAPI()
        results = bd_api.create_search_snapshots(keyword, num_posts=5)
        return {
            "keyword": keyword,
            "results_count": len(results),
            "results": results
        }
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 