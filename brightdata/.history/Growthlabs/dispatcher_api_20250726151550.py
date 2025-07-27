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

    def search_and_scrape(self, keyword: str, num_posts: int = 100) -> List[Dict[str, Any]]:
        """
        Search and scrape content using Bright Data API.
        Returns list of scraped content with URLs, titles, and text.
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
                "num_of_posts": num_posts
            }
        ]
        
        try:
            response = requests.post(
                self.base_url, 
                headers=self.headers, 
                params=params, 
                json=data,
                timeout=120
            )
            response.raise_for_status()
            
            result = response.json()
            print(f"Bright Data API response: {json.dumps(result, indent=2)}")
            
            # Extract scraped content from the response
            scraped_content = []
            if "data" in result:
                for item in result["data"]:
                    content = {
                        "url": item.get("url", ""),
                        "title": item.get("title", ""),
                        "text": item.get("text", ""),
                        "snippet": item.get("snippet", ""),
                        "source": self._classify_source(item.get("url", ""))
                    }
                    scraped_content.append(content)
            
            return scraped_content
            
        except requests.exceptions.RequestException as e:
            print(f"Bright Data API error: {e}")
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
            print(f"Searching for: {query}")
            
            # Use Bright Data API to search and scrape
            scraped_results = bd_api.search_and_scrape(query, num_posts=20)
            
            for result in scraped_results:
                if not result.get("url") or not result.get("text"):
                    continue
                
                # Analyze sentiment
                sentiment = simple_sentiment(result.get("text", ""))
                
                # Store in database
                review = Review(
                    run_id=run.id,
                    user_distinct_id=distinct_id,
                    source=result.get("source", "other"),
                    query=query,
                    url=result.get("url", ""),
                    title=result.get("title", ""),
                    snippet=result.get("snippet", ""),
                    excerpt=result.get("text", "")[:1000],  # Truncate long text
                    sentiment=sentiment
                )
                db.add(review)
            
            # Small delay between queries
            await asyncio.sleep(1)
        
        db.commit()
        print(f"Harvested {len(scraped_results)} results for user {distinct_id}")
        
    except Exception as e:
        print(f"Error harvesting for user {distinct_id}: {e}")
        db.rollback()

@app.post("/targets/sync")
async def sync_targets(req: Request, body: SyncBody, background: BackgroundTasks):
    """Sync target users and trigger harvesting."""
    secret = req.headers.get("X-Dispatcher-Secret")
    if secret != os.getenv("DISPATCHER_SECRET", "changeme"):
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
        results = bd_api.search_and_scrape(keyword, num_posts=5)
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