"""
Dispatcher (MCP version)
- Receives a target list from select_target_100.py
- For each user, uses Bright Data MCP tools to:
    1) search the web for Devpost reviews (Reddit + Devpost)
    2) scrape the result pages as Markdown
    3) compute a naive sentiment (positive/negative/neutral)
    4) store results in SQLite
- Exposes:
    GET /inbox  -> recent runs + per-user harvested URLs and sentiment
    POST /targets/sync (auth via X-Dispatcher-Secret)

Prereqs
- Bright Data MCP server running locally:
    export API_TOKEN=brd_xxx
    npx @brightdata/mcp
- Env for this app:
    export DISPATCHER_SECRET='your-shared-secret'
    export DATABASE_URL='sqlite:///./agents.db'   # default used if unset
"""

import asyncio
import os
import re
import time
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

# --- MCP Client (stdio) -----------------------------------------------------
from mcp import ClientSession, StdioServerParameters, types
from mcp.client.stdio import stdio_client

BRAND = "Devpost"
REGION = os.getenv("REGION", "US").lower()

SEARCH_QUERIES = [
    f"{BRAND} reviews",
    f"site:reddit.com {BRAND}",
    f"site:devpost.com discussions {BRAND}",
    f"{BRAND} hackathon feedback",
]

RESULTS_PER_QUERY = 5

class MCPManager:
    """Keeps one stdio session to the Bright Data MCP server."""
    def __init__(self):
        api_token = os.getenv("API_TOKEN")
        if not api_token:
            raise RuntimeError("API_TOKEN is required (Bright Data API token).")
        self.server_params = StdioServerParameters(
            command="npx",
            args=["@brightdata/mcp"],
            env={"API_TOKEN": api_token},
        )
        self._ctx = None  # (read, write)
        self._session: ClientSession | None = None
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        self._ctx = stdio_client(self.server_params)
        self._read, self._write = await self._ctx.__aenter__()
        self._session = ClientSession(self._read, self._write)
        await self._session.__aenter__()
        await self._session.initialize()
        await self._validate_tools()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session:
            await self._session.__aexit__(exc_type, exc, tb)
        if self._ctx:
            await self._ctx.__aexit__(exc_type, exc, tb)

    async def _validate_tools(self):
        """Ensure the expected tools exist."""
        tools = await self._session.list_tools()
        names = {t.name for t in tools.tools}
        required = {"search_engine", "scrape_as_markdown"}
        missing = required - names
        if missing:
            raise RuntimeError(f"MCP server missing tools: {missing}")
        # Optional: print them for visibility
        # print("MCP tools:", sorted(names))

    async def search(self, q: str, gl: str = REGION, num: int = RESULTS_PER_QUERY) -> List[Dict[str, Any]]:
        """Call MCP search_engine and return a list of URLs (with optional titles/snippets if parsable)."""
        async with self._lock:
            result = await self._session.call_tool("search_engine", arguments={"q": q, "gl": gl, "num": num})
        text_blocks = []
        if result.content:
            for block in result.content:
                if isinstance(block, types.TextContent):
                    text_blocks.append(block.text)
        text = "\n".join(text_blocks) if text_blocks else ""
        # Common output is markdown list with [title](url) — robustly extract links:
        urls = []
        titles = []
        for m in re.finditer(r"\[([^\]]+)\]\((https?://[^\s)]+)\)", text, flags=re.I):
            titles.append(m.group(1))
            urls.append(m.group(2))
        # Fallback: collect raw http(s) links if markdown didn’t parse
        if not urls:
            urls = re.findall(r"https?://[^\s)>\]]+", text)

        # Pair best effort titles with urls
        items = []
        for i, u in enumerate(urls[:num]):
            t = titles[i] if i < len(titles) else ""
            items.append({"url": u, "title": t})
        return items

    async def scrape_markdown(self, url: str) -> str:
        async with self._lock:
            result = await self._session.call_tool("scrape_as_markdown", arguments={"url": url})
        md = []
        if result.content:
            for block in result.content:
                if isinstance(block, types.TextContent):
                    md.append(block.text)
        return "\n".join(md)
# The Bright Data MCP tools documented here: search engine + scrape as markdown/html. :contentReference[oaicite:3]{index=3}

# --- Naive sentiment --------------------------------------------------------
POS = {"good", "great", "love", "helpful", "useful", "amazing", "excellent", "positive", "recommend", "win", "best"}
NEG = {"bad", "hate", "awful", "worst", "bug", "spam", "scam", "negative", "problem", "issue", "slow", "confusing"}

def simple_sentiment(text: str) -> str:
    t = text.lower()
    p = sum(t.count(w) for w in POS)
    n = sum(t.count(w) for w in NEG)
    if p - n >= 2: return "positive"
    if n - p >= 2: return "negative"
    return "neutral"

# --- FastAPI ---------------------------------------------------------------
app = FastAPI()
DISPATCHER_SECRET = os.getenv("DISPATCHER_SECRET", "dev-secret")

class TargetItem(BaseModel):
    distinct_id: str

class SyncBody(BaseModel):
    window: str
    insert: List[TargetItem] = []
    remove: List[TargetItem] = []

async def harvest_for_user(mcp: MCPManager, db, distinct_id: str, run: Run):
    queries = SEARCH_QUERIES
    for q in queries:
        try:
            results = await mcp.search(q, gl=REGION, num=RESULTS_PER_QUERY)
        except Exception as e:
            continue

        # keep only reddit/devpost first; then others
        ordered = []
        for r in results:
            u = r["url"]
            if "reddit.com" in u or "devpost.com" in u:
                ordered.append(r)
        for r in results:
            if r not in ordered:
                ordered.append(r)

        seen = set()
        for item in ordered[:RESULTS_PER_QUERY]:
            url = item["url"]
            if url in seen:
                continue
            seen.add(url)
            try:
                md = await mcp.scrape_markdown(url)
            except Exception:
                continue
            # produce a short title/snippet/excerpt
            title = item.get("title") or (md.splitlines()[0][:160] if md else "")
            excerpt = md[:800]
            snippet = ""
            # naive sentiment
            sent = simple_sentiment(md)
            src = "reddit" if "reddit.com" in url else ("devpost" if "devpost.com" in url else "other")
            r = Review(
                run_id=run.id,
                user_distinct_id=distinct_id,
                source=src,
                query=q,
                url=url,
                title=title,
                snippet=snippet,
                excerpt=excerpt,
                sentiment=sent,
            )
            db.add(r)
            db.commit()

@app.post("/targets/sync")
async def sync_targets(req: Request, body: SyncBody, background: BackgroundTasks):
    if req.headers.get("X-Dispatcher-Secret") != DISPATCHER_SECRET:
        raise HTTPException(status_code=401, detail="invalid secret")

    db = SessionLocal()
    run = Run(kind="targets_sync", metric=f"window:{body.window}", status="running")
    db.add(run); db.commit(); db.refresh(run)

    async def process():
        try:
            async with MCPManager() as mcp:
                for t in body.insert:
                    await harvest_for_user(mcp, db, t.distinct_id, run)
            run.status = "success"
            run.note = f"inserted={len(body.insert)} removed={len(body.remove)}"
        except Exception as e:
            run.status = "error"
            run.note = str(e)
        finally:
            db.add(run); db.commit(); db.close()

    background.add_task(process)
    return {"ok": True, "inserted": len(body.insert), "removed": len(body.remove)}

@app.get("/inbox")
def inbox(limit: int = 50):
    db = SessionLocal()
    runs = db.query(Run).order_by(Run.id.desc()).limit(limit).all()
    out = []
    for r in runs:
        items = []
        for v in r.results[:200]:
            items.append({
                "user": v.user_distinct_id,
                "source": v.source,
                "query": v.query,
                "url": v.url,
                "sentiment": v.sentiment,
                "title": v.title[:200] if v.title else "",
                "excerpt": v.excerpt[:400] if v.excerpt else "",
            })
        out.append({
            "id": r.id,
            "created_at": r.created_at.isoformat(),
            "kind": r.kind,
            "metric": r.metric,
            "status": r.status,
            "note": r.note,
            "items": items,
        })
    return JSONResponse(out)
