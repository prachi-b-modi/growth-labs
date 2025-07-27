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
    """Keeps one stdio session to the Bright Data MCP server, and parses results robustly."""
    def __init__(self):
        api_token = os.getenv("API_TOKEN")
        if not api_token:
            raise RuntimeError("API_TOKEN is required (Bright Data API token).")
        self.server_params = StdioServerParameters(
            command="npx",
            args=["@brightdata/mcp"],
            env={"API_TOKEN": api_token},
        )
        self._ctx = None
        self._read = None
        self._write = None
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
        tools = await self._session.list_tools()
        names = {t.name for t in tools.tools}
        required = {"search_engine", "scrape_as_markdown"}
        missing = required - names
        if missing:
            raise RuntimeError(f"MCP server missing tools: {missing}")

    # ---------- helpers to parse MCP content ----------
    @staticmethod
    def _decode_blob(block: types.BlobContent) -> bytes:
        # mcp BlobContent has .data (base64 or plain) and .mimeType (string)
        data = block.data
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        if isinstance(data, str):
            # attempt base64
            try:
                return base64.b64decode(data, validate=True)
            except Exception:
                return data.encode("utf-8", "ignore")
        return b""

    @staticmethod
    def _extract_links_from_text(text: str) -> list[dict]:
        items = []
        # Prefer Markdown [title](url)
        for m in re.finditer(r"\[([^\]]+)\]\((https?://[^\s)]+)\)", text, flags=re.I):
            items.append({"title": m.group(1), "url": m.group(2)})
        # Fallback: raw urls
        if not items:
            for u in re.findall(r"https?://[^\s)>\]]+", text):
                items.append({"title": "", "url": u})
        return items

    @staticmethod
    def _extract_links_from_json(obj: any) -> list[dict]:
        items = []
        if isinstance(obj, dict):
            obj = [obj]
        if isinstance(obj, list):
            for rec in obj:
                if not isinstance(rec, dict):
                    continue
                url = rec.get("url") or rec.get("link")
                title = rec.get("title") or rec.get("name") or ""
                if url:
                    items.append({"title": title, "url": url})
        return items

    # ---------- public methods ----------
    async def search(self, q: str, gl: str = "us", num: int = RESULTS_PER_QUERY) -> list[dict]:
        """Returns [{title, url}, ...]"""
        async with self._lock:
            result = await self._session.call_tool("search_engine", arguments={"q": q, "gl": gl, "num": num})

        links: list[dict] = []
        if not result.content:
            return links

        for block in result.content:
            if isinstance(block, types.TextContent):
                txt = block.text or ""
                # try JSON first
                parsed = None
                s = txt.strip()
                if s.startswith("{") or s.startswith("["):
                    try:
                        parsed = json.loads(s)
                    except Exception:
                        parsed = None
                if parsed is not None:
                    links.extend(self._extract_links_from_json(parsed))
                else:
                    links.extend(self._extract_links_from_text(txt))

            elif isinstance(block, types.BlobContent):
                mime = (block.mimeType or "").lower()
                data = self._decode_blob(block)
                if "json" in mime:
                    try:
                        parsed = json.loads(data.decode("utf-8", "ignore"))
                        links.extend(self._extract_links_from_json(parsed))
                    except Exception:
                        pass
                else:
                    # treat as text (markdown/html)
                    txt = data.decode("utf-8", "ignore")
                    links.extend(self._extract_links_from_text(txt))

        # de-dup preserving order
        seen = set()
        uniq = []
        for it in links:
            u = it.get("url")
            if u and u not in seen:
                uniq.append(it)
                seen.add(u)
        return uniq[:num]

    async def scrape_markdown(self, url: str) -> str:
        async with self._lock:
            result = await self._session.call_tool("scrape_as_markdown", arguments={"url": url})

        parts: list[str] = []
        if not result.content:
            return ""
        for block in result.content:
            if isinstance(block, types.TextContent):
                parts.append(block.text or "")
            elif isinstance(block, types.BlobContent):
                data = self._decode_blob(block)
                parts.append(data.decode("utf-8", "ignore"))
        return "\n".join(parts)
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

@app.get("/health")
async def health():
    try:
        async with MCPManager() as m:
            # if tools can be listed, API_TOKEN + npx are fine
            return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/debug/search")
async def debug_search(q: str):
    try:
        async with MCPManager() as m:
            items = await m.search(q=q, gl="us", num=5)
            return {"ok": True, "items": items}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/debug/scrape")
async def debug_scrape(url: str):
    try:
        async with MCPManager() as m:
            md = await m.scrape_markdown(url)
            return {"ok": True, "len": len(md), "preview": md[:800]}
    except Exception as e:
        return {"ok": False, "error": str(e)}

