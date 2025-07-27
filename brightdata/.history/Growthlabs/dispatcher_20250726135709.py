# dispatcher.py — Growthlabs Dispatcher (Bright Data SERP websearch, no cohorts)
from __future__ import annotations

import os, asyncio, httpx
from datetime import datetime, timedelta
from typing import List, Dict, Any

from fastapi import FastAPI, Request, HTTPException, Body
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, Integer, String, Float, JSON, DateTime, Text, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# -----------------------------
# Configuration via env vars
# -----------------------------
DISPATCHER_SECRET = os.getenv("DISPATCHER_SECRET", "changeme")
DATABASE_URL      = os.getenv("DATABASE_URL", "sqlite:///./agents.db")

# Bright Data: bearer token + one SERP dataset id (used for *all* web search)
BRIGHTDATA_TOKEN         = os.getenv("BRIGHTDATA_TOKEN")            # brd_xxx...
BRIGHTDATA_DATASET_SEARCH= os.getenv("BRIGHTDATA_DATASET_SEARCH")   # e.g., gd_abcdef for SERP
BRIGHTDATA_ENDPOINT      = "https://api.brightdata.com/datasets/v3/trigger"

# Worker tuning
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "5"))

# -----------------------------
# DB setup
# -----------------------------
Base = declarative_base()
engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)

class Trigger(Base):
    __tablename__ = "triggers"
    id = Column(Integer, primary_key=True)
    kind = Column(String, index=True)   # e.g., "targets_sync"
    metric = Column(String)
    prev_value = Column(Float, default=0)
    curr_value = Column(Float, default=0)
    pct_drop   = Column(Float, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    runs = relationship("ToolRun", back_populates="trigger", cascade="all, delete-orphan")

class ToolRun(Base):
    __tablename__ = "tool_runs"
    id = Column(Integer, primary_key=True)
    trigger_id = Column(Integer, ForeignKey("triggers.id"))
    tool = Column(String)                       # 'brightdata:trigger'
    status = Column(String, default="queued")   # queued|running|success|error|canceled
    input = Column(JSON)                        # {dataset_id, inputs[], format, ...}
    output = Column(JSON)                       # {snapshot_id, raw}
    error = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    trigger = relationship("Trigger", back_populates="runs")

class TargetUser(Base):
    __tablename__ = "target_users"
    id = Column(Integer, primary_key=True)
    distinct_id = Column(String, index=True, unique=True)
    window = Column(String, index=True)         # e.g., "2025-07-01..2025-07-26"
    active = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

class BDResult(Base):
    __tablename__ = "bd_results"
    id = Column(Integer, primary_key=True)
    snapshot_id = Column(String, index=True)
    source = Column(String, index=True)         # "web"
    url = Column(String)
    title = Column(String)
    text = Column(Text)
    sentiment = Column(String, index=True)      # positive|negative|neutral
    score = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(engine)

# -----------------------------
# FastAPI
# -----------------------------
app = FastAPI(title="Growthlabs Dispatcher")

class BulkSyncPayload(BaseModel):
    window: str
    distinct_ids: List[str]
    mode: str = "replace"

def _require_secret(request: Request):
    if request.query_params.get("secret") != DISPATCHER_SECRET:
        raise HTTPException(status_code=401, detail="invalid secret")

# -----------------------------
# Search query builder (SERP)
# -----------------------------
def build_search_inputs() -> List[Dict[str, Any]]:
    """
    Queries designed to surface both *positive* and *negative* discussions about Devpost,
    from the broader web and Reddit, via a single SERP dataset.
    Adjust as needed.
    """
    queries = [
        # generic sentiment-bearing queries
        "Devpost review",
        "Devpost experience",
        "Devpost pros and cons",
        "is Devpost legit",
        "Devpost scam",
        "Devpost complaints",
        "Devpost prizes winners feedback",
        "Devpost hackathon rules disqualified",
        # focus on site content
        'site:devpost.com discussions',
        'site:devpost.com "winners" OR "prize" OR "prizes"',
        'site:devpost.com "rules" OR "disqualified" OR "complaint"',
        # explicitly include Reddit results (but still via SERP)
        "site:reddit.com Devpost",
        '"Devpost" site:reddit.com/r/hackathons',
    ]
    inputs = []
    for q in queries:
        inputs.append({
            "q": q,
            "gl": "us",   # country (Google 'gl')
            "num": 20,    # request up to ~20 results per query
            "meta": {"source": "web", "q": q}
        })
    return inputs

# -----------------------------
# Routes
# -----------------------------
@app.post("/targets/bulk_sync")
def bulk_sync(p: BulkSyncPayload, request: Request):
    """
    Your selector pushes the 100 users here.
    We upsert the active set for the window and (as requested) *assume a drop*,
    so we enqueue ONE Bright Data SERP job that searches broadly for Devpost.
    """
    _require_secret(request)

    inserted, removed = 0, 0
    with SessionLocal() as db:
        existing = {t.distinct_id: t for t in db.query(TargetUser).filter(TargetUser.active == 1).all()}
        incoming = set(p.distinct_ids)

        if p.mode == "replace":
            for did, row in existing.items():
                if did not in incoming:
                    row.active = 0
                    row.updated_at = datetime.utcnow()
                    db.add(row); removed += 1

        for did in incoming:
            if did not in existing:
                db.add(TargetUser(distinct_id=did, window=p.window, active=1)); inserted += 1

        # Create a trigger for this sync/window
        trig = Trigger(kind="targets_sync", metric=f"window:{p.window}",
                       prev_value=0, curr_value=len(incoming), pct_drop=0.0)
        db.add(trig); db.commit(); db.refresh(trig)

        # Enqueue ONE SERP websearch job (covers Reddit via site:reddit.com inside queries)
        if not BRIGHTDATA_DATASET_SEARCH:
            # Allow running without BD configured (e.g., local demo)
            db.add(ToolRun(trigger_id=trig.id, tool="brightdata:trigger",
                           status="error", input={"reason":"missing dataset"},
                           error="Missing BRIGHTDATA_DATASET_SEARCH")); db.commit()
        else:
            db.add(ToolRun(
                trigger_id=trig.id,
                tool="brightdata:trigger",
                status="queued",
                input={
                    "dataset_id": BRIGHTDATA_DATASET_SEARCH,
                    "inputs": build_search_inputs(),  # list of {q, gl, num, meta}
                    "format": "json",
                    # If you want BD to POST results back automatically, uncomment:
                    # "notify": True,
                    # "endpoint": "https://<your-host>/webhooks/brightdata"
                }
            ))
            db.commit()

    return {"ok": True, "inserted": inserted, "removed": removed}

@app.get("/inbox")
def inbox():
    with SessionLocal() as db:
        rows = db.query(Trigger).order_by(Trigger.id.desc()).limit(50).all()
        out = []
        for t in rows:
            item = {
                "id": t.id, "created_at": t.created_at.isoformat(),
                "kind": t.kind, "metric": t.metric,
                "prev": t.prev_value, "curr": t.curr_value, "drop_pct": t.pct_drop,
                "runs": [{"id": r.id, "tool": r.tool, "status": r.status,
                          "input": r.input, "output": r.output, "error": r.error}
                         for r in t.runs]
            }
            # Attach a simple 24h sentiment rollup (from stored results)
            since = datetime.utcnow() - timedelta(hours=24)
            res = db.query(BDResult).filter(BDResult.created_at >= since).all()
            if res:
                pos = sum(1 for r in res if r.sentiment == "positive")
                neg = sum(1 for r in res if r.sentiment == "negative")
                neu = sum(1 for r in res if r.sentiment == "neutral")
                item["sentiment_summary_24h"] = {"positive": pos, "negative": neg, "neutral": neu}
                item["samples"] = [
                    {"title": r.title, "url": r.url, "sentiment": r.sentiment, "score": r.score}
                    for r in res[:10]
                ]
            out.append(item)
        return out

# If you enable notify+endpoint in the job, Bright Data will POST results here.
@app.post("/webhooks/brightdata")
def brightdata_webhook(payload: Dict[str, Any] = Body(...)):
    records = payload.get("records") or payload.get("data") or []
    snapshot_id = payload.get("snapshot_id") or payload.get("id") or "unknown"
    upsert_records(records, snapshot_id)
    return {"ok": True, "received": len(records)}

# -----------------------------
# Bright Data worker
# -----------------------------
async def _process_run_brightdata(run_id: int):
    token = BRIGHTDATA_TOKEN
    if not token:
        with SessionLocal() as db:
            run = db.get(ToolRun, run_id)
            if run:
                run.status = "error"; run.error = "Missing BRIGHTDATA_TOKEN"
                db.add(run); db.commit()
        return

    with SessionLocal() as db:
        run = db.get(ToolRun, run_id)
        if not run or run.status != "queued" or not run.input:
            return
        run.status = "running"; db.add(run); db.commit()
        payload = run.input
        dataset_id = payload.get("dataset_id")
        inputs     = payload.get("inputs", [])
        fmt        = payload.get("format", "json")
        include_errors = payload.get("include_errors")
        notify     = payload.get("notify")
        endpoint   = payload.get("endpoint")

    if not dataset_id or not isinstance(inputs, list) or not inputs:
        with SessionLocal() as db:
            run = db.get(ToolRun, run_id)
            if run:
                run.status = "error"; run.error = "Invalid input: need dataset_id and non-empty inputs[]"
                db.add(run); db.commit()
        return

    params = {"dataset_id": dataset_id, "format": fmt}
    if include_errors is not None:
        params["include_errors"] = "true" if include_errors else "false"
    if notify and endpoint:
        params["notify"] = "true"
        params["endpoint"] = endpoint

    attempt, last_err = 0, None
    while attempt < 3:
        attempt += 1
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                resp = await client.post(
                    BRIGHTDATA_ENDPOINT,
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    params=params,
                    json=inputs,
                )
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                wait = int(ra) if ra and ra.isdigit() else 2 ** attempt
                await asyncio.sleep(wait); continue
            resp.raise_for_status()
            data = resp.json()  # includes snapshot_id
            with SessionLocal() as db:
                run = db.get(ToolRun, run_id)
                if run:
                    run.output = {"snapshot_id": data.get("snapshot_id"), "raw": data}
                    run.status = "success"
                    db.add(run); db.commit()
            return
        except httpx.HTTPStatusError as e:
            last_err = f"HTTP {e.response.status_code}: {e.response.text[:400]}"
        except httpx.RequestError as e:
            last_err = f"RequestError: {e}"
        except Exception as e:
            last_err = f"Exception: {e}"
        await asyncio.sleep(2 ** attempt)

    with SessionLocal() as db:
        run = db.get(ToolRun, run_id)
        if run:
            run.status = "error"; run.error = last_err or "Unknown error"
            db.add(run); db.commit()

async def worker_loop():
    sem = asyncio.Semaphore(WORKER_CONCURRENCY)
    while True:
        try:
            with SessionLocal() as db:
                run_ids = [r.id for r in db.query(ToolRun)
                           .filter(ToolRun.status == "queued", ToolRun.tool == "brightdata:trigger")
                           .limit(WORKER_CONCURRENCY * 2).all()]
            if not run_ids:
                await asyncio.sleep(2); continue

            async def _wrap(rid: int):
                async with sem:
                    await _process_run_brightdata(rid)

            await asyncio.gather(*[_wrap(rid) for rid in run_ids])
        except Exception:
            await asyncio.sleep(2)

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(worker_loop())

# -----------------------------
# Result normalization + sentiment
# -----------------------------
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _vader = SentimentIntensityAnalyzer()
except Exception:
    _vader = None

def _pick_text(rec: Dict[str, Any]) -> str:
    for k in ["text", "snippet", "description", "body", "content"]:
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v
    return ""

def _pick_title(rec: Dict[str, Any]) -> str:
    for k in ["title", "headline", "page_title", "post_title", "q"]:
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v
    return ""

def _pick_url(rec: Dict[str, Any]) -> str:
    for k in ["url", "link", "permalink"]:
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v
    return ""

def _label(score: float) -> str:
    if score >= 0.3:  return "positive"
    if score <= -0.3: return "negative"
    return "neutral"

def upsert_records(records: List[Dict[str, Any]], snapshot_id: str):
    if not records:
        return
    with SessionLocal() as db:
        for rec in records:
            text = _pick_text(rec)
            title = _pick_title(rec)
            url = _pick_url(rec)
            if not (text or title):
                continue
            if _vader:
                s = _vader.polarity_scores(f"{title}. {text}")["compound"]
                sent = _label(s)
            else:
                s, sent = 0.0, "neutral"
            db.add(BDResult(
                snapshot_id=snapshot_id, source="web",
                url=url, title=title, text=text,
                sentiment=sent, score=s
            ))
        db.commit()
