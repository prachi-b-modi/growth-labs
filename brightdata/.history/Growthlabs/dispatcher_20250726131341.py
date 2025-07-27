# dispatcher.py — tiny cohort-free dispatcher
import os, asyncio
from datetime import datetime
from typing import List
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, JSON, DateTime, Text, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# --- config ---
DISPATCHER_SECRET = os.getenv("DISPATCHER_SECRET", "changeme")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./agents.db")

# --- DB setup ---
Base = declarative_base()
engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)

class Trigger(Base):
    __tablename__ = "triggers"
    id = Column(Integer, primary_key=True)
    kind = Column(String, index=True)
    metric = Column(String)
    prev_value = Column(Float, default=0)
    curr_value = Column(Float, default=0)
    pct_drop = Column(Float, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    runs = relationship("ToolRun", back_populates="trigger", cascade="all, delete-orphan")

class ToolRun(Base):
    __tablename__ = "tool_runs"
    id = Column(Integer, primary_key=True)
    trigger_id = Column(Integer, ForeignKey("triggers.id"))
    tool = Column(String)                       # e.g., 'brightdata:run_job'
    status = Column(String, default="queued")   # queued|running|success|error|canceled
    input = Column(JSON)
    output = Column(JSON)
    error = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    trigger = relationship("Trigger", back_populates="runs")

class TargetUser(Base):
    __tablename__ = "target_users"
    id = Column(Integer, primary_key=True)
    distinct_id = Column(String, index=True, unique=True)
    window = Column(String, index=True)         # "2025-07-01..2025-07-26"
    active = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(engine)

app = FastAPI(title="Growthlabs Dispatcher")

# ---- bulk sync payload from selector ----
class BulkSyncPayload(BaseModel):
    window: str
    distinct_ids: List[str]
    mode: str = "replace"  # replace the current active set

@app.post("/targets/bulk_sync")
def bulk_sync(p: BulkSyncPayload, request: Request):
    if request.query_params.get("secret") != DISPATCHER_SECRET:
        raise HTTPException(401, "invalid secret")

    inserted = removed = 0
    with SessionLocal() as db:
        existing = {t.distinct_id: t for t in db.query(TargetUser).filter(TargetUser.active == 1).all()}
        incoming = set(p.distinct_ids)

        # deactivate removed
        if p.mode == "replace":
            for did, row in existing.items():
                if did not in incoming:
                    row.active = 0
                    row.updated_at = datetime.utcnow()
                    db.add(row); removed += 1

        # add new & enqueue a ToolRun for each
        for did in incoming:
            if did not in existing:
                db.add(TargetUser(distinct_id=did, window=p.window, active=1)); inserted += 1
                trig = Trigger(kind="targets_sync", metric=f"window:{p.window}", prev_value=0, curr_value=1)
                db.add(trig); db.commit(); db.refresh(trig)
                db.add(ToolRun(trigger_id=trig.id, tool="brightdata:run_job",
                               input={"dataset":"devpost_discussions",
                                      "args":{"user_distinct_id": did},
                                      "format":"json"},
                               status="queued"))
        db.commit()
    return {"ok": True, "inserted": inserted, "removed": removed}

@app.get("/inbox")
def inbox():
    with SessionLocal() as db:
        rows = db.query(Trigger).order_by(Trigger.id.desc()).limit(50).all()
        return [{
          "id": t.id, "created_at": t.created_at.isoformat(),
          "kind": t.kind, "metric": t.metric,
          "runs": [{"id": r.id, "tool": r.tool, "status": r.status, "input": r.input, "output": r.output}
                   for r in t.runs]
        } for t in rows]

# very small worker that marks runs success (stub). Replace with Bright Data / Arcade calls later.
async def worker_loop():
    while True:
        try:
            with SessionLocal() as db:
                queued = db.query(ToolRun).filter(ToolRun.status=="queued").limit(10).all()
                for run in queued:
                    run.status = "running"; db.add(run); db.commit(); db.refresh(run)
                    # TODO: call your real tool here
                    run.output = {"ok": True, "note": "stubbed tool result"}
                    run.status = "success"; db.add(run); db.commit()
        except Exception:
            pass
        await asyncio.sleep(2)

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(worker_loop())
