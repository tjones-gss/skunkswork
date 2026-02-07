#!/usr/bin/env python3
"""Pre-Agent Hook - Executed before each agent starts."""
import json
from datetime import datetime, UTC
from pathlib import Path

def pre_agent_hook(agent_type: str, task: dict, job_id: str):
    log_path = Path("logs") / f"hooks_{datetime.now().strftime('%Y%m%d')}.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, 'a') as f:
        f.write(json.dumps({"event": "agent_start", "agent": agent_type, "job_id": job_id}) + '\n')
    task["_meta"] = {"job_id": job_id, "started_at": datetime.now(UTC).isoformat()}
    return task
