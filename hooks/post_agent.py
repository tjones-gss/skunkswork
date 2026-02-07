#!/usr/bin/env python3
"""Post-Agent Hook - Executed after each agent completes."""
import json
from datetime import datetime
from pathlib import Path

def post_agent_hook(agent_type: str, task: dict, result: dict, job_id: str):
    log_path = Path("logs") / f"hooks_{datetime.now().strftime('%Y%m%d')}.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, 'a') as f:
        f.write(json.dumps({
            "event": "agent_complete",
            "agent": agent_type,
            "job_id": job_id,
            "success": result.get("success", True)
        }) + '\n')
    return result
