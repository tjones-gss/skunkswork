#!/usr/bin/env python3
"""Error Hook - Executed when an agent encounters an error."""
import json
from datetime import datetime
from pathlib import Path

def on_error_hook(agent_type: str, task: dict, error_info: dict, job_id: str):
    log_path = Path("logs") / f"errors_{datetime.now().strftime('%Y%m%d')}.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, 'a') as f:
        f.write(json.dumps({
            "event": "agent_error",
            "agent": agent_type,
            "job_id": job_id,
            "error": error_info
        }) + '\n')
    
    attempt = error_info.get("attempt", 1)
    if attempt < 3:
        return {"action": "retry", "retry_delay": 5 ** attempt}
    return {"action": "skip"}
