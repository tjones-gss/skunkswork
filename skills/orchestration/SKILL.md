# Orchestration Skill

## Overview

The Orchestrator coordinates all pipeline operations, manages state, spawns sub-agents, and handles errors.

---

## Responsibilities

1. **Workflow Planning**: Create execution plans based on tasks
2. **Agent Spawning**: Dispatch sub-agents for specific operations
3. **State Management**: Track progress and enable resume from checkpoints
4. **Error Recovery**: Coordinate retries and graceful degradation
5. **Result Aggregation**: Combine outputs into unified dataset

---

## Workflow Modes

### Mode: Extract
```python
async def run_extraction(associations: list[str]):
    for association in associations:
        # 1. Discovery
        site_map = await spawn_agent("discovery/site_mapper", {
            "base_url": ASSOCIATIONS[association]["url"],
            "directory_patterns": ["/members", "/directory", "/member-list"]
        })
        
        # 2. Crawl URLs
        urls = await spawn_agent("discovery/link_crawler", {
            "entry_url": site_map["directory_url"],
            "pagination": site_map["pagination"]
        })
        
        # 3. Extract records
        records = await spawn_parallel("extraction/html_parser", 
            [{"url": u, "schema": association} for u in urls],
            max_concurrent=5
        )
        
        # 4. Save
        save_jsonl(f"data/raw/{association}/records.jsonl", records)
```

### Mode: Enrich
```python
async def run_enrichment(associations: list[str]):
    for association in associations:
        records = load_jsonl(f"data/raw/{association}/records.jsonl")
        
        # Batch processing
        for batch in chunks(records, 100):
            # Firmographics
            batch = await spawn_agent("enrichment/firmographic", {
                "records": batch,
                "providers": ["clearbit", "zoominfo"]
            })
            
            # Tech Stack
            batch = await spawn_agent("enrichment/tech_stack", {
                "records": batch,
                "methods": ["builtwith", "job_postings"]
            })
            
            # Contacts (only for qualified companies)
            qualified = [r for r in batch if r.get("employee_count_min", 0) > 50]
            if qualified:
                batch = await spawn_agent("enrichment/contact_finder", {
                    "records": qualified,
                    "target_titles": ["CIO", "VP IT", "COO", "CFO"]
                })
        
        save_jsonl(f"data/processed/{association}/enriched.jsonl", records)
```

### Mode: Validate
```python
async def run_validation():
    # Load all processed records
    all_records = []
    for path in glob("data/processed/*/enriched.jsonl"):
        all_records.extend(load_jsonl(path))
    
    # Deduplicate
    records = await spawn_agent("validation/dedupe", {
        "records": all_records,
        "threshold": 0.85
    })
    
    # Cross-reference
    records = await spawn_agent("validation/crossref", {
        "records": records,
        "validators": ["dns", "google_places"]
    })
    
    # Score
    records = await spawn_agent("validation/scorer", {
        "records": records,
        "min_score": 60
    })
    
    # Filter and save
    final = [r for r in records if r["quality_score"] >= 60]
    save_jsonl("data/validated/final_dataset.jsonl", final)
```

---

## State Management

### Checkpoint Format
```json
{
  "job_id": "uuid",
  "mode": "extract|enrich|validate",
  "association": "PMA",
  "stage": "discovery|extraction|enrichment|validation",
  "progress": {
    "total": 1134,
    "completed": 500,
    "failed": 3
  },
  "last_processed": "https://pma.org/member/500",
  "timestamp": "2026-01-29T12:00:00Z",
  "can_resume": true
}
```

### Save Checkpoint
```python
def save_checkpoint(job_id: str, stage: str, progress: dict):
    checkpoint = {
        "job_id": job_id,
        "stage": stage,
        "progress": progress,
        "timestamp": datetime.utcnow().isoformat(),
        "can_resume": True
    }
    Path(f"data/.checkpoints/{job_id}.json").write_text(json.dumps(checkpoint))
```

### Resume from Checkpoint
```python
def resume_from_checkpoint(job_id: str):
    checkpoint = json.loads(Path(f"data/.checkpoints/{job_id}.json").read_text())
    if checkpoint["can_resume"]:
        # Resume from last_processed
        return checkpoint
    raise CannotResumeError(job_id)
```

---

## Agent Spawning

### Spawn Single Agent
```python
async def spawn_agent(agent_type: str, task: dict, timeout: int = 300):
    """
    Spawn a single sub-agent and wait for result.
    """
    # Read agent's skill file
    skill_path = f"skills/{agent_type.split('/')[0]}/SKILL.md"
    
    # Execute agent logic based on skill
    result = await execute_agent(agent_type, task, timeout)
    
    return result
```

### Spawn Parallel Agents
```python
async def spawn_parallel(agent_type: str, tasks: list[dict], max_concurrent: int = 5):
    """
    Spawn multiple agents in parallel with concurrency limit.
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def limited_spawn(task):
        async with semaphore:
            return await spawn_agent(agent_type, task)
    
    results = await asyncio.gather(*[limited_spawn(t) for t in tasks])
    return results
```

---

## Error Handling

### Retry Policy
```python
RETRY_CONFIG = {
    "max_retries": 3,
    "backoff_base": 1,
    "backoff_multiplier": 5,
    "max_backoff": 300,
    "retryable_errors": [
        "TimeoutError",
        "ConnectionError",
        "RateLimitError",
        "HTTPError_5xx"
    ]
}
```

### Error Categories
| Category | Examples | Action |
|----------|----------|--------|
| Retryable | Timeout, 429, 5xx | Retry with backoff |
| Skippable | 404, 403, ParseError | Log and continue |
| Critical | DB down, API key invalid | Halt pipeline |

### Handle Error
```python
def handle_error(error: Exception, task: dict, attempt: int) -> str:
    error_type = type(error).__name__
    
    if error_type in RETRY_CONFIG["retryable_errors"]:
        if attempt < RETRY_CONFIG["max_retries"]:
            delay = min(
                RETRY_CONFIG["backoff_base"] * (RETRY_CONFIG["backoff_multiplier"] ** attempt),
                RETRY_CONFIG["max_backoff"]
            )
            return f"retry:{delay}"
        return "skip"
    
    if error_type in ["NotFoundError", "ForbiddenError", "ParseError"]:
        return "skip"
    
    return "abort"  # Critical error
```

---

## Logging

### Log Format
```
{timestamp} | {job_id} | {agent_type} | {level} | {message}
```

### Example Logs
```
2026-01-29T12:00:00Z | job-123 | orchestrator | INFO | Starting extraction for PMA
2026-01-29T12:00:01Z | job-123 | discovery/site_mapper | INFO | Found directory at /directory
2026-01-29T12:00:05Z | job-123 | discovery/link_crawler | INFO | Discovered 1134 member URLs
2026-01-29T12:01:00Z | job-123 | extraction/html_parser | INFO | Extracted 100/1134 records
2026-01-29T12:01:30Z | job-123 | extraction/html_parser | WARNING | Failed to parse https://... - skipping
```

---

## Configuration

### associations.yaml
```yaml
PMA:
  name: "Precision Metalforming Association"
  url: "https://pma.org"
  directory_url: "https://pma.org/directory/results.asp?n=2000"
  pagination:
    type: "query_param"
    param: "n"
  expected_members: 1134
  rate_limit: 0.5

NEMA:
  name: "National Electrical Manufacturers Association"
  url: "https://makeitelectric.org"
  directory_url: "https://makeitelectric.org/membership/membership-directory"
  pagination:
    type: "pages"
  expected_members: 300
  rate_limit: 0.5
```

---

## Best Practices

1. **Always checkpoint** before expensive operations
2. **Batch API calls** to respect rate limits
3. **Log extensively** - debugging agents is hard
4. **Fail fast** on critical errors
5. **Validate incrementally** - don't wait until the end
6. **Monitor memory** - large datasets can exhaust resources
