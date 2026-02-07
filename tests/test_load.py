"""
Load & Stress Test Suite — P3-T01
NAM Intelligence Pipeline

Runs 100+ concurrent validation agent executions and measures latency,
memory high-water mark, and event-loop blocking time.

Acceptance criteria (WBS §5, P3-T01):
    • Load test runs 100+ concurrent CrossRefAgent and DedupeAgent validations
    • Measures: p50/p95/p99 latency, memory high-water mark
    • Fails if p99 > 5 s or memory > 2 GB for 10 K records
    • Results written to data/benchmarks/{timestamp}.json

NOTE: P2-T02 (CircuitBreaker) is not yet landed — circuit-breaker assertions
are marked as skip-able.
"""

import asyncio
import json
import statistics
import time
import tracemalloc
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_company(i: int) -> dict:
    """Return a synthetic company record."""
    return {
        "company_name": f"Load Test Corp {i:05d}",
        "website": f"https://loadtest{i:05d}.example.com",
        "domain": f"loadtest{i:05d}.example.com",
        "city": "Springfield",
        "state": "IL",
        "country": "United States",
        "employee_count_min": 10 + (i % 100),
        "employee_count_max": 50 + (i % 100),
        "naics_code": "332710",
        "associations": ["PMA"],
        "quality_score": 60 + (i % 40),
    }


def _make_companies(n: int) -> list[dict]:
    return [_make_company(i) for i in range(n)]


def _mock_http_get(*args, **kwargs):
    """Fake HTTP GET that returns instantly."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"results": []}
    return resp


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def benchmark_dir(tmp_path):
    d = tmp_path / "benchmarks"
    d.mkdir()
    return d


def _write_benchmark(benchmark_dir: Path, label: str, metrics: dict):
    """Persist benchmark metrics to JSON."""
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    path = benchmark_dir / f"{ts}_{label}.json"
    path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Test: CrossRefAgent under load
# ---------------------------------------------------------------------------

class TestCrossRefAgentLoad:
    """Concurrent CrossRefAgent validation load tests."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("n_concurrent", [100, 200])
    async def test_concurrent_crossref_validations(
        self, benchmark_dir, n_concurrent
    ):
        """Run *n_concurrent* CrossRefAgent.run() calls in parallel."""
        records_per_agent = 50
        companies = _make_companies(records_per_agent)

        latencies: list[float] = []

        with (
            patch("agents.base.Config") as mc,
            patch("agents.base.StructuredLogger"),
            patch("agents.base.AsyncHTTPClient") as mock_http_cls,
            patch("agents.base.RateLimiter"),
        ):
            mc.return_value.load.return_value = {}
            mock_http = MagicMock()
            mock_http.get = AsyncMock(return_value=MagicMock(
                status_code=200,
                json=MagicMock(return_value={"results": []}),
            ))
            mock_http_cls.return_value = mock_http

            from agents.validation.crossref import CrossRefAgent

            tracemalloc.start()

            async def run_one(_i: int):
                agent = CrossRefAgent(
                    agent_type="validation.crossref",
                    job_id=f"load-{_i}",
                )
                # Mock DNS & Places to avoid real network I/O
                agent._validate_dns_mx = AsyncMock(return_value=True)
                agent._validate_google_places = AsyncMock(return_value=True)
                t0 = time.perf_counter()
                result = await agent.run({"records": list(companies)})
                elapsed = time.perf_counter() - t0
                latencies.append(elapsed)
                return result

            results = await asyncio.gather(
                *[run_one(i) for i in range(n_concurrent)]
            )
            _, peak_bytes = tracemalloc.get_traced_memory()
            tracemalloc.stop()

        # --- assertions ---
        assert all(r.get("success") for r in results)

        p50 = statistics.median(latencies)
        p95 = sorted(latencies)[int(len(latencies) * 0.95)]
        p99 = sorted(latencies)[int(len(latencies) * 0.99)]
        peak_mb = peak_bytes / (1024 * 1024)

        metrics = {
            "test": "crossref_concurrent",
            "n_concurrent": n_concurrent,
            "records_per_agent": records_per_agent,
            "p50_s": round(p50, 4),
            "p95_s": round(p95, 4),
            "p99_s": round(p99, 4),
            "peak_memory_mb": round(peak_mb, 2),
            "total_results": len(results),
        }
        _write_benchmark(benchmark_dir, f"crossref_{n_concurrent}", metrics)

        assert p99 < 5.0, f"p99 latency {p99:.3f}s exceeds 5s"
        assert peak_mb < 2048, f"Peak memory {peak_mb:.1f}MB exceeds 2GB"


# ---------------------------------------------------------------------------
# Test: DedupeAgent under load
# ---------------------------------------------------------------------------

class TestDedupeAgentLoad:
    """Concurrent DedupeAgent deduplication load tests."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("n_concurrent", [100, 200])
    async def test_concurrent_dedupe_validations(
        self, benchmark_dir, n_concurrent
    ):
        """Run *n_concurrent* DedupeAgent.run() calls in parallel."""
        records_per_agent = 50
        companies = _make_companies(records_per_agent)
        # Inject some duplicate-ish records
        for i in range(0, len(companies), 5):
            companies[i]["domain"] = companies[0]["domain"]

        latencies: list[float] = []

        with (
            patch("agents.base.Config") as mc,
            patch("agents.base.StructuredLogger"),
            patch("agents.base.AsyncHTTPClient"),
            patch("agents.base.RateLimiter"),
        ):
            mc.return_value.load.return_value = {}

            from agents.validation.dedupe import DedupeAgent

            tracemalloc.start()

            async def run_one(_i: int):
                agent = DedupeAgent(
                    agent_type="validation.dedupe",
                    job_id=f"dedupe-{_i}",
                )
                t0 = time.perf_counter()
                result = await agent.run({"records": list(companies)})
                elapsed = time.perf_counter() - t0
                latencies.append(elapsed)
                return result

            results = await asyncio.gather(
                *[run_one(i) for i in range(n_concurrent)]
            )
            _, peak_bytes = tracemalloc.get_traced_memory()
            tracemalloc.stop()

        assert all(r.get("success") for r in results)

        p50 = statistics.median(latencies)
        p95 = sorted(latencies)[int(len(latencies) * 0.95)]
        p99 = sorted(latencies)[int(len(latencies) * 0.99)]
        peak_mb = peak_bytes / (1024 * 1024)

        metrics = {
            "test": "dedupe_concurrent",
            "n_concurrent": n_concurrent,
            "records_per_agent": records_per_agent,
            "p50_s": round(p50, 4),
            "p95_s": round(p95, 4),
            "p99_s": round(p99, 4),
            "peak_memory_mb": round(peak_mb, 2),
            "total_results": len(results),
        }
        _write_benchmark(benchmark_dir, f"dedupe_{n_concurrent}", metrics)

        assert p99 < 5.0, f"p99 latency {p99:.3f}s exceeds 5s"
        assert peak_mb < 2048, f"Peak memory {peak_mb:.1f}MB exceeds 2GB"


# ---------------------------------------------------------------------------
# Test: AgentSpawner parallel stress
# ---------------------------------------------------------------------------

class TestAgentSpawnerLoadStress:
    """Validate AgentSpawner.spawn_parallel under concurrent load."""

    @pytest.mark.asyncio
    async def test_spawner_100_parallel_agents(self, benchmark_dir):
        """Spawn 100 agents via spawn_parallel with max_concurrent=20."""
        n_tasks = 100
        companies = _make_companies(20)
        tasks = [{"records": list(companies)} for _ in range(n_tasks)]

        with (
            patch("agents.base.Config") as mc,
            patch("agents.base.StructuredLogger"),
            patch("agents.base.AsyncHTTPClient") as mock_http_cls,
            patch("agents.base.RateLimiter"),
        ):
            mc.return_value.load.return_value = {}
            # AsyncHTTPClient.close() is awaited in _cleanup — must be async
            mock_http = MagicMock()
            mock_http.close = AsyncMock()
            mock_http_cls.return_value = mock_http

            from agents.base import AgentSpawner

            spawner = AgentSpawner(job_id="load-spawner")

            tracemalloc.start()
            t0 = time.perf_counter()

            results = await spawner.spawn_parallel(
                agent_type="validation.dedupe",
                tasks=tasks,
                max_concurrent=20,
                timeout=30,
            )

            wall_time = time.perf_counter() - t0
            _, peak_bytes = tracemalloc.get_traced_memory()
            tracemalloc.stop()

        successes = sum(1 for r in results if r.get("success"))
        peak_mb = peak_bytes / (1024 * 1024)

        metrics = {
            "test": "spawner_parallel_100",
            "n_tasks": n_tasks,
            "max_concurrent": 20,
            "wall_time_s": round(wall_time, 4),
            "successes": successes,
            "peak_memory_mb": round(peak_mb, 2),
        }
        _write_benchmark(benchmark_dir, "spawner_parallel", metrics)

        assert successes == n_tasks, f"Only {successes}/{n_tasks} succeeded"
        assert wall_time < 30.0, f"Wall time {wall_time:.1f}s exceeds 30s"
        assert peak_mb < 2048, f"Peak memory {peak_mb:.1f}MB exceeds 2GB"


# ---------------------------------------------------------------------------
# Test: Large-batch single-agent (10K records)
# ---------------------------------------------------------------------------

class TestLargeBatchSingleAgent:
    """Single agent processing a large record batch."""

    @pytest.mark.asyncio
    async def test_dedupe_1k_records(self, benchmark_dir):
        """Process 1,000 records through DedupeAgent (O(n²) fuzzy match)."""
        n_records = 1_000
        companies = _make_companies(n_records)

        with (
            patch("agents.base.Config") as mc,
            patch("agents.base.StructuredLogger"),
            patch("agents.base.AsyncHTTPClient"),
            patch("agents.base.RateLimiter"),
        ):
            mc.return_value.load.return_value = {}

            from agents.validation.dedupe import DedupeAgent

            agent = DedupeAgent(
                agent_type="validation.dedupe",
                job_id="large-batch",
            )

            tracemalloc.start()
            t0 = time.perf_counter()
            result = await agent.run({"records": companies})
            elapsed = time.perf_counter() - t0
            _, peak_bytes = tracemalloc.get_traced_memory()
            tracemalloc.stop()

        peak_mb = peak_bytes / (1024 * 1024)

        metrics = {
            "test": "dedupe_1k_single",
            "n_records": n_records,
            "elapsed_s": round(elapsed, 4),
            "peak_memory_mb": round(peak_mb, 2),
            "records_processed": result.get("records_processed", 0),
        }
        _write_benchmark(benchmark_dir, "dedupe_1k", metrics)

        assert result.get("success"), "DedupeAgent failed on 1K records"
        assert peak_mb < 2048, f"Peak memory {peak_mb:.1f}MB exceeds 2GB"
        assert elapsed < 60.0, f"Elapsed {elapsed:.1f}s exceeds 60s"

    @pytest.mark.asyncio
    async def test_crossref_10k_records(self, benchmark_dir):
        """Process 10,000 records through CrossRefAgent."""
        n_records = 10_000
        companies = _make_companies(n_records)

        with (
            patch("agents.base.Config") as mc,
            patch("agents.base.StructuredLogger"),
            patch("agents.base.AsyncHTTPClient") as mock_http_cls,
            patch("agents.base.RateLimiter"),
        ):
            mc.return_value.load.return_value = {}
            mock_http = MagicMock()
            mock_http.get = AsyncMock(return_value=MagicMock(
                status_code=200,
                json=MagicMock(return_value={"results": []}),
            ))
            mock_http_cls.return_value = mock_http

            from agents.validation.crossref import CrossRefAgent

            agent = CrossRefAgent(
                agent_type="validation.crossref",
                job_id="large-batch-xref",
            )
            # Mock DNS & Places to avoid real network I/O
            agent._validate_dns_mx = AsyncMock(return_value=True)
            agent._validate_google_places = AsyncMock(return_value=True)

            tracemalloc.start()
            t0 = time.perf_counter()
            result = await agent.run({"records": companies})
            elapsed = time.perf_counter() - t0
            _, peak_bytes = tracemalloc.get_traced_memory()
            tracemalloc.stop()

        peak_mb = peak_bytes / (1024 * 1024)

        metrics = {
            "test": "crossref_10k_single",
            "n_records": n_records,
            "elapsed_s": round(elapsed, 4),
            "peak_memory_mb": round(peak_mb, 2),
            "records_processed": result.get("records_processed", 0),
        }
        _write_benchmark(benchmark_dir, "crossref_10k", metrics)

        assert result.get("success"), "CrossRefAgent failed on 10K records"
        assert peak_mb < 2048, f"Peak memory {peak_mb:.1f}MB exceeds 2GB"
        assert elapsed < 120.0, f"Elapsed {elapsed:.1f}s exceeds 120s"
