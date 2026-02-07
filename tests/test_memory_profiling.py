"""
Memory Profiling Tests — P3-T04
NAM Intelligence Pipeline

Profile memory usage during large-batch record I/O and verify that
streaming helpers keep peak memory within acceptable bounds.

Acceptance criteria (WBS §5, P3-T04):
    • Memory at 10K records < 500 MB
    • Streaming load uses significantly less memory than load_records()
    • save_records() accepts generators without materializing full list
"""

import json
import tracemalloc
from pathlib import Path
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Helper: create a concrete BaseAgent
# ---------------------------------------------------------------------------

def _make_agent(job_id: str = "mem-test"):
    """Instantiate a minimal BaseAgent subclass with mocked infra."""
    with (
        patch("agents.base.Config") as mc,
        patch("agents.base.StructuredLogger"),
        patch("agents.base.AsyncHTTPClient"),
        patch("agents.base.RateLimiter"),
    ):
        mc.return_value.load.return_value = {}

        from agents.base import BaseAgent

        class MemTestAgent(BaseAgent):
            async def run(self, task):
                return {"success": True, "records_processed": 0}

        return MemTestAgent(agent_type="test.memory", job_id=job_id)


def _generate_records(n: int):
    """Yield *n* synthetic company dicts (generator — never all in memory)."""
    for i in range(n):
        yield {
            "company_name": f"Test Company {i:06d}",
            "website": f"https://company{i:06d}.example.com",
            "domain": f"company{i:06d}.example.com",
            "city": "Springfield",
            "state": "IL",
            "country": "United States",
            "employee_count_min": 10 + i,
            "employee_count_max": 50 + i,
            "naics_code": "332710",
            "associations": ["PMA"],
            "quality_score": 75,
        }


def _write_jsonl_file(path: Path, n: int):
    """Write *n* records to a JSONL file on disk."""
    with open(path, "w", encoding="utf-8") as f:
        for rec in _generate_records(n):
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSaveRecordsStreaming:
    """save_records() accepts generators and writes without buffering all."""

    def test_save_with_generator_source(self, tmp_path):
        agent = _make_agent()
        out = tmp_path / "gen_output.jsonl"

        count = agent.save_records(_generate_records(500), str(out))

        assert count == 500
        lines = out.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 500

    def test_save_with_list_still_works(self, tmp_path):
        agent = _make_agent()
        out = tmp_path / "list_output.jsonl"
        records = list(_generate_records(100))

        count = agent.save_records(records, str(out))

        assert count == 100


class TestLoadRecordsIter:
    """load_records_iter() yields records without loading all into memory."""

    def test_iter_yields_correct_count(self, tmp_path):
        agent = _make_agent()
        fpath = tmp_path / "input.jsonl"
        _write_jsonl_file(fpath, 200)

        count = sum(1 for _ in agent.load_records_iter(str(fpath)))
        assert count == 200

    def test_iter_returns_empty_for_missing_file(self, tmp_path):
        agent = _make_agent()
        count = sum(1 for _ in agent.load_records_iter(str(tmp_path / "nope.jsonl")))
        assert count == 0

    def test_iter_records_match_load_records(self, tmp_path):
        agent = _make_agent()
        fpath = tmp_path / "cmp.jsonl"
        _write_jsonl_file(fpath, 50)

        via_list = agent.load_records(str(fpath))
        via_iter = list(agent.load_records_iter(str(fpath)))

        assert via_list == via_iter


class TestMemoryProfile:
    """Tracemalloc-based profiling of record I/O at scale."""

    @pytest.mark.parametrize("n_records", [1_000, 10_000])
    def test_save_records_memory(self, tmp_path, n_records):
        """Peak memory during save_records with a generator source."""
        agent = _make_agent()
        out = tmp_path / f"save_{n_records}.jsonl"

        tracemalloc.start()
        agent.save_records(_generate_records(n_records), str(out))
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_mb = peak_bytes / (1024 * 1024)
        # With streaming write, peak should be well under 500 MB even at 10K
        assert peak_mb < 500, f"Peak memory {peak_mb:.1f} MB exceeds 500 MB"

    @pytest.mark.parametrize("n_records", [1_000, 10_000])
    def test_load_records_iter_memory(self, tmp_path, n_records):
        """Peak memory when iterating via load_records_iter."""
        agent = _make_agent()
        fpath = tmp_path / f"iter_{n_records}.jsonl"
        _write_jsonl_file(fpath, n_records)

        tracemalloc.start()
        for _rec in agent.load_records_iter(str(fpath)):
            pass  # consume without accumulating
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_mb = peak_bytes / (1024 * 1024)
        assert peak_mb < 500, f"Peak memory {peak_mb:.1f} MB exceeds 500 MB"

