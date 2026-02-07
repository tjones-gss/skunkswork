"""
Tests for StructuredLogger with JSON file logging and rotation.

Phase 2: Structured Logging Hardening
"""

import json
import logging

# =============================================================================
# TEST JSON FORMATTER
# =============================================================================


class TestJsonFormatter:
    """Tests for the JsonFormatter class."""

    def test_format_returns_valid_json(self):
        """format() returns a valid JSON string."""
        from skills.common.SKILL import JsonFormatter

        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="Test message", args=(), exc_info=None,
        )
        record.extra_fields = {"agent": "test_agent", "job_id": "123"}

        result = formatter.format(record)
        parsed = json.loads(result)

        assert parsed["level"] == "INFO"
        assert "Test message" in parsed["message"]
        assert parsed["agent"] == "test_agent"
        assert parsed["job_id"] == "123"

    def test_format_includes_timestamp(self):
        """format() includes timestamp in output."""
        from skills.common.SKILL import JsonFormatter

        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="Test", args=(), exc_info=None,
        )
        record.extra_fields = {}

        result = formatter.format(record)
        parsed = json.loads(result)

        assert "timestamp" in parsed

    def test_format_without_extra_fields(self):
        """format() works without extra_fields attribute."""
        from skills.common.SKILL import JsonFormatter

        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.WARNING, pathname="", lineno=0,
            msg="Warning message", args=(), exc_info=None,
        )
        # No extra_fields attribute

        result = formatter.format(record)
        parsed = json.loads(result)

        assert parsed["level"] == "WARNING"
        assert "Warning message" in parsed["message"]

    def test_format_handles_all_levels(self):
        """format() handles all log levels."""
        from skills.common.SKILL import JsonFormatter

        formatter = JsonFormatter()
        levels = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]
        level_names = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

        for level, name in zip(levels, level_names, strict=True):
            record = logging.LogRecord(
                name="test", level=level, pathname="", lineno=0,
                msg="Test", args=(), exc_info=None,
            )
            record.extra_fields = {}
            result = formatter.format(record)
            parsed = json.loads(result)
            assert parsed["level"] == name


# =============================================================================
# TEST STRUCTURED LOGGER INITIALIZATION
# =============================================================================


class TestStructuredLoggerInit:
    """Tests for StructuredLogger initialization."""

    def test_init_creates_logger(self):
        """Logger is created with correct name."""
        from skills.common.SKILL import StructuredLogger

        # Use a unique name to avoid handler accumulation
        logger = StructuredLogger("test_init_agent_1234", "job-1")

        assert logger.agent_type == "test_init_agent_1234"
        assert logger.job_id == "job-1"
        assert logger._json_file_handler is None

    def test_init_without_job_id(self):
        """Logger works without job_id."""
        from skills.common.SKILL import StructuredLogger

        logger = StructuredLogger("test_no_job_4567")

        assert logger.job_id is None


# =============================================================================
# TEST FILE LOGGING SETUP
# =============================================================================


class TestFileLogging:
    """Tests for setup_file_logging()."""

    def test_creates_log_directory(self, tmp_path):
        """setup_file_logging() creates the log directory."""
        from skills.common.SKILL import StructuredLogger

        log_dir = tmp_path / "custom_logs"
        logger = StructuredLogger("test_dir_create_agent")
        logger.setup_file_logging(log_dir=str(log_dir))

        assert log_dir.exists()

    def test_creates_log_file(self, tmp_path):
        """setup_file_logging() creates the log file on first write."""
        from skills.common.SKILL import StructuredLogger

        log_dir = tmp_path / "logs"
        logger = StructuredLogger("test_file_agent")
        logger.setup_file_logging(log_dir=str(log_dir))

        logger.info("Test message")

        log_file = log_dir / "test_file_agent.log"
        assert log_file.exists()

    def test_file_contains_valid_json(self, tmp_path):
        """Each line in the log file is valid JSON."""
        from skills.common.SKILL import StructuredLogger

        log_dir = tmp_path / "logs"
        logger = StructuredLogger("test_json_valid_agent")
        logger.setup_file_logging(log_dir=str(log_dir))

        logger.info("First message", key1="value1")
        logger.warning("Second message", key2="value2")

        log_file = log_dir / "test_json_valid_agent.log"
        lines = log_file.read_text().strip().split("\n")

        assert len(lines) == 2
        for line in lines:
            parsed = json.loads(line)
            assert "level" in parsed
            assert "message" in parsed
            assert "timestamp" in parsed

    def test_extra_kwargs_in_json(self, tmp_path):
        """Extra kwargs appear in JSON output."""
        from skills.common.SKILL import StructuredLogger

        log_dir = tmp_path / "logs"
        logger = StructuredLogger("test_kwargs_agent")
        logger.setup_file_logging(log_dir=str(log_dir))

        logger.info("Message with extras", records_count=42, association="PMA")

        log_file = log_dir / "test_kwargs_agent.log"
        content = log_file.read_text().strip()
        parsed = json.loads(content)

        assert parsed["records_count"] == 42
        assert parsed["association"] == "PMA"

    def test_stdout_still_works_with_file_logging(self, tmp_path):
        """Stdout handler still works alongside file handler."""
        from skills.common.SKILL import StructuredLogger

        log_dir = tmp_path / "logs"
        logger = StructuredLogger("test_both_handlers_agent")
        initial_handler_count = len(logger.logger.handlers)

        logger.setup_file_logging(log_dir=str(log_dir))

        # Should have one more handler now
        assert len(logger.logger.handlers) == initial_handler_count + 1

    def test_all_levels_emit_to_file(self, tmp_path):
        """All log levels write to file."""
        from skills.common.SKILL import StructuredLogger

        log_dir = tmp_path / "logs"
        logger = StructuredLogger("test_all_levels_agent")
        logger.logger.setLevel(logging.DEBUG)
        logger.setup_file_logging(log_dir=str(log_dir))

        logger.debug("debug msg")
        logger.info("info msg")
        logger.warning("warning msg")
        logger.error("error msg")

        log_file = log_dir / "test_all_levels_agent.log"
        lines = log_file.read_text().strip().split("\n")

        assert len(lines) == 4
        levels = [json.loads(line)["level"] for line in lines]
        assert "DEBUG" in levels
        assert "INFO" in levels
        assert "WARNING" in levels
        assert "ERROR" in levels

    def test_rotation_at_max_bytes(self, tmp_path):
        """Log file rotates when max_bytes is exceeded."""
        from skills.common.SKILL import StructuredLogger

        log_dir = tmp_path / "logs"
        logger = StructuredLogger("test_rotation_agent")
        # Very small max_bytes to trigger rotation quickly
        logger.setup_file_logging(
            log_dir=str(log_dir), max_bytes=200, backup_count=3,
        )

        for i in range(50):
            logger.info(f"Message number {i} with some padding data")

        # Check that backup files were created
        log_files = list(log_dir.glob("test_rotation_agent.log*"))
        assert len(log_files) > 1  # Main + at least 1 backup

    def test_backup_count_limits_files(self, tmp_path):
        """Rotation respects backup_count limit."""
        from skills.common.SKILL import StructuredLogger

        log_dir = tmp_path / "logs"
        logger = StructuredLogger("test_backup_count_agent")
        logger.setup_file_logging(
            log_dir=str(log_dir), max_bytes=100, backup_count=2,
        )

        for i in range(100):
            logger.info(f"Msg {i} with padding to fill the file quickly pad pad pad")

        log_files = list(log_dir.glob("test_backup_count_agent.log*"))
        # Should have at most main + 2 backups = 3 files
        assert len(log_files) <= 3

    def test_file_handler_stored_on_instance(self, tmp_path):
        """File handler is stored as _json_file_handler."""
        from skills.common.SKILL import StructuredLogger

        log_dir = tmp_path / "logs"
        logger = StructuredLogger("test_handler_ref_agent")
        logger.setup_file_logging(log_dir=str(log_dir))

        assert logger._json_file_handler is not None


# =============================================================================
# TEST _format METHOD
# =============================================================================


class TestStructuredLoggerFormat:
    """Tests for _format() method."""

    def test_format_includes_agent_type(self):
        """_format() includes agent type in output."""
        from skills.common.SKILL import StructuredLogger

        logger = StructuredLogger("my_agent", "job-abc")
        result = logger._format("Hello world")

        assert "agent=my_agent" in result
        assert "job_id=job-abc" in result

    def test_format_includes_kwargs(self):
        """_format() includes extra kwargs."""
        from skills.common.SKILL import StructuredLogger

        logger = StructuredLogger("agent", "job")
        result = logger._format("msg", records=10, url="https://example.com")

        assert "records=10" in result
        assert "url=https://example.com" in result

    def test_format_skips_none_values(self):
        """_format() skips None values in context."""
        from skills.common.SKILL import StructuredLogger

        logger = StructuredLogger("agent")
        result = logger._format("msg", key=None, other="value")

        assert "key=" not in result
        assert "other=value" in result
