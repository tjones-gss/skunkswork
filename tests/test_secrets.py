"""
Tests for middleware/secrets.py - SecretsManager

Tests the provider chain (Vault -> Env), TTL cache, invalidation,
thread safety, and BaseAgent integration.
"""

import threading
import time
from unittest.mock import MagicMock, patch

from middleware.secrets import (
    EnvSecretsProvider,
    SecretsManager,
    VaultSecretsProvider,
    _reset_secrets_manager,
    get_secrets_manager,
)

# =============================================================================
# FIXTURES
# =============================================================================


# =============================================================================
# TEST ENV SECRETS PROVIDER
# =============================================================================


class TestEnvSecretsProvider:
    """Tests for EnvSecretsProvider."""

    def test_returns_env_var(self, monkeypatch):
        """Returns value from os.environ."""
        monkeypatch.setenv("MY_SECRET", "hunter2")
        provider = EnvSecretsProvider()
        assert provider.get_secret("MY_SECRET") == "hunter2"

    def test_returns_none_for_missing(self, monkeypatch):
        """Returns None for missing env var."""
        monkeypatch.delenv("NONEXISTENT_KEY", raising=False)
        provider = EnvSecretsProvider()
        assert provider.get_secret("NONEXISTENT_KEY") is None

    def test_is_available_always_true(self):
        """is_available() always returns True."""
        provider = EnvSecretsProvider()
        assert provider.is_available() is True


# =============================================================================
# TEST VAULT SECRETS PROVIDER
# =============================================================================


class TestVaultSecretsProvider:
    """Tests for VaultSecretsProvider."""

    def test_reads_kv_v2_secret(self):
        """Reads a secret from Vault KV v2 (mocked hvac)."""
        mock_hvac = MagicMock()
        mock_client = MagicMock()
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"API_KEY": "vault-secret-123"}}
        }
        mock_hvac.Client.return_value = mock_client

        with patch.dict("sys.modules", {"hvac": mock_hvac}):
            provider = VaultSecretsProvider(
                addr="https://vault.test:8200",
                token="test-token",  # noqa: S106
            )
            provider._client = mock_client

            result = provider.get_secret("API_KEY")

        assert result == "vault-secret-123"

    def test_returns_none_for_missing_key(self):
        """Returns None when key is not in Vault data."""
        mock_client = MagicMock()
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"OTHER_KEY": "value"}}
        }

        provider = VaultSecretsProvider(
            addr="https://vault.test:8200",
            token="test-token",  # noqa: S106
        )
        provider._client = mock_client

        result = provider.get_secret("NONEXISTENT_KEY")
        assert result is None

    def test_authenticated_check(self):
        """is_available() returns True when client is authenticated."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True

        provider = VaultSecretsProvider(
            addr="https://vault.test:8200",
            token="test-token",  # noqa: S106
        )
        provider._client = mock_client

        assert provider.is_available() is True

    def test_unauthenticated_check(self):
        """is_available() returns False when client is not authenticated."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = False

        provider = VaultSecretsProvider(
            addr="https://vault.test:8200",
            token="test-token",  # noqa: S106
        )
        provider._client = mock_client

        assert provider.is_available() is False

    def test_no_client_returns_none(self):
        """Returns None when no client is configured."""
        provider = VaultSecretsProvider(addr="", token="")
        assert provider._client is None
        assert provider.get_secret("ANY_KEY") is None

    def test_connection_error_returns_none(self):
        """Returns None on Vault connection error."""
        mock_client = MagicMock()
        mock_client.secrets.kv.v2.read_secret_version.side_effect = (
            ConnectionError("Vault unreachable")
        )

        provider = VaultSecretsProvider(
            addr="https://vault.test:8200",
            token="test-token",  # noqa: S106
        )
        provider._client = mock_client

        result = provider.get_secret("API_KEY")
        assert result is None


# =============================================================================
# TEST SECRETS MANAGER
# =============================================================================


class TestSecretsManager:
    """Tests for SecretsManager."""

    def test_fallback_to_env_when_vault_unavailable(self, monkeypatch):
        """Falls back to EnvSecretsProvider when Vault returns None."""
        monkeypatch.setenv("MY_KEY", "env-value")

        vault = MagicMock(spec=VaultSecretsProvider)
        vault.get_secret.return_value = None

        env = EnvSecretsProvider()
        manager = SecretsManager(providers=[vault, env], cache_ttl=0)

        result = manager.get_secret("MY_KEY")
        assert result == "env-value"
        vault.get_secret.assert_called_once_with("MY_KEY")

    def test_vault_takes_priority_over_env(self, monkeypatch):
        """Vault value takes priority when available."""
        monkeypatch.setenv("MY_KEY", "env-value")

        vault = MagicMock(spec=VaultSecretsProvider)
        vault.get_secret.return_value = "vault-value"

        env = EnvSecretsProvider()
        manager = SecretsManager(providers=[vault, env], cache_ttl=0)

        result = manager.get_secret("MY_KEY")
        assert result == "vault-value"

    def test_cache_hit_within_ttl(self, monkeypatch):
        """Cache hit avoids querying providers again."""
        monkeypatch.setenv("MY_KEY", "cached-value")

        env = EnvSecretsProvider()
        manager = SecretsManager(providers=[env], cache_ttl=300)

        # First call populates cache
        result1 = manager.get_secret("MY_KEY")
        assert result1 == "cached-value"

        # Remove the env var — cache should still return the value
        monkeypatch.delenv("MY_KEY")
        result2 = manager.get_secret("MY_KEY")
        assert result2 == "cached-value"

    def test_cache_expiry_after_ttl(self, monkeypatch):
        """Cache expires after TTL and re-queries providers."""
        monkeypatch.setenv("MY_KEY", "original")

        env = EnvSecretsProvider()
        manager = SecretsManager(providers=[env], cache_ttl=0)  # 0 = always expire

        result1 = manager.get_secret("MY_KEY")
        assert result1 == "original"

        # Change value — should see new value since TTL is 0
        monkeypatch.setenv("MY_KEY", "updated")
        # Need to wait a tiny bit so monotonic clock advances
        time.sleep(0.001)
        result2 = manager.get_secret("MY_KEY")
        assert result2 == "updated"

    def test_invalidate_single_key(self, monkeypatch):
        """invalidate(key) clears one cache entry."""
        monkeypatch.setenv("KEY_A", "a")
        monkeypatch.setenv("KEY_B", "b")

        env = EnvSecretsProvider()
        manager = SecretsManager(providers=[env], cache_ttl=300)

        manager.get_secret("KEY_A")
        manager.get_secret("KEY_B")
        assert "KEY_A" in manager._cache
        assert "KEY_B" in manager._cache

        manager.invalidate("KEY_A")
        assert "KEY_A" not in manager._cache
        assert "KEY_B" in manager._cache

    def test_invalidate_all(self, monkeypatch):
        """invalidate() with no key clears entire cache."""
        monkeypatch.setenv("KEY_A", "a")
        monkeypatch.setenv("KEY_B", "b")

        env = EnvSecretsProvider()
        manager = SecretsManager(providers=[env], cache_ttl=300)

        manager.get_secret("KEY_A")
        manager.get_secret("KEY_B")

        manager.invalidate()
        assert len(manager._cache) == 0

    def test_auto_detect_without_vault(self, monkeypatch):
        """Auto-detect creates only EnvSecretsProvider when VAULT_ADDR not set."""
        monkeypatch.delenv("VAULT_ADDR", raising=False)
        monkeypatch.delenv("VAULT_TOKEN", raising=False)

        manager = SecretsManager()
        assert len(manager._providers) == 1
        assert isinstance(manager._providers[0], EnvSecretsProvider)

    def test_auto_detect_with_vault_configured(self, monkeypatch):
        """Auto-detect tries Vault when VAULT_ADDR and VAULT_TOKEN are set."""
        monkeypatch.setenv("VAULT_ADDR", "https://vault.test:8200")
        monkeypatch.setenv("VAULT_TOKEN", "test-token")

        # Mock hvac so VaultSecretsProvider can construct
        mock_hvac = MagicMock()
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_hvac.Client.return_value = mock_client

        with patch.dict("sys.modules", {"hvac": mock_hvac}):
            manager = SecretsManager()

        # Should have Vault + Env
        assert len(manager._providers) == 2
        assert isinstance(manager._providers[0], VaultSecretsProvider)
        assert isinstance(manager._providers[1], EnvSecretsProvider)

    def test_thread_safe_concurrent_access(self, monkeypatch):
        """Multiple threads can safely access get_secret concurrently."""
        monkeypatch.setenv("THREAD_KEY", "thread-value")

        env = EnvSecretsProvider()
        manager = SecretsManager(providers=[env], cache_ttl=300)

        results = []
        errors = []

        def read_secret():
            try:
                for _ in range(50):
                    val = manager.get_secret("THREAD_KEY")
                    results.append(val)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=read_secret) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert all(v == "thread-value" for v in results)
        assert len(results) == 500  # 10 threads x 50 reads

    def test_provider_exception_falls_through(self, monkeypatch):
        """Exception in one provider falls through to next."""
        monkeypatch.setenv("MY_KEY", "fallback-value")

        broken_provider = MagicMock(spec=VaultSecretsProvider)
        broken_provider.get_secret.side_effect = RuntimeError("Vault exploded")

        env = EnvSecretsProvider()
        manager = SecretsManager(providers=[broken_provider, env], cache_ttl=0)

        result = manager.get_secret("MY_KEY")
        assert result == "fallback-value"


# =============================================================================
# TEST SINGLETON
# =============================================================================


class TestGetSecretsManager:
    """Tests for get_secrets_manager singleton factory."""

    def test_returns_same_instance(self):
        """get_secrets_manager() returns the same instance."""
        mgr1 = get_secrets_manager()
        mgr2 = get_secrets_manager()
        assert mgr1 is mgr2

    def test_reset_clears_singleton(self):
        """_reset_secrets_manager() clears the singleton."""
        mgr1 = get_secrets_manager()
        _reset_secrets_manager()
        mgr2 = get_secrets_manager()
        assert mgr1 is not mgr2


# =============================================================================
# TEST BASEAGENT INTEGRATION
# =============================================================================


class TestBaseAgentSecretsIntegration:
    """Tests for BaseAgent.get_secret() integration."""

    def test_get_secret_returns_env_value(self, monkeypatch):
        """BaseAgent.get_secret() returns value from environment."""
        monkeypatch.setenv("TEST_API_KEY", "test-value-123")

        with patch("agents.base.Config") as mock_config, \
             patch("agents.base.StructuredLogger"), \
             patch("agents.base.AsyncHTTPClient"), \
             patch("agents.base.RateLimiter"):

            mock_config.return_value.load.return_value = {}

            from agents.enrichment.firmographic import FirmographicAgent
            agent = FirmographicAgent(
                agent_type="enrichment.firmographic",
                job_id="test-job"
            )

            result = agent.get_secret("TEST_API_KEY")
            assert result == "test-value-123"

    def test_check_api_keys_uses_secrets_manager(self, monkeypatch):
        """_check_api_keys() uses get_secret instead of os.environ.get."""
        monkeypatch.setenv("CLEARBIT_API_KEY", "key1")
        monkeypatch.setenv("APOLLO_API_KEY", "key2")

        with patch("agents.base.Config") as mock_config, \
             patch("agents.base.StructuredLogger"), \
             patch("agents.base.AsyncHTTPClient"), \
             patch("agents.base.RateLimiter"):

            mock_config.return_value.load.return_value = {}

            from agents.enrichment.firmographic import FirmographicAgent
            agent = FirmographicAgent(
                agent_type="enrichment.firmographic",
                job_id="test-job"
            )

            missing = agent._check_api_keys()
            assert missing == []  # Both keys are set

    def test_check_api_keys_reports_missing(self, monkeypatch):
        """_check_api_keys() reports missing keys via secrets manager."""
        monkeypatch.delenv("CLEARBIT_API_KEY", raising=False)
        monkeypatch.delenv("APOLLO_API_KEY", raising=False)

        with patch("agents.base.Config") as mock_config, \
             patch("agents.base.StructuredLogger"), \
             patch("agents.base.AsyncHTTPClient"), \
             patch("agents.base.RateLimiter"):

            mock_config.return_value.load.return_value = {}

            from agents.enrichment.firmographic import FirmographicAgent
            agent = FirmographicAgent(
                agent_type="enrichment.firmographic",
                job_id="test-job"
            )

            missing = agent._check_api_keys()
            assert "CLEARBIT_API_KEY" in missing
            assert "APOLLO_API_KEY" in missing
