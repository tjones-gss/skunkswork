"""
Secrets Manager
NAM Intelligence Pipeline

Centralizes secret access behind a provider chain with HashiCorp Vault
support and .env/os.environ fallback.
"""

import logging
import os
import threading
import time
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# Module-level singleton
_secrets_manager: "SecretsManager | None" = None
_singleton_lock = threading.Lock()


class SecretsProvider(ABC):
    """Abstract base class for secret providers."""

    @abstractmethod
    def get_secret(self, key: str) -> str | None:
        """Retrieve a secret by key. Returns None if not found."""

    @abstractmethod
    def is_available(self) -> bool:
        """Check if this provider is available and configured."""


class EnvSecretsProvider(SecretsProvider):
    """Reads secrets from os.environ (always-available fallback)."""

    def get_secret(self, key: str) -> str | None:
        return os.environ.get(key)

    def is_available(self) -> bool:
        return True


class VaultSecretsProvider(SecretsProvider):
    """Reads secrets from HashiCorp Vault KV v2.

    Lazy-imports hvac so the package is only required when Vault is configured.
    """

    def __init__(
        self,
        addr: str | None = None,
        token: str | None = None,
        mount: str | None = None,
        path: str | None = None,
    ):
        self._addr = addr or os.environ.get("VAULT_ADDR", "")
        self._token = token or os.environ.get("VAULT_TOKEN", "")
        self._mount = mount or os.environ.get("VAULT_MOUNT", "secret")
        self._path = path or os.environ.get("VAULT_PATH", "nam-pipeline")
        self._client = None
        self._secrets_cache: dict | None = None

        if self._addr and self._token:
            try:
                import hvac

                self._client = hvac.Client(url=self._addr, token=self._token)
            except ImportError:
                logger.warning(
                    "hvac package not installed; Vault provider unavailable. "
                    "Install with: pip install hvac"
                )
            except Exception as e:
                logger.warning(f"Failed to create Vault client: {e}")

    def get_secret(self, key: str) -> str | None:
        if self._client is None:
            return None

        try:
            if self._secrets_cache is None:
                response = self._client.secrets.kv.v2.read_secret_version(
                    path=self._path,
                    mount_point=self._mount,
                )
                self._secrets_cache = response.get("data", {}).get("data", {})

            return self._secrets_cache.get(key)

        except Exception as e:
            logger.warning(f"Vault read failed for key '{key}': {e}")
            return None

    def is_available(self) -> bool:
        if self._client is None:
            return False
        try:
            return self._client.is_authenticated()
        except Exception:
            return False

    def clear_cache(self):
        """Clear the Vault secrets cache to force a fresh read."""
        self._secrets_cache = None


class SecretsManager:
    """Manages a chain of secret providers with a TTL cache.

    Provider chain: Vault (if configured) -> Env (always).
    First non-None result wins.
    """

    def __init__(
        self,
        providers: list[SecretsProvider] | None = None,
        cache_ttl: int = 300,
    ):
        if providers is not None:
            self._providers = providers
        else:
            self._providers = self._auto_detect_providers()

        self._cache_ttl = cache_ttl
        self._cache: dict[str, tuple[str | None, float]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _auto_detect_providers() -> list[SecretsProvider]:
        """Build provider chain based on environment."""
        providers: list[SecretsProvider] = []

        if os.environ.get("VAULT_ADDR") and os.environ.get("VAULT_TOKEN"):
            vault = VaultSecretsProvider()
            if vault.is_available():
                providers.append(vault)
                logger.info("Vault secrets provider enabled")
            else:
                logger.warning(
                    "VAULT_ADDR/VAULT_TOKEN set but Vault is not authenticated"
                )

        providers.append(EnvSecretsProvider())
        return providers

    def get_secret(self, key: str) -> str | None:
        """Get a secret, checking cache first then providers."""
        with self._lock:
            if key in self._cache:
                value, cached_at = self._cache[key]
                if time.monotonic() - cached_at < self._cache_ttl:
                    return value
                # Expired — remove and fall through
                del self._cache[key]

        # Query providers outside the lock (Vault calls may be slow)
        value = None
        for provider in self._providers:
            try:
                result = provider.get_secret(key)
                if result is not None:
                    value = result
                    break
            except Exception as e:
                logger.warning(
                    f"Provider {type(provider).__name__} failed for '{key}': {e}"
                )
                continue

        # Cache the result (even None to avoid repeated misses)
        with self._lock:
            self._cache[key] = (value, time.monotonic())

        return value

    def invalidate(self, key: str | None = None):
        """Invalidate cached secrets.

        Args:
            key: Specific key to invalidate. If None, clears entire cache.
        """
        with self._lock:
            if key is None:
                self._cache.clear()
            else:
                self._cache.pop(key, None)

        # Also clear Vault provider's internal cache
        for provider in self._providers:
            if isinstance(provider, VaultSecretsProvider):
                provider.clear_cache()


def get_secrets_manager() -> SecretsManager:
    """Return the module-level SecretsManager singleton.

    Thread-safe via double-checked locking.
    """
    global _secrets_manager

    if _secrets_manager is None:
        with _singleton_lock:
            if _secrets_manager is None:
                _secrets_manager = SecretsManager()

    return _secrets_manager


def _reset_secrets_manager():
    """Reset the singleton (for testing only)."""
    global _secrets_manager
    with _singleton_lock:
        _secrets_manager = None
