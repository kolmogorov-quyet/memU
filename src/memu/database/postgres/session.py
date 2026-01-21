from __future__ import annotations

import logging
import os
from typing import Any
from dotenv import load_dotenv
load_dotenv()
try:  # Optional dependency for Postgres backend
    from sqlmodel import Session, create_engine
except ImportError as exc:  # pragma: no cover - optional dependency
    msg = "sqlmodel is required for Postgres storage support"
    raise ImportError(msg) from exc

logger = logging.getLogger(__name__)


def _get_pool_settings() -> dict[str, Any]:
    """Get connection pool settings from environment variables.

    Environment variables:
        MEMU_POOL_SIZE: Number of connections to keep in pool (default: 3)
        MEMU_POOL_MAX_OVERFLOW: Extra connections when pool exhausted (default: 5)
        MEMU_POOL_RECYCLE: Recycle connections after N seconds (default: 1800 = 30min)
        MEMU_POOL_TIMEOUT: Seconds to wait for connection (default: 30)
    """
    return {
        "pool_size": int(os.getenv("MEMU_POOL_SIZE", "3")),
        "max_overflow": int(os.getenv("MEMU_POOL_MAX_OVERFLOW", "5")),
        "pool_recycle": int(os.getenv("MEMU_POOL_RECYCLE", "1800")),
        "pool_timeout": int(os.getenv("MEMU_POOL_TIMEOUT", "30")),
    }


class SessionManager:
    """Handle engine lifecycle and session creation for Postgres store."""

    def __init__(self, *, dsn: str, engine_kwargs: dict[str, Any] | None = None) -> None:
        # Default pool settings: conservative to avoid "too many clients"
        kw = {
            "pool_pre_ping": True,
            **_get_pool_settings(),
        }
        if engine_kwargs:
            kw.update(engine_kwargs)
        logger.info(
            f"Creating Postgres engine with pool settings: "
            f"pool_size={kw.get('pool_size')}, "
            f"max_overflow={kw.get('max_overflow')}"
        )
        self._engine = create_engine(dsn, **kw)

    def session(self) -> Session:
        return Session(self._engine, expire_on_commit=False)

    def close(self) -> None:
        try:
            self._engine.dispose()
        except Exception:
            logger.exception("Failed to close Postgres engine")


__all__ = ["SessionManager"]