"""Server-side storage for forge OAuth tokens.

The session cookie carries only an opaque session id: the cookie is
signed but not encrypted, and server-side storage lets logout
invalidate the token immediately.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from .db_gen import tokens as q

if TYPE_CHECKING:
    import asyncpg


class TokenVault(Protocol):
    async def save(self, session_id: str, token: str, lifetime: int) -> None: ...

    async def get(self, session_id: str) -> str | None: ...

    async def delete(self, session_id: str) -> None: ...


class ForgeTokenStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def save(self, session_id: str, token: str, lifetime: int) -> None:
        await q.save_forge_token(
            self.pool,
            session_id=session_id,
            token=token,
            lifetime=float(lifetime),
        )

    async def get(self, session_id: str) -> str | None:
        return await q.get_forge_token(self.pool, session_id=session_id)

    async def delete(self, session_id: str) -> None:
        await q.delete_forge_token(self.pool, session_id=session_id)


class SessionRevocations(Protocol):
    async def revoke(self, session_id: str, lifetime: int) -> None: ...

    async def is_revoked(self, session_id: str) -> bool: ...


class RevokedSessionStore:
    """Logout denylist for the stateless session cookies: the cookie
    stays validly signed until expiry, so revocation must be recorded
    server-side and checked on every authenticated request."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def revoke(self, session_id: str, lifetime: int) -> None:
        # Lazy pruning: rows are only needed until the cookie itself
        # would have expired.
        await q.revoke_session(
            self.pool, session_id=session_id, lifetime=float(lifetime)
        )

    async def is_revoked(self, session_id: str) -> bool:
        return bool(await q.session_revoked(self.pool, session_id=session_id))
