"""Shared progress tracking for byte-level and dataset-level progress.

Provides DownloadStats for accumulating download metrics across
concurrent async tasks, and fmt_bytes() for human-readable sizes.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


@dataclass
class DownloadStats:
    """Coroutine-safe accumulator for global download statistics.

    Uses asyncio.Lock for safe concurrent updates from multiple async
    download tasks.  Note: this is NOT thread-safe — it relies on the
    asyncio event loop for synchronisation.  The extract command in
    cli.py uses threading.Lock separately for its thread-based workers.
    """

    total_bytes_downloaded: int = 0
    total_files_downloaded: int = 0
    total_datasets_done: int = 0
    total_datasets_failed: int = 0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def add_bytes(self, n: int) -> None:
        """Record n bytes downloaded."""
        async with self._lock:
            self.total_bytes_downloaded += n

    async def add_file(self) -> None:
        """Increment the completed file count by 1."""
        async with self._lock:
            self.total_files_downloaded += 1

    async def add_dataset_done(self) -> None:
        """Increment the successfully completed dataset count by 1."""
        async with self._lock:
            self.total_datasets_done += 1

    async def add_dataset_failed(self) -> None:
        """Increment the failed dataset count by 1."""
        async with self._lock:
            self.total_datasets_failed += 1


def fmt_bytes(n: int) -> str:
    """Format a byte count as a human-readable string.

    Examples: 0 → "0 B", 1536 → "1.5 KB", 1073741824 → "1.0 GB".
    Handles negative values via abs().  Scales up through
    B → KB → MB → GB → TB → PB.
    """
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024  # type: ignore[assignment]
    return f"{n:.1f} PB"
