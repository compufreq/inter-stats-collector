"""Persistent state for tracking download progress and enabling resumability.

Each data source gets its own JSON state file (e.g., `.eurostat_state.json`)
that records which datasets have been completed or failed.  On restart,
completed datasets are skipped entirely — no HEAD requests needed.

State is saved to disk after every mutation (mark_completed, mark_failed),
so a crash loses at most one in-progress dataset.

JSON structure::

    {
        "started_at": "2024-01-15T10:30:00",
        "last_updated": "2024-01-15T14:22:33",
        "completed": {
            "dataset_code": {
                "files": ["tsv", "sdmx", "dsd"],
                "timestamp": "2024-01-15T11:00:01"
            }
        },
        "failed": {
            "dataset_code": {
                "errors": [{"type": "http", "error": "429 Too Many Requests"}],
                "timestamp": "2024-01-15T11:00:05"
            }
        }
    }
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional


class CollectorState:
    """Tracks which datasets have been downloaded, failed, or are pending.

    Every mutating method (mark_started, mark_completed, mark_failed)
    triggers an immediate disk write via save().  This ensures crash
    safety at the cost of one file write per dataset.
    """

    def __init__(self, state_file: Path):
        self.state_file = state_file
        self._data: dict = {"completed": {}, "failed": {}, "started_at": None, "last_updated": None}
        self._load()

    def _load(self) -> None:
        """Load state from disk if the file exists.

        If the file does not exist, the default empty state is kept.
        Raises json.JSONDecodeError if the file exists but contains
        malformed JSON.
        """
        if self.state_file.exists():
            self._data = json.loads(self.state_file.read_text())

    def save(self) -> None:
        """Write current state to disk, updating the last_updated timestamp.

        Creates parent directories if they don't exist.  Called
        automatically by every mark_* method.
        """
        self._data["last_updated"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(json.dumps(self._data, indent=2))

    def mark_started(self) -> None:
        """Record the collection start time (only on the first invocation).

        Subsequent calls do not overwrite the original start time,
        preserving the timestamp of the very first run.
        """
        if not self._data.get("started_at"):
            self._data["started_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        self.save()

    def mark_completed(self, code: str, successes: list[str]) -> None:
        """Record a dataset as successfully downloaded.

        Args:
            code:      Dataset identifier (e.g., "prc_hicp_midx").
            successes: List of file type labels that succeeded
                       (e.g., ["tsv", "sdmx", "dsd", "info"]).

        Side effect: also removes the dataset from the failed set,
        since a successful re-download supersedes prior failures.
        """
        self._data["completed"][code] = {
            "files": successes,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        self._data["failed"].pop(code, None)
        self.save()

    def mark_failed(self, code: str, failures: list[tuple[str, str]]) -> None:
        """Record a dataset as failed.

        Args:
            code:     Dataset identifier.
            failures: List of (file_type, error_message) tuples
                      (e.g., [("csv", "429 Too Many Requests")]).

        Note: a dataset can appear in both completed and failed if
        some files succeeded and others did not (partial success).
        """
        self._data["failed"][code] = {
            "errors": [{"type": t, "error": e} for t, e in failures],
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        self.save()

    def is_completed(self, code: str) -> bool:
        """Check if a dataset is recorded as completed in state.

        Note: this does not verify files on disk — use --verify for that.
        A dataset with partial failures may still be marked completed
        if at least one data file type succeeded.
        """
        return code in self._data["completed"]

    def reset_completed(self, code: str) -> None:
        """Remove a dataset from the completed set so it will be re-downloaded.

        Used by --verify when disk inspection reveals missing data files.
        Does not call save() — caller should batch resets and save once.
        """
        self._data["completed"].pop(code, None)

    def get_completed_files(self, code: str) -> list[str]:
        """Return the list of file type labels recorded for a completed dataset.

        Returns e.g. ["info", "csv", "xls"] — these are success labels,
        not filenames.  Returns [] if the dataset is not in the completed set.
        """
        entry = self._data["completed"].get(code, {})
        return entry.get("files", [])

    @property
    def completed_count(self) -> int:
        """Number of datasets marked as completed."""
        return len(self._data["completed"])

    @property
    def failed_count(self) -> int:
        """Number of datasets with recorded failures."""
        return len(self._data["failed"])

    @property
    def completed_codes(self) -> set[str]:
        """Set of all dataset codes marked as completed."""
        return set(self._data["completed"].keys())

    @property
    def failed_codes(self) -> set[str]:
        """Set of all dataset codes with recorded failures."""
        return set(self._data["failed"].keys())

    def summary(self) -> dict:
        """Return a dict summarising the current state for display.

        Keys: completed (int), failed (int), started_at (str|None),
        last_updated (str|None).  Used by the ``status`` CLI command.
        """
        return {
            "completed": self.completed_count,
            "failed": self.failed_count,
            "started_at": self._data.get("started_at"),
            "last_updated": self._data.get("last_updated"),
        }
