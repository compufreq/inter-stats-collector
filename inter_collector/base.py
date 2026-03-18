"""Abstract base class and configuration for statistical data sources."""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from .download_utils import BytesCallback, DownloadResult


@dataclass
class SourceConfig:
    """Configuration for a statistical data source."""

    name: str                       # internal key: "eurostat", "ons", "swiss"
    display_name: str               # human label: "Eurostat", "UK ONS", "Swiss FSO"
    default_output_subdir: str      # subdirectory under -o: "eurostat", "ons", "swiss"
    state_filename: str             # e.g., ".eurostat_state.json"
    tree_index_filename: str        # e.g., "eurostat_tree_index.json"
    file_type_groups: dict[str, str] = field(default_factory=dict)
    """Extension groups for the status command. Keys are suffixes to match
    against filenames (e.g., ".tsv.gz"), values are descriptions."""
    recommended_concurrency: int | None = None
    """Optional concurrency cap for rate-limited APIs (e.g., ONS).
    When set, the collector uses min(user_concurrency, this value)."""
    data_file_types: set[str] = field(default_factory=set)
    """Success labels that represent actual data files (not just metadata).
    A dataset is only considered truly completed when at least one of these
    is in its successes list. E.g., {"csv", "xlsx"} for ONS, {"tsv", "sdmx"}
    for Eurostat. Empty set means any success counts (legacy behaviour)."""


class DataSource(abc.ABC):
    """Interface every statistical data source must implement.

    To add a new source (e.g., UK ONS), create a new module under
    ``sources/`` and implement all abstract methods.
    """

    @abc.abstractmethod
    def config(self) -> SourceConfig:
        """Return the static configuration for this source."""
        ...

    @abc.abstractmethod
    async def fetch_catalog(
        self,
        client: httpx.AsyncClient,
        *,
        output_dir: Path | None = None,
    ) -> Any:
        """Fetch the dataset catalog / table of contents.

        Args:
            client: httpx async client for API requests.
            output_dir: Optional output directory.  Sources may use this
                        to read/write cache files (e.g., deprecated database
                        lists) that speed up subsequent catalog fetches.

        Returns a source-specific catalog object (e.g., a TocEntry tree
        for Eurostat).
        """
        ...

    @abc.abstractmethod
    def collect_datasets(self, catalog: Any) -> list[Any]:
        """Flatten the catalog to a list of downloadable dataset entries."""
        ...

    @abc.abstractmethod
    async def download_dataset(
        self,
        client: httpx.AsyncClient,
        entry: Any,
        output_dir: Path,
        *,
        skip_existing: bool = True,
        on_bytes: BytesCallback | None = None,
        folder_style: str = "display",
    ) -> DownloadResult:
        """Download all files for a single dataset entry."""
        ...

    @abc.abstractmethod
    def save_tree_index(self, catalog: Any, output_dir: Path) -> Path:
        """Save the catalog tree as a JSON index file.

        Returns the path to the saved file.
        """
        ...

    def get_file_type_groups(self) -> dict[str, str]:
        """Return extension groups for the status command.

        Override in subclasses if the default from config isn't sufficient.
        """
        return self.config().file_type_groups
