"""Main orchestrator: crawls catalog, manages concurrency, downloads all datasets."""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

import httpx
from rich.console import Console, Group
from rich.live import Live
from rich.progress import (
    BarColumn,
    DownloadColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.table import Table

from .base import DataSource
from .download_utils import DownloadResult, FORMAT_SUFFIXES, get_data_suffixes
from .progress import DownloadStats, fmt_bytes
from .state import CollectorState

log = logging.getLogger(__name__)
console = Console()

# Download concurrency and retry defaults
DEFAULT_CONCURRENCY = 5    # max parallel downloads (semaphore slots)
DEFAULT_DELAY = 0.5        # seconds between download batches
DEFAULT_TIMEOUT = 300      # per-request timeout in seconds
HTTP_RETRIES = 3           # retry attempts for 429/5xx/connection errors
RETRY_BACKOFF = 2.0        # exponential backoff base (2^attempt seconds)


async def _download_with_retry(
    source: DataSource,
    client: httpx.AsyncClient,
    entry,
    output_dir: Path,
    semaphore: asyncio.Semaphore,
    stats: DownloadStats,
    dataset_progress: Progress,
    dataset_task_id: int,
    bytes_progress: Progress,
    bytes_task_id: int,
    active_progress: Progress,
    *,
    skip_existing: bool = True,
    retries: int = HTTP_RETRIES,
    folder_style: str = "display",
) -> DownloadResult:
    """Download a dataset with retry logic and concurrency control.

    Wraps source.download_dataset() with:
    - Semaphore-based concurrency limiting
    - Automatic retry on HTTP 429, 5xx, timeouts, and connection errors
    - Progress tracking across three Rich Progress bars:
        dataset_progress: dataset count bar (advances by 1 per dataset)
        bytes_progress:   byte transfer bar (advances per chunk received)
        active_progress:  live list of currently downloading dataset codes

    Returns a DownloadResult with successes/failures recorded.
    """
    code = entry.code if hasattr(entry, 'code') else str(entry)

    # Show this dataset as active
    active_task = active_progress.add_task(
        f"[cyan]{code}[/cyan]", total=None
    )

    def _on_bytes(chunk_size: int, content_length: int | None) -> None:
        bytes_progress.advance(bytes_task_id, chunk_size)

    async with semaphore:
        try:
            for attempt in range(1, retries + 1):
                try:
                    result = await source.download_dataset(
                        client, entry, output_dir,
                        skip_existing=skip_existing,
                        on_bytes=_on_bytes,
                        folder_style=folder_style,
                    )
                    return result
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429 and attempt < retries:
                        wait = RETRY_BACKOFF ** attempt * 5
                        log.warning("Rate limited on %s, waiting %.0fs...", code, wait)
                        await asyncio.sleep(wait)
                    elif e.response.status_code >= 500 and attempt < retries:
                        wait = RETRY_BACKOFF ** attempt
                        log.warning("Server error %d on %s, retry %d/%d in %.0fs",
                                    e.response.status_code, code, attempt, retries, wait)
                        await asyncio.sleep(wait)
                    else:
                        result = DownloadResult(code)
                        result.failures.append(("http", str(e)))
                        return result
                except (httpx.TimeoutException, httpx.ConnectError) as e:
                    if attempt < retries:
                        wait = RETRY_BACKOFF ** attempt
                        log.warning("Connection error on %s, retry %d/%d in %.0fs",
                                    code, attempt, retries, wait)
                        await asyncio.sleep(wait)
                    else:
                        result = DownloadResult(code)
                        result.failures.append(("connection", str(e)))
                        return result
        finally:
            active_progress.remove_task(active_task)

    result = DownloadResult(code)
    result.failures.append(("unknown", "exhausted retries"))
    return result


async def run_collection(
    source: DataSource,
    output_dir: Path,
    *,
    concurrency: int = DEFAULT_CONCURRENCY,
    delay: float = DEFAULT_DELAY,
    skip_existing: bool = True,
    retry_failed: bool = False,
    filter_path: str | None = None,
    filter_codes: list[str] | None = None,
    dry_run: bool = False,
    verify: bool = False,
    folder_style: str = "display",
) -> None:
    """Main entry point: fetch catalog, then download all datasets.

    Orchestrates the full collection pipeline:
    1. Fetch the remote catalog via source.fetch_catalog()
    2. Save the catalog tree index to disk
    3. Flatten to a list of downloadable datasets
    4. Apply filters (path, codes, skip-completed, retry-failed)
    5. Optionally verify completed datasets have data files on disk (--verify)
    6. Download remaining datasets with async concurrency and progress display

    Args:
        source:        DataSource implementation (Eurostat, Swiss, ONS, etc.)
        output_dir:    Root output directory (source subdir already included).
        concurrency:   Maximum parallel downloads (semaphore slots).
        delay:         Seconds between download batches.
        skip_existing: Skip datasets already marked completed in state.
        retry_failed:  Include previously failed datasets in this run.
        filter_path:   Only download datasets under this catalog path prefix.
        filter_codes:  Only download datasets with these codes.
        dry_run:       Show what would be downloaded without actually downloading.
        verify:        Check completed datasets for missing data files on disk;
                       reset any that lack actual data files so they get re-downloaded.
        folder_style:  "display" for human-readable names, "code" for raw codes.
    """
    cfg = source.config()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Use source-specific state file; also check for legacy name
    state_file = output_dir / cfg.state_filename
    legacy_state = output_dir / ".collector_state.json"
    if not state_file.exists() and legacy_state.exists():
        # Migrate legacy state file
        legacy_state.rename(state_file)
        console.print(f"[dim]Migrated state file: .collector_state.json → {cfg.state_filename}[/dim]")

    state = CollectorState(state_file)
    state.mark_started()

    # Respect source-recommended concurrency cap (e.g., ONS rate limits)
    if cfg.recommended_concurrency is not None:
        effective_concurrency = min(concurrency, cfg.recommended_concurrency)
        if effective_concurrency < concurrency:
            log.info(
                "%s recommends max concurrency %d (requested %d)",
                cfg.display_name, cfg.recommended_concurrency, concurrency,
            )
    else:
        effective_concurrency = concurrency
    concurrency = effective_concurrency

    console.print(f"[bold blue]{cfg.display_name} Bulk Data Collector[/bold blue]")
    console.print(f"Output directory: {output_dir}")
    console.print(f"Concurrency: {concurrency}")
    console.print(f"Folder style: {folder_style}")
    console.print()

    # Step 1: Fetch and parse catalog
    console.print(f"[yellow]Fetching {cfg.display_name} Table of Contents...[/yellow]")
    async with httpx.AsyncClient(
        follow_redirects=True,
        limits=httpx.Limits(max_connections=concurrency + 5, max_keepalive_connections=concurrency),
    ) as client:
        catalog = await source.fetch_catalog(client, output_dir=output_dir)

    index_path = source.save_tree_index(catalog, output_dir)
    console.print(f"[green]Category tree index saved to {index_path}[/green]")

    # Step 2: Collect all datasets
    all_datasets = source.collect_datasets(catalog)
    console.print(f"[green]Found {len(all_datasets)} datasets in catalog[/green]")

    if filter_path:
        all_datasets = [d for d in all_datasets if d.full_path.startswith(filter_path)]
        console.print(f"[yellow]Filtered to {len(all_datasets)} datasets under '{filter_path}'[/yellow]")

    if filter_codes:
        codes_set = set(filter_codes)
        all_datasets = [d for d in all_datasets if d.code in codes_set]
        console.print(f"[yellow]Filtered to {len(all_datasets)} datasets by code[/yellow]")

    # --verify: reset completed datasets that are missing actual data files on disk
    if verify and cfg.data_file_types:
        console.print("[yellow]Verifying completed datasets have data files on disk...[/yellow]")
        ds_by_code = {d.code: d for d in all_datasets}
        reset_count = 0
        data_suffixes = get_data_suffixes(cfg.data_file_types)
        for code in list(state.completed_codes):
            recorded_files = set(state.get_completed_files(code))
            has_data_in_state = bool(cfg.data_file_types & recorded_files)

            # Look up the dataset in the current catalog
            ds = ds_by_code.get(code)
            if ds is None:
                continue  # not in current catalog, skip

            ds_dir = output_dir / ds.get_folder_path(folder_style)
            if not ds_dir.exists():
                state.reset_completed(code)
                reset_count += 1
                continue

            # Check disk for actual data files
            data_on_disk = any(
                any(f.name.endswith(sfx) for sfx in data_suffixes)
                for f in ds_dir.iterdir() if f.is_file()
            )

            if not data_on_disk:
                # No data files on disk — reset regardless of state
                state.reset_completed(code)
                reset_count += 1
            elif not has_data_in_state and data_on_disk:
                # State has empty/metadata-only files list but data IS on disk.
                # Update state to reflect what's actually there.
                found_types = []
                for f in ds_dir.iterdir():
                    if not f.is_file():
                        continue
                    for ft, suffixes in FORMAT_SUFFIXES.items():
                        if any(f.name.endswith(sfx) for sfx in suffixes):
                            if ft not in found_types:
                                found_types.append(ft)
                            break
                state.mark_completed(code, found_types)
        if reset_count:
            state.save()
            console.print(f"[yellow]Reset {reset_count} datasets missing data files[/yellow]")
        else:
            console.print(f"[green]All {state.completed_count} completed datasets verified[/green]")

    if skip_existing and not retry_failed:
        all_datasets = [d for d in all_datasets if not state.is_completed(d.code)]
        console.print(f"[cyan]{state.completed_count} already completed, {len(all_datasets)} remaining[/cyan]")
    elif retry_failed:
        failed = state.failed_codes
        all_datasets = [d for d in all_datasets if d.code in failed or not state.is_completed(d.code)]
        console.print(f"[cyan]Retrying {len(failed)} failed + {len(all_datasets) - len(failed)} pending[/cyan]")

    if dry_run:
        console.print(f"\n[bold yellow]DRY RUN: Would download {len(all_datasets)} datasets[/bold yellow]")
        for d in all_datasets[:20]:
            console.print(f"  {d.full_path} ({d.code})")
        if len(all_datasets) > 20:
            console.print(f"  ... and {len(all_datasets) - 20} more")
        return

    if not all_datasets:
        console.print("[green]Nothing to download. All datasets are up to date.[/green]")
        return

    total = len(all_datasets)
    stats = DownloadStats()
    success_count = 0
    fail_count = 0
    start_time = time.monotonic()

    # --- Build the multi-row progress display ---
    dataset_progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold]Datasets[/bold]"),
        BarColumn(bar_width=40),
        MofNCompleteColumn(),
        TextColumn("[green]{task.fields[ok]}[/green] ok"),
        TextColumn("[red]{task.fields[err]}[/red] err"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        expand=False,
    )
    dataset_task = dataset_progress.add_task(
        "Datasets", total=total, ok=0, err=0
    )

    bytes_progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold]Transfer[/bold]"),
        BarColumn(bar_width=40, pulse_style="cyan"),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeElapsedColumn(),
        console=console,
        expand=False,
    )
    bytes_task = bytes_progress.add_task("Transfer", total=None)

    active_progress = Progress(
        SpinnerColumn("dots"),
        TextColumn("  {task.description}"),
        console=console,
        expand=False,
    )

    progress_group = Group(
        dataset_progress,
        bytes_progress,
        active_progress,
    )

    console.print(f"\n[bold]Starting download of {total} datasets...[/bold]\n")

    semaphore = asyncio.Semaphore(concurrency)

    with Live(progress_group, console=console, refresh_per_second=4):
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(DEFAULT_TIMEOUT, connect=30),
            limits=httpx.Limits(
                max_connections=concurrency + 5,
                max_keepalive_connections=concurrency,
            ),
        ) as client:

            # Batch size is 2x concurrency to keep the semaphore saturated:
            # while N tasks hold the semaphore, N more are queued and ready.
            batch_size = concurrency * 2
            for i in range(0, total, batch_size):
                batch = all_datasets[i : i + batch_size]
                tasks = [
                    _download_with_retry(
                        source, client, entry, output_dir, semaphore,
                        stats, dataset_progress, dataset_task,
                        bytes_progress, bytes_task,
                        active_progress,
                        skip_existing=skip_existing,
                        folder_style=folder_style,
                    )
                    for entry in batch
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for r in results:
                    if isinstance(r, Exception):
                        fail_count += 1
                        log.error("Unexpected error: %s", r)
                    elif isinstance(r, DownloadResult):
                        # Check if actual data files were downloaded (not just metadata)
                        has_data = bool(
                            cfg.data_file_types & set(r.successes)
                        ) if cfg.data_file_types else bool(r.successes)

                        if r.ok:
                            success_count += 1
                            state.mark_completed(r.dataset_code, r.successes)
                        elif has_data:
                            # Some data files succeeded — mark completed but record partial failures
                            state.mark_completed(r.dataset_code, r.successes)
                            state.mark_failed(r.dataset_code, r.failures)
                            success_count += 1
                        else:
                            # Only metadata or nothing — not a real completion
                            fail_count += 1
                            state.mark_failed(r.dataset_code, r.failures)

                    dataset_progress.advance(dataset_task)
                    dataset_progress.update(
                        dataset_task, ok=success_count, err=fail_count
                    )

                # Rate-limited sources (e.g., ONS) need longer inter-batch delays
                # to avoid triggering 429s; enforce a 2s minimum for those sources.
                effective_delay = max(delay, 2.0) if cfg.recommended_concurrency else delay
                if effective_delay > 0 and i + batch_size < total:
                    await asyncio.sleep(effective_delay)

    # --- Final summary ---
    elapsed = time.monotonic() - start_time
    total_bytes = bytes_progress.tasks[bytes_task].completed
    avg_speed = total_bytes / elapsed if elapsed > 0 else 0

    console.print()

    summary = Table(title="Collection Summary", show_header=False, box=None, padding=(0, 2))
    summary.add_column(style="bold")
    summary.add_column()
    summary.add_row("Source", cfg.display_name)
    summary.add_row("Total datasets", str(total))
    summary.add_row("Succeeded", f"[green]{success_count}[/green]")
    summary.add_row("Failed", f"[red]{fail_count}[/red]")
    summary.add_row("Previously done", str(state.completed_count))
    summary.add_row("Data downloaded", fmt_bytes(int(total_bytes)))
    summary.add_row("Avg speed", f"{fmt_bytes(int(avg_speed))}/s")
    summary.add_row("Elapsed", f"{elapsed:.0f}s ({elapsed/60:.1f}m)")
    summary.add_row("State file", str(state.state_file))
    console.print(summary)

    if fail_count > 0:
        console.print(f"\n[yellow]Run again with --retry-failed to retry {fail_count} failed datasets[/yellow]")
