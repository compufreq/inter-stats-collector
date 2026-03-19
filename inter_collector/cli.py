"""CLI interface for International Statistics Data Collector."""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.logging import RichHandler

console = Console()


def _setup_logging(verbose: bool) -> None:
    """Configure logging with Rich output.

    In non-verbose mode, sets level to WARNING and explicitly silences
    httpx/httpcore loggers (they are extremely chatty at INFO level with
    per-request connection pool messages).
    """
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[RichHandler(console=console, show_time=False, show_path=False)],
    )
    if not verbose:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)


@click.group()
@click.option("--stats", type=click.Choice(["all", "europe", "uk", "switzerland", "unhcr", "hdx", "netherlands", "germany"]),
              default="all", show_default=True,
              help="Statistical data source to use")
@click.option("--scope", default=None, type=str,
              help="Organization filter for Swiss/HDX. Use org slug (e.g., 'unhcr', 'wfp', 'iom') or 'all' for everything")
@click.option("--formats", default=None, type=str,
              help="Comma-separated data formats to download for Swiss source (default: csv,xls,ods,json)")
@click.option("--year-from", default=None, type=int,
              help="UNHCR: start year filter (default: 1951)")
@click.option("--year-to", default=None, type=int,
              help="UNHCR: end year filter (default: latest)")
@click.pass_context
def main(ctx, stats, scope, formats, year_from, year_to):
    """International Statistics Data Collector.

    Bulk-download statistical and humanitarian datasets from official sources.
    Use --stats to select the data source (defaults to all).

    \b
    Supported sources:
      all          All sources below (default)
      europe       Eurostat (EU) — ~10,400 datasets
      switzerland  Swiss FSO via opendata.swiss — ~3,200+ datasets
      uk           UK ONS — Office for National Statistics
      unhcr        UNHCR Refugee Statistics — 6 endpoints × 75 years
      hdx          HDX Humanitarian Data Exchange — 400+ organizations
      netherlands  Netherlands open data (data.overheid.nl) — ~18,900 datasets
      germany      Germany GovData (govdata.de) — ~149,000 datasets

    \b
    CKAN --scope option (organization filter, for Swiss/HDX/NL/DE):
      (default)    BFS for Swiss, UNHCR for HDX
      --scope all  All organizations on the platform
      --scope ORG  Specific org slug (e.g., wfp, iom, world-bank-group)

    \b
    Common HDX --scope values:
      unhcr, wfp, world-bank-group, world-health-organization,
      unicef-data, fao, international-organization-for-migration,
      unesco, unfpa, ifrc, acled, ocha-fiss, reach-initiative

    \b
    --formats: Comma-separated data formats (default: all available)
      e.g., --formats csv,xls

    \b
    UNHCR --year options:
      --year-from   Start year (default: 1951)
      --year-to     End year (default: latest available)
    """
    ctx.ensure_object(dict)
    from .sources import resolve_source, resolve_all_sources

    # Build kwargs for source-specific options
    source_kwargs = {}

    # --scope: organization filter for Swiss and HDX sources
    # "all" → empty string (no filter), any other value → org slug
    if scope is not None:
        if scope.lower() == "all":
            source_kwargs["org_filter"] = ""  # empty = no filter = all orgs
        else:
            source_kwargs["org_filter"] = scope.lower()
    if formats:
        source_kwargs["download_formats"] = {f.strip().upper() for f in formats.split(",")}

    # UNHCR-specific
    if year_from is not None:
        source_kwargs["year_from"] = year_from
    if year_to is not None:
        source_kwargs["year_to"] = year_to

    if stats == "all":
        ctx.obj["sources"] = resolve_all_sources(**source_kwargs)
    else:
        ctx.obj["sources"] = [resolve_source(stats, **source_kwargs)]
    ctx.obj["stats"] = stats


@main.command()
@click.option("-o", "--output", default="./stats_data", type=click.Path(),
              help="Root output directory for downloaded data")
@click.option("-c", "--concurrency", default=5, type=int,
              help="Maximum concurrent downloads (default: 5)")
@click.option("-d", "--delay", default=0.5, type=float,
              help="Delay in seconds between batches (default: 0.5)")
@click.option("--skip-existing/--no-skip-existing", default=True,
              help="Skip already downloaded files (default: true)")
@click.option("--retry-failed", is_flag=True, default=False,
              help="Retry previously failed datasets")
@click.option("--filter-path", default=None, type=str,
              help="Only download datasets under this catalog path")
@click.option("--filter-codes", default=None, type=str,
              help="Comma-separated dataset codes to download")
@click.option("--dry-run", is_flag=True, default=False,
              help="Show what would be downloaded without actually downloading")
@click.option("--verify", is_flag=True, default=False,
              help="Re-check completed datasets for missing data files and re-download them")
@click.option("--folder-style", default="display", type=click.Choice(["display", "code"]),
              help="Folder naming: 'display' for slugified titles, 'code' for raw codes")
@click.option("-v", "--verbose", is_flag=True, default=False,
              help="Enable verbose/debug logging")
@click.pass_context
def collect(ctx, output, concurrency, delay, skip_existing, retry_failed,
            filter_path, filter_codes, dry_run, verify, folder_style, verbose):
    """Download all datasets (CSV, SDMX, DSD, metadata)."""
    _setup_logging(verbose)

    from .collector import run_collection

    sources = ctx.obj["sources"]
    codes = None
    if filter_codes:
        codes = [c.strip() for c in filter_codes.split(",")]

    for i, source in enumerate(sources):
        cfg = source.config()
        output_dir = Path(output) / cfg.default_output_subdir

        if len(sources) > 1:
            if i > 0:
                console.print()
            console.print(f"[bold]{'═' * 60}[/bold]")
            console.print(f"[bold]  {cfg.display_name}[/bold]")
            console.print(f"[bold]{'═' * 60}[/bold]\n")

        try:
            asyncio.run(
                run_collection(
                    source,
                    output_dir=output_dir,
                    concurrency=concurrency,
                    delay=delay,
                    skip_existing=skip_existing,
                    retry_failed=retry_failed,
                    filter_path=filter_path,
                    filter_codes=codes,
                    dry_run=dry_run,
                    verify=verify,
                    folder_style=folder_style,
                )
            )
        except NotImplementedError as e:
            console.print(f"[red]{e}[/red]")


@main.command()
@click.option("-o", "--output", default="./stats_data", type=click.Path(),
              help="Output directory containing downloaded .gz files")
@click.option("-c", "--concurrency", default=0, type=int,
              help="Max parallel extraction workers (default: auto = 2x CPU cores, max 32)")
@click.option("-b", "--buffer", default=64, type=int,
              help="I/O buffer size in MB per worker (default: 64)")
@click.option("--force", is_flag=True, default=False,
              help="Re-extract even if the extracted file already exists")
@click.option("-v", "--verbose", is_flag=True, default=False,
              help="Enable verbose logging")
@click.pass_context
def extract(ctx, output, concurrency, buffer, force, verbose):
    """Extract all .gz files in the output directory using parallel workers."""
    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from .download_utils import _extract_gz
    from .progress import fmt_bytes

    _setup_logging(verbose)

    from rich.live import Live

    sources = ctx.obj["sources"]

    for src_idx, source in enumerate(sources):
        cfg = source.config()
        root = (Path(output) / cfg.default_output_subdir).resolve()

        if len(sources) > 1:
            if src_idx > 0:
                console.print()
            console.print(f"[bold]{'═' * 60}[/bold]")
            console.print(f"[bold]  {cfg.display_name}[/bold]")
            console.print(f"[bold]{'═' * 60}[/bold]\n")

        if not root.exists():
            console.print(f"[dim]Directory not found: {root} — skipping[/dim]")
            continue

        cpu_count = os.cpu_count() or 2
        if concurrency <= 0:
            workers = max(1, min(cpu_count * 2, 32))
        else:
            workers = concurrency

        buffer_mb = max(1, buffer)
        ram_est = (workers * buffer_mb) / 1024

        console.print(f"[bold blue]{cfg.display_name} .gz Extractor[/bold blue]")
        console.print(f"Directory: {root}")
        console.print(f"Workers:   {workers} (of {cpu_count} CPU cores)")
        console.print(f"Buffer:    {buffer_mb} MB/worker ({ram_est:.1f} GB RAM)")
        console.print()

        # Discover .gz files
        console.print("[dim]Scanning for .gz files...[/dim]")
        gz_files = []
        scan_count = 0
        with Live(console=console, refresh_per_second=4) as live:
            for f in root.rglob("*.gz"):
                gz_files.append(f)
                scan_count += 1
                if scan_count % 500 == 0:
                    live.update(f"  [cyan]Found {scan_count:,} .gz files so far...[/cyan]")
            live.update(f"  [green]Found {len(gz_files):,} .gz files[/green]")

        gz_files.sort()
        if not gz_files:
            console.print("[yellow]No .gz files found.[/yellow]")
            continue

        # Filter
        if not force:
            console.print("[dim]Checking which files need extraction...[/dim]")
            to_extract = []
            checked = 0
            with Live(console=console, refresh_per_second=4) as live:
                for gz in gz_files:
                    name = gz.name
                    extracted_name = name[:-3] if name.endswith(".gz") else name + ".extracted"
                    extracted_path = gz.parent / extracted_name
                    if not extracted_path.exists():
                        to_extract.append(gz)
                    checked += 1
                    if checked % 500 == 0:
                        live.update(
                            f"  [cyan]Checked {checked:,}/{len(gz_files):,} files, "
                            f"{len(to_extract):,} need extraction...[/cyan]"
                        )
                live.update(
                    f"  [green]Checked {checked:,} files, "
                    f"{len(to_extract):,} need extraction[/green]"
                )
            skipped = len(gz_files) - len(to_extract)
        else:
            to_extract = gz_files
            skipped = 0

        if not to_extract:
            console.print(f"[green]All {len(gz_files)} .gz files already extracted. "
                           f"Use --force to re-extract.[/green]")
            continue

        # Calculate total compressed size
        console.print("[dim]Calculating total size...[/dim]")
        total_compressed = 0
        sized = 0
        with Live(console=console, refresh_per_second=4) as live:
            for f in to_extract:
                try:
                    total_compressed += f.stat().st_size
                except OSError:
                    pass
                sized += 1
                if sized % 500 == 0:
                    live.update(
                        f"  [cyan]Sized {sized:,}/{len(to_extract):,} "
                        f"({fmt_bytes(total_compressed)})...[/cyan]"
                    )
            live.update(f"  [green]Total compressed: {fmt_bytes(total_compressed)}[/green]")

        console.print()
        console.print(f"Files to extract: [bold]{len(to_extract):,}[/bold]"
                       f"{f' (skipping {skipped:,} already done)' if skipped else ''}")
        console.print(f"Compressed size:  [bold]{fmt_bytes(total_compressed)}[/bold]")
        console.print()

        import time as _time
        import threading
        total_extracted_bytes = 0
        total_read_bytes = 0
        success = 0
        failed = 0
        counter_lock = threading.Lock()
        t_start = _time.monotonic()

        from rich.live import Live as RichLive
        from rich.table import Table

        def _fmt_time(seconds: float) -> str:
            s = int(seconds)
            if s >= 3600:
                return f"{s // 3600}h{(s % 3600) // 60:02d}m{s % 60:02d}s"
            elif s >= 60:
                return f"{s // 60}m{s % 60:02d}s"
            else:
                return f"{s}s"

        worker_state = [{"file": "", "status": "idle", "start": 0.0, "done_count": 0}
                        for _ in range(workers)]
        state_lock = threading.Lock()

        def _render_display():
            now = _time.monotonic()
            elapsed_total = now - t_start
            table = Table.grid(padding=(0, 2))
            table.add_column(width=6)
            table.add_column(width=40)
            table.add_column(width=14)
            table.add_column(width=12)
            table.add_column()

            pct = (success + failed) / len(to_extract) if to_extract else 1
            filled = int(pct * 30)
            bar = f"[bold cyan]{'━' * filled}[/bold cyan][dim]{'━' * (30 - filled)}[/dim]"
            done_n = success + failed
            if done_n > 0 and pct < 1:
                eta_sec = elapsed_total / pct * (1 - pct)
                eta_str = f"ETA {_fmt_time(eta_sec)}"
            elif pct >= 1:
                eta_str = "done"
            else:
                eta_str = "ETA --"
            table.add_row("[bold cyan]Total[/bold cyan]", bar,
                          f"[bold]{done_n:,}[/bold]/{len(to_extract):,}",
                          _fmt_time(elapsed_total), eta_str)

            read_mb = total_read_bytes / 1_048_576
            read_mb_s = read_mb / elapsed_total if elapsed_total > 0 else 0
            files_s = done_n / elapsed_total if elapsed_total > 0 else 0
            table.add_row("[bold blue]Read[/bold blue]",
                          f"[bold]{fmt_bytes(total_read_bytes)}[/bold] / {fmt_bytes(total_compressed)} compressed",
                          f"{files_s:.1f} files/s", f"{read_mb_s:.1f} MB/s", "")

            write_mb = total_extracted_bytes / 1_048_576
            write_mb_s = write_mb / elapsed_total if elapsed_total > 0 else 0
            ratio = total_extracted_bytes / total_read_bytes if total_read_bytes > 0 else 0
            table.add_row("[bold magenta]Write[/bold magenta]",
                          f"[bold]{fmt_bytes(total_extracted_bytes)}[/bold] decompressed",
                          f"{ratio:.1f}x ratio", f"{write_mb_s:.1f} MB/s", "")

            table.add_row("", "[dim]-[/dim]" * 30, "", "", "")

            with state_lock:
                for w in range(workers):
                    ws = worker_state[w]
                    label = f"[green]W-{w + 1:02d}[/green]"
                    if ws["status"] == "idle":
                        table.add_row(label, "[dim]idle[/dim]", "", "", "")
                    elif ws["status"] == "working":
                        w_elapsed = now - ws["start"]
                        dots = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
                        spin = dots[int(now * 10) % len(dots)]
                        sf = ws["file"]
                        if len(sf) > 50:
                            sf = "..." + sf[-47:]
                        table.add_row(label, f"[yellow]{spin} {sf}[/yellow]",
                                      f"[dim]#{ws['done_count'] + 1}[/dim]",
                                      f"[yellow]{_fmt_time(w_elapsed)}[/yellow]", "")
                    elif ws["status"] == "done":
                        sf = ws["file"]
                        if len(sf) > 50:
                            sf = "..." + sf[-47:]
                        table.add_row(label, f"[green]+ {sf}[/green]",
                                      f"[dim]#{ws['done_count']}[/dim]", "", "")
                    elif ws["status"] == "failed":
                        sf = ws["file"]
                        if len(sf) > 50:
                            sf = "..." + sf[-47:]
                        table.add_row(label, f"[red]x {sf}[/red]",
                                      f"[dim]#{ws['done_count']}[/dim]", "", "")
            return table

        future_to_info: dict = {}
        free_slots: list[int] = list(range(workers))

        with RichLive(_render_display(), console=console, refresh_per_second=10) as live:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                prefetch = max(workers * 3, 10)
                pending: dict = {}
                it = iter(to_extract)

                def _submit_next():
                    gz_next = next(it, None)
                    if gz_next is None:
                        return False
                    short = str(gz_next.relative_to(root))
                    with state_lock:
                        if free_slots:
                            slot = free_slots.pop(0)
                            worker_state[slot]["file"] = short
                            worker_state[slot]["status"] = "working"
                            worker_state[slot]["start"] = _time.monotonic()
                        else:
                            slot = -1
                    live.update(_render_display())
                    fut = executor.submit(_extract_gz, gz_next, buffer_mb)
                    future_to_info[fut] = (gz_next, slot)
                    pending[fut] = gz_next
                    return True

                for _ in range(min(prefetch, len(to_extract))):
                    if not _submit_next():
                        break

                while pending:
                    done = next(iter(as_completed(pending)))
                    gz = pending.pop(done)
                    _gz_path, slot = future_to_info.pop(done)
                    short = str(gz.relative_to(root))
                    try:
                        _extracted_path, comp_bytes, decomp_bytes = done.result()
                        with counter_lock:
                            total_read_bytes += comp_bytes
                            total_extracted_bytes += decomp_bytes
                            success += 1
                        if slot >= 0:
                            with state_lock:
                                worker_state[slot]["done_count"] += 1
                                worker_state[slot]["status"] = "done"
                                worker_state[slot]["file"] = short
                    except Exception as e:
                        console.print(f"[red]  FAILED: {short}: {e}[/red]")
                        with counter_lock:
                            failed += 1
                        if slot >= 0:
                            with state_lock:
                                worker_state[slot]["done_count"] += 1
                                worker_state[slot]["status"] = "failed"
                                worker_state[slot]["file"] = short
                    if slot >= 0:
                        with state_lock:
                            free_slots.append(slot)
                    live.update(_render_display())
                    _submit_next()

        elapsed = _time.monotonic() - t_start
        elapsed_str = _fmt_time(elapsed)
        files_per_sec = success / elapsed if elapsed > 0 else 0
        read_mb_sec = (total_read_bytes / 1_048_576) / elapsed if elapsed > 0 else 0
        write_mb_sec = (total_extracted_bytes / 1_048_576) / elapsed if elapsed > 0 else 0
        ratio = total_extracted_bytes / total_read_bytes if total_read_bytes > 0 else 0

        console.print()
        console.print("[bold]Extraction Summary[/bold]")
        console.print(f"  Extracted:         [green]{success:,}[/green] files")
        if failed:
            console.print(f"  Failed:            [red]{failed:,}[/red]")
        if skipped:
            console.print(f"  Skipped:           {skipped:,}")
        console.print(f"  Workers used:      {workers}")
        console.print(f"  Compressed (read): {fmt_bytes(total_read_bytes)}")
        console.print(f"  Decompressed (out):{fmt_bytes(total_extracted_bytes)}  ({ratio:.1f}x expansion)")
        console.print(f"  Elapsed:           {elapsed_str}")
        console.print(f"  Read throughput:   {read_mb_sec:.1f} MB/s")
        console.print(f"  Write throughput:  {write_mb_sec:.1f} MB/s")
        console.print(f"  File throughput:   {files_per_sec:.1f} files/s")


@main.command()
@click.option("-o", "--output", default="./stats_data", type=click.Path(),
              help="Output directory containing the downloaded data")
@click.argument("target", type=click.Choice(["display", "code"]))
@click.option("--dry-run", is_flag=True, default=False,
              help="Preview what would be renamed without making changes")
@click.option("-v", "--verbose", is_flag=True, default=False,
              help="Enable verbose logging")
@click.pass_context
def rename(ctx, output, target, dry_run, verbose):
    """Rename existing directories between naming styles.

    TARGET is the desired folder naming style:

    \b
      display  -- human-readable slugified titles
      code     -- raw statistical office codes
    """
    _setup_logging(verbose)

    from .renamer import rename_directories

    sources = ctx.obj["sources"]

    for src_idx, source in enumerate(sources):
        cfg = source.config()
        root = (Path(output) / cfg.default_output_subdir).resolve()

        if len(sources) > 1:
            if src_idx > 0:
                console.print()
            console.print(f"[bold]{'═' * 60}[/bold]")
            console.print(f"[bold]  {cfg.display_name}[/bold]")
            console.print(f"[bold]{'═' * 60}[/bold]\n")

        if not root.exists():
            console.print(f"[dim]Directory not found: {root} — skipping[/dim]")
            continue

        direction = "to-display" if target == "display" else "to-code"
        rename_directories(
            root, direction,
            tree_index_filename=cfg.tree_index_filename,
            source_display_name=cfg.display_name,
            dry_run=dry_run,
            console=console,
        )


@main.command()
@click.option("-o", "--output", default="./stats_data", type=click.Path(),
              help="Output directory to check status of")
@click.pass_context
def status(ctx, output):
    """Show collection progress and disk statistics."""
    from .progress import fmt_bytes
    from .state import CollectorState
    from rich.live import Live
    from rich.table import Table

    sources = ctx.obj["sources"]

    for src_idx, source in enumerate(sources):
        cfg = source.config()
        root = (Path(output) / cfg.default_output_subdir).resolve()

        if len(sources) > 1:
            if src_idx > 0:
                console.print()
            console.print(f"[bold]{'═' * 60}[/bold]")
            console.print(f"[bold]  {cfg.display_name}[/bold]")
            console.print(f"[bold]{'═' * 60}[/bold]\n")

        if not root.exists():
            console.print(f"[dim]Directory not found: {root} — skipping[/dim]")
            continue

        console.print(f"[bold blue]{cfg.display_name} Collection Status[/bold blue]")
        console.print(f"Directory: {root}")
        console.print()

        # State file
        state_file = root / cfg.state_filename
        if not state_file.exists():
            legacy = root / ".collector_state.json"
            if legacy.exists():
                state_file = legacy

        info = None
        if state_file.exists():
            console.print("[dim]Reading collector state...[/dim]")
            state = CollectorState(state_file)
            info = state.summary()
            console.print(f"[bold]Collector State[/bold] [dim](from {state_file.name})[/dim]")
            console.print(f"  Started:       {info['started_at'] or 'N/A'}")
            console.print(f"  Last update:   {info['last_updated'] or 'N/A'}")
            console.print(f"  Completed:     [green]{info['completed']}[/green]")
            console.print(f"  Failed:        [red]{info['failed']}[/red]")
            if state.failed_count > 0:
                console.print(f"\n  [yellow]Failed datasets:[/yellow]")
                for code in sorted(state.failed_codes):
                    console.print(f"    - {code}")
            console.print()
        else:
            console.print(f"[dim]No {cfg.state_filename} found (no collect run recorded)[/dim]\n")

        # Disk scan
        file_type_groups = cfg.file_type_groups
        ext_groups: dict[str, list] = {k: [] for k in file_type_groups}
        other_files: list = []
        file_count = 0
        total_bytes_scanned = 0

        console.print("[bold]Disk Statistics[/bold]")
        console.print("[dim]Scanning files...[/dim]")

        with Live(console=console, refresh_per_second=4) as live:
            for f in root.rglob("*"):
                if not f.is_file():
                    continue
                try:
                    fsize = f.stat().st_size
                except OSError:
                    continue
                file_count += 1
                total_bytes_scanned += fsize
                matched = False
                for suffix in ext_groups:
                    if f.name.endswith(suffix):
                        ext_groups[suffix].append((f, fsize))
                        matched = True
                        break
                if not matched:
                    other_files.append((f, fsize))
                if file_count % 200 == 0:
                    live.update(f"  [cyan]Scanned {file_count:,} files ({fmt_bytes(total_bytes_scanned)})...[/cyan]")
            live.update(f"  [green]Scan complete: {file_count:,} files ({fmt_bytes(total_bytes_scanned)})[/green]")

        console.print()
        table = Table(show_header=True, box=None, padding=(0, 2))
        table.add_column("File Type", style="bold")
        table.add_column("Count", justify="right")
        table.add_column("Size", justify="right")

        total_size = 0
        total_count = 0
        for suffix, entries in ext_groups.items():
            size = sum(s for _, s in entries)
            count = len(entries)
            total_size += size
            total_count += count
            if count:
                table.add_row(suffix, f"{count:,}", fmt_bytes(size))
            else:
                table.add_row(f"[dim]{suffix}[/dim]", "[dim]0[/dim]", "[dim]--[/dim]")

        if other_files:
            other_size = sum(s for _, s in other_files)
            total_size += other_size
            total_count += len(other_files)
            table.add_row("[dim]other[/dim]", f"{len(other_files):,}", fmt_bytes(other_size))

        table.add_row("", "", "")
        table.add_row("[bold]Total[/bold]", f"[bold]{total_count:,}[/bold]", f"[bold]{fmt_bytes(total_size)}[/bold]")
        console.print(table)

        # Extraction status
        gz_suffixes = [k for k in ext_groups if k.endswith(".gz")]
        gz_files_list = []
        for s in gz_suffixes:
            gz_files_list.extend(ext_groups[s])
        if gz_files_list:
            extracted_missing = sum(
                1 for gz_path, _ in gz_files_list
                if not (gz_path.parent / gz_path.name[:-3]).exists()
            )
            console.print()
            if extracted_missing == 0:
                console.print(f"[green]All {len(gz_files_list):,} .gz files have been extracted.[/green]")
            else:
                console.print(f"[yellow]{extracted_missing:,}/{len(gz_files_list):,} .gz files not yet extracted.[/yellow]")

        # Dataset folder count
        info_entries = ext_groups.get("_info.json", [])
        if info_entries:
            dataset_dirs = {f.parent for f, _ in info_entries}
            console.print(f"\n[cyan]Dataset folders on disk: {len(dataset_dirs):,}[/cyan]")


@main.command()
@click.option("-o", "--output", default="./stats_data", type=click.Path(),
              help="Output directory")
@click.option("--depth", default=3, type=int,
              help="Max tree depth to display")
@click.pass_context
def tree(ctx, output, depth):
    """Fetch and display the category tree from the data source."""
    import asyncio as _asyncio
    import httpx

    sources = ctx.obj["sources"]

    for src_idx, source in enumerate(sources):
        cfg = source.config()

        if len(sources) > 1:
            if src_idx > 0:
                console.print()
            console.print(f"[bold]{'═' * 60}[/bold]")
            console.print(f"[bold]  {cfg.display_name}[/bold]")
            console.print(f"[bold]{'═' * 60}[/bold]\n")

        async def _show_tree():
            try:
                async with httpx.AsyncClient(follow_redirects=True) as client:
                    catalog = await source.fetch_catalog(client)
            except NotImplementedError as e:
                console.print(f"[red]{e}[/red]")
                return

            datasets = source.collect_datasets(catalog)
            console.print(f"[bold]{cfg.display_name} Category Tree[/bold] ({len(datasets)} datasets)\n")

            def _print(entry, indent=0):
                if indent > depth:
                    return
                prefix = "  " * indent
                if entry.is_dataset:
                    console.print(f"{prefix}[dim]{entry.code}[/dim] {entry.title}")
                else:
                    console.print(f"{prefix}[bold]{entry.code}[/bold] {entry.title}")
                    for child in entry.children:
                        _print(child, indent + 1)

            for child in catalog.children:
                _print(child)

        _asyncio.run(_show_tree())


if __name__ == "__main__":
    main()
