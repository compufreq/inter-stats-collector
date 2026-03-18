"""Rename existing directories between code-based and display (slugified) naming."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

from .sources.eurostat.toc import _slugify

log = logging.getLogger(__name__)


def _collect_segment_renames(
    node: dict,
    direction: str,
    parent_actual: str = "",
) -> list[tuple[str, str, str]]:
    """Walk the tree and collect per-segment renames: (parent_dir, old_name, new_name).

    Yields renames level by level (shallowest first via BFS-like recursion)
    so parents are renamed before their children.

    direction:
        "to-display"  — code segment → slugified display segment
        "to-code"     — slugified display segment → code segment
    """
    results: list[tuple[str, str, str]] = []

    code = node.get("code", "")
    title = node.get("title", "")
    node_type = node.get("type", "")
    is_dataset = node_type in ("dataset", "table")

    # Determine the code-based and display-based segment names
    code_name = code
    if is_dataset:
        display_name = code  # datasets keep their code
    else:
        display_name = _slugify(title) if title else code

    # Only rename if the two differ (i.e., it's a category folder whose slug != code)
    if code_name != display_name:
        if direction == "to-display":
            old_name = code_name
            new_name = display_name
        else:
            old_name = display_name
            new_name = code_name
        results.append((parent_actual, old_name, new_name))
        # After this rename, the actual path for children uses the new name
        current_actual = f"{parent_actual}/{new_name}" if parent_actual else new_name
    else:
        current_actual = f"{parent_actual}/{code_name}" if parent_actual else code_name

    # Recurse into children
    for child in node.get("children", []):
        results.extend(_collect_segment_renames(child, direction, current_actual))

    return results


def rename_directories(
    output_dir: Path,
    direction: str,
    *,
    tree_index_filename: str = "eurostat_tree_index.json",
    source_display_name: str = "Eurostat",
    dry_run: bool = False,
    console: Console | None = None,
) -> tuple[int, int, int]:
    """Rename directories in output_dir according to direction.

    Args:
        output_dir: Root output directory (contains tree index JSON)
        direction: "to-display" or "to-code"
        tree_index_filename: Name of the tree index JSON file
        source_display_name: Display name of the data source
        dry_run: If True, only print what would happen
        console: Rich console for output

    Returns:
        (renamed_count, skipped_count, error_count)
    """
    if console is None:
        console = Console()

    index_file = output_dir / tree_index_filename
    if not index_file.exists():
        console.print(f"[red]No {tree_index_filename} found. Run 'collect' first "
                       f"or provide the correct output directory.[/red]")
        return (0, 0, 0)

    tree = json.loads(index_file.read_text())

    # Verify tree has folder_path data
    first_child = (tree.get("children") or [{}])[0] if tree.get("children") else {}
    if not first_child.get("folder_path"):
        console.print("[red]Tree index does not contain folder_path data. "
                       "Re-run 'collect' to regenerate the index.[/red]")
        return (0, 0, 0)

    renames = _collect_segment_renames(tree, direction)
    if not renames:
        console.print("[green]No renames needed — paths already match target style.[/green]")
        return (0, 0, 0)

    # Filter to only renames where old_name != new_name (should already be the case)
    renames = [(parent, old, new) for parent, old, new in renames if old != new]

    label = "display names" if direction == "to-display" else f"{source_display_name} codes"
    console.print(f"[bold blue]{source_display_name} Directory Renamer[/bold blue]")
    console.print(f"Directory: {output_dir}")
    console.print(f"Target style: [bold]{label}[/bold]")
    console.print(f"Segments to rename: {len(renames)}")

    if dry_run:
        console.print(f"\n[bold yellow]DRY RUN — no changes will be made[/bold yellow]\n")

    renamed = 0
    skipped = 0
    errors = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold]Renaming[/bold]"),
        BarColumn(bar_width=40),
        MofNCompleteColumn(),
        TextColumn("{task.fields[current]}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Renaming", total=len(renames), current="")

        for parent_rel, old_name, new_name in renames:
            parent_abs = output_dir / parent_rel if parent_rel else output_dir
            old_abs = parent_abs / old_name
            new_abs = parent_abs / new_name

            progress.update(task, current=f"[dim]{old_name} → {new_name}[/dim]")

            if not old_abs.exists():
                skipped += 1
                progress.advance(task)
                continue

            if new_abs.exists():
                skipped += 1
                progress.advance(task)
                continue

            if dry_run:
                rel_old = f"{parent_rel}/{old_name}" if parent_rel else old_name
                rel_new = f"{parent_rel}/{new_name}" if parent_rel else new_name
                console.print(f"  [dim]would rename:[/dim] {rel_old} → {rel_new}")
                renamed += 1
                progress.advance(task)
                continue

            try:
                old_abs.rename(new_abs)
                renamed += 1
            except OSError as e:
                errors += 1
                console.print(f"  [red]ERROR: {old_name} → {new_name}: {e}[/red]")

            progress.advance(task)

    console.print()
    console.print("[bold]Rename Summary[/bold]")
    console.print(f"  Renamed:   [green]{renamed}[/green]")
    console.print(f"  Skipped:   {skipped}")
    if errors:
        console.print(f"  Errors:    [red]{errors}[/red]")
    if dry_run:
        console.print(f"\n[yellow]This was a dry run. Remove --dry-run to apply changes.[/yellow]")

    return (renamed, skipped, errors)
