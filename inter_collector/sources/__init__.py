"""Data source registry — maps --stats values to source implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..base import DataSource

# Maps the --stats CLI value to (module_path, class_name) for lazy loading.
SOURCE_REGISTRY: dict[str, tuple[str, str]] = {
    "europe": ("inter_collector.sources.eurostat.source", "EurostatSource"),
    "uk": ("inter_collector.sources.ons.source", "ONSSource"),
    "switzerland": ("inter_collector.sources.swiss.source", "SwissSource"),
    "unhcr": ("inter_collector.sources.unhcr.source", "UNHCRSource"),
    "hdx": ("inter_collector.sources.hdx.source", "HDXSource"),
    "netherlands": ("inter_collector.sources.netherlands.source", "NetherlandsSource"),
    "germany": ("inter_collector.sources.germany.source", "GermanySource"),
}


def resolve_source(stats_key: str, **kwargs) -> "DataSource":
    """Resolve a --stats key to an instantiated DataSource.

    Extra kwargs are forwarded only if the source constructor accepts them.
    Unrecognised kwargs are silently ignored (e.g., org_filter is ignored
    for EurostatSource which doesn't support it).
    """
    if stats_key not in SOURCE_REGISTRY:
        raise ValueError(f"Unknown source: {stats_key!r}. Available: {list(SOURCE_REGISTRY.keys())}")

    module_path, class_name = SOURCE_REGISTRY[stats_key]

    import importlib
    import inspect
    mod = importlib.import_module(module_path)
    cls = getattr(mod, class_name)
    # Only pass kwargs that the constructor accepts
    sig = inspect.signature(cls.__init__)
    accepted = {k: v for k, v in kwargs.items() if k in sig.parameters}
    return cls(**accepted)


def resolve_all_sources(**kwargs) -> list["DataSource"]:
    """Instantiate all registered data sources.

    Extra kwargs are forwarded only to sources whose constructors accept them.
    """
    import inspect
    sources = []
    for key in SOURCE_REGISTRY:
        module_path, class_name = SOURCE_REGISTRY[key]
        import importlib
        mod = importlib.import_module(module_path)
        cls = getattr(mod, class_name)
        # Only pass kwargs that the constructor accepts
        sig = inspect.signature(cls.__init__)
        accepted = {k: v for k, v in kwargs.items() if k in sig.parameters}
        sources.append(cls(**accepted))
    return sources
