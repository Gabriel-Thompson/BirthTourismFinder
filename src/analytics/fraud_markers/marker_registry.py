from __future__ import annotations

from typing import Callable, Dict, Type

MARKER_REGISTRY: Dict[str, Type[object]] = {}


def register_marker(name: str) -> Callable[[Type[object]], Type[object]]:
    def decorator(cls: Type[object]) -> Type[object]:
        MARKER_REGISTRY[name] = cls
        return cls

    return decorator


def get_registered_markers() -> Dict[str, Type[object]]:
    return dict(MARKER_REGISTRY)
