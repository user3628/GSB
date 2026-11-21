"""Cooperative, chunk-boundary stop control."""

from __future__ import annotations

import threading
from pathlib import Path

from .errors import GracefulStop


class StopController:
    def __init__(
        self,
        *,
        stop_file: str | Path | None = None,
        stop_after_units: int | None = None,
        reset_stop_file: bool = True,
    ) -> None:
        self.stop_file = Path(stop_file) if stop_file is not None else None
        self.stop_after_units = stop_after_units
        self.event = threading.Event()
        self.units_completed = 0
        if reset_stop_file and self.stop_file is not None:
            self.stop_file.parent.mkdir(parents=True, exist_ok=True)
            try:
                self.stop_file.unlink()
            except FileNotFoundError:
                pass

    def request_stop(self) -> None:
        self.event.set()
        if self.stop_file is not None:
            self.stop_file.parent.mkdir(parents=True, exist_ok=True)
            if not self.stop_file.exists():
                self.stop_file.write_text("stop\n", encoding="utf-8")

    def _stop_requested(self) -> bool:
        if self.event.is_set():
            return True
        return self.stop_file is not None and self.stop_file.exists()

    def tick(self, *, stage: str, location: str, next_unit: int | None = None) -> None:
        """Call immediately after completing one indivisible unit of work."""

        self.units_completed += 1
        limit_reached = (
            self.stop_after_units is not None
            and self.units_completed >= self.stop_after_units
        )
        if limit_reached or self._stop_requested():
            raise GracefulStop(
                stage=stage,
                location=location,
                completed_units=self.units_completed,
                next_unit=next_unit,
            )

    def stage_boundary(self, *, stage: str, location: str = "stage_boundary") -> None:
        if self._stop_requested():
            raise GracefulStop(
                stage=stage,
                location=location,
                completed_units=self.units_completed,
                next_unit=None,
            )
