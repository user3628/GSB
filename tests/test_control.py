from __future__ import annotations

from pathlib import Path

from image_workbench.control import StopController


def test_stop_flag_coordinates_separate_process_controllers(tmp_path: Path) -> None:
    flag = tmp_path / "stop.request"
    server_controller = StopController(flag)
    cli_controller = StopController(flag)
    cli_controller.request_stop()
    assert server_controller.is_requested() is True
    cli_controller.resume()
    assert server_controller.is_requested() is False
