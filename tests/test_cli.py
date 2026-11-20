from __future__ import annotations

from pathlib import Path

from quant_audit.cli import main


def test_cli_full_incremental_query_history(demo_paths: dict[str, Path], capsys) -> None:
    workspace = demo_paths["workspace"].root
    data = demo_paths["data"]

    assert main(["calibrate", "--workspace", str(workspace), "--mode", "full"]) == 0
    capsys.readouterr()
    assert (
        main(
            [
                "calibrate",
                "--workspace",
                str(workspace),
                "--mode",
                "incremental",
                "--config",
                str(data / "config_incremental.yaml"),
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert main(["query", "--workspace", str(workspace), "--layers-only"]) == 0
    query_output = capsys.readouterr().out
    assert '"version":2' in query_output.replace(" ", "")
    assert "linear_05" in query_output
    assert main(["history", "--workspace", str(workspace)]) == 0
    history_output = capsys.readouterr().out
    assert '"latest_version":2' in history_output.replace(" ", "")


def test_cli_business_error_exit_code(demo_paths: dict[str, Path], capsys) -> None:
    workspace = demo_paths["workspace"].root
    assert main(["query", "--workspace", str(workspace), "--version", "99"]) == 2
    output = capsys.readouterr().out
    assert "E_INVALID_STAGE" in output

