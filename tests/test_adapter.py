from __future__ import annotations

from pathlib import Path

from transcript_workbench.audio import StubTranscriptionAdapter
from transcript_workbench.models import Word
from transcript_workbench.workspace import Workspace


class SingleTokenAdapter:
    name = "test-single-token-adapter"

    def transcribe(
        self,
        *,
        chunk_id: str,
        audio_path: Path,
        duration_ms: int,
        reference_text: str,
        language: str,
        source_version: int,
    ) -> list[Word]:
        del audio_path, reference_text
        return [
            Word(
                entry_id=f"adapter-{chunk_id}",
                chunk_id=chunk_id,
                start_ms=50,
                end_ms=max(51, duration_ms - 50),
                text=f"adapter:{language}",
                confidence=0.777,
                language=language,
                source_version=source_version,
            )
        ]


def test_adapter_is_replaceable_without_changing_version_semantics(demo_data):
    workspace = Workspace(demo_data["root"] / "artifacts" / "adapter-workspace")
    result = workspace.bootstrap(demo_data["manifest_path"], SingleTokenAdapter())
    assert result["version"] == 1
    state = workspace.load_state()
    assert len(state.words) == 3
    assert all(word.entry_id.startswith("adapter-") for word in state.words)
    assert all(word.confidence == 0.777 for word in state.words)

    again = workspace.bootstrap(demo_data["manifest_path"], StubTranscriptionAdapter())
    assert again["version"] == 1
    assert again["word_count"] == 3
    assert workspace.snapshot_info()["archived_versions"] == [1]
