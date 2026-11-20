import pytest

from lora_bench.config import Budget, ExperimentConfig, TaskConfig
from lora_bench.data import Sample
from lora_bench.errors import BudgetExceededError
from lora_bench.scheduler import (
    REASON_GLOBAL,
    REASON_QUOTA,
    admit_tasks,
    assert_can_start,
    estimated_sample_consumption,
)


def _task(task_id, batch=2, accum=2, steps=10):
    return TaskConfig(
        task_id=task_id,
        rank=4,
        alpha=8.0,
        target_modules=("q_proj",),
        learning_rate=0.01,
        batch_size=batch,
        grad_accum=accum,
        max_steps=steps,
    )


def _samples(n=6):
    return [
        Sample(f"s{i}", f"instruction {i}", f"output {i}", "zh" if i % 2 else "en", "t")
        for i in range(n)
    ]


def test_estimate_counts_reuse():
    task = _task("t", batch=4, accum=4, steps=20)
    assert estimated_sample_consumption(task, 6) == 320


def test_quota_and_global_cap_enforced():
    budget = Budget(max_workers=2, per_task_sample_quota=100, global_sample_cap=150)
    tasks = (_task("a", steps=10), _task("b", steps=10), _task("c", steps=20))
    config = ExperimentConfig(
        experiment="e",
        model_path="m",
        samples_path="s",
        seed=1,
        checkpoint_every=2,
        budget=budget,
        tasks=tasks,
    )
    admitted, decisions, consumed = admit_tasks(config, _samples())
    assert [task.task_id for task in admitted] == ["a", "b"]
    assert consumed == [40, 40]
    by_id = {item.task_id: item for item in decisions}
    assert by_id["c"].admitted is False
    assert by_id["c"].reason == REASON_GLOBAL
    assert by_id["c"].estimated_samples == 80


def test_per_task_quota_reason():
    budget = Budget(max_workers=2, per_task_sample_quota=30, global_sample_cap=1000)
    config = ExperimentConfig(
        "e", "m", "s", 1, 2, budget, (_task("big", batch=4, accum=4, steps=20),)
    )
    admitted, decisions, _ = admit_tasks(config, _samples())
    assert admitted == []
    assert decisions[0].reason == REASON_QUOTA


def test_assert_can_start_raises_budget_error():
    budget = Budget(1, 50, 50)
    with pytest.raises(BudgetExceededError) as exc:
        assert_can_start(_task("t", steps=10), 60, budget, 50)
    assert exc.value.code == "E_BUDGET_EXCEEDED"
    assert exc.value.task_id == "t"
