import copy

import torch

from lora_bench.config import TaskConfig
from lora_bench.data import Sample, build_batch, samples_fingerprint
from lora_bench.determinism import set_global_seed, tensor_digest
from lora_bench.generator import DEFAULT_MODEL_CONFIG, generate_model_weights
from lora_bench.model import (
    TinyTransformer,
    adapter_summary,
    inject_lora,
    lora_parameters,
    lora_state_dict,
)
from lora_bench.training import build_optimizer, run_step


def _samples():
    return [
        Sample("s1", "翻译这句话", "translate this", "zh", "t"),
        Sample("s2", "summarize text", "总结文本", "en", "t"),
        Sample("s3", "关键词提取", "extract keywords", "zh", "t"),
        Sample("s4", "rewrite question", "改写问题", "en", "t"),
    ]


def _task(**overrides):
    base = dict(
        task_id="t",
        rank=4,
        alpha=8.0,
        target_modules=("q_proj", "v_proj"),
        learning_rate=0.05,
        batch_size=2,
        grad_accum=2,
        max_steps=4,
    )
    base.update(overrides)
    return TaskConfig(**base)


def _fresh_model(model_path):
    state = torch.load(model_path, map_location="cpu")
    model = TinyTransformer(DEFAULT_MODEL_CONFIG)
    model.load_state_dict(state)
    return model


def test_lora_b_zero_and_only_adapters_train(tmp_path):
    model_path = str(tmp_path / "m.pt")
    generate_model_weights(model_path)
    model = _fresh_model(model_path)
    inject_lora(model, ["q_proj", "v_proj"], rank=4, alpha=8.0, base_seed=123)
    for name, module in model.named_modules():
        if module.__class__.__name__ == "LoRALinear":
            assert torch.count_nonzero(module.lora_b) == 0
            assert module.lora_a.requires_grad and module.lora_b.requires_grad
            assert not module.base.weight.requires_grad
    assert all(param.requires_grad for param in lora_parameters(model))
    assert all(
        not param.requires_grad
        for name, param in model.named_parameters()
        if "lora_" not in name
    )


def test_bit_identical_runs(tmp_path):
    model_path = str(tmp_path / "m.pt")
    generate_model_weights(model_path)
    samples = _samples()
    task = _task()
    losses_runs = []
    digests_runs = []
    for _ in range(2):
        set_global_seed(7)
        model = _fresh_model(model_path)
        inject_lora(model, task.target_modules, task.rank, task.alpha, base_seed=7)
        optimizer = build_optimizer(model, task)
        losses = []
        cursor = 0
        for _ in range(task.max_steps):
            batches = []
            for _ in range(task.grad_accum):
                chunk = []
                for _ in range(task.batch_size):
                    chunk.append(samples[cursor % len(samples)])
                    cursor += 1
                batches.append(build_batch(chunk, DEFAULT_MODEL_CONFIG.max_seq_len))
            losses.append(run_step(model, optimizer, batches))
        losses_runs.append(losses)
        digests_runs.append(adapter_summary(model))
    assert losses_runs[0] == losses_runs[1]
    assert digests_runs[0] == digests_runs[1]


def test_lora_scaling_applied(tmp_path):
    model_path = str(tmp_path / "m.pt")
    generate_model_weights(model_path)
    model = _fresh_model(model_path)
    inject_lora(model, ["q_proj"], rank=4, alpha=8.0, base_seed=99)
    wrapper = None
    for module in model.modules():
        if module.__class__.__name__ == "LoRALinear":
            wrapper = module
            break
    assert wrapper.scaling == 2.0


def test_sample_fingerprint_stable():
    samples = _samples()
    assert samples_fingerprint(samples) == samples_fingerprint(copy.deepcopy(samples))
    changed = _samples()
    changed[0] = Sample("s1", "different", "translate this", "zh", "t")
    assert samples_fingerprint(changed) != samples_fingerprint(samples)
