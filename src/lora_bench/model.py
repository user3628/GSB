"""Tiny, fully local Transformer encoder variant with LoRA injection.

The model is intentionally small so a whole experiment runs in seconds on
CPU.  All weights come from fixed-seed generators; no pretrained checkpoint or
network access is ever used.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable

import torch
from torch import nn

from .determinism import torch_rng


@dataclass
class ModelConfig:
    vocab_size: int
    max_seq_len: int = 64
    d_model: int = 48
    n_heads: int = 4
    d_ff: int = 96
    n_layers: int = 2
    dropout: float = 0.0

    def to_dict(self) -> dict:
        return {
            "vocab_size": self.vocab_size,
            "max_seq_len": self.max_seq_len,
            "d_model": self.d_model,
            "n_heads": self.n_heads,
            "d_ff": self.d_ff,
            "n_layers": self.n_layers,
            "dropout": self.dropout,
        }


class LoRALinear(nn.Module):
    """Wraps a frozen ``nn.Linear`` and adds ``(alpha/rank) * B @ A``."""

    def __init__(self, base: nn.Linear, rank: int, alpha: float, seed: int) -> None:
        super().__init__()
        if rank <= 0:
            raise ValueError(f"rank must be positive, got {rank}")
        self.base = base
        for param in self.base.parameters():
            param.requires_grad_(False)
        in_features = base.in_features
        out_features = base.out_features
        self.rank = rank
        self.alpha = alpha
        self.scaling = alpha / rank
        gen = torch_rng(seed)
        # A: Kaiming-uniform style small values from a fixed generator; B: zeros.
        bound = 1.0 / math.sqrt(in_features)
        self.lora_a = nn.Parameter(
            torch.empty((rank, in_features)).uniform_(-bound, bound, generator=gen)
        )
        self.lora_b = nn.Parameter(torch.zeros((out_features, rank)))
        self.lora_a.requires_grad_(True)
        self.lora_b.requires_grad_(True)

    def forward(self, inputs: torch.Tensor) -> torch.Tensor:
        base_out = self.base(inputs)
        delta = torch.nn.functional.linear(
            torch.nn.functional.linear(inputs, self.lora_a), self.lora_b
        )
        return base_out + delta * self.scaling


class MultiHeadAttention(nn.Module):
    def __init__(self, cfg: ModelConfig) -> None:
        super().__init__()
        assert cfg.d_model % cfg.n_heads == 0
        self.n_heads = cfg.n_heads
        self.head_dim = cfg.d_model // cfg.n_heads
        self.q_proj = nn.Linear(cfg.d_model, cfg.d_model)
        self.k_proj = nn.Linear(cfg.d_model, cfg.d_model)
        self.v_proj = nn.Linear(cfg.d_model, cfg.d_model)
        self.out_proj = nn.Linear(cfg.d_model, cfg.d_model)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        batch, seq, d_model = x.shape
        queries = self.q_proj(x).view(batch, seq, self.n_heads, self.head_dim).transpose(1, 2)
        keys = self.k_proj(x).view(batch, seq, self.n_heads, self.head_dim).transpose(1, 2)
        values = self.v_proj(x).view(batch, seq, self.n_heads, self.head_dim).transpose(1, 2)
        scores = torch.matmul(queries, keys.transpose(-2, -1)) / math.sqrt(self.head_dim)
        attn = torch.softmax(scores, dim=-1)
        context = torch.matmul(attn, values)
        context = context.transpose(1, 2).contiguous().view(batch, seq, d_model)
        return self.out_proj(context)


class TransformerBlock(nn.Module):
    def __init__(self, cfg: ModelConfig) -> None:
        super().__init__()
        self.attn = MultiHeadAttention(cfg)
        self.ff1 = nn.Linear(cfg.d_model, cfg.d_ff)
        self.ff2 = nn.Linear(cfg.d_ff, cfg.d_model)
        self.norm1 = nn.LayerNorm(cfg.d_model)
        self.norm2 = nn.LayerNorm(cfg.d_model)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.norm1(x + self.attn(x))
        fed = torch.nn.functional.gelu(self.ff1(x))
        x = self.norm2(x + self.ff2(fed))
        return x


class TinyTransformer(nn.Module):
    def __init__(self, cfg: ModelConfig) -> None:
        super().__init__()
        self.cfg = cfg
        self.token_emb = nn.Embedding(cfg.vocab_size, cfg.d_model)
        gen = torch_rng(20240517)
        position = torch.empty(cfg.max_seq_len, cfg.d_model).normal_(0.0, 0.02, generator=gen)
        self.register_buffer("position_emb", position)
        self.blocks = nn.ModuleList([TransformerBlock(cfg) for _ in range(cfg.n_layers)])
        self.head = nn.Linear(cfg.d_model, cfg.vocab_size)

    def forward(self, token_ids: torch.Tensor) -> torch.Tensor:
        x = self.token_emb(token_ids) + self.position_emb[: token_ids.size(1)]
        for block in self.blocks:
            x = block(x)
        return self.head(x)


def init_model_weights(model: nn.Module, seed: int) -> None:
    """Initialise every parameter deterministically (embeddings/linears/LayerNorm)."""
    gen = torch_rng(seed)
    for name, param in model.named_parameters():
        if param.dim() >= 2:
            bound = 1.0 / math.sqrt(param.shape[-1])
            param.data.uniform_(-bound, bound, generator=gen)
        else:
            param.data.normal_(0.0, 0.02, generator=gen)


# Names exposed for LoRA targeting, e.g. "blocks.0.attn.q_proj".
LORA_TARGETABLE = {
    "q_proj",
    "k_proj",
    "v_proj",
    "out_proj",
    "ff1",
    "ff2",
    "head",
}


def _resolve_linear(root: nn.Module, dotted_name: str) -> nn.Linear:
    module: nn.Module = root
    for part in dotted_name.split("."):
        module = getattr(module, part)
    if not isinstance(module, nn.Linear):
        raise ValueError(f"LoRA target {dotted_name!r} is not an nn.Linear")
    return module


def inject_lora(
    model: TinyTransformer,
    target_modules: Iterable[str],
    rank: int,
    alpha: float,
    base_seed: int,
) -> list[str]:
    """Replace each declared target Linear with a :class:`LoRALinear` wrapper.

    Suffix names accepted by the config (e.g. ``q_proj``) match every layer;
    fully dotted names (``blocks.0.attn.q_proj``) match a single module.
    Wrapper seeds are derived deterministically from ``base_seed`` and the
    module name so identical configurations reproduce identical adapters.
    """
    targets = sorted(set(target_modules))
    replaced: list[str] = []
    for param in model.parameters():
        param.requires_grad_(False)
    for name, module in list(model.named_modules()):
        leaf = name.split(".")[-1]
        if leaf in targets or name in targets:
            if not isinstance(module, nn.Linear):
                continue
            parent = model
            parts = name.split(".")
            for part in parts[:-1]:
                parent = getattr(parent, part)
            seed = base_seed ^ (abs(hash_deterministic(name)) % (2**31))
            wrapper = LoRALinear(module, rank, alpha, seed)
            setattr(parent, parts[-1], wrapper)
            replaced.append(name)
    if not replaced:
        raise ValueError("no LoRA target module matched the configured names")
    return sorted(replaced)


def hash_deterministic(text: str) -> int:
    import hashlib

    return int.from_bytes(hashlib.sha256(text.encode("utf-8")).digest()[:8], "big")


def lora_parameters(model: nn.Module) -> list[nn.Parameter]:
    return [p for p in model.parameters() if p.requires_grad]


def lora_state_dict(model: nn.Module) -> dict[str, torch.Tensor]:
    return {
        name: param.detach().cpu().clone()
        for name, param in model.named_parameters()
        if param.requires_grad
    }


def adapter_summary(model: nn.Module) -> dict[str, str]:
    """Stable per-tensor sha256 fingerprint of every trainable adapter tensor."""
    from .determinism import tensor_digest

    return {
        name: tensor_digest(param)
        for name, param in model.named_parameters()
        if param.requires_grad
    }
