"""Engine configuration: quantization, projection rule, concat, weights."""

import copy

from .util import canonical_json_bytes, fingerprint_obj

DEFAULT_CONFIG = {
    "quantization": {"bits": 16},
    "projection": {"scale_rule": "pct_div_100"},
    "concat": {
        "order": "text_then_vision",
        "text_gain": 1.0,
        "vision_gain": 1.0,
    },
    "operator_weights": {
        "op-encoder-a": 1.0,
        "op-encoder-b": 0.8,
        "default": 0.5,
    },
}


class EngineConfig:
    def __init__(self, data=None):
        self.data = copy.deepcopy(data if data is not None else DEFAULT_CONFIG)

    @property
    def bits(self):
        return int(self.data["quantization"]["bits"])

    @property
    def scale_rule(self):
        return self.data["projection"]["scale_rule"]

    @property
    def concat_rule(self):
        return self.data["concat"]

    @property
    def operator_weights(self):
        return self.data["operator_weights"]

    def to_dict(self):
        return copy.deepcopy(self.data)

    def quant_fp(self):
        return fingerprint_obj("quant", canonical_json_bytes(self.data["quantization"]))

    def concat_fp(self):
        payload = {"concat": self.data["concat"],
                   "operator_weights": self.data["operator_weights"]}
        return fingerprint_obj("concat", canonical_json_bytes(payload))

    def config_version(self):
        """Config version fingerprint surfaced in the alignment summary."""
        return fingerprint_obj(
            "config", canonical_json_bytes(self.data))[:16]
