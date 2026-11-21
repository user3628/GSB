"""Parameter injection entry point used to exercise failure chains.

The four supported injections deliberately trigger the documented business
codes:

* dimension mismatch -> E_DIM_MISMATCH (stage 1)
* missing reference -> E_MISSING_REFERENCE (stage 2)
* NaN pixels         -> E_NUMERIC (stage 1)
* negative weight    -> E_INVALID_CONFIG (stage 4)
"""

from dataclasses import dataclass, field, asdict

from .errors import BusinessError, E_INVALID_CONFIG

VALID_KINDS = (
    "dimension_mismatch",
    "missing_reference",
    "nan_pixels",
    "negative_weight",
)


@dataclass(frozen=True)
class Injection:
    kind: str = "none"
    image_id: str = ""
    value: float = -1.0

    def __post_init__(self):
        if self.kind != "none" and self.kind not in VALID_KINDS:
            raise BusinessError(
                E_INVALID_CONFIG,
                f"unknown injection kind {self.kind}",
                {"kind": self.kind, "allowed": list(VALID_KINDS)},
            )

    def to_dict(self):
        return asdict(self)

    def targets(self, image_id):
        return self.kind != "none" and (not self.image_id or self.image_id == image_id)
