from __future__ import annotations

import numpy as np

from qoplib.io_util import load_npz
from qoplib.quantization import (
    QuantConfig,
    has_non_finite,
    non_finite_summary,
    quantize,
)


def test_demo_dataset_contract(demo_data):
    shard00 = load_npz(demo_data / "shard_00.npz")
    shard02 = load_npz(demo_data / "shard_02.npz")
    shard03 = load_npz(demo_data / "shard_03.npz")

    # Outliers are present and survive an 8-bit symmetric quantization.
    x = shard00["activation"].astype(np.float64)
    assert np.abs(x).max() >= 4.0
    qx = quantize(x, QuantConfig(8, "symmetric", "tensor"))
    assert qx.qvalues.shape == x.shape
    assert qx.qmin == -127 and qx.qmax == 127

    # All-zero output channel keeps a finite scale and a constant q value.
    w = shard02["w_fc1"].astype(np.float64)
    assert np.all(w[7] == 0.0)
    qw = quantize(w, QuantConfig(8, "asymmetric", "channel",
                                 channel_axis=1))
    assert np.all(np.isfinite(qw.scale))
    zp = qw.zero_point.reshape(-1)[7]
    assert np.all(qw.qvalues[7] == zp)

    # NaN and both infinities are detected and counted.
    bad = shard03["activation"].astype(np.float64)
    assert has_non_finite(bad)
    summary = non_finite_summary(bad)
    assert summary == {"nan_count": 1, "pos_inf_count": 1,
                       "neg_inf_count": 1}


def test_demo_manifest_covers_required_matrix(demo_data):
    import yaml
    manifest = yaml.safe_load((demo_data / "manifest.yaml").read_text())
    bitwidths = set()
    modes = set()
    granularities = set()
    for shard in manifest["shards"]:
        for layer in shard["layers"]:
            bitwidths.add(layer["bitwidth"])
            modes.add(layer["mode"])
            granularities.add(layer["granularity"])
    assert bitwidths == {4, 8}
    assert modes == {"symmetric", "asymmetric"}
    assert "channel" in granularities
    assert len(manifest["shards"]) >= 4
