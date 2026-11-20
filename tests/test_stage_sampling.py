from prefalign.core.stage_sampling import largest_remainder, run


def test_largest_remainder_proportional():
    weights = {"a::en": 0.5, "b::en": 0.3, "c::en": 0.2}
    capacities = {"a::en": 1000, "b::en": 1000, "c::en": 1000}
    allocations, _ = largest_remainder(weights, capacities, total_seats=300)
    assert sum(allocations.values()) == 300
    assert allocations == {"a::en": 150, "b::en": 90, "c::en": 60}


def test_largest_remainder_remainder_tie_lexical():
    weights = {"z::en": 0.5, "a::en": 0.5}
    capacities = {"z::en": 100, "a::en": 100}
    allocations, _ = largest_remainder(weights, capacities, total_seats=11)
    assert sum(allocations.values()) == 11
    # Identical remainders: lexical key "a::en" wins the leftover seat.
    assert allocations == {"z::en": 5, "a::en": 6}


def test_largest_remainder_capacity_clip_redistributes():
    weights = {"a::en": 0.8, "b::en": 0.2}
    capacities = {"a::en": 3, "b::en": 100}
    allocations, _ = largest_remainder(weights, capacities, total_seats=103)
    assert sum(allocations.values()) == 103
    assert allocations == {"a::en": 3, "b::en": 100}


def test_zero_weight_layer_diagnostic():
    weights = {"a::en": 1.0, "b::en": 0.0}
    capacities = {"a::en": 5, "b::en": 5}
    allocations, diagnostics = largest_remainder(weights, capacities, total_seats=10)
    assert allocations["b::en"] == 0
    assert any(d["code"] == "E_ALLOCATION_INVALID" and d["layer"] == "b::en"
               for d in diagnostics)


def test_zero_weight_sum():
    _, diagnostics = largest_remainder({"a::en": 0.0}, {"a::en": 5}, total_seats=5)
    assert diagnostics[0]["code"] == "E_ALLOCATION_INVALID"


def test_empty_layer_diagnostic():
    weights = {"a::en": 0.6, "b::en": 0.4}
    capacities = {"a::en": 5, "b::en": 0}
    allocations, diagnostics = largest_remainder(weights, capacities, total_seats=5)
    assert sum(allocations.values()) == 5
    assert any(d["layer"] == "b::en" for d in diagnostics)


def test_deterministic_repeat():
    weights = {"z::en": 0.5, "a::en": 0.5}
    capacities = {"z::en": 30, "a::en": 40}
    first, _ = largest_remainder(weights, capacities, total_seats=70)
    second, _ = largest_remainder(weights, capacities, total_seats=70)
    assert first == second


def test_pipeline_sampling(built_pipeline):
    result = built_pipeline.load_artifact("sampling")
    ids_a = result["mixture_summary"]["selected_pair_ids"]
    built_pipeline.run(reason="noop")
    result2 = built_pipeline.load_artifact("sampling")
    assert ids_a == result2["mixture_summary"]["selected_pair_ids"]
    assert all(layer["allocated"] <= layer["available_pairs"]
               for layer in result["layers"])
