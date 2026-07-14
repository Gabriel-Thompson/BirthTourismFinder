from __future__ import annotations

import pandas as pd

from src.correlation.indexes import (
    build_dataframe_index,
    build_entity_id_index,
    build_group_index,
    build_relationship_endpoint_index,
    candidate_pairs_from_groups,
)


def test_build_dataframe_index_indexes_shared_values() -> None:
    df = pd.DataFrame(
        [
            {"entity_id": "1", "normalized_name": "ALPHA", "building_key": "100 MAIN ST"},
            {"entity_id": "2", "normalized_name": "ALPHA", "building_key": "200 OAK ST"},
        ]
    )

    index = build_dataframe_index(df, fields=["normalized_name", "building_key"])

    assert len(index[("normalized_name", "ALPHA")]) == 2
    assert index[("building_key", "100 MAIN ST")][0]["entity_id"] == "1"


def test_candidate_pairs_from_groups_only_uses_shared_keys() -> None:
    left = {"ALPHA": [{"entity_id": "l1"}], "BETA": [{"entity_id": "l2"}]}
    right = {"ALPHA": [{"entity_id": "r1"}], "GAMMA": [{"entity_id": "r2"}]}

    pairs = candidate_pairs_from_groups(left, right)

    assert pairs == [({"entity_id": "l1"}, {"entity_id": "r1"}, "ALPHA")]


def test_relationship_endpoint_index_maps_both_endpoints() -> None:
    df = pd.DataFrame(
        [
            {"relationship_id": "r1", "source_entity_id": "e1", "target_entity_id": "e2"},
            {"relationship_id": "r2", "source_entity_id": "e2", "target_entity_id": "e3"},
        ]
    )

    endpoint_index = build_relationship_endpoint_index(df)

    assert len(endpoint_index["e2"]) == 2
    assert {row["relationship_id"] for row in endpoint_index["e2"]} == {"r1", "r2"}


def test_entity_and_group_indexes_are_deterministic() -> None:
    df = pd.DataFrame(
        [
            {"entity_id": "e1", "display_name": "ACME"},
            {"entity_id": "e2", "display_name": "ACME"},
        ]
    )

    first_entity_index = build_entity_id_index(df)
    second_entity_index = build_entity_id_index(df)
    first_group_index = build_group_index(df, field="display_name")
    second_group_index = build_group_index(df, field="display_name")

    assert first_entity_index == second_entity_index
    assert first_group_index == second_group_index
