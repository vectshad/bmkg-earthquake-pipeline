import json
from datetime import timezone
from pathlib import Path

import pytest

from pipeline.bmkg import parse, validate

SAMPLE = json.loads((Path(__file__).parent / "fixtures" / "gempaterkini_sample.json").read_text())


def test_parse_types_and_values():
    rows = parse(SAMPLE)
    assert len(rows) == 2
    first = rows[0]
    assert first["event_time"].tzinfo == timezone.utc
    assert first["event_time"].isoformat() == "2026-10-22T10:57:49+00:00"
    assert (first["latitude"], first["longitude"]) == (-3.11, 102.14)
    assert first["magnitude"] == 5.2
    assert first["depth_km"] == 120
    assert first["region"] == "6 km BaratLaut LEBONG-BENGKULU"


def test_sample_rows_are_valid():
    assert all(validate(r) == [] for r in parse(SAMPLE))


@pytest.mark.parametrize(
    "field,value,expected",
    [
        ("magnitude", 11.0, "magnitude out of range"),
        ("depth_km", -5, "depth out of range"),
        ("latitude", 40.0, "coordinates outside BMKG area"),
        ("longitude", 10.0, "coordinates outside BMKG area"),
        ("region", "", "region is empty"),
    ],
)
def test_validate_flags_bad_rows(field, value, expected):
    row = parse(SAMPLE)[0] | {field: value}
    assert any(p.startswith(expected) for p in validate(row))


def test_parse_rejects_unexpected_shape():
    with pytest.raises(KeyError):
        parse({"Infogempa": {"gempa": [{"DateTime": "2026-10-22T10:57:49+00:00"}]}})
