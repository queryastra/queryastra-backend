"""Testes de `app/normalizers/metric_names.py` — nomes canonicos de metric_name."""

from __future__ import annotations

from app.normalizers import metric_names


def _all_string_constants() -> list[str]:
    values = []
    for name in dir(metric_names):
        if name.startswith("_"):
            continue
        value = getattr(metric_names, name)
        if isinstance(value, str):
            values.append(value)
    return values


def test_all_metric_name_constants_are_strings() -> None:
    values = _all_string_constants()
    assert len(values) > 0
    assert all(isinstance(v, str) for v in values)


def test_all_metric_name_constants_are_unique() -> None:
    values = _all_string_constants()
    assert len(values) == len(set(values))


def test_raw_counter_mapping_values_are_known_metric_names() -> None:
    known_metric_names = set(_all_string_constants())
    for metric_name in metric_names.SQLSERVER_RAW_COUNTER_TO_METRIC_NAME.values():
        assert metric_name in known_metric_names


def test_raw_counter_mapping_keys_are_unique_and_non_empty() -> None:
    keys = list(metric_names.SQLSERVER_RAW_COUNTER_TO_METRIC_NAME.keys())
    assert len(keys) == len(set(keys))
    assert all(isinstance(k, str) and k for k in keys)
