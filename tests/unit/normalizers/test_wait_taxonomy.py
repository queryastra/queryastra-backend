"""Testes da taxonomia canonica de wait — `Docs/wait-taxonomy.md`."""

from __future__ import annotations

import pytest

from app.normalizers.wait_taxonomy import (
    WaitBucket,
    WaitCategory,
    map_wait_event,
    wait_bucket_for,
)


def test_nine_canonical_categories() -> None:
    assert {c.value for c in WaitCategory} == {
        "CPU",
        "IO",
        "LOCK",
        "LATCH",
        "MEMORY",
        "NETWORK",
        "IPC",
        "IDLE",
        "OTHER",
    }


@pytest.mark.parametrize(
    ("native_value", "expected"),
    [
        ("IO", WaitCategory.IO),
        ("Lock", WaitCategory.LOCK),
        ("LWLock", WaitCategory.LATCH),
        ("Client", WaitCategory.NETWORK),
        ("IPC", WaitCategory.IPC),
        ("Timeout", WaitCategory.IDLE),
        ("Extension", WaitCategory.OTHER),
        (None, WaitCategory.CPU),  # ausencia de wait_event no Postgres = rodando em CPU
    ],
)
def test_map_wait_event_postgresql(native_value: str | None, expected: WaitCategory) -> None:
    assert map_wait_event("postgresql", native_value) == expected


@pytest.mark.parametrize(
    ("native_value", "expected"),
    [
        ("SOS_SCHEDULER_YIELD", WaitCategory.CPU),
        ("PAGEIOLATCH_SH", WaitCategory.IO),
        ("WRITELOG", WaitCategory.IO),
        ("LCK_M_X", WaitCategory.LOCK),
        ("PAGELATCH_EX", WaitCategory.LATCH),
        ("RESOURCE_SEMAPHORE", WaitCategory.MEMORY),
        ("ASYNC_NETWORK_IO", WaitCategory.NETWORK),
        ("CXPACKET", WaitCategory.IPC),
        ("SLEEP_TASK", WaitCategory.IDLE),
        ("SOME_UNKNOWN_WAIT_TYPE", WaitCategory.OTHER),
    ],
)
def test_map_wait_event_sqlserver(native_value: str, expected: WaitCategory) -> None:
    assert map_wait_event("sqlserver", native_value) == expected


def test_map_wait_event_unknown_engine_raises() -> None:
    with pytest.raises(ValueError):
        map_wait_event("oracle", "whatever")


@pytest.mark.parametrize(
    ("category", "expected_bucket"),
    [
        (WaitCategory.IO, WaitBucket.DISK),
        (WaitCategory.CPU, WaitBucket.CPU_MEM),
        (WaitCategory.MEMORY, WaitBucket.CPU_MEM),
        (WaitCategory.LOCK, WaitBucket.OTHER),
        (WaitCategory.OTHER, WaitBucket.OTHER),
    ],
)
def test_wait_bucket_for(category: WaitCategory, expected_bucket: WaitBucket) -> None:
    assert wait_bucket_for(category) == expected_bucket
