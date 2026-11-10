from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.repositories.anomaly_repository import (
    HOUR,
    HOURLY_ORIGIN,
    METRIC_DEFS,
    bucket_start,
    compute_counter_rates,
    floor_hour,
    metrics_for_engine,
)
from app.workers.anomaly_detection_worker import (
    Baseline,
    advance_streaks,
    daily_buckets,
    decide_transition,
    evaluate,
    robust_center_spread,
    select_baseline,
    weekly_buckets,
)

T0 = datetime(2026, 11, 9, 10, 0, tzinfo=UTC)  # segunda-feira 10:00 UTC


# ── buckets ─────────────────────────────────────────────────────────────────────────────


def test_floor_hour_and_custom_origin() -> None:
    assert floor_hour(T0 + timedelta(minutes=59, seconds=59)) == T0
    origin = T0 + timedelta(minutes=7)
    width = timedelta(minutes=15)
    assert bucket_start(origin + timedelta(minutes=14), origin, width) == origin
    assert bucket_start(origin + timedelta(minutes=15), origin, width) == origin + width
    assert bucket_start(origin - timedelta(seconds=1), origin, width) == origin - width


def test_weekly_and_daily_buckets() -> None:
    assert weekly_buckets(T0, 3) == [T0 - timedelta(weeks=k) for k in (1, 2, 3)]
    assert daily_buckets(T0)[0] == T0 - timedelta(days=1)
    assert len(daily_buckets(T0)) == 14


# ── taxa de contadores ──────────────────────────────────────────────────────────────────


def _rates(points, start=T0, end=T0 + timedelta(hours=2)):
    return compute_counter_rates(points, start=start, end=end, origin=HOURLY_ORIGIN, width=HOUR)


def test_counter_rate_simple() -> None:
    points = [("a", T0 + timedelta(seconds=10 * i), 100.0 + 50 * i) for i in range(7)]
    assert _rates(points) == {T0: pytest.approx(5.0)}


def test_counter_rate_sums_series_and_skips_reset() -> None:
    commit = [
        ("commit", T0, 1000.0),
        ("commit", T0 + timedelta(seconds=10), 1100.0),
        # reset (restart do motor): delta negativo e descartado, nao vira taxa negativa
        ("commit", T0 + timedelta(seconds=20), 5.0),
        ("commit", T0 + timedelta(seconds=30), 105.0),
    ]
    rollback = [
        ("rollback", T0, 0.0),
        ("rollback", T0 + timedelta(seconds=10), 10.0),
    ]
    rates = _rates(commit + rollback)
    # commit: (100 + 100) / 20s = 10/s ; rollback: 10/10s = 1/s
    assert rates[T0] == pytest.approx(11.0)


def test_counter_rate_assigns_delta_to_later_bucket_and_ignores_lookback() -> None:
    points = [
        ("a", T0 - timedelta(seconds=30), 0.0),  # lookback antes da janela
        ("a", T0 + timedelta(seconds=30), 600.0),
        ("a", T0 + HOUR + timedelta(seconds=30), 600.0 + 3600.0),
    ]
    rates = _rates(points)
    assert rates[T0] == pytest.approx(10.0)
    assert rates[T0 + HOUR] == pytest.approx(1.0)


def test_counter_rate_outside_window_ignored() -> None:
    points = [("a", T0 - timedelta(seconds=20), 0.0), ("a", T0 - timedelta(seconds=10), 50.0)]
    assert _rates(points) == {}


# ── baseline / z-score ──────────────────────────────────────────────────────────────────


def test_robust_spread_uses_floor_for_flat_series() -> None:
    median, spread = robust_center_spread([50.0, 50.0, 50.0, 50.0], abs_floor=1.0)
    assert median == 50.0
    assert spread == pytest.approx(5.0)  # 10% da mediana
    median, spread = robust_center_spread([0.0, 0.0, 0.0], abs_floor=2.0)
    assert spread == 2.0


def test_robust_spread_ignores_single_outlier() -> None:
    median, spread = robust_center_spread([10, 11, 9, 10, 500], abs_floor=0.1)
    assert median == 10
    assert spread == pytest.approx(1.4826 * 1.0)


def test_select_baseline_prefers_weekly_then_daily() -> None:
    assert select_baseline([1, 2, 3], []).kind == "same_hour_same_weekday"
    assert select_baseline([1, 2], [1] * 7).kind == "same_hour_any_day"
    assert select_baseline([1, 2], [1] * 6) is None


def test_evaluate_high_only_metric() -> None:
    metric = METRIC_DEFS["cpu_percent"]
    baseline = Baseline("same_hour_same_weekday", [20.0, 22.0, 18.0, 20.0])
    # mediana 20, MAD 1 -> 1.4826 ; piso 10% = 2.0 ; abs 5.0 -> spread 5
    normal = evaluate(30.0, baseline, metric, 3.0, 4.0)
    assert normal.status == "normal" and normal.z == pytest.approx(2.0)
    warn = evaluate(36.0, baseline, metric, 3.0, 4.0)
    assert warn.status == "warning" and warn.direction == "high"
    crit = evaluate(45.0, baseline, metric, 3.0, 4.0)
    assert crit.status == "critical"
    assert crit.expected == 20.0 and crit.low == 5.0 and crit.high == 35.0
    # queda grande em metrica so-para-cima nao alerta
    assert evaluate(0.0, Baseline("same_hour_same_weekday", [80.0] * 3), metric, 3, 4).status == (
        "normal"
    )


def test_evaluate_both_directions_for_throughput() -> None:
    metric = METRIC_DEFS["transactions_per_sec"]
    baseline = Baseline("same_hour_any_day", [100.0] * 7)
    ev = evaluate(40.0, baseline, metric, 3.0, 4.0)  # spread 10 -> z = -6
    assert ev.status == "critical" and ev.direction == "low"
    assert ev.low == 70.0


def test_evaluate_insufficient_data() -> None:
    metric = METRIC_DEFS["active_sessions"]
    assert evaluate(5.0, None, metric, 3, 4).status == "insufficient_data"
    ev = evaluate(None, Baseline("same_hour_same_weekday", [1, 2, 3]), metric, 3, 4)
    assert ev.status == "insufficient_data" and ev.expected == 2 and ev.baseline_samples == 3


# ── persistencia ────────────────────────────────────────────────────────────────────────


def test_streaks_and_transitions() -> None:
    anom, norm = advance_streaks(0, 5, True)
    assert (anom, norm) == (1, 0)
    assert decide_transition(False, anom, norm, True) == "none"  # 1a vez: ainda nao abre
    anom, norm = advance_streaks(anom, norm, True)
    assert decide_transition(False, anom, norm, True) == "open"
    assert decide_transition(True, anom + 1, 0, True) == "update"
    anom, norm = advance_streaks(anom, norm, False)
    assert decide_transition(True, anom, norm, False) == "none"  # 1 normal: mantem aberto
    anom, norm = advance_streaks(anom, norm, False)
    assert decide_transition(True, anom, norm, False) == "close"
    assert decide_transition(False, 0, 3, False) == "none"


def test_metrics_for_engine() -> None:
    keys = {m.key for m in metrics_for_engine("sqlserver")}
    assert "cpu_percent" in keys and "transactions_per_sec" not in keys
    pg = {m.key for m in metrics_for_engine("postgresql")}
    assert "transactions_per_sec" in pg and "cpu_percent" not in pg
    assert "transactions_per_sec" in {m.key for m in metrics_for_engine("mysql")}
    assert "transactions_per_sec" not in {m.key for m in metrics_for_engine("oracle")}
