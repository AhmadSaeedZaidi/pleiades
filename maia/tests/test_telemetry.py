"""Actual cycle progress, failures, cancellation and bounded reporting history."""

from maia.telemetry import CycleMonitor


def test_cycle_cadence_is_measured_from_finish_and_long_work_is_visible():
    clock = [0.0]
    monitor = CycleMonitor(lambda: clock[0])
    assert monitor.snapshot() is None
    monitor.register("tracker", 60)
    monitor.start("tracker")
    clock[0] = 901
    assert monitor.snapshot()["cycles"][0]["state"] == "slow"
    monitor.finish("tracker", {"videos_updated": 5})
    clock[0] = 1000
    assert monitor.snapshot()["cycles"][0]["state"] == "idle"
    clock[0] = 1022
    assert monitor.snapshot()["cycles"][0]["state"] == "late"


def test_failure_and_recovery_keep_payloads_out_of_telemetry():
    clock = [0.0]
    monitor = CycleMonitor(lambda: clock[0])
    monitor.register("janitor", 900)
    monitor.start("janitor")
    monitor.finish("janitor", {"vault_flush_error": "private backend details", "vault_flushed": 4})
    row = monitor.snapshot()["cycles"][0]
    assert row["state"] == "failed" and row["failures"] == 1
    assert row["error"] == "PartialFailure"
    assert "private" not in str(row)
    monitor.start("janitor")
    monitor.finish("janitor", error=RuntimeError("secret endpoint"))
    assert monitor.snapshot()["cycles"][0]["failures"] == 2
    monitor.start("janitor")
    monitor.finish("janitor", {"vault_flushed": 6})
    row = monitor.snapshot()["cycles"][0]
    assert row["failures"] == 0 and row["state"] == "idle"
    assert row["progress_1h"] == {"vault_flushed": 10}
    clock[0] = 3601
    assert monitor.snapshot()["cycles"][0]["progress_1h"] == {}


def test_discord_nondelivery_and_cancellation_are_not_successful_cycles():
    monitor = CycleMonitor(lambda: 1.0)
    monitor.register("heartbeat", 900)
    monitor.start("heartbeat")
    monitor.finish("heartbeat", {"notified": False})
    assert monitor.snapshot()["cycles"][0]["error"] == "DiscordDeliveryFailed"
    monitor.start("heartbeat")
    monitor.cancel("heartbeat")
    row = monitor.snapshot()["cycles"][0]
    assert row["state"] == "interrupted"
    assert row["last_success_age_seconds"] is None


def test_history_is_bounded_and_only_safe_integer_counters_survive():
    monitor = CycleMonitor(lambda: 1.0)
    monitor.register("topics", 600)
    for _ in range(100):
        monitor.start("topics")
        monitor.finish("topics", {"observed": 1, "unavailable": True, "secret": "private"})
    assert len(monitor.cycles["topics"].history) == 60
    assert monitor.snapshot()["cycles"][0]["progress_1h"] == {"observed": 60}
