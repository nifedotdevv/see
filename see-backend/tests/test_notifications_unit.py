from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError
from sqlalchemy.dialects import postgresql

from app.api.v1.endpoints import webhooks
from app.core.config import settings
from app.schemas.notification import DeviceRegistration, Preferences
from app.services.notifications import put_notification, quiet_time
from app.workers import celery_app as worker_module


class RecordingSession:
    def __init__(self) -> None:
        self.statement = None

    async def execute(self, statement, params=None):
        self.statement = statement
        return None


def test_quiet_time_moves_alert_to_local_wake_hour():
    prefs = Preferences(timezone="Africa/Lagos", quiet_start=22, quiet_end=7)
    # 23:30 UTC is 00:30 in Lagos (UTC+1), inside quiet hours.
    at = datetime(2026, 10, 21, 23, 30, tzinfo=timezone.utc)
    shifted = quiet_time(at, prefs)
    assert shifted == datetime(2026, 10, 22, 6, 0, tzinfo=timezone.utc)


def test_quiet_time_leaves_daytime_alerts_untouched():
    prefs = Preferences(timezone="Africa/Lagos", quiet_start=22, quiet_end=7)
    at = datetime(2026, 10, 21, 12, 0, tzinfo=timezone.utc)
    assert quiet_time(at, prefs) == at


def test_preferences_reject_unknown_timezone():
    with pytest.raises(ValidationError):
        Preferences(timezone="Not/AZone")


def test_device_registration_requires_expo_token_and_valid_zone():
    assert DeviceRegistration(token="ExponentPushToken[abc_123]", timezone="Africa/Lagos").timezone == "Africa/Lagos"
    with pytest.raises(ValidationError):
        DeviceRegistration(token="not-a-token")
    with pytest.raises(ValidationError):
        DeviceRegistration(token="ExponentPushToken[abc_123]", timezone="Mars/Olympus")


def test_cancelled_schedule_is_reactivated_without_resending_sent_alerts():
    db = RecordingSession()
    asyncio.run(put_notification(
        db,
        UUID("11111111-1111-1111-1111-111111111111"),
        "reminder",
        None,
        datetime(2026, 10, 21, 12, 0, tzinfo=timezone.utc),
        "/reminders/11111111-1111-1111-1111-111111111111",
        "Review reminder",
        "reminder:1:2026-10-21T12:00:00+00:00",
    ))
    sql = str(db.statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert "ON CONFLICT (dedupe_key) DO UPDATE" in sql
    assert "WHERE notifications.status = 'cancelled'" in sql


def test_worker_gate_is_bypassed_in_development(monkeypatch):
    monkeypatch.setattr(settings, "APP_ENV", "development")
    assert asyncio.run(webhooks._worker_available()) is True


def test_worker_gate_rejects_missing_heartbeat_in_production(monkeypatch):
    monkeypatch.setattr(settings, "APP_ENV", "production")

    class Client:
        async def get(self, key):
            return None

        async def aclose(self):
            return None

    monkeypatch.setattr(webhooks.redis_async, "from_url", lambda *args, **kwargs: Client())
    assert asyncio.run(webhooks._worker_available()) is False


def test_worker_heartbeat_sets_expiring_readiness_key(monkeypatch):
    calls = []

    class Client:
        def set(self, key, value, ex):
            calls.append((key, value, ex))

        def close(self):
            calls.append(("closed",))

    monkeypatch.setattr(worker_module.redis.Redis, "from_url", lambda *args, **kwargs: Client())
    worker_module._write_worker_heartbeat()

    assert calls == [
        (worker_module.WORKER_HEARTBEAT_KEY, "1", worker_module.WORKER_HEARTBEAT_TTL_SECONDS),
        ("closed",),
    ]


def test_worker_heartbeat_signal_does_not_crash_worker(monkeypatch):
    def fail():
        raise OSError("redis unavailable")

    monkeypatch.setattr(worker_module, "_write_worker_heartbeat", fail)
    worker_module.worker_heartbeat()


# --- Batched scheduling --------------------------------------------------------

from datetime import timedelta
from types import SimpleNamespace

from sqlalchemy.sql.dml import Insert

from app.services import notifications as notifications_service


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class ReconcileSession:
    """Answers reconcile_user's projections and records every statement."""

    def __init__(self, reminders, jobs):
        self.reminders, self.jobs, self.statements = reminders, jobs, []

    async def execute(self, statement, params=None):
        self.statements.append(statement)
        sql = str(statement)
        if getattr(statement, "is_select", False) and "FROM reminders" in sql:
            return _Result(self.reminders)
        if getattr(statement, "is_select", False) and "FROM job_applications" in sql:
            return _Result(self.jobs)
        return _Result([])

    async def flush(self):
        pass

    async def get(self, model, key):
        return None  # default preferences

    async def scalar(self, statement):
        raise AssertionError("agenda counts must not issue separate COUNT queries")


def test_put_notifications_dedupes_and_reactivates_only_cancelled_rows():
    db = RecordingSession()
    at = datetime(2026, 10, 21, 12, 0, tzinfo=timezone.utc)
    user = UUID("11111111-1111-1111-1111-111111111111")
    row = notifications_service._notification_row(user, "reminder", None, at, "/r", "Body", "same-key")
    duplicate = notifications_service._notification_row(user, "reminder", None, at, "/r", "Body", "same-key")
    other = notifications_service._notification_row(user, "reminder", None, at, "/r", "Body", "other-key")
    asyncio.run(notifications_service.put_notifications(db, [row, duplicate, other]))
    compiled = db.statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert "ON CONFLICT (dedupe_key) DO UPDATE" in sql
    assert "excluded.send_at" in sql
    assert "WHERE notifications.status =" in sql
    assert len([key for key in compiled.params if key.startswith("dedupe_key")]) == 2


def test_reconcile_user_schedules_everything_in_one_upsert():
    now = datetime(2026, 10, 21, 9, 0, tzinfo=timezone.utc)
    due = now + timedelta(days=1)
    reminders = [SimpleNamespace(id=UUID(int=i), due_date=due) for i in range(1, 6)]
    jobs = [SimpleNamespace(id=UUID(int=99), deadline=due, interview_at=due, assessment_at=None, follow_up_at=None)]
    db = ReconcileSession(reminders, jobs)
    asyncio.run(notifications_service.reconcile_user(db, UUID("11111111-1111-1111-1111-111111111111"), now))
    inserts = [s for s in db.statements if isinstance(s, Insert)]
    assert len(inserts) == 1
    # 5 reminders x (due, advance, overdue) + 2 job dates x (due, advance).
    params = inserts[0].compile(dialect=postgresql.dialect()).params
    assert len([key for key in params if key.startswith("dedupe_key")]) == 19
