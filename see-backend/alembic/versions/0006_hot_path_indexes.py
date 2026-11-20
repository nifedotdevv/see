"""Indexes for the notification dispatcher and imported-note lookups.

The dispatcher scans notification_deliveries every minute by status and
next_attempt_at/updated_at, and deactivated devices fail their deliveries by
device_id. Notes listing checks email_ingestions.linked_note_id per note.

Indexes are built CONCURRENTLY so the upgrade does not block writes.
"""
from alembic import op

revision = '0006_hot_path_indexes'
down_revision = '0005_notifications'
branch_labels = depends_on = None

INDEXES = [
    ('ix_notification_deliveries_status_next_attempt', 'notification_deliveries', 'status, next_attempt_at', ''),
    ('ix_notification_deliveries_status_updated', 'notification_deliveries', 'status, updated_at', ''),
    ('ix_notification_deliveries_device_id', 'notification_deliveries', 'device_id', ''),
    ('ix_email_ingestions_linked_note_id', 'email_ingestions', 'linked_note_id', ' WHERE linked_note_id IS NOT NULL'),
]


def upgrade():
    with op.get_context().autocommit_block():
        for name, table, columns, where in INDEXES:
            op.execute(f'CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} ON {table} ({columns}){where}')


def downgrade():
    with op.get_context().autocommit_block():
        for name, _, _, _ in INDEXES:
            op.execute(f'DROP INDEX CONCURRENTLY IF EXISTS {name}')
