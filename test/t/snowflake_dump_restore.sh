#!/bin/bash
#
# snowflake_dump_restore.sh
#
# End-to-end regression for the SUP-140 dump/restore failure.
#
# A sequence converted to snowflake must survive a full pg_dump + restore.
# Before the 2.5 fix, convert_sequence_to_snowflake() left MAXVALUE at
# (old last_value + 1) while snowflake.nextval() bypasses MAXVALUE and writes
# snowflake-sized values into last_value.  pg_dump then emitted a huge
# setval() against a tiny ceiling and the restore aborted with:
#
#   ERROR: setval: value <big_snowflake_id> is out of bounds for
#          sequence "orders_id_seq" (1..<small>)
#
# This test builds a converted sequence, dumps it, restores into a fresh
# database, and verifies the data and sequence position survive intact.
#
# NOTE: intentionally NOT listed in test/schedule_files/script_file.  Run it
# explicitly:  python runner.py -c <config.env> -t test/t/snowflake_dump_restore.sh
#
set -euo pipefail

# Locate the n1 binaries / connection params the way runner.py does, falling
# back to PATH and defaults so the script also runs outside the harness.
PGBIN="${EDGE_CLUSTER_DIR:-}/n1/pgedge/${EDGE_COMPONENT:-pg17}/bin"
[ -x "$PGBIN/psql" ] || PGBIN=""
PSQL="${PGBIN:+$PGBIN/}psql"
PGDUMP="${PGBIN:+$PGBIN/}pg_dump"
CREATEDB="${PGBIN:+$PGBIN/}createdb"
DROPDB="${PGBIN:+$PGBIN/}dropdb"

HOST="${EDGE_HOST:-127.0.0.1}"
PORT="${EDGE_START_PORT:-5432}"
SUPERUSER="${PGSUPERUSER:-postgres}"

CONN=( -h "$HOST" -p "$PORT" -U "$SUPERUSER" )
RUN=( "$PSQL" -X -q -v ON_ERROR_STOP=1 "${CONN[@]}" )

SRC=snowflake_dump_src
DST=snowflake_dump_dst
DUMP="$(mktemp "/tmp/snowflake_dump.XXXXXX.sql")"

cleanup() {
    "$DROPDB" "${CONN[@]}" --if-exists "$SRC" >/dev/null 2>&1 || true
    "$DROPDB" "${CONN[@]}" --if-exists "$DST" >/dev/null 2>&1 || true
    rm -f "$DUMP"
}
trap cleanup EXIT

echo "# building source database with a converted bigserial sequence"
"$DROPDB" "${CONN[@]}" --if-exists "$SRC" >/dev/null 2>&1 || true
"$CREATEDB" "${CONN[@]}" "$SRC"
"${RUN[@]}" -d "$SRC" <<'SQL'
CREATE EXTENSION snowflake;
SET snowflake.node = 1;
CREATE TABLE orders (id bigserial PRIMARY KEY, customer text NOT NULL);
INSERT INTO orders (customer) VALUES ('Alice'), ('Bob'), ('Carol');
SELECT snowflake.convert_sequence_to_snowflake('orders_id_seq'::regclass);
-- Produce a real snowflake-sized id so the dump's setval() is huge.
INSERT INTO orders (customer) VALUES ('Dave');
SQL

src_count="$("${RUN[@]}" -At -d "$SRC" -c "SELECT count(*) FROM orders;")"
src_last="$("${RUN[@]}" -At -d "$SRC" -c "SELECT last_value FROM orders_id_seq;")"
echo "# source: ${src_count} rows, sequence last_value=${src_last}"

echo "# dump + restore into a fresh database"
"$PGDUMP" "${CONN[@]}" "$SRC" > "$DUMP"
"$DROPDB" "${CONN[@]}" --if-exists "$DST" >/dev/null 2>&1 || true
"$CREATEDB" "${CONN[@]}" "$DST"
# With the bug this aborts on the out-of-bounds setval; ON_ERROR_STOP makes
# that a hard failure of this test.
"${RUN[@]}" -d "$DST" -f "$DUMP" >/dev/null

dst_count="$("${RUN[@]}" -At -d "$DST" -c "SELECT count(*) FROM orders;")"
dst_last="$("${RUN[@]}" -At -d "$DST" -c "SELECT last_value FROM orders_id_seq;")"
echo "# restored: ${dst_count} rows, sequence last_value=${dst_last}"

if [ "$src_count" != "$dst_count" ] || [ "$src_last" != "$dst_last" ]; then
    echo "FAIL: restored state differs from source"
    exit 1
fi

# The restored sequence must still mint snowflake ids for new inserts.
new_id="$("${RUN[@]}" -At -d "$DST" -c "SET snowflake.node = 1; INSERT INTO orders (customer) VALUES ('Eve') RETURNING id;")"
if [ "$new_id" -le "$src_last" ]; then
    echo "FAIL: post-restore insert id ${new_id} did not advance past ${src_last}"
    exit 1
fi

echo "PASS: dump/restore preserved ${dst_count} rows; sequence intact and still minting (new id ${new_id})"
