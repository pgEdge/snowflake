import sys, os, json, util_test, subprocess

## Print Script
print(f"Starting - {os.path.basename(__file__)}")

#
# snowflake_version.py - guard the extension version metadata.
#
# The v2.6.0 release was tagged, but the version metadata was never bumped:
# snowflake.control still carried default_version = '2.5.0' and no
# snowflake--2.6.0.sql (nor a snowflake--2.5.0--2.6.0.sql upgrade script)
# existed. As a result the extension installed and reported itself as 2.5.0
# even when built from the v2.6.0 tag, so QA could not tell a real 2.6.0
# install apart from 2.5.0.
#
# This test reads the on-disk metadata (which is exactly what the release
# forgot to bump) and, if the extension is installed, its reported version,
# and asserts everything says 2.6.0.
#

EXPECTED_VERSION = "2.6.0"

## Get Test Settings
util_test.set_env()

port = int(os.getenv("EDGE_START_PORT", 6432))
usr = os.getenv("EDGE_USERNAME", "lcusr")
pw = os.getenv("EDGE_PASSWORD", "password")
host = os.getenv("EDGE_HOST", "localhost")
dbname = os.getenv("EDGE_DB", "lcdb")

failures = []

# ----------------------------------------------------------------------
# 1. default_version in snowflake.control (read via pg_available_extensions).
#    This is the field the release forgot to bump.
# ----------------------------------------------------------------------
row = util_test.read_psql(
    "SELECT default_version FROM pg_available_extensions WHERE name = 'snowflake'",
    host, dbname, port, pw, usr)
print(row)
print("*" * 100)
available = json.loads(row)
if not available:
    failures.append("snowflake is not listed in pg_available_extensions")
else:
    default_version = available[0][0]
    if default_version != EXPECTED_VERSION:
        failures.append(
            f"default_version is '{default_version}', expected '{EXPECTED_VERSION}'")

# ----------------------------------------------------------------------
# 2. A snowflake--2.6.0.sql install script must exist on disk, i.e. 2.6.0
#    must be an offered version.
# ----------------------------------------------------------------------
row = util_test.read_psql(
    "SELECT version FROM pg_available_extension_versions WHERE name = 'snowflake'",
    host, dbname, port, pw, usr)
print(row)
print("*" * 100)
offered = [v[0] for v in json.loads(row)]
if EXPECTED_VERSION not in offered:
    failures.append(
        f"'{EXPECTED_VERSION}' is not an offered version (no snowflake--{EXPECTED_VERSION}.sql); "
        f"offered: {offered}")

# ----------------------------------------------------------------------
# 3. If the extension is installed, its reported version must be 2.6.0.
# ----------------------------------------------------------------------
row = util_test.read_psql(
    "SELECT extversion FROM pg_extension WHERE extname = 'snowflake'",
    host, dbname, port, pw, usr)
print(row)
print("*" * 100)
installed = json.loads(row)
if installed:
    extversion = installed[0][0]
    if extversion != EXPECTED_VERSION:
        failures.append(
            f"installed extension reports version '{extversion}', expected '{EXPECTED_VERSION}'")
else:
    print("snowflake extension is not installed in this database; "
          "skipping installed-version check")

# ----------------------------------------------------------------------
# Verdict
# ----------------------------------------------------------------------
if failures:
    for f in failures:
        print(f"FAIL: {f}")
    util_test.EXIT_FAIL()
else:
    print(f"All version metadata reports {EXPECTED_VERSION}")
    util_test.EXIT_PASS()
