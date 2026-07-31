#!/usr/bin/env bash
# common.sh - packaging environment for Snowflake.
#
# Snowflake is a PostgreSQL extension: one build per PG major.
# pgedge-detect-build-matrix reads this to fan the matrix out over pg_versions.
PER_PG_VERSION=true

export PG_VERSION="${PG_VERSION:-17}"
export PG_MAJOR_VERSION="$(echo "$PG_VERSION" | cut -d. -f1)"

export PG_SNOWFLAKE_REPO="https://github.com/pgEdge/snowflake.git"
export SNOWFLAKE_BRANCH="${COMPONENT_BRANCH:-v2.6.0}"

# Upstream version, suffix-stripped (e.g. 2.6.0). Names the source tarball's
# internal directory and the RPM Version.
export SNOWFLAKE_VERSION="${COMPONENT_VERSION:-2.6.0}"
export SNOWFLAKE_BUILDNUM=${COMPONENT_BUILDNUM:-1}

export REPO_TYPE="${REPO_TYPE:-daily}"

# DEB only: move a pre-release pretag (COMPONENT_BUILDNUM='rc1_1') into the
# upstream version with a leading '~' so pre-releases sort BELOW stable in
# dpkg/reprepro: 2.6.0~rc1-1.noble < 2.6.0-1.noble.
#
# The '~' form goes in a SEPARATE variable used only by the debian/changelog:
# SNOWFLAKE_VERSION itself must stay clean because it names the source tarball
# and its unpack directory (a '~' there would break %setup and the DEB extract).
export SNOWFLAKE_DEB_VERSION="${SNOWFLAKE_VERSION}"
if command -v apt-get &>/dev/null; then
    if [[ "$SNOWFLAKE_BUILDNUM" == *_* ]]; then
        SNOWFLAKE_PRETAG="${SNOWFLAKE_BUILDNUM%%_*}"
        export SNOWFLAKE_DEB_VERSION="${SNOWFLAKE_VERSION}~${SNOWFLAKE_PRETAG}"
        SNOWFLAKE_BUILDNUM="${SNOWFLAKE_BUILDNUM##*_}"
    fi
fi

# release.yml stages the source tarball built from THIS run's checkout here.
export ARTIFACT_DIR="${ARTIFACT_DIR:-$(pwd)/release-artifacts}"
export SRC_TARBALL="snowflake-${SNOWFLAKE_VERSION}.tar.gz"

# Prefer the workflow-staged tarball (so branch / simulate_tag runs build the
# exact commit under test and need no network). The SNOWFLAKE_BRANCH clone is an
# opt-in fallback for local builds: set SNOWFLAKE_ALLOW_CLONE_FALLBACK=1.
stage_source() {
  local dest="$1"
  if [ -f "${ARTIFACT_DIR}/${SRC_TARBALL}" ]; then
    echo "Staging ${SRC_TARBALL} from ${ARTIFACT_DIR}"
    cp "${ARTIFACT_DIR}/${SRC_TARBALL}" "${dest}"
  elif [ -z "${SNOWFLAKE_ALLOW_CLONE_FALLBACK:-}" ]; then
    # A staged tarball is required by default: cloning SNOWFLAKE_BRANCH instead
    # would ship a package built from a different commit than COMPONENT_VERSION
    # claims.
    echo "::error::${ARTIFACT_DIR}/${SRC_TARBALL} not found. release.yml stages it with git archive; for a local build, stage it yourself or set SNOWFLAKE_ALLOW_CLONE_FALLBACK=1 to clone ${SNOWFLAKE_BRANCH} instead." >&2
    return 1
  else
    echo "Fetching Snowflake source code (${SNOWFLAKE_BRANCH})"
    rm -rf "snowflake-${SNOWFLAKE_VERSION}"
    git clone --depth=1 --branch "$SNOWFLAKE_BRANCH" "$PG_SNOWFLAKE_REPO" "snowflake-${SNOWFLAKE_VERSION}"
    rm -rf "snowflake-${SNOWFLAKE_VERSION}/.git"
    tar -czf "${SRC_TARBALL}" "snowflake-${SNOWFLAKE_VERSION}"
    rm -rf "snowflake-${SNOWFLAKE_VERSION}"
    mv "${SRC_TARBALL}" "${dest}"
  fi
}
