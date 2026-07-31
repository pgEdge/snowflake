#!/usr/bin/env bash
set -euo pipefail

# Environment variables
BUILD_DIR="/tmp/pg_deb_build"
SRC_DIR="${BUILD_DIR}/src"

export DEBIAN_FRONTEND=noninteractive

prepare() {

  setup_apt_build_env

  # This function is for debugging purpose if you have your own keys. GH workflow does not need it.
  #import_gpg_keys

  rm -rf "$SRC_DIR"
  mkdir -p "$SRC_DIR"

  stage_source "${BUILD_DIR}/${SRC_TARBALL}"
  tar -C "$BUILD_DIR" -xzf "${BUILD_DIR}/${SRC_TARBALL}"

  echo "Moving Debian packaging into source directory..."
  cp -rp "${COMPONENT_DIR}/deb/debian" "$BUILD_DIR/snowflake-${SNOWFLAKE_VERSION}/"
  cp $BUILD_DIR/snowflake-${SNOWFLAKE_VERSION}/debian/control.in $BUILD_DIR/snowflake-${SNOWFLAKE_VERSION}/debian/control
  sed -i "s|PG_MAJOR_VERSION|${PG_MAJOR_VERSION}|g" $BUILD_DIR/snowflake-${SNOWFLAKE_VERSION}/debian/control
  mv $BUILD_DIR/snowflake-${SNOWFLAKE_VERSION}/debian/pgedge-postgresql-snowflake.install $BUILD_DIR/snowflake-${SNOWFLAKE_VERSION}/debian/pgedge-postgresql-${PG_MAJOR_VERSION}-snowflake.install
  sed -i "s|PG_MAJOR_VERSION|${PG_MAJOR_VERSION}|g" $BUILD_DIR/snowflake-${SNOWFLAKE_VERSION}/debian/pgedge-postgresql-${PG_MAJOR_VERSION}-snowflake.install

  echo "Installing build dependencies..."
  cd "$BUILD_DIR/snowflake-${SNOWFLAKE_VERSION}"
  sudo apt-get update
  sudo apt-get build-dep -y .
}

build() {

  cd "$BUILD_DIR/snowflake-${SNOWFLAKE_VERSION}"
  echo "Building Debian package..."
  DISTRO=$(lsb_release -cs)
  # SNOWFLAKE_DEB_VERSION carries the '~<pretag>' form for pre-releases so they
  # sort below stable; it equals SNOWFLAKE_VERSION for a GA build.
  # A Debian changelog entry needs a blank line after the header and before the
  # maintainer trailer. Written in final form, so no dch pass is needed — dch
  # with this same version appended a duplicate entry instead of editing.
  rm -rf debian/changelog
  {
      echo "pgedge-snowflake (${SNOWFLAKE_DEB_VERSION}-${SNOWFLAKE_BUILDNUM}.${DISTRO}) ${DISTRO}; urgency=low"
      echo ""
      echo "  * Update Release."
      echo ""
      echo " -- pgEdge Build Team <support@pgedge.com>  $(date -R)"
  } > debian/changelog

  PATH=/usr/lib/postgresql/${PG_MAJOR_VERSION}/bin:$PATH USE_PGXS=1 dpkg-buildpackage -us -uc -b
}

post_build() {
  echo "Copying .deb packages to output..."
  sudo mkdir -p "/output"
  # Rename .ddeb files to .deb files
  rename_ddeb_packages $BUILD_DIR
  sudo cp "$BUILD_DIR"/*.deb "/output" || echo "No .deb packages found."
}
