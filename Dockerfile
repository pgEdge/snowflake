ARG PGVER=17
FROM postgres:${PGVER}-alpine

RUN apk add --no-cache \
    make \
    gcc \
    musl-dev \
    postgresql-dev \
    git

WORKDIR /home/postgres/snowflake

COPY . /home/postgres/snowflake/

RUN USE_PGXS=1 make with_llvm=no && USE_PGXS=1 make with_llvm=no install

EXPOSE 5432

# Drop privileges: the build steps above need root to install into the
# PostgreSQL lib/share directories, but the server itself must not run as root.
#
# Safe only because PGDATA is never bind-mounted here: docker-entrypoint.sh
# repairs PGDATA ownership only when it starts as UID 0. The image's own
# /var/lib/postgresql/data is already postgres-owned, and a fresh named volume
# inherits that. If a root-owned host bind mount is ever needed, drop this line
# and let the entrypoint gosu down to postgres itself.
USER postgres

ENTRYPOINT ["docker-entrypoint.sh"]

CMD ["postgres"]
