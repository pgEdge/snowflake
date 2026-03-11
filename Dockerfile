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

ENTRYPOINT ["docker-entrypoint.sh"]

CMD ["postgres"]
