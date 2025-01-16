FROM postgres:16-alpine

RUN apk add --no-cache \
    make \
    gcc \
    musl-dev \
    postgresql-dev \
    git \
    clang \
    llvm

WORKDIR /home/postgres/snowflake

COPY . /home/postgres/snowflake/

RUN pwd && ls -la

RUN USE_PGXS=1 make && USE_PGXS=1 make install

EXPOSE 5432

ENTRYPOINT ["docker-entrypoint.sh"]

CMD ["postgres"]
