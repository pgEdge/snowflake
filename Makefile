# snowflake/Makefile

MODULES = snowflake
OBJS = \
	$(WIN32RES) \
	snowflake.o \

EXTENSION = snowflake
DATA = snowflake--1.0.sql \
	   snowflake--1.0--1.1.sql \
	   snowflake--1.1.sql \
	   snowflake--1.1--1.2.sql \
	   snowflake--1.2.sql \
	   snowflake--1.2--2.0.sql \
	   snowflake--2.0.sql \
	   snowflake--2.0--2.2.sql \
	   snowflake--2.2.sql \
	   snowflake--2.2--2.3.sql \
	   snowflake--2.3.sql \
	   snowflake--2.3--2.4.sql
PGFILEDESC = "snowflake - snowflake style IDs for PostgreSQL"

REGRESS = conversion

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/snowflake
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
