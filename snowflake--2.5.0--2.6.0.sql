/* snowflake--2.5.0--2.6.0.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION snowflake UPDATE TO '2.6.0'" to load this file. \quit

-- ----------------------------------------------------------------------
-- 2.5.0 -> 2.6.0 - add support for PostgreSQL 19.
--
-- The 2.6.0 release is a build/C-level change (porting the extension to
-- the PostgreSQL 19 server API); there are no catalog objects to alter.
-- This upgrade script exists so that the extension version can be moved
-- to 2.6.0 via ALTER EXTENSION and so a 2.6.0 install is offered/reachable.
-- ----------------------------------------------------------------------
