-- Reviewable scope for the optional direct PostgreSQL datasource.
-- Apply only after approval. psql variables: monitor_password, airflow_database.
-- No SELECT on raw metadata tables, Connection, Variable, XCom, or DAG conf.
BEGIN;
CREATE ROLE workbench_airflow_ro LOGIN PASSWORD :'monitor_password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
    CONNECTION LIMIT 4;
ALTER ROLE workbench_airflow_ro SET default_transaction_read_only = on;
ALTER ROLE workbench_airflow_ro SET statement_timeout = '2s';
ALTER ROLE workbench_airflow_ro SET lock_timeout = '500ms';
ALTER ROLE workbench_airflow_ro SET idle_in_transaction_session_timeout = '5s';

CREATE SCHEMA workbench_monitor;
REVOKE ALL ON SCHEMA workbench_monitor FROM PUBLIC;
CREATE VIEW workbench_monitor.dag_runs WITH (security_barrier=true) AS
SELECT dag_id, run_id, state, run_type, run_after, start_date, end_date,
       extract(epoch FROM run_after)::double precision AS run_ts,
       extract(epoch FROM start_date)::double precision AS start_ts,
       extract(epoch FROM end_date)::double precision AS end_ts,
       CASE WHEN end_date >= start_date
            THEN extract(epoch FROM end_date-start_date)::double precision END AS duration_seconds
FROM public.dag_run;
CREATE VIEW workbench_monitor.task_instances WITH (security_barrier=true) AS
SELECT dag_id, task_id, run_id, map_index, state, start_date, end_date,
       duration, try_number, pool, pool_slots, queue, operator, queued_dttm
FROM public.task_instance;
CREATE VIEW workbench_monitor.dags WITH (security_barrier=true) AS
SELECT dag_id, is_paused, has_import_errors, max_active_tasks, max_active_runs,
       next_dagrun, last_parsed_time
FROM public.dag;
CREATE VIEW workbench_monitor.dag_tags WITH (security_barrier=true) AS
SELECT dag_id, name FROM public.dag_tag;
CREATE VIEW workbench_monitor.pools WITH (security_barrier=true) AS
SELECT pool, slots, include_deferred FROM public.slot_pool;

GRANT CONNECT ON DATABASE :"airflow_database" TO workbench_airflow_ro;
GRANT USAGE ON SCHEMA workbench_monitor TO workbench_airflow_ro;
GRANT SELECT ON workbench_monitor.dag_runs,
                workbench_monitor.task_instances,
                workbench_monitor.dags,
                workbench_monitor.dag_tags,
                workbench_monitor.pools TO workbench_airflow_ro;
COMMIT;
