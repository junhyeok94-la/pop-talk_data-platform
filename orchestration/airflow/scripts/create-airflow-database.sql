SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'airflow_user', :'airflow_password')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'airflow_user'
) \gexec

SELECT format('CREATE DATABASE %I OWNER %I', 'airflow', :'airflow_user')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'airflow'
) \gexec

SELECT format('ALTER DATABASE %I OWNER TO %I', 'airflow', :'airflow_user') \gexec
