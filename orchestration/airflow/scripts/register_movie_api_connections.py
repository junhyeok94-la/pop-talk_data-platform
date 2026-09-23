"""Administrative CLI only, run in Airflow container; secrets arrive on stdin."""
import json
import sys

from airflow.models.connection import Connection
from airflow.utils.session import create_session


def main():
    values = json.load(sys.stdin)
    with create_session() as session:
        for env_name, conn_id in (("KOFIC_API_KEY", "pop_talk_kofic"), ("KMDB_API_KEY", "pop_talk_kmdb")):
            value = values.get(env_name)
            if not isinstance(value, str) or not value.strip():
                raise RuntimeError(f"Missing {env_name}")
            conn = session.query(Connection).filter(Connection.conn_id == conn_id).one_or_none()
            if conn is None:
                conn = Connection(conn_id=conn_id, conn_type="generic")
                session.add(conn)
            conn.password = value
            conn.description = "Movie raw collector API credential; imported from legacy scheduler env"
    print("Registered pop_talk_kofic and pop_talk_kmdb (values hidden).")


if __name__ == "__main__":
    main()
