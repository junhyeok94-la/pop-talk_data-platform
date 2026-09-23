"""Generate local configuration without printing or overwriting secrets."""
from pathlib import Path
import base64
import secrets

ROOT = Path(__file__).resolve().parents[1]

def write_new(relative, lines):
    path = ROOT / relative
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

def read_env(relative):
    values = {}
    for line in (ROOT / relative).read_text(encoding="utf-8-sig").splitlines():
        if line.strip() and not line.lstrip().startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip("\"'")
    return values

def required(values, names):
    for name in names:
        if not values.get(name) or values[name].startswith("replace-"):
            raise SystemExit(f"Set {name} in .env before generating dependent configuration.")

def main():
    write_new(".env", ["POSTGRES_USER=pop_talk_platform", f"POSTGRES_PASSWORD={secrets.token_hex(24)}",
        "POSTGRES_DB=pop_talk_platform", "POSTGRES_PORT=55433",
        "POSTGRES_VOLUME_NAME=pop-talk-data-platform_postgres_data"])
    values = read_env(".env")
    required(values, ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"))
    password = secrets.token_hex(24)
    fernet = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
    write_new(".local/config/airflow.env", ["AIRFLOW_DB_USER=airflow_user",
        f"AIRFLOW_DB_PASSWORD={password}",
        f"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_user:{password}@db:5432/airflow",
        f"AIRFLOW__CORE__FERNET_KEY={fernet}", f"AIRFLOW__API_AUTH__JWT_SECRET={secrets.token_hex(32)}",
        "AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
        "_AIRFLOW_WWW_USER_CREATE=true", "_AIRFLOW_WWW_USER_USERNAME=airflow",
        f"_AIRFLOW_WWW_USER_PASSWORD={secrets.token_hex(16)}",
        "POP_TALK_POSTGRES_HOST=host.docker.internal", "POP_TALK_POSTGRES_PORT=55432",
        "POP_TALK_POSTGRES_DB=pop_talk_local", "POP_TALK_POSTGRES_USER=pop_talk_local",
        "POP_TALK_POSTGRES_PASSWORD="])
    # Upgrade existing local configuration without changing existing credentials.
    airflow_path = ROOT / '.local/config/airflow.env'
    airflow_values = read_env('.local/config/airflow.env')
    additions = {
        'POP_TALK_PLATFORM_POSTGRES_USER': 'platform_pipeline',
        'POP_TALK_PLATFORM_POSTGRES_PASSWORD': secrets.token_hex(24),
    }
    missing = [f'{key}={value}' for key,value in additions.items() if key not in airflow_values]
    if missing:
        with airflow_path.open('a', encoding='utf-8') as stream:
            stream.write('\n' + '\n'.join(missing) + '\n')
    for folder in (".local/data/airflow-workbench/lab-artifacts", ".local/qa",
                   ".docker-local", "orchestration/airflow/logs"):
        (ROOT / folder).mkdir(parents=True, exist_ok=True)
    print("Platform configuration is ready. Existing files were preserved; secrets were not printed.")
    print("Configure AWS and movie API Connections for raw collection; see README.md.")

if __name__ == "__main__":
    main()
