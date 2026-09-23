"""Generate local installation credentials; never replace existing secrets."""

import argparse
import base64
import json
import os
from pathlib import Path
import secrets
from urllib.parse import urlsplit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--ollama-url", default="http://host.docker.internal:11434")
    args = parser.parse_args()
    parsed = urlsplit(args.ollama_url)
    if not 1 <= args.port <= 65535 or parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        parser.error("Use a valid port and an HTTP(S) inference URL without credentials/query/fragment.")
    if any(c in args.ollama_url for c in "\n\r'"):
        parser.error("Invalid URL characters")
    token = secrets.token_hex(32)
    values = {
        "AIRFLOW_PORT": str(args.port),
        "POSTGRES_PASSWORD": secrets.token_hex(24),
        "AIRFLOW_ADMIN_USERNAME": "admin",
        "AIRFLOW_ADMIN_PASSWORD": secrets.token_urlsafe(24),
        "AIRFLOW_FERNET_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
        "AIRFLOW_JWT_SECRET": secrets.token_hex(32),
        "MODEL_WORKER_TOKEN": token,
        "AIRFLOW_CONN_MODEL_LAB_OLLAMA": json.dumps({"conn_type": "http", "host": args.ollama_url}),
        "AIRFLOW_CONN_MODEL_LAB_WORKER": json.dumps({"conn_type": "http", "host": "http://model-worker:8091", "password": token}),
    }
    path = Path(__file__).resolve().parents[1] / ".env"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
        stream.write("# Local credentials. Do not commit or distribute this file.\n")
        stream.writelines(f"{key}='{value}'\n" for key, value in values.items())
    print("Created .env. Read AIRFLOW_ADMIN_USERNAME/PASSWORD there to sign in.")


if __name__ == "__main__":
    main()
