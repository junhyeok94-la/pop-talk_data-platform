// Default is a description only. --apply changes PostgreSQL ACLs and stores a
// local credential; it must not be run until the user has approved that scope.
const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const { spawnSync } = require("node:child_process");
const root = path.resolve(__dirname, "../../..");
const file = path.join(root, ".local/config/airflow.env");
const key = "AIRFLOW_CONN_WORKBENCH_RO_AIRFLOW";
if (!process.argv.includes("--apply")) {
  console.log(
    JSON.stringify(
      {
        executed: false,
        role: "workbench_airflow_ro",
        connection: "workbench_ro_airflow",
        database: "airflow",
        schema: "workbench_monitor",
        views: ["dag_runs", "task_instances", "dags", "dag_tags", "pools"],
        grants: "CONNECT, schema USAGE, SELECT on the five views only",
        credentials: ".local/config/airflow.env (never printed)",
        sql: "orchestration/airflow/scripts/workbench-monitoring.sql",
      },
      null,
      2,
    ),
  );
  process.exit(0);
}
const saved = fs.readFileSync(file, "utf8");
if (saved.split(/\r?\n/).some((line) => line.startsWith(key + "="))) {
  console.log("Connection already exists; no role or credential was changed.");
  process.exit(0);
}
const env = Object.fromEntries(
  saved
    .split(/\r?\n/)
    .filter((l) => l.includes("=") && !l.startsWith("#"))
    .map((l) => [l.slice(0, l.indexOf("=")), l.slice(l.indexOf("=") + 1)]),
);
const url = new URL(env.AIRFLOW__DATABASE__SQL_ALCHEMY_CONN);
const database = decodeURIComponent(url.pathname.slice(1));
if (url.hostname !== "db" || !/^\w+$/.test(database))
  throw Error("Expected local db service and a simple database identifier.");
const password = crypto.randomBytes(32).toString("hex");
const config = {
  conn_type: "postgres",
  host: "db",
  port: 5432,
  schema: database,
  login: "workbench_airflow_ro",
  password,
};
const sql =
  `\\set monitor_password '${password}'\n\\set airflow_database '${database}'\n` +
  fs.readFileSync(
    path.join(root, "orchestration/airflow/scripts/workbench-monitoring.sql"),
    "utf8",
  );
const pending = file + ".workbench-pending";
// Prepare the local file before committing PostgreSQL changes. Keep this file if
// the final rename fails, so a committed login never loses its credential.
fs.writeFileSync(
  pending,
  saved + "\n" + key + "=" + JSON.stringify(config) + "\n",
  { flag: "wx", mode: 0o600 },
);
const run = spawnSync(
  "docker",
  [
    "--config",
    ".docker-local",
    "compose",
    "exec",
    "-T",
    "db",
    "sh",
    "-c",
    'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$1"',
    "workbench-monitor",
    database,
  ],
  { cwd: root, input: sql, encoding: "utf8" },
);
if (run.status !== 0) {
  // Do not print SQL/stderr: a database error can echo credential-bearing SQL.
  console.error(
    "Database transaction failed or outcome is uncertain. No retry was made. Keep the .workbench-pending file and inspect the role before recovery.",
  );
  process.exit(1);
}
if (fs.readFileSync(file, "utf8") !== saved)
  throw Error(
    "Airflow environment changed concurrently. Credential is preserved in .workbench-pending; merge that single Connection line before recovery.",
  );
fs.renameSync(pending, file);
console.log(
  "Five monitoring views and the read-only Airflow Connection are ready. Recreate the API server after checking active DAG runs.",
);
