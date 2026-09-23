// 로컬 DW의 공개 데이터 게시 메타데이터에만 SELECT 권한을 부여한다.
// 비밀번호는 출력하거나 argv에 전달하지 않고 Airflow의 로컬 env에 보관한다.
const fs = require("node:fs");
const crypto = require("node:crypto");
const { spawnSync } = require("node:child_process");
const path = require("node:path");
const file = path.resolve(__dirname, "../../../.local/config/airflow.env");
const env = Object.fromEntries(
  fs
    .readFileSync(".env", "utf8")
    .split(/\r?\n/)
    .filter((l) => l.includes("=") && !l.startsWith("#"))
    .map((l) => [l.slice(0, l.indexOf("=")), l.slice(l.indexOf("=") + 1)]),
);
const saved = fs.readFileSync(file, "utf8");
const name = "AIRFLOW_CONN_WORKBENCH_RO_SERVING";
const line = saved.split(/\r?\n/).find((l) => l.startsWith(name + "="));
const role = "workbench_monitor_ro";
const config = line
  ? JSON.parse(line.slice(name.length + 1))
  : {
      conn_type: "postgres",
      host: "db",
      port: 5432,
      schema: env.POSTGRES_DB,
      login: role,
      password: crypto.randomBytes(32).toString("hex"),
    };
if (
  !/^[a-zA-Z0-9_]+$/.test(config.schema) ||
  !/^[a-f0-9]{64}$/.test(config.password) ||
  config.login !== role
)
  throw new Error("기존 모니터링 Connection 설정을 확인하세요.");
const sql = `BEGIN;
DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='${role}') THEN CREATE ROLE ${role} LOGIN PASSWORD '${config.password}' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS; END IF; END $$;
ALTER ROLE ${role} SET default_transaction_read_only=on;
ALTER ROLE ${role} SET statement_timeout='2s';
GRANT CONNECT ON DATABASE "${config.schema}" TO ${role};
GRANT USAGE ON SCHEMA dw_serving TO ${role};
GRANT SELECT ON dw_serving.publish_attempts_v3,dw_serving.active_publications_v3,dw_serving.gold_build_registry_v3,dw_serving.dataset_publications_v3 TO ${role};
COMMIT;
`;
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
    'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"',
  ],
  { input: sql, encoding: "utf8" },
);
if (run.status !== 0) {
  console.error("읽기 계정 준비 실패. DB 연결과 지정 테이블을 확인하세요.");
  process.exit(1);
}
if (!line)
  fs.appendFileSync(file, "\n" + name + "=" + JSON.stringify(config) + "\n");
console.log(
  "Workbench 모니터링 SELECT 계정과 로컬 Airflow Connection 준비 완료.",
);
