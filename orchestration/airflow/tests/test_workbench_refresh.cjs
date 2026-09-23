const test = require("node:test");
const assert = require("node:assert/strict");
const vm = require("node:vm");
const fs = require("node:fs");
const path = require("node:path");
const root = path.join(
  __dirname,
  "../plugins/airflow_workbench",
);

test("HTTP errors retain retry policy and readable messages for both dashboard tabs", async () => {
  let response = {
    ok: false,
    status: 503,
    headers: new Headers({ "Retry-After": "45" }),
    json: async () => ({
      detail: {
        message: "잠시 대기",
        reason: "database_active",
        retry_after: 30,
      },
    }),
  };
  const context = vm.createContext({ fetch: async () => response });
  vm.runInContext(
    fs.readFileSync(path.join(root, "shared/static/ui.js"), "utf8"),
    context,
  );
  const api = vm.runInContext("WorkbenchUI.api", context);
  await assert.rejects(
    api("work/summary"),
    (e) =>
      e.status === 503 &&
      e.retryAfter === 45 &&
      e.reason === "database_active" &&
      e.message === "잠시 대기",
  );
  response = {
    ...response,
    status: 403,
    json: async () => ({ detail: "권한 없음" }),
  };
  await assert.rejects(
    api("studio/boards/query"),
    (e) => e.status === 403 && e.message === "권한 없음",
  );
});

test("deferral blocks manual refresh, respects the server delay, and preserves auto-off", () => {
  let now = 100000;
  const button = { classList: { toggle() {} } };
  const context = vm.createContext({
    WorkbenchUI: { $: (s) => (s === "#scope-refresh" ? button : null) },
    Date: { now: () => now },
    Math: Object.assign(Object.create(Math), { random: () => 0.5 }),
    setTimeout: () => 1,
    clearTimeout() {},
  });
  vm.runInContext(
    fs.readFileSync(path.join(root, "dashboard/static/workspace.js"), "utf8"),
    context,
  );
  const scope = vm.runInContext("WorkbenchScope", context);
  scope.update({
    profile: { version: 1, refresh_seconds: 0, hours: 24 },
    dags: [{ dag_id: "one" }],
  });
  const key = scope.scopeKey();
  scope.deferred({ retryAfter: 45 });
  assert.equal(scope.canRefresh(), false);
  assert.equal(button.disabled, true);
  assert.equal(scope.seconds(), 0);
  assert.ok(scope.intervalMs() >= 45000 && scope.intervalMs() <= 51750);
  scope.update({
    profile: { version: 2, refresh_seconds: 60, hours: 24 },
    dags: [{ dag_id: "one" }],
  });
  assert.equal(scope.scopeKey(), key);
  scope.update({
    profile: { version: 3, refresh_seconds: 60, hours: 48 },
    dags: [{ dag_id: "one" }],
  });
  assert.notEqual(scope.scopeKey(), key);
  now += 46000;
  assert.equal(scope.canRefresh(), true);
  scope.recovered({ stale: true, retry_after: 30 });
  assert.equal(scope.canRefresh(), false);
  scope.recovered({ stale: false });
  scope.busy(false);
  assert.equal(scope.canRefresh(), true);
  assert.equal(button.disabled, false);
});
