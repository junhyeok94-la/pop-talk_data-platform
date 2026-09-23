const test = require("node:test");
const assert = require("node:assert/strict");
const vm = require("node:vm");
const fs = require("node:fs");
const path = require("node:path");
const root = path.join(
  __dirname,
  "../plugins/airflow_workbench/shared/static",
);

test("host uses shared React and forwards only this frame and allowed native routes", () => {
  const frame = { current: null },
    calls = [],
    events = new Map();
  let cleanup;
  const context = {
    URL,
    document: { baseURI: "https://airflow.example/airflow/" },
    React: {
      useRef: () => frame,
      useEffect: (f) => {
        cleanup = f();
      },
      createElement: (type, props) => ({ type, props }),
    },
    ReactRouterDOM: { useNavigate: () => (route) => calls.push(route) },
    window: {
      addEventListener: (name, fn) => events.set(name, fn),
      removeEventListener: (name) => events.delete(name),
    },
  };
  vm.runInNewContext(
    fs.readFileSync(path.join(root, "host.js"), "utf8"),
    context,
  );
  for (const name of ["내 대시보드", "운영 모니터링", "Model Lab"])
    assert.equal(typeof context[name], "function");
  const rendered = context["내 대시보드"]();
  assert.equal(rendered.type, "iframe");
  assert.equal(
    rendered.props.src,
    "https://airflow.example/airflow/workbench/dashboard",
  );
  assert.ok(!rendered.props.sandbox.includes("allow-top-navigation"));
  const child = { postMessage: () => {} },
    source = child;
  frame.current = { contentWindow: child };
  const receive = events.get("message");
  const message = {
    origin: "https://airflow.example",
    source,
    data: {
      type: "workbench:navigate",
      href: "https://airflow.example/dags/test/runs/manual__a%2Bb",
    },
  };
  receive(message);
  assert.deepEqual(calls, ["/dags/test/runs/manual__a%2Bb"]);
  receive({ ...message, origin: "https://evil.example" });
  receive({ ...message, source: {} });
  for (const href of [
    "https://evil.example/dags/x",
    "javascript:alert(1)",
    "https://u:p@airflow.example/dags/x",
    "https://airflow.example/api/v2/dags",
    "https://airflow.example/auth/token",
  ])
    receive({ ...message, data: { ...message.data, href } });
  assert.equal(calls.length, 1);
  receive({ ...message, data: { ...message.data, href: "https://airflow.example/connections" } });
  assert.equal(calls.at(-1), "/connections");
  cleanup();
  assert.equal(events.size, 0);
});

test("embedded link click is sent to the host; standalone and foreign links keep browser semantics", () => {
  const events = {},
    sent = [],
    parent = { postMessage: (...args) => sent.push(args) };
  const context = {
    URL,
    location: {
      origin: "https://airflow.example",
      href: "https://airflow.example/workbench/dashboard",
    },
    window: { parent, addEventListener: (name, fn) => (events[name] = fn) },
    document: { addEventListener: (name, fn) => (events[name] = fn) },
  };
  vm.runInNewContext(
    fs.readFileSync(path.join(root, "navigation.js"), "utf8"),
    context,
  );
  let stopped = 0;
  const click = {
    target: {
      closest: () => ({ href: "https://airflow.example/dags/example" }),
    },
    button: 0,
    preventDefault: () => stopped++,
  };
  events.click(click);
  assert.equal(stopped, 0);
  events.message({
    origin: context.location.origin,
    source: {},
    data: { type: "workbench:host-ready" },
  });
  events.click(click);
  assert.equal(stopped, 0);
  events.message({
    origin: context.location.origin,
    source: parent,
    data: { type: "workbench:host-ready" },
  });
  events.click(click);
  assert.equal(stopped, 1);
  assert.equal(sent[0][0].href, "https://airflow.example/dags/example");
  assert.equal(sent[0][1], context.location.origin);
  events.click({ ...click, ctrlKey: true });
  events.click({
    ...click,
    target: { closest: () => ({ href: "https://other.example/dags/a" }) },
  });
  assert.equal(stopped, 1);
});
