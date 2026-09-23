const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const path = require("node:path");
const staticDir = path.join(
  __dirname,
  "../plugins/airflow_workbench/shared/static",
);

function harness({
  mode = "light",
  systemDark = false,
  embedded = true,
  blocked = false,
} = {}) {
  const values = new Map();
  const root = {
    dataset: {},
    style: {
      setProperty: (key, value) => values.set(key, value),
      removeProperty: (key) => values.delete(key),
    },
  };
  const host = {
    mode,
    tokens: { "--chakra-colors-bg": "#ffffff" },
    getAttribute: () => null,
    classList: { contains: (name) => host.mode === name },
  };
  const events = new Map();
  let mediaListener, observer;
  const media = {
    matches: systemDark,
    addEventListener: (_, fn) => {
      mediaListener = fn;
    },
  };
  const window = {
    matchMedia: () => media,
    requestAnimationFrame: (fn) => fn(),
    addEventListener: (type, fn) =>
      events.set(type, [...(events.get(type) || []), fn]),
    dispatchEvent: (event) => {
      for (const fn of events.get(event.type) || []) fn(event);
    },
  };
  const getComputedStyle = (node) => ({
    colorScheme: node.mode,
    getPropertyValue: (key) =>
      node === host ? host.tokens[key] || "" : values.get(key) || "",
  });
  window.parent = embedded
    ? { document: { documentElement: host }, getComputedStyle }
    : window;
  if (blocked)
    Object.defineProperty(window.parent, "document", {
      get() {
        throw new Error("cross-origin");
      },
    });
  const context = vm.createContext({
    window,
    document: { documentElement: root, addEventListener() {} },
    getComputedStyle,
    CustomEvent: class {
      constructor(type, data) {
        this.type = type;
        Object.assign(this, data);
      }
    },
    MutationObserver: class {
      constructor(fn) {
        observer = fn;
      }
      observe() {}
    },
  });
  vm.runInContext(
    fs.readFileSync(path.join(staticDir, "theme.js"), "utf8"),
    context,
  );
  return {
    root,
    host,
    values,
    window,
    context,
    mutate: () => observer(),
    system: (dark) => {
      media.matches = dark;
      mediaListener();
    },
  };
}

test("native Light overrides a dark OS and updates to native Dark without navigation", () => {
  const h = harness({ systemDark: true });
  assert.equal(h.root.dataset.theme, "light");
  h.host.mode = "dark";
  h.host.tokens["--chakra-colors-bg"] = "oklch(0.23 0.03 266)";
  h.mutate();
  assert.equal(h.root.dataset.theme, "dark");
  assert.equal(h.values.get("--bg"), "oklch(0.23 0.03 266)");
  h.system(false);
  assert.equal(
    h.root.dataset.theme,
    "dark",
    "explicit native theme keeps precedence",
  );
});

test("Follow System tracks the host's resolved theme in both directions", () => {
  const h = harness();
  h.system(true);
  h.host.mode = "dark";
  h.mutate();
  assert.equal(h.root.dataset.theme, "dark");
  h.system(false);
  h.host.mode = "light";
  h.mutate();
  assert.equal(h.root.dataset.theme, "light");
});

test("standalone and inaccessible hosts follow live OS preference", () => {
  for (const options of [{ embedded: false }, { blocked: true }]) {
    const h = harness(options);
    assert.equal(h.root.dataset.theme, "light");
    h.system(true);
    assert.equal(h.root.dataset.theme, "dark");
    h.system(false);
    assert.equal(h.root.dataset.theme, "light");
  }
});

test("custom host token changes notify once; removed tokens fall back to CSS", () => {
  const h = harness();
  let calls = 0;
  h.window.addEventListener("workbench-theme-change", () => calls++);
  h.mutate();
  assert.equal(calls, 0);
  h.host.tokens["--chakra-colors-bg"] = "#fafafa";
  h.mutate();
  assert.equal(calls, 1);
  delete h.host.tokens["--chakra-colors-bg"];
  h.mutate();
  assert.equal(h.values.has("--bg"), false);
  assert.equal(calls, 2);
});

test("chart recoloring preserves custom series color, legend selection and zoom", () => {
  const h = harness();
  const source = fs
    .readFileSync(path.join(staticDir, "../../dashboard/static/studio.js"), "utf8")
    .replace(
      "return { start };",
      "return { start, makeOption, charts, chartSources };",
    );
  h.context.WorkbenchLib = {};
  h.context.WorkbenchTheme = {
    color: (name) => `${h.root.dataset.theme}:${name}`,
  };
  vm.runInContext(source, h.context);
  const ui = vm.runInContext("BoardUI", h.context);
  const panel = {
    chart: "line",
    visual: {
      color: "#ff00ff",
      x: "time",
      y: "count",
      series: "",
      legend: true,
      zoom: true,
      threshold: null,
      overrides: [],
      time_axis: false,
      advanced: {},
    },
  };
  const rows = [
    { time: "a", count: 2 },
    { time: "b", count: 4 },
  ];
  const original = ui.makeOption(panel, rows, ["A"]);
  assert.equal(original.textStyle.color, "light:muted");
  let next;
  const chart = {
    isDisposed: () => false,
    getOption: () => ({
      legend: [{ selected: { count: false } }],
      dataZoom: [
        { start: 20, end: 80 },
        { start: 20, end: 80 },
      ],
    }),
    setOption: (option) => {
      next = option;
    },
  };
  ui.charts.set("canvas", chart);
  ui.chartSources.set("canvas", { panel, rows, refs: ["A"] });
  h.host.mode = "dark";
  h.mutate();
  assert.equal(next.textStyle.color, "dark:muted");
  assert.equal(next.tooltip.backgroundColor, "dark:card");
  assert.equal(next.xAxis.axisLabel.color, "dark:muted");
  assert.equal(next.series[0].itemStyle.color, "#ff00ff");
  assert.equal(next.legend.selected.count, false);
  assert.equal(next.dataZoom[0].start, 20);
  assert.equal(next.dataZoom[1].end, 80);
  assert.equal(ui.charts.get("canvas"), chart);
});
