"use strict";
const BoardUI = (() => {
  const { echarts, GridStack } = WorkbenchLib;
  const clone = (v) => structuredClone(v);
  const panelIcon = (paths) =>
    `<svg viewBox="0 0 24 24" aria-hidden="true">${paths}</svg>`;
  const panelIcons = {
    inspect: panelIcon(
      '<circle cx="10.5" cy="10.5" r="6.5"/><path d="m16 16 5 5"/>',
    ),
    copy: panelIcon(
      '<rect x="8" y="8" width="12" height="12" rx="2"/><path d="M16 8V4H4v12h4"/>',
    ),
    close: panelIcon('<path d="m6 6 12 12M18 6 6 18"/>'),
    grip: panelIcon(
      '<path d="M8 5h.01M16 5h.01M8 12h.01M16 12h.01M8 19h.01M16 19h.01" stroke-width="4"/>',
    ),
  };
  const palette = [
    "#73b4ff",
    "#65ddbe",
    "#efb971",
    "#bd9bff",
    "#ff929a",
    "#70d5ef",
  ];
  const types = [
    ["line", "시계열 · 선"],
    ["area", "시계열 · 영역"],
    ["bar", "막대"],
    ["stat", "숫자"],
    ["gauge", "게이지"],
    ["pie", "도넛"],
    ["table", "테이블"],
    ["scatter", "산점도"],
    ["heatmap", "히트맵"],
  ];
  const aggregates = [
    ["count", "건수"],
    ["sum", "합계"],
    ["avg", "평균"],
    ["min", "최솟값"],
    ["max", "최댓값"],
    ["p95", "P95"],
    ["percent", "일치 비율 (%)"],
  ];
  let boards = [],
    sources = [],
    board = null,
    frames = {},
    grid = null,
    editing = false,
    dirty = false,
    timer = null,
    refreshSerial = 0,
    editor = null;
  const charts = new Map();
  const chartSources = new Map();
  const observers = new Map();
  function bind(root, selector, event, fn) {
    root.querySelectorAll(selector).forEach((el) =>
      el.addEventListener(
        event,
        guarded((e) => fn(e, el)),
      ),
    );
  }
  function input(name, label, value, type = "text", attrs = "") {
    return `<label>${label}<input name="${name}" type="${type}" value="${esc(value ?? "")}" ${attrs}></label>`;
  }
  function select(name, label, values, value, attrs = "") {
    return `<label>${label}<select name="${name}" ${attrs}>${values
      .map((item) => {
        const [v, l] = Array.isArray(item) ? item : [item, item];
        return `<option value="${esc(v)}" ${String(v) === String(value) ? "selected" : ""}>${esc(l)}</option>`;
      })
      .join("")}</select></label>`;
  }
  function check(name, label, value) {
    return `<label class="studio-check"><input type="checkbox" name="${name}" ${value ? "checked" : ""}>${label}</label>`;
  }
  function context() {
    const result = Object.fromEntries(
      [
        "hours",
        "bucket_seconds",
        "variables",
        "from_ts",
        "to_ts",
        "follow_work_scope",
        "run_state",
      ].map((k) => [k, board[k]]),
    );
    result.variables = { ...result.variables };
    return result;
  }
  function source(id) {
    return sources.find((s) => s.id === id);
  }
  function columnsFor(q) {
    return source(q.datasource)?.tables?.[q.builder.dataset] || [];
  }
  function markDirty() {
    dirty = true;
    const el = $("#save-state");
    if (el) el.textContent = "저장하지 않은 변경";
  }
  function dispose(el) {
    if (!el) return;
    const chart = charts.get(el);
    if (chart) {
      chart.dispose();
      charts.delete(el);
      chartSources.delete(el);
    }
    observers.get(el)?.disconnect();
    observers.delete(el);
  }
  function disposeAll() {
    for (const el of charts.keys()) dispose(el);
  }
  function numericValue(value) {
    return value !== null && value !== "" && Number.isFinite(Number(value))
      ? Number(value)
      : null;
  }
  function reduce(values, how) {
    const v = values.map(numericValue).filter((x) => x !== null);
    if (!v.length) return null;
    return how === "count"
      ? v.length
      : how === "sum"
        ? v.reduce((a, b) => a + b, 0)
        : how === "avg"
          ? v.reduce((a, b) => a + b, 0) / v.length
          : how === "min"
            ? Math.min(...v)
            : how === "max"
              ? Math.max(...v)
              : how === "first"
                ? v[0]
                : v.at(-1);
  }
  function format(value, v) {
    return Number(value).toLocaleString("ko-KR", {
      minimumFractionDigits: v.decimals,
      maximumFractionDigits: v.decimals,
    });
  }
  function dataset(panel, result) {
    const selected = (result || []).filter(
      (f) => panel.visual.data_ref === "all" || f.ref === panel.visual.data_ref,
    );
    const valid = selected.filter((f) => !f.error);
    return {
      selected,
      valid,
      columns: [...new Set(valid.flatMap((f) => f.columns))],
      rows: valid.flatMap((f) =>
        f.rows.map((row) => ({ ...row, __query: f.ref })),
      ),
    };
  }
  function makeOption(panel, rows, refs) {
    const theme = WorkbenchTheme.color;
    const v = panel.visual,
      colors = [v.color, ...palette.filter((c) => c !== v.color)];
    const value = reduce(
      rows.map((r) => r[v.y]),
      v.reduce,
    );
    const color =
      v.threshold !== null &&
      (v.threshold_mode === "below"
        ? value <= v.threshold
        : value >= v.threshold)
        ? theme("amber")
        : v.color;
    const base = {
      animation: false,
      backgroundColor: "transparent",
      color: colors,
      textStyle: {
        fontFamily: getComputedStyle(document.documentElement).getPropertyValue(
          "--font-body",
        ),
        color: theme("muted"),
      },
      tooltip: {
        trigger: "axis",
        renderMode: "richText",
        confine: true,
        backgroundColor: theme("card"),
        borderColor: theme("line"),
        textStyle: { color: theme("text") },
        axisPointer: { lineStyle: { color: theme("muted") } },
      },
      legend: {
        show: v.legend,
        type: "scroll",
        bottom: 0,
        textStyle: { color: theme("muted") },
        inactiveColor: theme("muted"),
        pageTextStyle: { color: theme("muted") },
        pageIconColor: theme("text"),
        pageIconInactiveColor: theme("line"),
      },
      grid: {
        left: 16,
        right: 20,
        top: 22,
        bottom: v.zoom ? 72 : 42,
        containLabel: true,
      },
    };
    if (panel.chart === "gauge")
      return {
        ...base,
        legend: { show: false },
        tooltip: { show: false },
        series: [
          {
            type: "gauge",
            min: v.minimum,
            max: v.maximum,
            startAngle: 180,
            endAngle: 0,
            radius: "160%",
            center: ["50%", "92%"],
            progress: { show: true, width: 7, itemStyle: { color } },
            axisLine: { lineStyle: { width: 7, color: [[1, theme("line")]] } },
            splitLine: { show: false },
            axisTick: { show: false },
            axisLabel: { show: false },
            pointer: { show: false },
            anchor: { show: false },
            title: { show: false },
            detail: {
              valueAnimation: false,
              offsetCenter: [0, "-22%"],
              fontSize: 22,
              color: theme("text"),
              formatter: (n) => format(n, v) + v.unit,
            },
            data: [{ value }],
          },
        ],
      };
    if (panel.chart === "pie")
      return {
        ...base,
        tooltip: { ...base.tooltip, trigger: "item" },
        series: [
          {
            type: "pie",
            radius: ["44%", "72%"],
            center: ["50%", "43%"],
            avoidLabelOverlap: true,
            itemStyle: {
              borderRadius: 3,
              borderColor: theme("card"),
              borderWidth: 2,
            },
            label: { show: false },
            emphasis: {
              label: { show: true, color: theme("text"), fontSize: 13 },
            },
            data: rows
              .filter((r) => numericValue(r[v.y]) !== null)
              .map((r) => ({
                name: String(r[v.x]),
                value: Number(r[v.y]),
                itemStyle: {
                  color:
                    v.overrides.find((o) => o.name === String(r[v.x]))?.color ||
                    {
                      success: theme("success"),
                      failed: theme("red"),
                      running: theme("blue"),
                      queued: theme("amber"),
                    }[String(r[v.x])],
                },
              })),
          },
        ],
      };
    const axisStyle = {
      axisLine: { lineStyle: { color: theme("line") } },
      axisLabel: { color: theme("muted"), hideOverlap: true },
      splitLine: { lineStyle: { color: theme("line"), type: "dashed" } },
    };
    if (panel.chart === "heatmap") {
      const xs = [...new Set(rows.map((r) => String(r[v.x])))],
        ys = [...new Set(rows.map((r) => String(r[v.series])))];
      const values = rows
        .filter((r) => numericValue(r[v.y]) !== null)
        .map((r) => [
          xs.indexOf(String(r[v.x])),
          ys.indexOf(String(r[v.series])),
          Number(r[v.y]),
        ]);
      return {
        ...base,
        legend: { show: false },
        tooltip: { ...base.tooltip, trigger: "item" },
        grid: { left: 20, right: 20, top: 20, bottom: 65, containLabel: true },
        xAxis: { ...axisStyle, type: "category", data: xs },
        yAxis: { ...axisStyle, type: "category", data: ys },
        visualMap: {
          min: values.length ? Math.min(...values.map((x) => x[2])) : 0,
          max: values.length ? Math.max(...values.map((x) => x[2])) : 1,
          orient: "horizontal",
          left: "center",
          bottom: 0,
          textStyle: { color: theme("muted") },
          inRange: { color: [theme("surface"), v.color] },
        },
        series: [{ type: "heatmap", data: values }],
      };
    }
    const groups = new Map();
    const ys = v.y
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean);
    for (const r of rows)
      for (const y of ys) {
        const key =
          [
            refs.length > 1 ? r.__query : "",
            v.series ? r[v.series] : "",
            ys.length > 1 ? y : "",
          ]
            .filter((x) => x !== "")
            .join(" · ") || y;
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push([r[v.x], numericValue(r[y])]);
      }
    const xs = [...new Set(rows.map((r) => String(r[v.x])))];
    const xval = (x) =>
      v.time_axis ? (typeof x === "number" ? x * 1000 : Date.parse(x)) : x;
    const horizontal = v.horizontal && panel.chart === "bar";
    const valueAxis = {
      ...axisStyle,
      type: "value",
      min: v.axis_min ?? undefined,
      max: v.axis_max ?? undefined,
      name: v.unit,
      nameTextStyle: { color: theme("muted") },
    };
    const categoryAxis = {
      ...axisStyle,
      type:
        panel.chart === "scatter" ? "value" : v.time_axis ? "time" : "category",
      data: v.time_axis || panel.chart === "scatter" ? undefined : xs,
      axisLabel: { ...axisStyle.axisLabel, width: 140, overflow: "truncate" },
      boundaryGap: panel.chart === "bar",
    };
    const hasRight = v.overrides.some((o) => o.axis === "right") && !horizontal;
    const series = [...groups.entries()].map(([name, points], i) => {
      const override = v.overrides.find((o) => o.name === name);
      const ordered =
        v.time_axis || panel.chart === "scatter"
          ? points.map(([x, y]) => [xval(x), y]).sort((a, b) => a[0] - b[0])
          : xs.map((x) => {
              const matches = points.filter((p) => String(p[0]) === x);
              return matches.length ? matches.at(-1)[1] : null;
            });
      return {
        name,
        type: panel.chart === "area" ? "line" : panel.chart,
        stack: v.stack ? "total" : undefined,
        smooth: v.smooth,
        showSymbol: v.point_size > 0,
        symbolSize: v.point_size,
        lineStyle: { width: v.line_width },
        connectNulls: false,
        areaStyle: panel.chart === "area" ? { opacity: 0.22 } : undefined,
        itemStyle: {
          color:
            override?.color ||
            {
              success: theme("success"),
              failed: theme("red"),
              running: theme("blue"),
              queued: theme("amber"),
            }[name] ||
            colors[i % colors.length],
        },
        yAxisIndex: override?.axis === "right" && !horizontal ? 1 : 0,
        data: ordered,
        markLine:
          i === 0 && v.threshold !== null
            ? {
                silent: true,
                symbol: "none",
                lineStyle: { color: theme("amber"), type: "dashed" },
                label: {
                  color: theme("amber"),
                  formatter: String(v.threshold),
                },
                data: [
                  horizontal ? { xAxis: v.threshold } : { yAxis: v.threshold },
                ],
              }
            : undefined,
      };
    });
    const result = {
      ...base,
      xAxis: horizontal ? valueAxis : categoryAxis,
      yAxis: horizontal
        ? categoryAxis
        : hasRight
          ? [
              valueAxis,
              {
                ...valueAxis,
                position: "right",
                name: "",
                min: undefined,
                max: undefined,
                splitLine: { show: false },
              },
            ]
          : valueAxis,
      series,
    };
    if (v.zoom)
      result.dataZoom = [
        {
          type: "inside",
          ...(horizontal ? { yAxisIndex: 0 } : { xAxisIndex: 0 }),
        },
        {
          type: "slider",
          height: 14,
          bottom: v.legend ? 25 : 8,
          borderColor: theme("line"),
          backgroundColor: theme("surface"),
          fillerColor: theme("selected-bg"),
          textStyle: { color: theme("muted") },
          ...(horizontal
            ? {
                yAxisIndex: 0,
                orient: "vertical",
                right: 0,
                bottom: 30,
                top: 20,
                width: 12,
              }
            : { xAxisIndex: 0 }),
        },
      ];
    Object.assign(result, v.advanced);
    return result;
  }
  function renderTable(el, rows, columns, airflowLinks = false) {
    let direction = 1,
      last = "",
      page = 0;
    const sorted = [...rows];
    function cell(row, column) {
      if (["state", "status"].includes(column)) return badge(row[column]);
      const label = esc(row[column] ?? "—");
      if (
        !airflowLinks ||
        !row.dag_id ||
        !["dag_id", "run_id"].includes(column) ||
        !row[column]
      )
        return label;
      const path =
        `/dags/${encodeURIComponent(row.dag_id)}` +
        (column === "run_id" ? `/runs/${encodeURIComponent(row.run_id)}` : "");
      return `<a target="_top" href="${path}" title="${column === "run_id" ? "실행·로그 열기" : "DAG 상세 열기"}">${label}</a>`;
    }
    function draw() {
      el.innerHTML = `<div class="studio-table-wrap"><table><thead><tr>${columns.map((c) => `<th><button data-sort="${esc(c)}">${esc(c)}${last === c ? (direction === 1 ? " ↑" : " ↓") : ""}</button></th>`).join("")}</tr></thead><tbody>${sorted
        .slice(page * 50, (page + 1) * 50)
        .map(
          (row) =>
            `<tr>${columns.map((c) => `<td>${cell(row, c)}</td>`).join("")}</tr>`,
        )
        .join(
          "",
        )}</tbody></table></div>${rows.length > 50 ? `<div class="table-pages"><button data-page="-1" ${page === 0 ? "disabled" : ""}>이전</button><span>${page + 1} / ${Math.ceil(rows.length / 50)} · ${rows.length}행</span><button data-page="1" ${(page + 1) * 50 >= rows.length ? "disabled" : ""}>다음</button></div>` : ""}`;
      bind(el, "[data-sort]", "click", (_, b) => {
        direction = last === b.dataset.sort ? -direction : 1;
        last = b.dataset.sort;
        sorted.sort((a, b) => {
          const aa = a[last],
            bb = b[last];
          if (aa == null) return 1;
          if (bb == null) return -1;
          return (
            direction *
            (typeof aa === "number" && typeof bb === "number"
              ? aa - bb
              : String(aa).localeCompare(String(bb)))
          );
        });
        page = 0;
        draw();
      });
      bind(el, "[data-page]", "click", (_, b) => {
        page += Number(b.dataset.page);
        draw();
      });
    }
    draw();
  }
  function paint(el, panel, result) {
    if (!el) return;
    dispose(el);
    el.innerHTML = "";
    if (!result) {
      el.innerHTML = empty("조회 중…");
      return;
    }
    const { selected, valid, rows, columns } = dataset(panel, result);
    const errors = selected.filter((f) => f.error);
    if (!rows.length) {
      el.innerHTML = errors.length
        ? `<div class="panel-error">${errors.map((f) => `<strong>쿼리 ${f.ref}</strong><p>${esc(f.error)}</p>`).join("")}</div>`
        : empty("이 기간과 필터에 해당하는 데이터가 없습니다.");
      return;
    }
    if (panel.chart === "table") {
      renderTable(
        el,
        rows,
        columns,
        panel.queries
          .filter((q) => q.enabled)
          .every((q) => q.datasource === "airflow"),
      );
      return;
    }
    const v = panel.visual,
      required = [
        ...v.y.split(",").map((x) => x.trim()),
        ...(["stat", "gauge"].includes(panel.chart) ? [] : [v.x]),
        ...(panel.chart === "heatmap" ? [v.series] : []),
      ];
    if (required.some((c) => !columns.includes(c))) {
      el.innerHTML = empty(
        `결과 필드와 매핑을 확인하세요: ${required.filter((c) => !columns.includes(c)).join(", ")}`,
      );
      return;
    }
    if (panel.chart === "stat") {
      const value = reduce(
        rows.map((r) => r[v.y]),
        v.reduce,
      );
      if (value === null) {
        el.innerHTML = empty("숫자 데이터가 없습니다.");
        return;
      }
      const warning =
        v.threshold !== null &&
        (v.threshold_mode === "below"
          ? value <= v.threshold
          : value >= v.threshold);
      el.innerHTML = `<div class="studio-stat" style="--stat-color:${warning ? "var(--amber)" : v.color}"><strong>${esc(format(value, v))}<small>${esc(v.unit)}</small></strong><span>${esc({ last: "최근 값", first: "첫 값", sum: "합계", avg: "평균", min: "최솟값", max: "최댓값", count: "건수" }[v.reduce])}${v.threshold !== null ? ` · 임계값 ${esc(v.threshold)}${v.threshold_mode === "below" ? " 이하" : " 이상"}` : ""}</span></div>`;
      return;
    }
    if (
      panel.chart === "gauge" &&
      reduce(
        rows.map((r) => r[v.y]),
        v.reduce,
      ) === null
    ) {
      el.innerHTML = empty("계산 가능한 값이 없습니다.");
      return;
    }
    const c = echarts.init(el, null, { renderer: "canvas" });
    charts.set(el, c);
    chartSources.set(el, {
      panel,
      rows: rows.slice(0, 2000),
      refs: valid.map((f) => f.ref),
    });
    c.setOption(
      makeOption(
        panel,
        rows.slice(0, 2000),
        valid.map((f) => f.ref),
      ),
      true,
    );
    const observer = new ResizeObserver(() => {
      if (!c.isDisposed()) c.resize();
    });
    observer.observe(el);
    observers.set(el, observer);
  }
  window.addEventListener("workbench-theme-change", () => {
    // Re-style the existing canvases without rebuilding the grid or editor.
    // Preserve the viewer's legend selections and zoom range as well as drafts.
    for (const [el, chart] of charts) {
      if (chart.isDisposed()) continue;
      const { panel, rows, refs } = chartSources.get(el);
      const previous = chart.getOption();
      const next = makeOption(panel, rows, refs);
      if (next.legend && previous.legend?.[0]?.selected)
        next.legend.selected = previous.legend[0].selected;
      next.dataZoom?.forEach((zoom, i) => {
        const saved = previous.dataZoom?.[i];
        if (saved) {
          zoom.start = saved.start;
          zoom.end = saved.end;
        }
      });
      chart.setOption(next, true);
    }
  });
  function panelFoot(p) {
    const result = frames[p.id] || [];
    const truncated = result.some((r) => r.truncated),
      limited = result.some((r) => r.limited),
      error = result.some((r) => r.error);
    const times = result.map((r) => r.to_ts || r.sampled_at).filter(Boolean);
    const asOf = times.length
      ? `${new Date(Math.min(...times) * 1000).toLocaleTimeString("ko-KR", { hour12: false })} 기준 · `
      : "";
    return `<span>${esc(p.description || [...new Set(p.queries.map((q) => source(q.datasource)?.name || q.datasource))].join(" · "))}</span><span class="${error ? "error-text" : ""}">${error ? "쿼리 오류 · " : ""}${truncated ? "표본 제한 · " : ""}${limited ? "결과 행 제한 · " : ""}${asOf}${result.length ? Math.round(result.reduce((s, r) => s + (r.elapsed_ms || 0), 0)) + " ms" : ""}</span>`;
  }
  function drawGrid() {
    disposeAll();
    if (grid) {
      grid.destroy(false);
      grid = null;
    }
    const host = $("#studio-grid");
    host.innerHTML = "";
    if (!board.panels.length) {
      host.innerHTML = `<div class="studio-blank"><div class="blank-icon">▦</div><h2>첫 번째 패널을 만들어 보세요</h2><p>데이터를 고르고, 쿼리와 시각화를 연결하세요.</p><button class="primary" id="blank-add" ${!state.config.can_edit_dashboard ? "disabled" : ""}>패널 추가</button></div>`;
      on("#blank-add", "click", () => {
        editing = true;
        openGallery();
      });
      return;
    }
    grid = GridStack.init(
      {
        column: 12,
        cellHeight: 36,
        margin: 8,
        float: false,
        staticGrid: !editing,
        handle: ".studio-panel-head",
        animate: false,
        acceptWidgets: false,
        resizable: { handles: "se" },
        minRow: 1,
      },
      host,
    );
    grid.batchUpdate();
    board.panels.forEach((p) => {
      const el = document.createElement("section");
      el.className = "grid-stack-item";
      el.dataset.panel = p.id;
      el.innerHTML = `<div class="grid-stack-item-content studio-panel"><div class="studio-panel-head"><h2 title="${esc(p.title)}">${editing ? `<span class="grip">${panelIcons.grip}</span>` : ""}<span class="studio-panel-title">${esc(p.title)}</span></h2><div class="studio-panel-actions"><button data-inspect="${p.id}" aria-label="${esc(p.title)} 데이터 확인" title="데이터 확인">${panelIcons.inspect}</button>${editing ? `<button data-edit="${p.id}" aria-label="${esc(p.title)} 편집">편집</button><button data-duplicate="${p.id}" aria-label="${esc(p.title)} 복제" title="복제">${panelIcons.copy}</button><button data-remove="${p.id}" aria-label="${esc(p.title)} 삭제" title="삭제">${panelIcons.close}</button>` : ""}</div></div><div class="studio-panel-body" id="chart-${p.id}"></div><div class="studio-panel-foot" id="foot-${p.id}">${panelFoot(p)}</div></div>`;
      host.append(el);
      grid.makeWidget(el, {
        id: p.id,
        x: p.x,
        y: p.y,
        w: p.w,
        h: p.h,
        minW: 2,
        minH: 4,
      });
    });
    grid.batchUpdate(false);
    board.panels.forEach((p) => paint($("#chart-" + p.id), p, frames[p.id]));
    grid.on("change", (_, items) => {
      if (!editing) return;
      for (const n of items) {
        const p = board.panels.find((x) => x.id === n.id);
        if (p) Object.assign(p, { x: n.x, y: n.y, w: n.w, h: n.h });
      }
      markDirty();
    });
    bind(host, "[data-edit]", "click", (_, b) =>
      openEditor(board.panels.find((p) => p.id === b.dataset.edit)),
    );
    bind(host, "[data-inspect]", "click", (_, b) =>
      inspectPanel(board.panels.find((p) => p.id === b.dataset.inspect)),
    );
    bind(host, "[data-duplicate]", "click", (_, b) => {
      if (board.panels.length >= 40) throw Error("패널은 최대 40개입니다.");
      const p = clone(board.panels.find((p) => p.id === b.dataset.duplicate));
      p.id = crypto.randomUUID();
      p.title += " 복사";
      p.x = 0;
      p.y = Math.max(...board.panels.map((p) => p.y + p.h));
      board.panels.push(p);
      frames[p.id] = frames[b.dataset.duplicate];
      markDirty();
      drawGrid();
    });
    bind(host, "[data-remove]", "click", (_, b) => {
      board.panels = board.panels.filter((p) => p.id !== b.dataset.remove);
      markDirty();
      drawGrid();
    });
  }
  function render() {
    clearTimeout(timer);
    document.body.classList.remove("panel-editing");
    WorkbenchScope.mode(board.follow_work_scope !== false);
    const scopeMode = select(
      "scope-mode",
      "분석 범위",
      [
        ["assigned", "담당 DAG와 공유"],
        ["independent", "별도 분석"],
      ],
      board.follow_work_scope !== false ? "assigned" : "independent",
    );
    $("#app").innerHTML =
      `<header class="studio-heading"><div><div class="studio-breadcrumb"><button id="board-library">내 대시보드</button><span>/</span><span>${esc(board.category)}</span><span class="pill" title="이 Airflow 계정에만 저장되는 구성입니다.">개인</span></div><h2 class="studio-board-title">${esc(board.title)}</h2><p>${esc(board.description)}</p></div><div class="studio-heading-actions">${scopeMode}<button id="sources-open">데이터소스</button><button id="board-settings" ${!state.config.can_edit_dashboard ? "disabled" : ""}>설정</button>${editing ? '<button id="board-discard">편집 종료</button><button id="board-save" class="primary">저장</button>' : `<button id="board-edit" class="primary" ${!state.config.can_edit_dashboard ? "disabled" : ""}>대시보드 편집</button>`}</div></header><div class="studio-toolbar ${board.follow_work_scope !== false ? "is-shared" : ""}"><div class="variable-controls">${Object.entries(
        board.variables,
      )
        .filter(
          ([key, value]) =>
            key !== "dag" || value || board.follow_work_scope === false,
        )
        .map(([k, v]) =>
          input(
            "var-" + k,
            k === "dag" && board.follow_work_scope !== false
              ? "추가 DAG 조건"
              : k,
            v,
            "text",
            `data-variable="${esc(k)}"`,
          ),
        )
        .join("")}</div><div class="time-controls">${
        board.follow_work_scope !== false
          ? '<span class="scope-analysis-note">공통 범위 안에서 패널별 추가 조건을 적용합니다.</span>'
          : select(
              "period",
              "분석 기간",
              [
                [1, "최근 1시간"],
                [6, "최근 6시간"],
                [24, "최근 1일"],
                [168, "최근 7일"],
                [720, "최근 30일"],
                [2160, "최근 90일"],
                [0, "직접 지정"],
              ],
              board.from_ts ? 0 : board.hours,
            ) +
            '<button id="time-custom" title="시작·종료 시각 지정">◷</button>'
      }</div></div><div class="studio-caption"><div><span class="live-dot"></span><span id="refresh-status">실행 이력과 실험 기록</span>${board.follow_work_scope === false && board.from_ts ? `<span> · ${esc(formatDate(board.from_ts))} — ${esc(formatDate(board.to_ts))}</span>` : ""}</div><div class="studio-edit-tools"><span id="save-state">${dirty ? "저장하지 않은 변경" : `저장됨 · v${board.version}`}</span>${editing ? '<button id="add-panel">＋ 패널 추가</button><button id="board-undo">저장 상태로 되돌리기</button>' : ""}</div></div><div id="studio-grid" class="grid-stack"></div>`;
    on("#board-edit", "click", () => {
      editing = true;
      render();
    });
    on("#board-save", "click", save);
    on("#board-discard", "click", () => {
      if (dirty && !confirm("저장하지 않은 변경을 버리고 편집을 종료할까요?"))
        return;
      board = clone(boards.find((b) => b.id === board.id) || board);
      dirty = false;
      editing = false;
      render();
      refresh();
    });
    on("#board-undo", "click", () => {
      if (!confirm("최근 저장 상태로 되돌릴까요?")) return;
      board = clone(boards.find((b) => b.id === board.id) || board);
      dirty = false;
      render();
      refresh();
    });
    on("#board-library", "click", openLibrary);
    on("#board-settings", "click", settings);
    on("#sources-open", "click", sourceDialog);
    on("#add-panel", "click", openGallery);
    bind($("#app"), '[name="scope-mode"]', "change", (_, el) => {
      board.follow_work_scope = el.value === "assigned";
      if (editing) markDirty();
      render();
      refresh();
    });
    on("#time-custom", "click", customTime);
    bind($("#app"), "[data-variable]", "change", (_, el) => {
      board.variables[el.dataset.variable] = el.value;
      if (editing) markDirty();
      refresh();
    });
    bind($("#app"), '[name="period"]', "change", (_, el) => {
      if (el.value === "0") {
        customTime();
        return;
      }
      board.hours = Number(el.value);
      board.from_ts = null;
      board.to_ts = null;
      if (editing) markDirty();
      render();
      refresh();
    });
    drawGrid();
    schedule();
  }
  function schedule() {
    clearTimeout(timer);
    if (WorkbenchScope.seconds() && !editing && !editor && !document.hidden)
      timer = setTimeout(
        () => (WorkbenchScope.editing ? schedule() : refresh()),
        WorkbenchScope.intervalMs(),
      );
  }
  async function refresh() {
    clearTimeout(timer);
    if (!WorkbenchScope.canRefresh()) {
      schedule();
      return;
    }
    const serial = ++refreshSerial;
    WorkbenchScope.busy(true);
    const snapshot = clone(board);
    const status = $("#refresh-status");
    if (status) status.textContent = "데이터 조회 중…";
    try {
      const previousScope = WorkbenchScope.scopeKey();
      await WorkbenchScope.load();
      if (
        previousScope !== WorkbenchScope.scopeKey() &&
        board.follow_work_scope !== false
      ) {
        frames = {};
        drawGrid();
      }
      const result = await api("studio/boards/query", "POST", snapshot);
      if (serial !== refreshSerial || board.id !== snapshot.id || editor)
        return;
      WorkbenchScope.recovered(
        Object.values(result)
          .flat()
          .find((f) => f.stale),
      );
      frames = result;
      board.panels.forEach((p) => {
        paint($("#chart-" + p.id), p, frames[p.id]);
        const foot = $("#foot-" + p.id);
        if (foot) foot.innerHTML = panelFoot(p);
      });
      if (status)
        status.textContent = `${new Date().toLocaleTimeString("ko-KR", { hour12: false })} 갱신 · ${board.panels.length}개 패널`;
    } catch (e) {
      if (serial === refreshSerial) {
        if (e.status === 401 || e.status === 403) {
          frames = {};
          drawGrid();
        }
        if (e.status === 503 || e.status === 429) WorkbenchScope.deferred(e);
        if (status) status.textContent = e.message;
      }
    } finally {
      if (serial === refreshSerial) WorkbenchScope.busy(false);
      if (serial === refreshSerial) schedule();
    }
  }
  async function save() {
    const result = await api(`studio/boards/${board.id}`, "PUT", board);
    board = result;
    boards = boards.filter((b) => b.id !== board.id).concat([clone(board)]);
    dirty = false;
    toast("내 Airflow 계정에 대시보드를 저장했습니다.");
    render();
  }
  function modal(title, content, wide = false) {
    const d = document.createElement("dialog");
    d.className = "studio-dialog" + (wide ? " wide" : "");
    d.innerHTML = `<div class="dialog-heading"><h2>${esc(title)}</h2><button class="dialog-close" aria-label="닫기">×</button></div>${content}`;
    document.body.append(d);
    d.querySelector(".dialog-close").onclick = () => d.close();
    d.addEventListener("close", () => d.remove());
    d.showModal();
    return d;
  }
  function customTime() {
    const toInput = (t) => {
      const d = new Date(t * 1000);
      return new Date(d.getTime() - d.getTimezoneOffset() * 60000)
        .toISOString()
        .slice(0, 16);
    };
    const end = board.to_ts || Date.now() / 1000,
      start = board.from_ts || end - board.hours * 3600;
    const d = modal(
      "조회 기간 지정",
      `<form>${input("start", "시작 · 로컬 시간", toInput(start), "datetime-local", "required")}${input("end", "종료 · 로컬 시간", toInput(end), "datetime-local", "required")}<button class="primary">적용</button></form>`,
    );
    bind(d, "form", "submit", async (e) => {
      e.preventDefault();
      const form = new FormData(e.target),
        candidate = {
          ...board,
          from_ts: new Date(form.get("start")).getTime() / 1000,
          to_ts: new Date(form.get("end")).getTime() / 1000,
        };
      board = await api("studio/validate", "POST", candidate);
      if (editing) dirty = true;
      d.close();
      render();
      refresh();
    });
  }
  async function openLibrary() {
    const preferences = await api("studio/preferences");
    boards = await api("studio/boards");
    const d = modal(
      "내 대시보드",
      `<p class="helper">현재 Airflow 계정의 구성입니다. 마지막으로 연 대시보드는 다음 접속에도 표시됩니다.</p><div class="library-actions"><input id="board-search" aria-label="대시보드 검색" placeholder="이름·카테고리로 검색"><button id="board-new" class="primary" ${!state.config.can_edit_dashboard ? "disabled" : ""}>새 대시보드</button></div><div id="board-list"></div>${state.config.can_edit ? `<details class="legacy-archive"><summary>기존 공용 구성 · 관리자</summary><p class="helper">계정별 저장 이전의 보관본입니다. 가져오면 대시보드·저장 이력·패널 템플릿을 내 계정에 복사합니다.</p><button id="shared-import" ${preferences.legacy_imported ? "disabled" : ""}>${preferences.legacy_imported ? "내 계정으로 가져오기 완료" : "공용 구성 가져오기"}</button> <button id="shared-export">공용 구성 JSON 내려받기</button> <button id="legacy-export">이전 v2 보드 JSON 내려받기</button></details>` : ""}`,
      true,
    );
    function list() {
      const text = $("#board-search", d).value.toLowerCase();
      $("#board-list", d).innerHTML = boards
        .filter((b) =>
          (b.title + " " + b.category).toLowerCase().includes(text),
        )
        .map(
          (b) =>
            `<button class="board-tile" data-board="${b.id}"><span class="eyebrow">${esc(b.category)}</span><strong>${esc(b.title)}</strong><span>${b.panels.length}개 패널 · v${b.version}</span></button>`,
        )
        .join("");
      bind(d, "[data-board]", "click", async (_, el) => {
        if (dirty && !confirm("저장하지 않은 변경을 버리고 이동할까요?"))
          return;
        await api("studio/preferences", "PUT", {
          last_board_id: el.dataset.board,
        });
        refreshSerial++;
        board = clone(boards.find((b) => b.id === el.dataset.board));
        frames = {};
        dirty = false;
        editing = false;
        d.close();
        render();
        refresh();
      });
    }
    list();
    bind(d, "#board-search", "input", list);
    bind(d, "#board-new", "click", async () => {
      if (dirty && !confirm("현재 변경을 버리고 새 대시보드를 만들까요?"))
        return;
      board = await api("studio/validate", "POST", {
        title: "새 대시보드",
        category: "운영",
        panels: [],
      });
      frames = {};
      dirty = true;
      editing = true;
      d.close();
      render();
      settings();
    });
    bind(d, "#legacy-export", "click", async () =>
      download("workbench-legacy-boards.json", await api("boards")),
    );
    bind(d, "#shared-export", "click", async () =>
      download("workbench-shared-studio.json", await api("studio/legacy")),
    );
    bind(d, "#shared-import", "click", async (_, el) => {
      el.disabled = true;
      try {
        const result = await api("studio/legacy/import", "POST");
        boards = await api("studio/boards");
        list();
        const count = result.boards.length;
        el.textContent =
          count || result.templates
            ? "내 계정으로 가져오기 완료"
            : "가져올 공용 구성이 없습니다";
        toast(
          `대시보드 ${count}개 · 패널 템플릿 ${result.templates}개를 내 계정에 복사했습니다.`,
        );
      } catch (e) {
        el.disabled = false;
        throw e;
      }
    });
  }
  async function settings() {
    const d = modal(
      "대시보드 설정",
      `<form id="board-form"><div class="fields">${input("title", "이름", board.title, "text", "required maxlength='100'")}${input("category", "카테고리", board.category, "text", "required maxlength='100'")}</div>${input("description", "설명", board.description, "text", "maxlength='1000'")}<div class="fields">${select(
        "bucket_seconds",
        "시간 집계 간격",
        [
          [60, "1분"],
          [300, "5분"],
          [900, "15분"],
          [3600, "1시간"],
          [21600, "6시간"],
          [86400, "1일"],
        ],
        board.bucket_seconds,
      )}</div><label>대시보드 변수 · JSON<textarea name="variables" rows="4" spellcheck="false">${esc(JSON.stringify(board.variables, null, 2))}</textarea></label><p class="helper">필터 값과 PostgreSQL SQL에서 :변수명을 사용합니다. 예: {"dag":"model_lab"}</p><div class="actions"><button class="primary">적용</button><button type="button" id="board-export">JSON 내보내기</button><label class="file-label">JSON 가져오기<input type="file" id="board-import" accept="application/json,.json"></label><button type="button" id="revision-open">저장 이력</button></div></form>`,
      true,
    );
    bind(d, "form", "submit", async (e) => {
      e.preventDefault();
      const f = Object.fromEntries(new FormData(e.target));
      board = await api("studio/validate", "POST", {
        ...board,
        ...f,
        bucket_seconds: Number(f.bucket_seconds),
        variables: JSON.parse(f.variables),
      });
      dirty = true;
      editing = true;
      d.close();
      render();
      refresh();
    });
    bind(d, "#board-export", "click", () =>
      download(`dashboard-${board.id}.json`, board),
    );
    bind(d, "#board-import", "change", async (_, el) => {
      const file = el.files[0];
      if (!file) return;
      if (file.size > 240000) throw Error("JSON은 240KB 이하여야 합니다.");
      const candidate = JSON.parse(await file.text());
      board = await api("studio/validate", "POST", {
        ...candidate,
        id: board.id,
        version: board.version,
      });
      dirty = true;
      editing = true;
      d.close();
      render();
      refresh();
    });
    bind(d, "#revision-open", "click", async () => {
      const revisions = await api(`studio/boards/${board.id}/revisions`);
      const history = modal(
        "최근 저장 이력",
        revisions.length
          ? revisions
              .map(
                (r, i) =>
                  `<div class="revision-row"><span><strong>v${r.version}</strong> · ${esc(r.title)} · ${r.panels.length}개 패널</span><button data-restore="${i}">이 버전 불러오기</button></div>`,
              )
              .join("")
          : empty("이전 저장 이력이 없습니다."),
      );
      bind(history, "[data-restore]", "click", (_, el) => {
        board = {
          ...clone(revisions[Number(el.dataset.restore)]),
          version: board.version,
        };
        dirty = true;
        editing = true;
        history.close();
        d.close();
        render();
        refresh();
        toast("이전 버전을 불러왔습니다. 저장하면 새 버전으로 기록됩니다.");
      });
    });
  }
  async function openGallery() {
    const saved = await api("studio/templates");
    const d = modal(
      "패널 추가",
      `<p class="muted">시각화를 고른 뒤 데이터와 쿼리를 연결하세요.</p><div class="viz-gallery">${types.map(([id, label]) => `<button data-chart="${id}"><span class="viz-symbol">${{ line: "⌁", area: "◩", bar: "▥", stat: "42", gauge: "◔", pie: "◉", table: "▦", scatter: "⁙", heatmap: "▧" }[id]}</span><strong>${label}</strong></button>`).join("")}</div><h3>저장한 패널 템플릿</h3><div class="template-list">${saved.length ? saved.map((p, i) => `<button data-template="${i}">${esc(p.title)} <small>${esc(p.chart)}</small></button>`).join("") : empty("편집기에서 ‘템플릿 저장’으로 재사용할 패널을 남기세요.")}</div>`,
      true,
    );
    bind(d, "[data-chart]", "click", async (_, el) => {
      const result = await api("studio/validate", "POST", {
        ...board,
        panels: [
          ...board.panels,
          {
            chart: el.dataset.chart,
            queries: [
              { builder: { group_by: ["state"], measures: [{ op: "count" }] } },
            ],
          },
        ],
      });
      d.close();
      openEditor(result.panels.at(-1), true);
    });
    bind(d, "[data-template]", "click", (_, el) => {
      const p = clone(saved[Number(el.dataset.template)]);
      p.id = crypto.randomUUID();
      p.x = 0;
      p.y = Math.max(0, ...board.panels.map((p) => p.y + p.h));
      d.close();
      openEditor(p, true);
    });
  }
  function inspectPanel(p) {
    const data = dataset(p, frames[p.id]);
    const d = modal(
      p.title + " · 데이터 확인",
      `<div class="actions"><button id="inspect-json">JSON 내려받기</button><span class="muted">${data.rows.length}행 · ${data.valid.length}개 쿼리</span></div><div id="inspect-table" class="inspect-table"></div><details><summary>쿼리 및 조회 상태</summary><pre class="code">${esc(JSON.stringify({ queries: p.queries, results: (frames[p.id] || []).map(({ rows, ...r }) => r) }, null, 2))}</pre></details>`,
      true,
    );
    renderTable($("#inspect-table", d), data.rows, [
      "__query",
      ...data.columns,
    ]);
    bind(d, "#inspect-json", "click", () =>
      download(`panel-${p.id}.json`, { panel: p, frames: frames[p.id] }),
    );
  }
  function sourceDialog() {
    const d = modal(
      "데이터소스",
      `<div class="source-cards">${sources.map((s) => `<article><span class="pill ${s.error ? "" : "online"}">${s.error ? "연결 대기" : "사용 가능"}</span><h3>${esc(s.name)}</h3><p>${esc(s.dialect)}</p><small>${esc(s.description || s.error || "전용 SELECT 계정으로 PostgreSQL을 직접 조회합니다.")}</small></article>`).join("")}</div><details><summary>PostgreSQL 연결 등록</summary><p class="helper">Airflow Connections의 전용 읽기 계정을 사용합니다. 비밀번호를 브라우저에 전달하지 않습니다.</p><form id="pg-source-form"><div class="fields">${input("id", "소스 ID", "pg_airflow", "text", "required")}${input("name", "표시 이름", "Airflow PostgreSQL 직접 조회", "text", "required")}${input("category", "카테고리", "파이프라인", "text", "required")}${input("connection_id", "Airflow Connection", "workbench_ro_airflow", "text", "required")}</div><label>조회 테이블·뷰 · 줄마다 하나<textarea name="tables" rows="5">workbench_monitor.dag_runs\nworkbench_monitor.task_instances\nworkbench_monitor.dags\nworkbench_monitor.dag_tags\nworkbench_monitor.pools</textarea></label><button class="primary" ${!state.config.can_edit ? "disabled" : ""}>연결 검사 후 등록</button><p id="pg-source-status" class="helper"></p></form></details>`,
      true,
    );
    bind(d, "form", "submit", async (e) => {
      e.preventDefault();
      const spec = Object.fromEntries(new FormData(e.target));
      spec.tables = spec.tables
        .split(/\r?\n/)
        .map((s) => s.trim())
        .filter(Boolean);
      try {
        $("#pg-source-status", d).textContent = "연결과 SELECT 권한 확인 중…";
        await api("datasources", "POST", spec);
        sources = await api("studio/catalog");
        d.close();
        sourceDialog();
      } catch (e) {
        $("#pg-source-status", d).textContent = e.message;
      }
    });
  }
  function openEditor(original, isNew = false) {
    clearTimeout(timer);
    refreshSerial++;
    disposeAll();
    if (grid) {
      grid.destroy(false);
      grid = null;
    }
    const p = clone(original);
    if (isNew) {
      p.x = 0;
      p.y = Math.max(0, ...board.panels.map((x) => x.y + x.h));
    }
    editor = {
      panel: p,
      active: 0,
      results: clone(frames[p.id] || []),
      sqlEditor: null,
      serial: 0,
      previewTable: false,
      changed: false,
    };
    document.body.classList.add("panel-editing");
    $("#app").innerHTML =
      `<section class="panel-workspace"><header class="editor-heading"><div><button id="editor-back">← 대시보드로</button><span class="muted">${esc(board.title)} / 패널 편집</span></div><div><button id="template-save">템플릿 저장</button><button id="panel-apply" class="primary">패널 적용</button></div></header><div class="editor-layout"><div class="editor-main"><section class="editor-preview"><div class="preview-heading"><strong id="preview-title">${esc(p.title)}</strong><div><button id="preview-mode">데이터 테이블</button><span class="pill">LIVE PREVIEW</span></div></div><div id="panel-preview"></div><div id="preview-warning" class="helper"></div></section><section class="query-workspace"><div class="query-tabs"><div id="query-tabs"></div><button id="query-add">＋ 쿼리</button><button id="query-run" class="primary">쿼리 실행</button></div><div id="query-controls"></div><div class="query-status-row"><span id="query-status">데이터와 쿼리를 설정한 뒤 실행하세요.</span><span>${esc(board.from_ts ? "직접 지정 기간" : `최근 ${board.hours}시간`)}</span></div></section></div><aside class="visual-options"><form id="visual-form"><h3>패널</h3>${input("title", "제목", p.title, "text", "required maxlength='100'")}${input("description", "설명", p.description, "text", "maxlength='500'")}${select("chart", "시각화", types, p.chart)}<details open><summary>데이터 매핑</summary>${select("data_ref", "표시할 쿼리", [["all", "모든 쿼리"], ...p.queries.map((q) => [q.ref, `쿼리 ${q.ref}`])], p.visual.data_ref)}${input("x", "X / 시간 필드", p.visual.x, "text", 'list="result-fields"')}${input("y", "값 필드 · 여러 개는 쉼표로 구분", p.visual.y, "text", 'list="result-fields"')}${input("series", "시리즈 / 히트맵 Y 필드", p.visual.series, "text", 'list="result-fields"')}<datalist id="result-fields"></datalist>${select(
        "reduce",
        "숫자·게이지 집계",
        [
          ["last", "최근 값"],
          ["first", "첫 값"],
          ["sum", "합계"],
          ["avg", "평균"],
          ["min", "최솟값"],
          ["max", "최댓값"],
          ["count", "건수"],
        ],
        p.visual.reduce,
      )}</details><details open><summary>표현</summary><div class="fields">${input("unit", "단위", p.visual.unit)}${input("decimals", "소수 자릿수", p.visual.decimals, "number", "min='0' max='6' required")}${input("color", "기본 색상", p.visual.color, "color")}${input("line_width", "선 두께", p.visual.line_width, "number", "min='1' max='6' required")}${input("point_size", "포인트 크기", p.visual.point_size, "number", "min='0' max='20' required")}</div>${check("legend", "범례 표시 · 클릭으로 시리즈 숨기기", p.visual.legend)}${check("stack", "누적 시리즈", p.visual.stack)}${check("smooth", "곡선 보간", p.visual.smooth)}${check("zoom", "범위 확대·축소", p.visual.zoom)}${check("horizontal", "가로 막대", p.visual.horizontal)}${check("time_axis", "X축을 시간으로 해석", p.visual.time_axis)}</details><details><summary>축·임계값</summary><div class="fields">${input("axis_min", "Y축 최솟값 · 자동", p.visual.axis_min, "number", "step='any'")}${input("axis_max", "Y축 최댓값 · 자동", p.visual.axis_max, "number", "step='any'")}${input("minimum", "게이지 최솟값", p.visual.minimum, "number", "step='any' required")}${input("maximum", "게이지 최댓값", p.visual.maximum, "number", "step='any' required")}${input("threshold", "임계값", p.visual.threshold, "number", "step='any'")}${select(
        "threshold_mode",
        "경고 조건",
        [
          ["above", "이상"],
          ["below", "이하"],
        ],
        p.visual.threshold_mode,
      )}</div></details><details><summary>시리즈별 설정</summary><div id="series-overrides"></div><button type="button" id="override-add">＋ 시리즈 설정</button><p class="helper">범례에 표시된 이름으로 색상과 왼쪽·오른쪽 축을 지정합니다.</p></details><details><summary>고급 ECharts 옵션</summary><p class="helper">grid, xAxis, yAxis, legend, textStyle, color, backgroundColor, animation을 JSON으로 재정의합니다.</p><textarea name="advanced" rows="8" spellcheck="false">${esc(JSON.stringify(p.visual.advanced, null, 2))}</textarea></details></form></aside></div></section>`;
    const session = editor;
    function preview() {
      if (editor !== session) return;
      const columns = dataset(p, session.results).columns;
      $("#result-fields").innerHTML = columns
        .map((c) => `<option value="${esc(c)}">`)
        .join("");
      $("#preview-title").textContent = p.title;
      paint(
        $("#panel-preview"),
        session.previewTable ? { ...p, chart: "table" } : p,
        session.results,
      );
      const warnings = session.results.filter(
        (r) => r.error || r.truncated || r.limited,
      );
      $("#preview-warning").textContent = warnings
        .map(
          (r) =>
            `쿼리 ${r.ref}: ${r.error || (r.truncated ? `${r.total_rows}건 중 ${r.source_rows}건 표본` : "결과 행 제한")}`,
        )
        .join(" · ");
    }
    function readVisual() {
      const form = $("#visual-form");
      if (!form.reportValidity()) throw Error("패널 설정을 확인하세요.");
      const v = Object.fromEntries(new FormData(form));
      p.title = v.title;
      p.description = v.description;
      p.chart = v.chart;
      delete v.title;
      delete v.description;
      delete v.chart;
      for (const key of Object.keys(v))
        if (key.startsWith("override-")) delete v[key];
      for (const k of [
        "decimals",
        "minimum",
        "maximum",
        "line_width",
        "point_size",
      ])
        v[k] = Number(v[k]);
      for (const k of ["threshold", "axis_min", "axis_max"])
        v[k] = v[k] === "" ? null : Number(v[k]);
      for (const k of [
        "legend",
        "stack",
        "smooth",
        "zoom",
        "horizontal",
        "time_axis",
      ])
        v[k] = form.elements[k].checked;
      v.advanced = JSON.parse(v.advanced || "{}");
      v.overrides = p.visual.overrides;
      p.visual = v;
      session.changed = true;
    }
    function overrides() {
      const host = $("#series-overrides");
      host.innerHTML = p.visual.overrides
        .map(
          (v, i) =>
            `<div class="override-row" data-override="${i}">${input("override-name", "시리즈 이름", v.name)}<div class="fields">${input("override-color", "색상", v.color, "color")}${select(
              "override-axis",
              "축",
              [
                ["left", "왼쪽"],
                ["right", "오른쪽"],
              ],
              v.axis,
            )}</div><button type="button" data-drop-override="${i}">제거</button></div>`,
        )
        .join("");
      bind(
        host,
        "[data-override] input,[data-override] select",
        "change",
        (_, el) => {
          const row = el.closest("[data-override]"),
            v = p.visual.overrides[Number(row.dataset.override)];
          v.name = row.querySelector('[name="override-name"]').value;
          v.color = row.querySelector('[name="override-color"]').value;
          v.axis = row.querySelector('[name="override-axis"]').value;
          session.changed = true;
          preview();
        },
      );
      bind(host, "[data-drop-override]", "click", (_, el) => {
        p.visual.overrides.splice(Number(el.dataset.dropOverride), 1);
        overrides();
        preview();
      });
    }
    function tabs() {
      const el = $("#query-tabs");
      el.innerHTML = p.queries
        .map(
          (q, i) =>
            `<button class="${session.active === i ? "active" : ""}" data-query="${i}">${q.ref}<span>${esc(source(q.datasource)?.name || q.datasource)}</span>${q.enabled ? "" : " · 숨김"}</button>`,
        )
        .join("");
      bind(el, "[data-query]", "click", (_, b) => {
        session.active = Number(b.dataset.query);
        tabs();
        queryControls();
      });
      $("#query-add").disabled = p.queries.length >= 6;
      const selectEl = $('[name="data_ref"]');
      selectEl.innerHTML = [
        ["all", "모든 쿼리"],
        ...p.queries.map((q) => [q.ref, `쿼리 ${q.ref}`]),
      ]
        .map(
          ([id, label]) =>
            `<option value="${id}" ${id === p.visual.data_ref ? "selected" : ""}>${label}</option>`,
        )
        .join("");
    }
    function invalidate() {
      session.serial++;
      session.results = [];
      session.changed = true;
      $("#query-status").textContent =
        "쿼리가 변경되었습니다. 다시 실행하세요.";
      preview();
    }
    function queryControls() {
      session.sqlEditor?.destroy();
      session.sqlEditor = null;
      const q = p.queries[session.active],
        s = source(q.datasource),
        isSQL = s?.kind === "sql";
      const host = $("#query-controls");
      host.innerHTML = `<div class="query-source-row">${select(
        "datasource",
        "데이터소스",
        sources.map((s) => [s.id, `${s.category} / ${s.name}`]),
        q.datasource,
      )}${check("query-enabled", "활성", q.enabled)}<button id="query-delete" ${p.queries.length === 1 ? "disabled" : ""}>쿼리 삭제</button></div><p class="source-path">${esc(s?.dialect || q.datasource)}${s?.error ? ` · <span class="error-text">${esc(s.error)}</span>` : ""}</p>${isSQL ? '<div class="sql-workspace"><details class="sql-schema"><summary>테이블·컬럼</summary><div id="sql-schema"></div></details><div id="sql-editor"></div></div><p class="helper">Ctrl+Space 자동완성 · :from_ts, :to_ts (Unix 초), :bucket_seconds · 보드 변수는 :이름으로 바인딩</p>' : '<div id="builder-controls"></div>'}`;
      bind(host, '[name="datasource"]', "change", (_, el) => {
        q.datasource = el.value;
        const s = source(q.datasource);
        if (s?.kind === "builder")
          q.builder = {
            dataset: Object.keys(s.tables)[0],
            filters: [],
            group_by: [],
            bucket: "",
            measures: [],
            columns: [],
            sort: "",
            descending: false,
            limit: 2000,
          };
        else if (!q.sql)
          q.sql =
            "SELECT state, count(*) AS value\nFROM workbench_monitor.dag_runs\nWHERE run_ts BETWEEN :from_ts AND :to_ts\nGROUP BY state\nORDER BY state";
        invalidate();
        tabs();
        queryControls();
      });
      bind(host, '[name="query-enabled"]', "change", (_, el) => {
        q.enabled = el.checked;
        invalidate();
        tabs();
      });
      bind(host, "#query-delete", "click", () => {
        p.queries.splice(session.active, 1);
        session.active = Math.max(0, session.active - 1);
        if (!p.queries.some((q) => q.ref === p.visual.data_ref))
          p.visual.data_ref = "all";
        invalidate();
        tabs();
        queryControls();
      });
      if (isSQL) {
        const schema = Object.fromEntries(
          Object.entries(s.tables || {}).map(([k, cols]) => [
            k,
            cols.map((c) => c.split(" ")[0]),
          ]),
        );
        $("#sql-schema").innerHTML =
          Object.entries(schema)
            .map(
              ([name, cols]) =>
                `<strong>${esc(name)}</strong><p>${esc(cols.join(", "))}</p>`,
            )
            .join("") ||
          "<p>읽기 전용 Connection을 연결하면 실제 스키마를 불러옵니다.</p>";
        session.sqlEditor = WorkbenchLib.editor(
          $("#sql-editor"),
          q.sql,
          schema,
          (sql) => {
            q.sql = sql;
            invalidate();
          },
        );
      } else builderControls(q);
    }
    function builderControls(q) {
      const b = q.builder,
        fields = columnsFor(q),
        host = $("#builder-controls");
      host.innerHTML = `<div class="builder-row">${select("dataset", "데이터셋", Object.keys(source(q.datasource)?.tables || {}), b.dataset)}${input("group_by", "그룹 필드 · 쉼표로 구분", b.group_by.join(", "), "text", 'list="source-fields"')}${select("bucket", "시간 버킷", [["", "사용 안 함"], ...fields.filter((f) => f.endsWith("_ts") || ["created", "finished"].includes(f))], b.bucket)}</div><datalist id="source-fields">${fields.map((f) => `<option value="${esc(f)}">`).join("")}</datalist><div class="builder-section"><div class="builder-label"><strong>집계</strong><button type="button" id="measure-add">＋ 집계</button></div><div id="measure-list"></div></div><div class="builder-section"><div class="builder-label"><strong>필터</strong><button type="button" id="filter-add">＋ 필터</button></div><div id="filter-list"></div></div><details><summary>정렬·표시 필드</summary><div class="builder-row">${input("sort", "정렬 필드", b.sort, "text", 'list="source-fields"')}${check("descending", "내림차순", b.descending)}${input("limit", "최대 행", b.limit, "number", "min='1' max='2000'")}</div>${input("columns", "표시 필드 · 비워 두면 전체", b.columns.join(", "))}</details><p class="helper">조회 필드: ${esc(fields.join(", "))}<br>필터 값에 :변수명을 입력하면 상단 대시보드 변수와 연결됩니다. SQL 중간 DB를 만들지 않습니다.</p>`;
      const split = (v) =>
        v
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean);
      bind(host, '[name="dataset"]', "change", (_, el) => {
        q.builder = {
          dataset: el.value,
          filters: [],
          group_by: [],
          bucket: "",
          measures: [],
          columns: [],
          sort: "",
          descending: false,
          limit: 2000,
        };
        invalidate();
        builderControls(q);
      });
      for (const key of [
        "group_by",
        "columns",
        "sort",
        "bucket",
        "limit",
        "descending",
      ])
        bind(host, `[name="${key}"]`, "change", (_, el) => {
          b[key] = ["group_by", "columns"].includes(key)
            ? split(el.value)
            : key === "limit"
              ? Number(el.value)
              : key === "descending"
                ? el.checked
                : el.value;
          invalidate();
        });
      function measures() {
        const list = $("#measure-list");
        list.innerHTML =
          b.measures
            .map(
              (m, i) =>
                `<div class="measure-row" data-measure="${i}">${select("measure-op", "함수", aggregates, m.op)}${select("measure-field", "필드", [["", "—"], ...fields], m.field)}${input("measure-alias", "결과 이름", m.alias)}${m.op === "percent" ? input("measure-match", "일치 값", m.match) : ""}<button data-measure-remove="${i}" aria-label="집계 ${i + 1} 제거">×</button></div>`,
            )
            .join("") ||
          '<p class="helper">집계 없이 원본 행을 가져옵니다.</p>';
        bind(list, "input,select", "change", (_, el) => {
          const row = el.closest("[data-measure]"),
            m = b.measures[Number(row.dataset.measure)];
          m.op = row.querySelector('[name="measure-op"]').value;
          m.field = row.querySelector('[name="measure-field"]').value;
          m.alias = row.querySelector('[name="measure-alias"]').value;
          m.match =
            row.querySelector('[name="measure-match"]')?.value || m.match;
          invalidate();
          if (el.name === "measure-op") measures();
        });
        bind(list, "[data-measure-remove]", "click", (_, el) => {
          b.measures.splice(Number(el.dataset.measureRemove), 1);
          invalidate();
          measures();
        });
      }
      measures();
      function filters() {
        const list = $("#filter-list");
        list.innerHTML =
          b.filters
            .map(
              (f, i) =>
                `<div class="filter-row" data-filter="${i}">${select("filter-field", "필드", fields, f.field)}${select(
                  "filter-op",
                  "조건",
                  [
                    ["eq", "같음"],
                    ["ne", "다름"],
                    ["contains", "포함"],
                    ["in", "목록에 포함"],
                    ["gte", "이상"],
                    ["lte", "이하"],
                    ["not_null", "값 있음"],
                  ],
                  f.op,
                )}${input("filter-value", "값 / :대시보드변수", f.value)}<button data-filter-remove="${i}" aria-label="필터 ${i + 1} 제거">×</button></div>`,
            )
            .join("") ||
          '<p class="helper">선택한 기간의 모든 허용 레코드를 조회합니다.</p>';
        bind(list, "input,select", "change", (_, el) => {
          const row = el.closest("[data-filter]"),
            f = b.filters[Number(row.dataset.filter)];
          f.field = row.querySelector('[name="filter-field"]').value;
          f.op = row.querySelector('[name="filter-op"]').value;
          f.value = row.querySelector('[name="filter-value"]').value;
          invalidate();
        });
        bind(list, "[data-filter-remove]", "click", (_, el) => {
          b.filters.splice(Number(el.dataset.filterRemove), 1);
          invalidate();
          filters();
        });
      }
      filters();
      bind(host, "#measure-add", "click", () => {
        if (b.measures.length >= 6) throw Error("집계는 최대 6개입니다.");
        b.measures.push({
          op: "count",
          field: "",
          alias: b.measures.length ? `value_${b.measures.length + 1}` : "value",
          match: "success",
        });
        invalidate();
        measures();
      });
      bind(host, "#filter-add", "click", () => {
        if (b.filters.length >= 12) throw Error("필터는 최대 12개입니다.");
        b.filters.push({ field: fields[0], op: "eq", value: "" });
        invalidate();
        filters();
      });
    }
    function leave() {
      session.serial++;
      session.sqlEditor?.destroy();
      dispose($("#panel-preview"));
      editor = null;
      editing = true;
      render();
      refresh();
    }
    bind($("#app"), "#visual-form", "change", (e) => {
      if (e.target.name?.startsWith("override-")) return;
      try {
        readVisual();
        preview();
      } catch (e) {
        $("#preview-warning").textContent = e.message;
      }
    });
    on("#override-add", "click", () => {
      if (p.visual.overrides.length >= 20)
        throw Error("시리즈 설정은 최대 20개입니다.");
      p.visual.overrides.push({
        name: "",
        color: palette[p.visual.overrides.length % palette.length],
        axis: "left",
      });
      overrides();
    });
    on("#editor-back", "click", () => {
      if (session.changed && !confirm("적용하지 않은 패널 변경을 버릴까요?"))
        return;
      leave();
    });
    on("#preview-mode", "click", () => {
      session.previewTable = !session.previewTable;
      $("#preview-mode").textContent = session.previewTable
        ? "시각화 미리보기"
        : "데이터 테이블";
      preview();
    });
    on("#query-add", "click", () => {
      const ref = [..."ABCDEF"].find(
        (ref) => !p.queries.some((q) => q.ref === ref),
      );
      if (!ref) return;
      const q = clone(p.queries[session.active]);
      q.ref = ref;
      p.queries.push(q);
      session.active = p.queries.length - 1;
      invalidate();
      tabs();
      queryControls();
    });
    on("#query-run", "click", async () => {
      readVisual();
      const serial = ++session.serial;
      const requests = p.queries.filter((q) => q.enabled).map((q) => clone(q));
      const ctx = context();
      if (ctx.from_ts == null) {
        ctx.to_ts = Date.now() / 1000;
        ctx.from_ts = ctx.to_ts - ctx.hours * 3600;
      }
      $("#query-status").textContent = "쿼리 실행 중…";
      $("#query-run").disabled = true;
      try {
        const result = await Promise.all(
          requests.map(async (q) => {
            try {
              return await api("studio/query", "POST", { ...ctx, query: q });
            } catch (e) {
              return { ref: q.ref, error: e.message, rows: [], columns: [] };
            }
          }),
        );
        if (editor !== session || serial !== session.serial) return;
        session.results = result;
        preview();
        $("#query-status").textContent = result
          .map(
            (r) =>
              `${r.ref}: ${r.error ? "오류" : `${r.rows.length}행 · ${r.elapsed_ms}ms`}`,
          )
          .join(" / ");
      } finally {
        if (editor === session) $("#query-run").disabled = false;
      }
    });
    on("#panel-apply", "click", async () => {
      readVisual();
      const candidate = clone(board);
      if (isNew) {
        if (candidate.panels.length >= 40)
          throw Error("패널은 최대 40개입니다.");
        candidate.panels.push(p);
      } else
        candidate.panels[candidate.panels.findIndex((x) => x.id === p.id)] = p;
      board = await api("studio/validate", "POST", candidate);
      frames[p.id] = session.results;
      dirty = true;
      leave();
    });
    on("#template-save", "click", async () => {
      readVisual();
      await api("studio/templates", "POST", p);
      toast("패널 템플릿을 저장했습니다.");
    });
    overrides();
    tabs();
    queryControls();
    preview();
  }
  async function start() {
    WorkbenchScope.init({
      changed: () => {
        if (board?.follow_work_scope !== false) {
          frames = {};
          drawGrid();
        }
        return refresh();
      },
      refresh,
    });
    await WorkbenchScope.load();
    let preferences;
    [boards, sources, preferences] = await Promise.all([
      api("studio/boards"),
      api("studio/catalog"),
      api("studio/preferences"),
    ]);
    board = clone(
      boards.find((b) => b.id === preferences.last_board_id) ||
        boards.find((b) => b.id === "operations") ||
        boards[0],
    );
    render();
    await refresh();
  }
  window.addEventListener("beforeunload", (e) => {
    if (dirty || editor?.changed) {
      e.preventDefault();
      e.returnValue = "";
    }
  });
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) clearTimeout(timer);
    else if (board) schedule();
  });
  return { start };
})();
