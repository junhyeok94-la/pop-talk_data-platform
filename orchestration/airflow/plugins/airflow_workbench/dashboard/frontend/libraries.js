import * as echarts from "echarts/core";
import {
  LineChart,
  BarChart,
  PieChart,
  GaugeChart,
  ScatterChart,
  HeatmapChart,
} from "echarts/charts";
import {
  GridComponent,
  TooltipComponent,
  LegendComponent,
  DataZoomComponent,
  VisualMapComponent,
  MarkLineComponent,
  DatasetComponent,
  TitleComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import { GridStack } from "gridstack";
import "gridstack/dist/gridstack.min.css";
import { EditorView, basicSetup } from "codemirror";
import { sql, PostgreSQL } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import { Compartment } from "@codemirror/state";
echarts.use([
  LineChart,
  BarChart,
  PieChart,
  GaugeChart,
  ScatterChart,
  HeatmapChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
  DataZoomComponent,
  VisualMapComponent,
  MarkLineComponent,
  DatasetComponent,
  TitleComponent,
  CanvasRenderer,
]);
window.WorkbenchLib = {
  echarts,
  GridStack,
  editor(parent, value, schema, onChange) {
    const theme = new Compartment();
    const appearance = () => [
      EditorView.theme(
        {
          "&": { backgroundColor: "var(--bg)", color: "var(--text)" },
          ".cm-gutters": {
            backgroundColor: "var(--subtle-bg)",
            color: "var(--muted)",
            borderColor: "var(--line)",
          },
          ".cm-cursor, .cm-dropCursor": { borderLeftColor: "var(--text)" },
          ".cm-activeLine, .cm-activeLineGutter": {
            backgroundColor: "var(--surface)",
          },
          "&.cm-focused .cm-selectionBackground, .cm-selectionBackground, ::selection":
            { backgroundColor: "var(--selected-bg)" },
          ".cm-tooltip, .cm-panels": {
            backgroundColor: "var(--card)",
            color: "var(--text)",
            borderColor: "var(--line)",
          },
          ".cm-tooltip-autocomplete > ul > li[aria-selected]": {
            backgroundColor: "var(--selected-bg)",
            color: "var(--accent)",
          },
          ".cm-searchMatch": {
            backgroundColor: "var(--warning-bg)",
            outline: "1px solid var(--amber)",
          },
        },
        { dark: window.WorkbenchTheme.mode === "dark" },
      ),
      ...(window.WorkbenchTheme.mode === "dark" ? [oneDark] : []),
    ];
    const view = new EditorView({
      doc: value,
      parent,
      extensions: [
        basicSetup,
        theme.of(appearance()),
        sql({ dialect: PostgreSQL, schema }),
        EditorView.lineWrapping,
        EditorView.contentAttributes.of({
          "aria-label": "PostgreSQL SQL 편집기",
        }),
        EditorView.updateListener.of((update) => {
          if (update.docChanged) onChange(update.state.doc.toString());
        }),
      ],
    });
    const updateTheme = () =>
      view.dispatch({ effects: theme.reconfigure(appearance()) });
    window.addEventListener("workbench-theme-change", updateTheme);
    const destroy = view.destroy.bind(view);
    view.destroy = () => {
      window.removeEventListener("workbench-theme-change", updateTheme);
      destroy();
    };
    return view;
  },
};
