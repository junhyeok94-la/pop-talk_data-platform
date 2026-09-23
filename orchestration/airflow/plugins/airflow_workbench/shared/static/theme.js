"use strict";
(() => {
  const root = document.documentElement;
  const system = window.matchMedia("(prefers-color-scheme: dark)");
  const tokens = {
    bg: "bg",
    card: "bg-panel",
    surface: "bg-muted",
    "subtle-bg": "bg-subtle",
    "hover-bg": "bg-emphasized",
    line: "border",
    "input-border": "border-emphasized",
    text: "fg",
    muted: "fg-muted",
    accent: "brand-fg",
    "primary-bg": "brand-solid",
    "primary-text": "brand-contrast",
    "selected-bg": "brand-subtle",
    blue: "fg-info",
    red: "fg-error",
    amber: "fg-warning",
    success: "fg-success",
    "info-bg": "bg-info",
    "error-bg": "bg-error",
    "warning-bg": "bg-warning",
    "success-bg": "bg-success",
    focus: "brand-focus-ring",
  };
  let host = null;
  let signature = "";
  let pending = false;
  const colors = new Map();
  // The native plugin route embeds this view with allow-same-origin. Standalone
  // and cross-origin hosts use the OS preference without touching Airflow storage.
  try {
    if (window.parent !== window) host = window.parent.document.documentElement;
  } catch (_) {
    /* A cross-origin embedding cannot expose its resolved theme. */
  }

  function sync() {
    pending = false;
    const styles = host ? window.parent.getComputedStyle(host) : null;
    const declared =
      host?.getAttribute("data-theme") || host?.getAttribute("data-color-mode");
    const scheme = styles?.colorScheme;
    const mode = host?.classList.contains("dark")
      ? "dark"
      : host?.classList.contains("light")
        ? "light"
        : ["light", "dark"].includes(declared)
          ? declared
          : ["light", "dark"].includes(scheme)
            ? scheme
            : system.matches
              ? "dark"
              : "light";
    const values = Object.fromEntries(
      Object.entries(tokens).map(([name, token]) => [
        name,
        styles?.getPropertyValue(`--chakra-colors-${token}`).trim() || "",
      ]),
    );
    values["font-body"] =
      styles?.getPropertyValue("--chakra-fonts-body").trim() || "";
    const next = JSON.stringify([mode, values]);
    if (next === signature) return;
    signature = next;
    root.dataset.theme = mode;
    for (const [name, value] of Object.entries(values)) {
      if (value) root.style.setProperty(`--${name}`, value);
      else root.style.removeProperty(`--${name}`);
    }
    colors.clear();
    window.dispatchEvent(
      new CustomEvent("workbench-theme-change", { detail: { mode } }),
    );
  }
  function schedule() {
    if (!pending) {
      pending = true;
      window.requestAnimationFrame(sync);
    }
  }
  window.WorkbenchTheme = {
    get mode() {
      return root.dataset.theme;
    },
    color(name) {
      if (!colors.has(name)) {
        const value = getComputedStyle(root)
          .getPropertyValue(`--${name}`)
          .trim();
        // Chakra may use OKLCH. Resolve through the browser into sRGB for ECharts,
        // whose canvas renderer also parses colors itself (including gradients).
        const canvas = document.createElement("canvas");
        canvas.width = canvas.height = 1;
        const ctx = canvas.getContext("2d", { willReadFrequently: true });
        ctx.fillStyle = value;
        ctx.fillRect(0, 0, 1, 1);
        const [r, g, b, a] = ctx.getImageData(0, 0, 1, 1).data;
        colors.set(name, `rgba(${r},${g},${b},${a / 255})`);
      }
      return colors.get(name);
    },
  };
  if (host)
    new MutationObserver(schedule).observe(host, {
      attributes: true,
      attributeFilter: ["class", "style", "data-theme", "data-color-mode"],
    });
  system.addEventListener("change", schedule);
  window.addEventListener("pageshow", schedule);
  sync();
})();
