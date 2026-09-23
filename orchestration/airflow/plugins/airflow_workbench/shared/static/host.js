"use strict";
// Airflow's documented React plugin globals are shared with the host. This tiny
// adapter has no private React runtime and leaves feature assets inside the frame.
(() => {
  const React = globalThis.React;
  const { useNavigate } = globalThis.ReactRouterDOM;
  const base = new URL("workbench/", document.baseURI);
  const routes =
    /^\/(?:dags(?:\/[^?#]*)?|connections(?:\/[^?#]*)?|plugin\/workbench-(?:dashboard|monitoring|models))$/;
  for (const [name, page] of [
    ["내 대시보드", "dashboard"],
    ["운영 모니터링", "monitoring"],
    ["Model Lab", "models"],
  ]) {
    globalThis[name] = function WorkbenchHost() {
      const frame = React.useRef(null);
      const navigate = useNavigate();
      React.useEffect(() => {
        function receive(event) {
          if (
            event.origin !== base.origin ||
            event.source !== frame.current?.contentWindow
          )
            return;
          if (
            event.data?.type !== "workbench:navigate" ||
            typeof event.data.href !== "string"
          )
            return;
          let url;
          try {
            url = new URL(event.data.href, base);
          } catch {
            return;
          }
          if (url.origin !== base.origin || url.username || url.password)
            return;
          // The child sends native app routes (without deployment basename).
          if (url.pathname !== "/" && !routes.test(url.pathname)) return;
          navigate(url.pathname + url.search + url.hash);
        }
        window.addEventListener("message", receive);
        return () => window.removeEventListener("message", receive);
      }, [navigate]);
      return React.createElement("iframe", {
        ref: frame,
        src: new URL(page, base).href,
        title: name,
        // Outgoing native navigation uses the checked host route above, not
        // top-navigation permission. Popups support explicit original-view links.
        sandbox:
          "allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox allow-downloads",
        onLoad: () =>
          frame.current?.contentWindow?.postMessage(
            { type: "workbench:host-ready" },
            base.origin,
          ),
        style: {
          border: 0,
          display: "block",
          width: "100%",
          height: "100%",
          minHeight: 0,
          flex: "1 1 0%",
        },
      });
    };
  }
})();
