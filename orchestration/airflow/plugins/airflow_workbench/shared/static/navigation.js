"use strict";
(() => {
  let ready = false;
  window.addEventListener("message", (event) => {
    if (
      event.source === window.parent &&
      event.origin === location.origin &&
      event.data?.type === "workbench:host-ready"
    )
      ready = true;
  });
  document.addEventListener("click", (event) => {
    const anchor = event.target.closest?.('a[target="_top"]');
    if (
      !anchor ||
      !ready ||
      event.defaultPrevented ||
      event.button !== 0 ||
      event.ctrlKey ||
      event.metaKey ||
      event.shiftKey ||
      event.altKey
    )
      return;
    const url = new URL(anchor.href, location.href);
    if (url.origin !== location.origin) return;
    event.preventDefault();
    window.parent.postMessage(
      { type: "workbench:navigate", href: url.href },
      location.origin,
    );
  });
})();
