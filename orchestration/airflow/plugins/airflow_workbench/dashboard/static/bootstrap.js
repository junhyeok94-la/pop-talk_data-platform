"use strict";
async function startDashboard() {
  try {
    state.config = await api("dashboard/config");
    await BoardUI.start();
  } catch (e) {
    const deferred = e.status === 503 || e.status === 429;
    if (deferred) WorkbenchScope.deferred(e);
    $("#app").innerHTML =
      '<div class="notice">' +
      esc(e.message) +
      '</div><div class="actions"><button id="dashboard-retry"' +
      (deferred ? " disabled" : "") +
      '>다시 불러오기</button><a target="_top" href="/">Airflow 홈으로 이동</a></div>';
    const button = $("#dashboard-retry");
    button.onclick = () => {
      button.disabled = true;
      startDashboard();
    };
    if (deferred)
      setTimeout(
        () => {
          button.disabled = false;
        },
        Math.max(1, Math.min(300, e.retryAfter || 30)) * 1000 + 10,
      );
  }
}
startDashboard();
