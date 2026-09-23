async function init() {
  state.config = await api("model-lab/config");
  state.environmentId = state.config.environments[0]?.id || "";
  state.presets = await api("presets");
  await LabUI.start();
}
window.addEventListener("beforeunload", (e) => {
  if (state.busy) {
    e.preventDefault();
    e.returnValue = "";
  }
});
init().catch((e) => {
  $("#app").innerHTML =
    `<div class="notice">${esc(e.message)}</div><p><a target="_top" href="/">Airflow 홈으로 이동</a></p>`;
});
