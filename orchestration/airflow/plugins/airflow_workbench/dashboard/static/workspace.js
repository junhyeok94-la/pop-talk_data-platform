"use strict";
// Account scope and view filters shared by both dashboard tabs.
const WorkbenchScope = (() => {
  const { $, esc, api, guarded, toast } = WorkbenchUI;
  const refreshIcon =
    '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20 7v5h-5M4 17v-5h5M6.1 7a7 7 0 0 1 11.6-1L20 9M4 15l2.3 3A7 7 0 0 0 17.9 17"/></svg>';
  const resetIcon =
    '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m6 6 12 12M18 6 6 18"/></svg>';
  let value,
    changed = () => {},
    manual = () => {},
    configure,
    draft = false,
    saving = false,
    shared = true,
    retryUntil = 0,
    resumeTimer,
    protectionMessage = "";
  const profile = () => value?.profile;
  const seconds = () => profile()?.refresh_seconds ?? 30;
  function options(entries, current) {
    return entries
      .map(
        ([v, label]) =>
          `<option value="${v}" ${String(current) === String(v) ? "selected" : ""}>${label}</option>`,
      )
      .join("");
  }
  function controls() {
    return `<div class="scope-refresh"><button type="button" id="scope-auto" role="switch" aria-label="자동 갱신" aria-checked="${Boolean(seconds())}" title="자동 갱신 ${seconds() ? "끄기" : "켜기"}"><span class="scope-switch-track" aria-hidden="true"><span></span></span><span>자동</span></button><select id="scope-interval" aria-label="자동 갱신 간격" ${seconds() ? "" : "disabled"}>${options(
      [
        [30, "30초"],
        [60, "1분"],
        [300, "5분"],
      ],
      seconds() || 30,
    )}</select><button type="button" id="scope-refresh" class="scope-icon" aria-label="새로고침" title="새로고침">${refreshIcon}</button></div>`;
  }
  function render() {
    const host = $("#scope-bar"),
      p = profile();
    if (!host || !p) return;
    host.innerHTML = `<div class="scope-heading"><div><strong>${shared ? "공통 조회 범위·조건" : "별도 분석 범위"}</strong><span class="scope-caption">${shared ? `${value.assigned_count}개 담당 DAG 중 ${value.dags.length}개 조회 · 두 탭에 동일 적용` : "담당 DAG·검색·상태·기간 필터를 적용하지 않습니다. 쿼리별 권한은 유지됩니다."}</span></div><div class="scope-actions">${configure ? '<button id="scope-config" class="scope-config-link" type="button">담당 범위 설정</button>' : '<a class="scope-config-link" href="dashboard?configure=1">담당 범위 설정</a>'}${controls()}</div></div>${
      shared
        ? `<form id="scope-filters"><label class="scope-search">DAG 검색<input name="search" type="search" maxlength="100" value="${esc(p.search)}" placeholder="이름 · 태그 · 소유자 (공백으로 함께 검색)"></label><label>실행 상태<select name="run_state">${options(
            [
              ["", "모든 실행"],
              ["failed", "실패"],
              ["running", "실행 중"],
              ["queued", "대기"],
              ["success", "성공"],
            ],
            p.run_state,
          )}</select></label><label>DAG 운영 상태<select name="paused">${options(
            [
              ["all", "전체"],
              ["active", "활성"],
              ["paused", "일시중지"],
            ],
            p.paused,
          )}</select></label><label>조회 기간<select name="hours">${options([[1, "최근 1시간"], [6, "최근 6시간"], [24, "최근 1일"], [168, "최근 7일"], [720, "최근 30일"], ...([1, 6, 24, 168, 720].includes(p.hours) ? [] : [[p.hours, `최근 ${p.hours}시간`]])], p.hours)}</select></label><button type="submit">적용</button><button type="button" id="scope-reset" class="scope-icon" aria-label="검색 필터 초기화" title="검색·상태 필터 초기화">${resetIcon}</button></form><p class="scope-help" id="scope-help">검색과 상태는 담당 범위를 좁힙니다. 실행 상태는 실행 목록·집계에 적용합니다.</p>`
        : ""
    }<p class="scope-error" id="scope-error" role="alert"></p>`;
    showProtection();
    busy(false);
    const form = $("#scope-filters");
    form?.addEventListener("input", () => {
      draft = true;
      $("#scope-help").textContent =
        "필터를 변경했습니다. 적용 또는 Enter로 두 탭에 반영합니다.";
    });
    form?.addEventListener("change", () => {
      draft = true;
    });
    form?.addEventListener(
      "submit",
      guarded(async (e) => {
        e.preventDefault();
        await commit({
          search: form.elements.search.value.trim(),
          run_state: form.elements.run_state.value,
          paused: form.elements.paused.value,
          hours: Number(form.elements.hours.value),
        });
      }),
    );
    $("#scope-reset")?.addEventListener(
      "click",
      guarded(() => commit({ search: "", run_state: "", paused: "all" })),
    );
    $("#scope-auto").onclick = guarded(() =>
      commit({ refresh_seconds: seconds() ? 0 : 30 }, true),
    );
    $("#scope-interval").onchange = guarded((e) =>
      commit({ refresh_seconds: Number(e.target.value) }, true),
    );
    $("#scope-refresh").onclick = guarded(
      () => Date.now() >= retryUntil && manual(),
    );
    if (configure) $("#scope-config").onclick = guarded(configure);
  }
  async function commit(patch, preserveDraft = false) {
    if (saving) return;
    const form = $("#scope-filters");
    const savedDraft =
      preserveDraft && draft && form
        ? Object.fromEntries(
            ["search", "run_state", "paused", "hours"].map((k) => [
              k,
              form.elements[k].value,
            ]),
          )
        : null;
    saving = true;
    $("#scope-bar").setAttribute("aria-busy", "true");
    try {
      await api("work/profile", "PUT", { ...profile(), ...patch });
      draft = false;
      await load();
      if (savedDraft) {
        for (const [k, v] of Object.entries(savedDraft))
          $("#scope-filters").elements[k].value = v;
        draft = true;
      }
      await changed();
    } catch (e) {
      $("#scope-error").textContent = e.message;
      if (e.status === 503 || e.status === 429) deferred(e);
      else {
        try {
          value = await api("work/scope");
        } catch {}
      }
      toast(e.message);
    } finally {
      saving = false;
      $("#scope-bar").removeAttribute("aria-busy");
    }
  }
  function update(next) {
    const version = profile()?.version,
      count = value?.dags.length;
    value = next;
    if (
      !draft &&
      (version !== profile().version ||
        count !== value.dags.length ||
        !$("#scope-bar")?.children.length)
    )
      render();
  }
  async function load() {
    update(await api("work/scope"));
    return value;
  }
  function init(opts) {
    changed = opts.changed;
    manual = opts.refresh;
    configure = opts.configure;
    if (value) render();
  }
  function mode(flag) {
    if (shared !== flag) {
      shared = flag;
      draft = false;
    }
    if (!draft) render();
  }
  function busy(flag) {
    const b = $("#scope-refresh");
    if (b) {
      b.disabled = flag || Date.now() < retryUntil;
      b.classList.toggle("is-refreshing", flag);
    }
  }
  function showProtection() {
    const host = $("#scope-bar");
    if (!host) return;
    let note = $("#scope-protection");
    if (!note) {
      note = document.createElement("p");
      note.id = "scope-protection";
      note.className = "scope-protection";
      note.setAttribute("role", "status");
      host.append(note);
    }
    note.hidden = !protectionMessage;
    note.textContent = protectionMessage;
  }
  function deferred(info) {
    const retry = Math.max(
      1,
      Math.min(300, info.retryAfter || info.retry_after || 30),
    );
    retryUntil = Math.max(retryUntil, Date.now() + retry * 1000);
    protectionMessage =
      "파이프라인 처리 우선 · 대시보드 갱신을 잠시 미룹니다. 표시된 값은 마지막 조회 결과입니다.";
    showProtection();
    busy(false);
    clearTimeout(resumeTimer);
    resumeTimer = setTimeout(() => busy(false), retryUntil - Date.now() + 10);
  }
  function recovered(info) {
    if (info?.stale) return deferred(info);
    retryUntil = 0;
    protectionMessage = "";
    clearTimeout(resumeTimer);
    showProtection();
  }
  function intervalMs() {
    const wait =
      retryUntil > Date.now() ? retryUntil - Date.now() : seconds() * 1000;
    return wait * (1 + Math.random() * 0.15);
  }
  function scopeKey() {
    if (!value) return "";
    const { version, refresh_seconds, ...filters } = profile();
    return JSON.stringify({
      filters,
      dags: value.dags.map((d) => d.dag_id).sort(),
    });
  }
  return {
    scopeKey,
    canRefresh: () => Date.now() >= retryUntil,
    deferred,
    recovered,
    intervalMs,
    init,
    update,
    load,
    profile,
    seconds,
    mode,
    busy,
    get data() {
      return value;
    },
    get editing() {
      return draft || saving;
    },
  };
})();
