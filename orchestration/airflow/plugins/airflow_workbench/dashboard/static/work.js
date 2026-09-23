"use strict";
(() => {
  const { $, esc, api, badge, formatDate, table, guarded, toast } = WorkbenchUI;
  let data,
    dataScope = "",
    timer,
    loading = false,
    dialogOpen = false,
    pending = null;
  const dagUrl = (id) => `/dags/${encodeURIComponent(id)}`;
  const runUrl = (r) =>
    `${dagUrl(r.dag_id)}/runs/${encodeURIComponent(r.run_id)}`;
  const link = (r, label = "실행·로그 열기") =>
    `<a target="_top" href="${runUrl(r)}">${label} ↗</a>`;
  function dialog(title) {
    clearTimeout(timer);
    dialogOpen = true;
    const d = document.createElement("dialog");
    d.className = "work-dialog";
    d.innerHTML = `<header class="page-heading"><h2>${esc(title)}</h2><button type="button" aria-label="닫기">×</button></header><div class="work-dialog-body"></div>`;
    document.body.append(d);
    d.querySelector("button").onclick = () => d.close();
    d.addEventListener("close", () => {
      dialogOpen = false;
      d.remove();
      schedule();
    });
    d.showModal();
    return d;
  }
  function schedule() {
    clearTimeout(timer);
    if (!document.hidden && !dialogOpen && WorkbenchScope.seconds())
      timer = setTimeout(
        () => (WorkbenchScope.editing ? schedule() : refresh()),
        WorkbenchScope.intervalMs(),
      );
  }
  function closeAndRefresh(d) {
    // The native close event is queued. Refresh after it clears dialogOpen.
    if (d.open) {
      d.addEventListener("close", refresh, { once: true });
      d.close();
    } else refresh();
  }
  async function refresh() {
    if (loading || dialogOpen) return;
    if (!WorkbenchScope.canRefresh()) {
      schedule();
      return;
    }
    loading = true;
    const requestedScope = WorkbenchScope.scopeKey();
    let retryForScope = false;
    WorkbenchScope.busy(true);
    try {
      const result = await api("work/summary");
      if (requestedScope !== WorkbenchScope.scopeKey()) {
        retryForScope = true;
        return;
      }
      data = result;
      WorkbenchScope.update(data);
      dataScope = WorkbenchScope.scopeKey();
      WorkbenchScope.recovered(data);
      render();
      if (pending) await track();
    } catch (e) {
      if (
        dataScope !== WorkbenchScope.scopeKey() ||
        e.status === 401 ||
        e.status === 403
      )
        data = null;
      if (e.status === 503 || e.status === 429) WorkbenchScope.deferred(e);
      if (!data)
        $("#app").innerHTML =
          `<div class="notice">${esc(e.message)}</div><div class="actions"><button id="work-retry-load">다시 불러오기</button><button id="work-reconfigure">담당 범위 설정</button></div>`;
      else toast(e.message);
      $("#work-retry-load")?.addEventListener("click", refresh);
      $("#work-reconfigure")?.addEventListener("click", guarded(configure));
    } finally {
      loading = false;
      WorkbenchScope.busy(false);
      schedule();
      if (retryForScope) refresh();
    }
  }
  function renderRuns(rows, kind) {
    if (!rows.length) return '<div class="empty">표시할 실행이 없습니다.</div>';
    return `<ul class="work-list">${rows.map((r, i) => `<li><div class="work-item-heading"><strong>${esc(r.dag_id)}</strong>${badge(r.state)}</div><div class="mono work-run-id" title="${esc(r.run_id)}">${esc(r.run_id)}</div><div class="work-item-footer"><time>${esc(formatDate(r.run_after))}</time><div class="work-actions">${link(r)}${r.state === "failed" ? `<button data-retry="${kind}:${i}">재실행 대상 확인</button>` : ""}</div></div></li>`).join("")}</ul>`;
  }
  function renderDags() {
    return `<ul class="work-list">${data.dags.map((d, i) => `<li><div class="work-item-heading"><a class="work-dag-title" target="_top" href="${dagUrl(d.dag_id)}">${esc(d.dag_display_name || d.dag_id)}</a><span class="work-operation ${d.is_paused ? "paused" : ""}">${d.is_paused ? "일시중지" : "활성"}</span></div><div class="work-dag-status"><span>조건 내 최근 실행 ${d.latest ? badge(d.latest.state) : "없음"}</span><span>기간 내 마지막 성공 · ${esc(formatDate(d.last_success))}</span></div><div class="work-actions"><a target="_top" href="${dagUrl(d.dag_id)}">DAG 상세·새 실행 ↗</a><button data-dag-state="${i}" aria-label="${esc(d.dag_id)} ${d.is_paused ? "활성화" : "일시중지"}">${d.is_paused ? "활성화" : "일시중지"}</button></div></li>`).join("")}</ul>`;
  }
  function render() {
    const p = data.profile,
      c = data.counts;
    $("#app").innerHTML = `<div id="work-pending"></div>${
      data.dags.length
        ? `<div class="work-caption"><span>담당 DAG ${data.dags.length}개 · 최근 ${p.hours >= 24 ? p.hours / 24 + "일" : p.hours + "시간"} 및 진행 중 실행 · ${p.long_running_minutes}분 이상 실행 시 주의 표시</span><span>${esc(formatDate(data.as_of))} 기준 · ${p.refresh_seconds ? p.refresh_seconds + "초 자동 갱신" : "자동 갱신 꺼짐"}</span></div><div class="work-stats"><div class="work-stat failed"><span>실패한 실행</span><strong>${c.failed}</strong></div><div class="work-stat"><span>실행 중</span><strong>${c.running}</strong></div><div class="work-stat"><span>대기 중</span><strong>${c.queued}</strong></div><div class="work-stat"><span>성공한 실행</span><strong>${c.success}</strong></div></div><div class="work-panels"><section class="work-section work-attention" aria-label="주의가 필요한 실행"><h2>주의가 필요한 실행 <small>${data.attention.length}${data.attention_limited ? "+" : ""}</small></h2><div class="work-panel-body">${data.attention.length ? renderRuns(data.attention, "attention") : '<div class="empty">선택한 범위에 실패하거나 오래 실행 중인 작업이 없습니다.</div>'}</div>${data.attention_limited ? '<p class="work-note">최근 100개를 표시합니다. 조회 기간이나 담당 범위를 좁혀 주세요.</p>' : ""}</section><section class="work-section work-assigned" aria-label="담당 DAG"><h2>담당 DAG <small>${data.dags.length}</small></h2><div class="work-panel-body">${renderDags()}</div></section><section class="work-section work-recent" aria-label="최근 실행"><h2>최근 실행 <small>최대 30개</small></h2><div class="work-panel-body">${renderRuns(data.recent, "recent")}</div></section></div>`
        : `<section class="work-section work-empty"><h2>${data.assigned_count ? "조회 조건에 맞는 담당 DAG가 없습니다" : "담당 DAG를 선택해 시작하세요"}</h2><p>${data.assigned_count ? "검색 필터를 초기화하거나 검색어·DAG 운영 상태를 바꿔 주세요." : "관심 DAG를 직접 고르거나 업무 태그를 등록하면 상태와 실패 실행을 모아서 보여드립니다."}</p><button class="primary" id="work-get-started">담당 범위 설정</button><p class="work-note">접근 가능한 DAG만 표시하며, 선택 설정은 현재 Airflow 계정에 저장됩니다.</p></section>`
    }`;
    $("#work-get-started")?.addEventListener("click", guarded(configure));
    document.querySelectorAll("[data-retry]").forEach(
      (b) =>
        (b.onclick = () => {
          const [kind, i] = b.dataset.retry.split(":");
          preview(data[kind][Number(i)]);
        }),
    );
    if (pending) renderPending();
    document
      .querySelectorAll("[data-dag-state]")
      .forEach(
        (b) =>
          (b.onclick = () =>
            changeDagState(data.dags[Number(b.dataset.dagState)])),
      );
  }
  async function changeDagState(dag) {
    const d = dialog("DAG 운영 상태 변경"),
      body = $(".work-dialog-body", d);
    body.innerHTML =
      '<div class="empty">현재 상태와 변경 권한을 확인하고 있습니다…</div>';
    try {
      const current = await api(
        "work/dag-control?dag_id=" + encodeURIComponent(dag.dag_id),
      );
      if (!d.open) return;
      const blocked = !current.can_edit
        ? "이 DAG를 변경할 권한이 없습니다."
        : current.is_stale
            ? "DAG 파일이 없거나 더 이상 파싱되지 않는 상태입니다. 파일 배포를 먼저 확인하세요."
            : current.is_paused && current.has_import_errors
              ? "DAG 파싱 오류를 먼저 해결하세요."
              : "";
      const action = current.is_paused ? "활성화" : "일시중지";
      body.innerHTML = `<p><strong>${esc(dag.dag_id)}</strong></p><p>현재 상태 · ${current.is_paused ? "일시중지" : "활성"}</p><p class="work-note">스케줄 · ${esc(current.timetable || "설정된 스케줄 없음")}<br>다음 예약 시각 · ${esc(formatDate(current.next_run))}</p>${blocked ? `<p class="notice">${esc(blocked)}</p>` : `<p class="notice">${current.is_paused ? "일시중지를 해제합니다. 스케줄·catchup·대기 중인 실행에 따라 작업이 시작될 수 있습니다. 실패 태스크를 재실행하는 것과는 별도 작업입니다." : "새로운 실행의 스케줄링을 일시중지합니다. 이미 실행 중인 태스크를 강제 종료하는 기능은 아닙니다."}</p>`}<p id="work-state-error" class="work-error" role="alert"></p><div class="actions"><a target="_top" href="${dagUrl(dag.dag_id)}">DAG 상세 열기 ↗</a>${current.model_lab ? '<a target="_top" href="/plugin/workbench-models">Model Lab 열기 ↗</a>' : ""}${blocked ? "" : `<button id="work-state-confirm" class="primary">${action} 확인</button>`}</div>`;
      if (current.model_lab) {
        const note = document.createElement("p");
        note.className = "work-note";
        note.textContent = "DAG 운영 상태와 Model Lab 평가·학습 준비 상태는 별도입니다. 실행 요청 시 연결·DAG·Pool 설정을 다시 확인합니다.";
        $(".actions", body).before(note);
      }
      $("#work-state-confirm", d)?.addEventListener("click", async () => {
        const button = $("#work-state-confirm", d);
        button.disabled = true;
        try {
          const changed = await api("work/dag-state", "PUT", {
            dag_id: dag.dag_id,
            is_paused: !current.is_paused,
            expected_is_paused: current.is_paused,
          });
          closeAndRefresh(d);
          toast(
            changed.is_paused
              ? "DAG를 일시중지했습니다."
              : "DAG를 활성화했습니다.",
          );
        } catch (e) {
          $("#work-state-error", d).textContent =
            e.message + " 현재 상태를 다시 확인한 뒤 요청하세요.";
        }
      });
    } catch (e) {
      body.innerHTML = `<p class="work-error">${esc(e.message)}</p>`;
    }
  }
  async function configure() {
    const d = dialog("담당 범위 설정"),
      body = $(".work-dialog-body", d),
      p = data?.profile || (await api("work/profile"));
    let chosen = new Set(p.dag_ids),
      serial = 0;
    body.innerHTML = `<form id="work-profile-form"><p class="work-note">직접 선택한 DAG와 등록한 태그에 속하는 DAG를 함께 표시합니다. 태그가 같아도 Airflow 조회 권한이 없는 DAG는 표시하지 않습니다.</p><label>업무 태그 · 쉼표로 구분<input name="tags" value="${esc(p.tags.join(", "))}" placeholder="team:analytics, domain:orders"></label><div class="actions"><label class="work-period">조회 기간<select name="hours"><option value="24" ${p.hours === 24 ? "selected" : ""}>최근 1일</option><option value="168" ${p.hours === 168 ? "selected" : ""}>최근 7일</option><option value="720" ${p.hours === 720 ? "selected" : ""}>최근 30일</option></select></label><label>오래 실행 중인 작업 기준 · 분<input name="long" type="number" min="5" max="10080" value="${p.long_running_minutes}" required></label></div><div class="actions"><input id="work-search" type="search" aria-label="DAG 또는 태그 검색" placeholder="DAG 이름 또는 태그 검색"><button type="button" id="work-search-button">검색</button></div><div id="work-chosen"></div><div id="work-options" class="work-select-list"></div><p id="work-list-help" class="work-note"></p><p id="work-profile-error" class="work-error" role="alert"></p><div class="actions"><button type="submit" class="primary" id="work-profile-save">담당 범위 저장</button><button type="button" id="work-clear-choice">직접 선택 해제</button></div></form>`;
    function chosenLabel() {
      const root = $("#work-chosen", d);
      root.innerHTML = `<p class="work-note">직접 선택 ${chosen.size}개</p><div class="actions">${[...chosen].map((id, i) => `<button type="button" data-unchoose="${i}" title="선택 해제">${esc(id)} ×</button>`).join("")}</div>`;
      root.querySelectorAll("[data-unchoose]").forEach(
        (b) =>
          (b.onclick = () => {
            chosen.delete([...chosen][Number(b.dataset.unchoose)]);
            chosenLabel();
            d.querySelectorAll("[data-dag]").forEach(
              (c) => (c.checked = chosen.has(c.dataset.dag)),
            );
          }),
      );
    }
    async function search() {
      const ticket = ++serial;
      try {
        const result = await api(
          "work/catalog?q=" + encodeURIComponent($("#work-search", d).value),
        );
        if (ticket !== serial || !d.open) return;
        $("#work-options", d).innerHTML = result.dags.length
          ? result.dags
              .map(
                (x) =>
                  `<label class="work-select-row"><input data-dag="${esc(x.dag_id)}" type="checkbox" ${chosen.has(x.dag_id) ? "checked" : ""}><span>${esc(x.dag_display_name || x.dag_id)}<small>${esc(x.dag_id)} · ${esc(x.tags.join(" · "))}${x.is_paused ? " · 일시중지" : ""}</small></span></label>`,
              )
              .join("")
          : '<div class="empty">조회 가능한 DAG가 없습니다.</div>';
        $("#work-list-help", d).textContent = result.limited
          ? "검색 결과가 많아 100개까지 표시합니다. 검색어를 좁혀 주세요."
          : "직접 선택과 태그는 합집합으로 적용됩니다.";
        d.querySelectorAll("[data-dag]").forEach(
          (c) =>
            (c.onchange = () => {
              c.checked
                ? chosen.add(c.dataset.dag)
                : chosen.delete(c.dataset.dag);
              chosenLabel();
            }),
        );
      } catch (e) {
        $("#work-profile-error", d).textContent = e.message;
      }
    }
    $("#work-search-button", d).onclick = search;
    $("#work-search", d).onkeydown = (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        search();
      }
    };
    $("#work-clear-choice", d).onclick = () => {
      chosen.clear();
      chosenLabel();
      d.querySelectorAll("[data-dag]").forEach((c) => (c.checked = false));
    };
    $("#work-profile-form", d).onsubmit = async (e) => {
      e.preventDefault();
      const form = e.currentTarget;
      $("#work-profile-save", d).disabled = true;
      try {
        await api("work/profile", "PUT", {
          ...p,
          dag_ids: [...chosen],
          tags: form.elements.tags.value
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean),
          hours: Number(form.elements.hours.value),
          long_running_minutes: Number(form.elements.long.value),
        });
        closeAndRefresh(d);
      } catch (err) {
        $("#work-profile-error", d).textContent = err.message;
        $("#work-profile-save", d).disabled = false;
      }
    };
    chosenLabel();
    await search();
  }
  async function preview(run) {
    const d = dialog("실패 작업 재실행"),
      body = $(".work-dialog-body", d);
    body.innerHTML =
      '<div class="empty">현재 권한과 재실행 대상을 확인하고 있습니다…</div>';
    try {
      const result = await api("work/retry-preview", "POST", {
        dag_id: run.dag_id,
        run_id: run.run_id,
      });
      if (!d.open) return;
      body.innerHTML = `<p><strong>${esc(run.dag_id)}</strong></p><p class="mono">${esc(run.run_id)}</p><p class="notice">선택한 기존 실행에서 실패 및 상위 작업 실패로 대기한 태스크 ${result.tasks.length}개를 다시 실행합니다. 성공한 태스크·다른 날짜의 실행은 유지하며 기존 DAG 버전을 사용합니다.</p>${table(
        ["태스크", "매핑 인덱스", "현재 상태", "시도"],
        result.tasks.map((t) => [
          esc(t.task_id),
          String(t.map_index),
          badge(t.state),
          String(t.try_number),
        ]),
      )}<label>작업 메모<input id="work-retry-note" maxlength="500" value="내 대시보드에서 실패 작업 재실행"></label><p class="work-note">대상 확인은 5분간 유효합니다. 확인 이후 상태가 바뀌면 다시 미리보기 해야 합니다.</p><p id="work-retry-error" class="work-error" role="alert"></p><div class="actions">${link(run)}<button id="work-confirm-retry" class="primary">${result.tasks.length}개 태스크 재실행 요청</button></div>`;
      $("#work-confirm-retry", d).onclick = async () => {
        const button = $("#work-confirm-retry", d);
        button.disabled = true;
        try {
          await api("work/retry", "POST", {
            preview_id: result.preview_id,
            note: $("#work-retry-note", d).value,
          });
          pending = { ...run, state: "queued" };
          closeAndRefresh(d);
          toast("재실행을 요청했습니다. Airflow 실행 상태를 확인합니다.");
        } catch (e) {
          $("#work-retry-error", d).textContent =
            e.message +
            " 원본 실행 상태를 확인한 뒤 필요하면 미리보기를 다시 열어 주세요.";
        }
      };
    } catch (e) {
      body.innerHTML = `<p class="work-error">${esc(e.message)}</p>${link(run)}<p class="work-note">재실행에는 해당 DAG의 태스크 변경 권한이 필요합니다. Model Lab 작업은 Model Lab에서 다시 요청합니다.</p>`;
    }
  }
  function renderPending() {
    const root = $("#work-pending");
    if (root && pending)
      root.innerHTML = `<div class="work-pending"><div class="actions"><strong>요청한 실행 추적</strong>${badge(pending.state)}${link(pending)}<button id="work-dismiss">닫기</button></div><p class="mono">${esc(pending.dag_id)} / ${esc(pending.run_id)}</p></div>`;
    $("#work-dismiss")?.addEventListener("click", () => {
      pending = null;
      root.replaceChildren();
      schedule();
    });
  }
  async function track() {
    try {
      const state = await api(
        "work/run-status?dag_id=" +
          encodeURIComponent(pending.dag_id) +
          "&run_id=" +
          encodeURIComponent(pending.run_id),
      );
      pending.state = state.state;
      renderPending();
      if (["success", "failed"].includes(state.state)) {
        toast(
          state.state === "success"
            ? "재실행이 완료되었습니다."
            : "재실행이 실패했습니다. 실행 로그를 확인하세요.",
        );
        pending = null;
      }
    } catch (e) {
      toast(e.message);
      pending = null;
    }
  }
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) clearTimeout(timer);
    else schedule();
  });
  WorkbenchScope.init({
    changed: () => {
      if (dataScope !== WorkbenchScope.scopeKey()) {
        data = null;
        $("#app").innerHTML =
          '<div class="empty">변경한 조회 조건의 결과를 불러오는 중입니다.</div>';
      }
      return refresh();
    },
    refresh,
    configure,
  });
  refresh().then(() => {
    if (new URL(location.href).searchParams.get("configure") === "1")
      guarded(configure)();
  });
})();
