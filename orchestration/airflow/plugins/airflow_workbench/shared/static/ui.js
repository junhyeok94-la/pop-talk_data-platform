"use strict";
const WorkbenchUI = (() => {
  const $ = (s, root = document) => root.querySelector(s);
  const esc = (value) =>
    String(value ?? "").replace(
      /[&<>"']/g,
      (c) =>
        ({
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;",
        })[c],
    );
  const formatDate = (v) =>
    v
      ? new Date(typeof v === "number" ? v * 1000 : v).toLocaleString("ko-KR", {
          hour12: false,
        })
      : "—";
  const badge = (s) =>
    `<span class="badge ${["success", "failed", "interrupted", "running", "queued"].includes(s) ? s : ""}">${esc(s)}</span>`;
  const empty = (t) => `<div class="empty">${esc(t)}</div>`;
  let toastTimer;
  function toast(message) {
    const el = $("#toast");
    el.textContent = message;
    el.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => (el.hidden = true), 7000);
  }
  async function api(path, method = "GET", data) {
    const response = await fetch(`api/${path}`, {
      method,
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
        "X-Workbench-Request": "1",
      },
      ...(data !== undefined ? { body: JSON.stringify(data) } : {}),
    });
    let value;
    try {
      value = await response.json();
    } catch {
      throw new Error(
        "서버 응답을 읽을 수 없습니다. Airflow 로그인을 확인하세요.",
      );
    }
    if (!response.ok) {
      const d = value.detail;
      const error = new Error(
        response.status === 401
          ? "Airflow에서 다시 로그인하세요."
          : Array.isArray(d)
            ? d.map((e) => `${e.loc?.slice(1).join(".")}: ${e.msg}`).join("\n")
            : d && typeof d === "object"
              ? d.message || d.blockers?.join("\n") || JSON.stringify(d)
              : d || `요청 실패 (${response.status})`,
      );
      error.status = response.status;
      error.retryAfter = Number(
        response.headers.get("Retry-After") || d?.retry_after || 0,
      );
      error.reason = d?.reason;
      throw error;
    }
    return value;
  }
  function download(name, data) {
    const url = URL.createObjectURL(
      new Blob([JSON.stringify(data, null, 2)], { type: "application/json" }),
    );
    const a = document.createElement("a");
    a.href = url;
    a.download = name;
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  const guarded =
    (fn) =>
    async (...args) => {
      try {
        await fn(...args);
      } catch (e) {
        toast(e.message);
      }
    };
  function on(id, event, fn) {
    const el = $(id);
    if (el) el.addEventListener(event, guarded(fn));
  }
  function table(headers, rows) {
    return rows.length
      ? `<div class="table-scroll"><table><thead><tr>${headers.map((x) => `<th>${esc(x)}</th>`).join("")}</tr></thead><tbody>${rows.map((r) => `<tr>${r.map((x) => `<td>${x}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`
      : empty("표시할 기록이 없습니다.");
  }

  async function changeDagState(dagId) {
    const dialog = document.createElement("dialog");
    dialog.className = "wb-state-dialog";
    dialog.innerHTML = `<div class="section-heading"><h2>DAG 운영 상태 변경</h2><button type="button" data-close>닫기</button></div><div data-body aria-live="polite">현재 상태와 권한을 확인하고 있습니다…</div>`;
    document.body.append(dialog);
    let changed = false;
    const closed = new Promise((resolve) => dialog.addEventListener("close", () => { dialog.remove(); resolve(changed); }, { once: true }));
    $("[data-close]", dialog).onclick = () => dialog.close();
    dialog.showModal();
    const body = $("[data-body]", dialog);
    try {
      const current = await api("work/dag-control?dag_id=" + encodeURIComponent(dagId));
      if (!dialog.open) return closed;
      const blocked = !current.can_edit ? "이 DAG를 변경할 Airflow 권한이 없습니다." : current.is_stale ? "DAG 파일 반영을 먼저 확인하세요." : current.is_paused && current.has_import_errors ? "DAG 파싱 오류를 먼저 해결하세요." : "";
      const action = current.is_paused ? "활성화" : "일시중지";
      body.innerHTML = `<p class="mono">${esc(dagId)}</p><p>현재 상태: ${current.is_paused ? "일시중지" : "활성"}</p><p>${esc(blocked || (current.is_paused ? "스케줄이나 대기 중인 실행에 따라 작업이 시작될 수 있습니다." : "새 실행의 스케줄링을 일시중지합니다. 이미 실행 중인 작업은 강제 종료하지 않습니다."))}</p>${current.model_lab ? '<p class="helper">평가·학습 준비 상태는 별도이며, 실행 요청 시 설정을 다시 확인합니다.</p>' : ""}<p data-error class="notice" role="alert" hidden></p>${blocked ? "" : `<button data-confirm class="primary">${action} 확인</button>`}`;
      $("[data-confirm]", dialog)?.addEventListener("click", async (event) => {
        event.target.disabled = true;
        try {
          await api("work/dag-state", "PUT", { dag_id: dagId, is_paused: !current.is_paused, expected_is_paused: current.is_paused });
          changed = true;
          toast(`DAG를 ${action}했습니다.`);
          dialog.close();
        } catch (error) {
          const notice = $("[data-error]", dialog);
          notice.hidden = false;
          notice.textContent = error.message + " 닫은 뒤 상태를 다시 확인하세요.";
        }
      });
    } catch (error) {
      body.textContent = error.message;
    }
    return closed;
  }

  return {
    $,
    esc,
    formatDate,
    badge,
    empty,
    toast,
    download,
    guarded,
    on,
    table,
    api,
    changeDagState,
  };
})();
