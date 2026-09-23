"use strict";
(() => {
  const { $, esc, api, toast, guarded } = WorkbenchUI;
  let config,
    selected,
    editing = false;
  function target(screen) {
    const url = new URL(screen.url);
    if (screen.theme === "grafana")
      url.searchParams.set("theme", WorkbenchTheme.mode || "light");
    return url.href;
  }
  function show() {
    const screens = config.screens.filter((s) => s.enabled);
    selected = screens.find((s) => s.id === selected?.id) || screens[0];
    $("#app").innerHTML =
      `<header class="page-heading monitor-heading"><div><h1>운영 모니터링</h1><p class="description">팀에서 관리하는 공통 운영 화면을 확인합니다.</p></div><div class="actions">${screens.length ? `<label class="monitor-select">운영 화면<select id="screen-choice">${screens.map((s) => `<option value="${esc(s.id)}" ${s.id === selected.id ? "selected" : ""}>${esc(s.name)}</option>`).join("")}</select></label><a id="screen-original" target="_blank" rel="noopener noreferrer">원본 열기 ↗</a><button id="screen-reload">새로고침</button>` : ""}${config.can_edit ? '<button id="monitor-settings">연결 설정</button>' : ""}</div></header>${selected ? `<p class="description">${esc(selected.description)}</p><details class="monitor-help"><summary>로그인 또는 화면 표시가 필요한 경우</summary>원본 열기에서 외부 서비스에 로그인한 뒤 새로고침하세요. 외부 서비스가 Airflow의 임베딩을 허용해야 하며 브라우저의 쿠키 정책에 따라 원본 화면에서만 열릴 수도 있습니다. 조회·편집 권한은 외부 서비스에서 관리합니다. Airflow 로그인 정보는 전달하지 않습니다.</details><div id="frame-container"></div>` : `<section class="monitor-card"><h2>등록된 운영 화면이 없습니다</h2><p>${config.can_edit ? "연결 설정에서 이미 만들어진 대시보드의 URL을 등록하세요." : "운영 담당자에게 공통 대시보드 등록을 요청하세요."}</p><p class="description">대시보드 제작·데이터 수집·서버 운영은 연결할 서비스에서 담당합니다.</p></section>`}`;
    if (selected) loadFrame();
    $("#screen-choice")?.addEventListener("change", (e) => {
      selected = screens.find((s) => s.id === e.target.value);
      show();
    });
    $("#screen-reload")?.addEventListener("click", loadFrame);
    $("#monitor-settings")?.addEventListener("click", edit);
  }
  function loadFrame() {
    const url = target(selected),
      frame = document.createElement("iframe");
    frame.className = "monitor-frame";
    frame.title = selected.name;
    frame.src = url;
    frame.referrerPolicy = "no-referrer";
    frame.setAttribute(
      "sandbox",
      "allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox allow-downloads",
    );
    $("#frame-container").replaceChildren(frame);
    $("#screen-original").href = url;
  }
  function edit() {
    editing = true;
    let draft = structuredClone(config.screens);
    function draw() {
      $("#app").innerHTML =
        `<section class="monitor-editor"><header class="page-heading"><div><h1>운영 화면 연결 설정</h1><p class="description">관리자가 등록한 화면을 이 Airflow의 Plugins 조회 사용자에게 제공합니다.</p></div><button id="edit-back">돌아가기</button></header><p class="notice">외부 화면의 인증·조회 권한은 외부 서비스 설정을 따릅니다. 비밀번호나 API 토큰이 포함된 URL은 등록하지 마세요.</p><form id="monitor-form">${draft.map((s, i) => `<fieldset class="screen-editor" data-screen="${i}"><legend>화면 ${i + 1}</legend><label>이름<input name="name" required maxlength="100" value="${esc(s.name)}"></label><label>대시보드 URL<input name="url" type="url" required placeholder="https://monitoring.example.com/d/…" value="${esc(s.url)}"></label><label>설명<textarea name="description" maxlength="500">${esc(s.description)}</textarea></label><label>테마 연동<select name="theme"><option value="none" ${s.theme === "none" ? "selected" : ""}>외부 화면 설정 사용</option><option value="grafana" ${s.theme === "grafana" ? "selected" : ""}>Grafana · Airflow Light / Dark 따라가기</option></select></label><div class="actions"><label><input name="enabled" type="checkbox" ${s.enabled ? "checked" : ""}> 메뉴에서 표시</label><button type="button" data-remove="${i}">삭제</button></div></fieldset>`).join("")}<div class="actions"><button type="button" id="screen-add" ${draft.length >= 20 ? "disabled" : ""}>화면 추가</button><button class="primary" id="screen-save" type="submit">저장</button></div><p id="monitor-error" class="monitor-error" role="alert"></p></form></section>`;
      const read = () =>
        (draft = draft.map((s, i) => {
          const f = $(`[data-screen="${i}"]`);
          return {
            ...s,
            name: f.elements.name.value,
            url: f.elements.url.value,
            description: f.elements.description.value,
            theme: f.elements.theme.value,
            enabled: f.elements.enabled.checked,
          };
        }));
      $("#edit-back").onclick = () => {
        editing = false;
        show();
      };
      $("#screen-add").onclick = () => {
        read();
        draft.push({
          id: crypto.randomUUID(),
          name: "",
          url: "",
          description: "",
          enabled: true,
          theme: "none",
        });
        draw();
      };
      document.querySelectorAll("[data-remove]").forEach(
        (b) =>
          (b.onclick = () => {
            read();
            draft.splice(Number(b.dataset.remove), 1);
            draw();
          }),
      );
      $("#monitor-form").onsubmit = async (e) => {
        e.preventDefault();
        read();
        $("#screen-save").disabled = true;
        try {
          await api("monitoring", "PUT", {
            version: config.version,
            screens: draft,
          });
          location.reload();
        } catch (err) {
          $("#monitor-error").textContent = err.message;
          $("#screen-save").disabled = false;
        }
      };
    }
    draw();
  }
  window.addEventListener("workbench-theme-change", () => {
    if (!editing && selected?.theme === "grafana" && $("#frame-container"))
      loadFrame();
  });
  api("monitoring")
    .then((c) => {
      config = c;
      show();
    })
    .catch((e) => {
      $("#app").innerHTML = `<div class="notice">${esc(e.message)}</div>`;
    });
})();
