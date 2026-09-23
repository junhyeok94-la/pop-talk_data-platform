"use strict";
const LabUI = (() => {
  const lab = {
    active: false,
    project: "",
    tab: "overview",
    data: {},
    bootstrap: null,
    experiment: "",
    run: "",
    drafts: {},
    poll: null,
    saveTimer: null,
    endpointEditorRequest: 0,
  };
  const tabs = [
    ["overview", "개요"],
    ["dataset", "데이터·평가셋"],
    ["experiment", "실험"],
    ["run", "실행"],
    ["model", "모델·적용 구성"],
    ["storage", "저장소"],
    ["settings", "환경·연결"],
  ];
  const titles = Object.fromEntries(tabs);
  const endpointList = () => lab.bootstrap.endpoints.map((e) => e.spec);
  const project = () =>
    lab.bootstrap.projects.find((p) => p.id === lab.project);
  const canManage = () =>
    lab.bootstrap.can_admin ||
    project()?.owner === lab.bootstrap.subject ||
    project()?.members[lab.bootstrap.subject] === "manager";
  const canRun = () =>
    canManage() || project()?.members[lab.bootstrap.subject] === "developer";
  const path = (suffix) =>
    `lab/projects/${encodeURIComponent(lab.project)}/${suffix}`;
  const list = (kind) => lab.data[kind] || [];
  const selectedExperiment = () =>
    list("experiment").find((e) => e.id === lab.experiment);
  const option = (value, label, selected) =>
    `<option value="${esc(value)}" ${value === selected ? "selected" : ""}>${esc(label)}</option>`;
  const status = (s) =>
    ({
      success: "완료",
      failed: "실패",
      running: "실행 중",
      queued: "대기",
      submitting: "제출 중",
      submission_unknown: "제출 확인 필요",
      submission_rejected: "제출 거부",
      cancelled: "취소 완료",
      result_missing: "결과 확인 필요",
      approved: "검토 승인",
      rejected: "검토 반려",
      pending_review: "검토 대기",
      configuration_ready: "적용 구성 준비",
    })[s] || s;
  const mark = (s) =>
    `<span class="badge ${["success", "approved"].includes(s) ? "success" : ["failed", "rejected", "submission_rejected", "result_missing"].includes(s) ? "failed" : ["running", "queued", "submitting"].includes(s) ? "running" : ""}">${esc(status(s))}</span>`;
  const pct = (n) => (n == null ? "미산정" : `${(n * 100).toFixed(1)}%`);
  const asJSON = (v) => esc(JSON.stringify(v, null, 2));
  const goButton = (tab, text) => `<button data-go="${tab}">${text}</button>`;
  const titleBand = (title, action = "") =>
    `<div class="section-heading"><h2>${esc(title)}</h2>${action}</div>`;
  const card = (title, body, action = "") =>
    `<section class="card">${titleBand(title, action)}${body}</section>`;
  function wireNavigation() {
    document
      .querySelectorAll("[data-go]")
      .forEach((b) => (b.onclick = guarded(() => navigate(b.dataset.go))));
  }
  async function load() {
    lab.bootstrap = await api("lab/bootstrap");
    if (!lab.bootstrap.projects.some((p) => p.id === lab.project))
      lab.project =
        lab.bootstrap.selected_project || lab.bootstrap.projects[0]?.id || "";
    if (lab.project) {
      const kinds = [
        "dataset",
        "experiment",
        "run",
        "model",
        "release",
        "preset",
        "storage",
      ];
      const values = await Promise.all(kinds.map((k) => api(path(k))));
      kinds.forEach((k, i) => {
        lab.data[k] = values[i];
      });
      state.presets = lab.data.preset;
      if (!list("experiment").some((e) => e.id === lab.experiment))
        lab.experiment = list("experiment")[0]?.id || "";
      if (
        ["dataset", "experiment", "training"].includes(lab.tab) &&
        !lab.drafts[lab.project + ":" + lab.tab]
      ) {
        lab.drafts[lab.project + ":" + lab.tab] = (
          await api(path(`draft/${lab.tab}`))
        ).values;
      }
    } else lab.data = {};
  }
  function capture() {
    const form = $("#lab-form") || $("#training-form");
    if (form) {
      const values = Object.fromEntries(new FormData(form));
      lab.drafts[lab.project + ":" + lab.tab] = values;
    }
  }
  async function navigate(tab) {
    capture();
    clearInterval(lab.poll);
    state.tab = "lab";
    lab.tab = tab;
    await load();
    render();
  }
  function restoreDraft(form) {
    const value = lab.drafts[lab.project + ":" + lab.tab];
    if (!value || !form) return;
    for (const [key, val] of Object.entries(value)) {
      const field = form.elements.namedItem(key);
      if (!field) continue;
      if (field.type === "checkbox")
        field.checked = val === true || val === "on";
      else
        field.value =
          typeof val === "object" ? JSON.stringify(val, null, 2) : val;
    }
  }
  function render() {
    clearInterval(lab.poll);
    state.busy = false;
    state.tab = "lab";
    $("#app").innerHTML =
      `<div class="page-heading"><div><h1>Model Lab</h1><p class="description">기준선을 기록하고, 후보를 비교하고, 검증한 모델을 챗봇 개발에 연결하세요.</p></div><div class="lab-project"><label>프로젝트<select id="lab-project">${lab.bootstrap.projects.map((p) => option(p.id, p.name, lab.project)).join("") || option("", "프로젝트를 만들어 시작하세요", "")}</select></label><button id="new-project">프로젝트 만들기</button></div></div><div class="tabs" role="tablist" aria-label="Model Lab 메뉴">${tabs.map(([id, label]) => `<button role="tab" aria-selected="${lab.tab === id || (id === "experiment" && lab.tab === "training")}" class="${lab.tab === id || (id === "experiment" && lab.tab === "training") ? "active" : ""}" data-go="${id}">${label}${id === "run" ? ` <span class="lab-count">${list("run").length}</span>` : ""}</button>`).join("")}</div><div id="lab-content"></div>`;
    on("#lab-project", "change", async (e) => {
      capture();
      lab.project = e.target.value;
      lab.experiment = "";
      lab.run = "";
      await api("lab/preferences", "PUT", { project_id: lab.project });
      await load();
      render();
    });
    on("#new-project", "click", projectForm);
    wireNavigation();
    if (!lab.project && lab.tab !== "settings") {
      $("#lab-content").innerHTML = card(
        "모델 개발 작업 공간",
        `<p>프로젝트별로 평가셋, 실험, 모델 버전과 팀 접근 권한을 관리합니다. 프로젝트를 만든 뒤 사용할 추론 연결을 선택하세요.</p><button id="start-project" class="primary">첫 프로젝트 만들기</button>`,
      );
      on("#start-project", "click", projectForm);
      return;
    }
    (
      ({
        overview,
        dataset: datasets,
        experiment: experiments,
        run: runs,
        model: modelsPage,
        settings,
        training,
        storage,
      })[lab.tab] || overview
    )();
    wireNavigation();
  }
  function projectForm() {
    $("#lab-content").innerHTML = card(
      "프로젝트 만들기",
      `<form id="lab-form"><div class="fields"><label>이름<input name="name" maxlength="100" required placeholder="챗봇 모델 개발"></label><label>ID<input name="id" pattern="[a-zA-Z0-9_-]{1,80}" required placeholder="chatbot-development"></label></div><label>목적<textarea name="description" rows="3" placeholder="개선하려는 사용자 경험과 모델의 역할"></textarea></label><button class="primary">프로젝트 생성</button></form>`,
    );
    on("#lab-form", "submit", async (e) => {
      e.preventDefault();
      const data = Object.fromEntries(new FormData(e.target));
      await api(`lab/projects/${encodeURIComponent(data.id)}`, "PUT", data);
      lab.project = data.id;
      await api("lab/preferences", "PUT", { project_id: lab.project });
      await navigate("overview");
    });
  }
  function overview() {
    const completed = list("run").filter((r) => r.status === "success");
    const steps = [
      [
        "dataset",
        "평가셋 준비",
        list("dataset").length,
        "사례·정답·코퍼스 버전",
      ],
      [
        "experiment",
        "기준선과 후보",
        list("experiment").length,
        "같은 조건으로 비교",
      ],
      ["run", "실행 추적", completed.length, "완료된 평가·학습"],
      ["model", "검토와 등록", list("model").length, "추론 모델과 평가 근거"],
    ];
    $("#lab-content").innerHTML =
      `<div class="lab-flow">${steps.map(([tab, title, n, desc], i) => `<button class="lab-step" data-go="${tab}"><span class="muted">0${i + 1} · ${title}</span><strong>${n}</strong><span>${desc}</span></button>`).join("")}</div><div class="lab-columns">${card("다음 행동", `<p class="muted">${esc(project().description || "기존 모델의 기준선을 먼저 만든 뒤, 실패 원인에 맞는 개선을 선택하세요.")}</p><div class="lab-next">${!endpointList().length ? goButton("settings", "추론 연결 준비") : !list("dataset").length ? goButton("dataset", "평가셋 만들기") : goButton("experiment", "기준선 평가 시작")}${goButton("run", "진행 중 실행 확인")}</div><p class="helper">파인튜닝은 개선 방법 중 하나입니다. 프롬프트·검색 설정·모델 교체도 같은 실험에서 비교할 수 있습니다.</p>`)}${card("프로젝트 범위", `<dl class="lab-facts"><dt>저장</dt><dd>Airflow PostgreSQL</dd><dt>모델 계산</dt><dd>선택한 추론 서버·외부 실행기</dd><dt>평가 원칙</dt><dd>데이터 버전 고정 · 미라벨 분리</dd><dt>권한</dt><dd>${canManage() ? "프로젝트 관리" : canRun() ? "실험 개발" : "조회"}</dd></dl><p class="helper">실행 성공과 품질 검토를 구분합니다. 적용 구성 생성은 서비스 배포 완료를 의미하지 않습니다.</p>`)}</div>${card("최근 실행", runTable(list("run").slice(0, 6)))}`;
    bindRuns();
  }
  function example(kind = "generation") {
    return {
      name: "새 평가셋",
      kind,
      description: "",
      split: "development",
      cases:
        kind === "embedding"
          ? [
              {
                id: "case-1",
                input: "비밀번호를 재설정하려면?",
                category: "account",
                review_status: "reviewed",
                relevance: { "doc-1": 2, "doc-2": 0 },
              },
            ]
          : [
              {
                id: "case-1",
                input: "응답으로 준비 완료라고만 말하세요.",
                category: "instruction",
                review_status: "reviewed",
                required_text: ["준비 완료"],
              },
            ],
      corpus:
        kind === "embedding"
          ? [
              {
                id: "doc-1",
                text: "로그인 화면의 비밀번호 재설정에서 이메일 인증을 진행합니다.",
              },
              { id: "doc-2", text: "알림 설정은 계정 메뉴에서 변경합니다." },
            ]
          : [],
      corpus_version: kind === "embedding" ? "sample-v1" : "",
    };
  }
  function datasets() {
    $("#lab-content").innerHTML = `<div class="lab-columns">${card(
      "평가셋 버전",
      table(
        ["이름 / 버전", "종류", "사례", "검토 완료", ""],
        list("dataset").map((d) => [
          `${esc(d.name)}<small class="lab-sub mono">${esc(d.sha256.slice(0, 12))}</small>`,
          esc(d.spec.kind),
          d.spec.cases.length,
          d.spec.cases.filter((c) => c.review_status === "reviewed").length,
          `<button data-dataset="${d.id}">보기·새 버전</button>`,
        ]),
      ) +
        `<p class="helper">저장은 항상 새 버전을 만듭니다. 이미 실행한 평가셋은 수정되지 않습니다.</p>`,
    )}${card("평가셋 만들기", `<form id="lab-form"><label>JSON 파일 가져오기<input id="dataset-file" type="file" accept=".json,application/json"></label><div class="actions"><button type="button" id="sample-generation">생성 평가 예제</button><button type="button" id="sample-embedding">검색 평가 예제</button><button type="button" id="quick-case">질문 한 개로 만들기</button></div><label>평가셋 JSON<textarea class="mono" name="dataset" rows="18" spellcheck="false" required>${asJSON(example())}</textarea></label><p class="helper">cases: id·input·category·review_status. 생성 채점: required_text / expected_json의 부분 구조. 임베딩 채점: corpus 문서 ID별 relevance(0~3). draft 또는 미라벨 사례는 점수 산정에서 제외합니다.</p><button class="primary" ${canRun() ? "" : "disabled"}>새 버전 저장</button><button type="button" id="export-dataset">JSON 내보내기</button></form>`)}</div>`;
    restoreDraft($("#lab-form"));
    on("#dataset-file", "change", async (e) => {
      const f = e.target.files[0];
      if (!f) return;
      if (f.size > 250000) throw Error("평가셋 파일은 250KB 이하여야 합니다.");
      $("[name=dataset]").value = JSON.stringify(
        JSON.parse(await f.text()),
        null,
        2,
      );
    });
    for (const kind of ["generation", "embedding"])
      on(`#sample-${kind}`, "click", () => {
        $("[name=dataset]").value = JSON.stringify(example(kind), null, 2);
      });
    on("#quick-case", "click", quickCase);
    on("#export-dataset", "click", () =>
      download("evaluation-suite.json", JSON.parse($("[name=dataset]").value)),
    );
    on("#lab-form", "submit", async (e) => {
      e.preventDefault();
      await api(
        path("dataset"),
        "POST",
        JSON.parse(e.target.elements.dataset.value),
      );
      toast("평가셋의 새 버전을 저장했습니다.");
      await navigate("dataset");
    });
    document.querySelectorAll("[data-dataset]").forEach(
      (b) =>
        (b.onclick = () => {
          $("[name=dataset]").value = JSON.stringify(
            list("dataset").find((d) => d.id === b.dataset.dataset).spec,
            null,
            2,
          );
        }),
    );
  }
  function quickCase() {
    $("#lab-content").innerHTML = card(
      "질문 한 개로 평가셋 만들기",
      `<form id="lab-form"><label>평가셋 이름<input name="name" required maxlength="100"></label><label>질문<textarea name="input" required rows="4"></textarea></label><label>답변에 포함되어야 할 표현 · 선택<input name="expected" placeholder="쉼표로 구분"></label><label>기대 JSON 구조 · 선택<textarea name="expected_json" rows="4" class="mono" placeholder='{"intent":"search"}'></textarea></label><label class="check"><input type="checkbox" name="reviewed">정답 조건을 검토했습니다</label><button class="primary">평가셋 저장</button></form>`,
    );
    on("#lab-form", "submit", async (e) => {
      e.preventDefault();
      const f = Object.fromEntries(new FormData(e.target));
      await api(path("dataset"), "POST", {
        name: f.name,
        kind: "generation",
        cases: [
          {
            id: "case-1",
            input: f.input,
            required_text: f.expected
              ? f.expected
                  .split(",")
                  .map((v) => v.trim())
                  .filter(Boolean)
              : [],
            expected_json: f.expected_json ? JSON.parse(f.expected_json) : null,
            review_status: f.reviewed ? "reviewed" : "draft",
          },
        ],
      });
      await navigate("experiment");
    });
  }
  function experiments() {
    const exp = selectedExperiment();
    $("#lab-content").innerHTML =
      `<div class="lab-toolbar"><label>실험<select id="experiment-select">${list(
        "experiment",
      )
        .map((e) => option(e.id, e.name, lab.experiment))
        .join(
          "",
        )}</select></label><button id="new-experiment">새 실험</button>${exp ? `<span class="muted">${exp.baseline_run_id ? "기준선 지정됨" : "첫 평가를 완료하고 기준선을 지정하세요."}</span>` : ""}</div><div id="experiment-workspace"></div>`;
    on("#experiment-select", "change", (e) => {
      capture();
      lab.experiment = e.target.value;
      experiments();
    });
    on("#new-experiment", "click", newExperiment);
    if (exp) {
      $("#new-experiment").insertAdjacentHTML(
        "afterend",
        '<button id="edit-experiment">실험 설정</button>',
      );
      on("#edit-experiment", "click", () => newExperiment(exp));
    }
    if (!exp) {
      newExperiment();
      return;
    }
    const endpoints = endpointList();
    $("#experiment-workspace").innerHTML =
      `${exp.hypothesis ? `<p class="notice info">개선 목적: ${esc(exp.hypothesis)}</p>` : ""}<div class="lab-columns">${card(
        "평가 실행",
        `<form id="lab-form"><label>평가셋 버전<select name="dataset_id" required>${list(
          "dataset",
        )
          .map((d) =>
            option(
              d.id,
              `${d.name} · ${d.spec.kind} · ${d.sha256.slice(0, 8)}`,
            ),
          )
          .join(
            "",
          )}</select></label><label>추론 연결<select name="endpoint_id" required>${endpoints.map((e) => option(e.id, `${e.name} · ${e.provider}`)).join("")}</select></label><label>모델<input name="model" list="lab-model-list" required placeholder="추론 서버에 등록된 모델 이름"><datalist id="lab-model-list"></datalist></label><button type="button" id="fetch-catalog">모델 목록 확인</button><div id="catalog-status" class="helper"></div><label>시스템 프롬프트<textarea name="system" rows="4" placeholder="평가에 적용할 역할과 응답 규칙"></textarea></label><div class="fields"><label>Temperature<input name="temperature" type="number" min="0" max="2" step="0.05" value="0.2"></label><label>최대 생성 토큰<input name="max_tokens" type="number" min="16" max="2048" value="256"></label><label>검색 K<input name="k" type="number" min="1" max="100" value="3"></label></div><input type="hidden" name="request_id" value="${crypto.randomUUID()}"><button class="primary" ${canRun() && list("dataset").length && endpoints.length ? "" : "disabled"}>평가 DAG 실행</button><p class="helper">실행 입력을 고정해 저장합니다. 탭 이동·새로고침 후에도 실행 메뉴에서 이어서 확인할 수 있습니다.</p></form>`,
      )}${card("개선 방법", `<p>기준선의 실패 사례에 맞춰 다음 후보를 만드세요.</p><ol class="lab-guidance"><li>프롬프트·출력 규칙 조정</li><li>다른 생성 모델 또는 임베딩 모델 평가</li><li>프로젝트 검색·에이전트 API 평가</li><li>데이터가 준비되면 파인튜닝</li></ol><button id="open-training" ${canRun() ? "" : "disabled"}>이 실험에서 학습 후보 만들기</button><p class="helper">학습은 선택한 외부 실행기에서 진행합니다. 학습 산출물은 추론 엔진에서 로드할 수 있는 형태로 준비한 뒤 같은 평가셋으로 검증하세요.</p>${goButton("dataset", "평가셋 준비")}${goButton("settings", "연결·실행 환경 확인")}`)}</div>${card("이 실험의 실행", runTable(list("run").filter((r) => r.experiment_id === lab.experiment)))}`;
    restoreDraft($("#lab-form"));
    const form = $("#lab-form");
    const evaluateButton = form.querySelector("button.primary");
    evaluateButton.insertAdjacentHTML(
      "beforebegin",
      '<p id="evaluation-permission" class="notice" role="status" hidden></p>',
    );
    const updateEvaluationPermission = () => {
      const permission =
        lab.bootstrap.evaluation_permissions?.[form.elements.endpoint_id.value];
      const allowed = canRun() && permission?.can_trigger === true;
      evaluateButton.disabled =
        !allowed || !list("dataset").length || !endpoints.length;
      const notice = $("#evaluation-permission");
      notice.hidden = allowed || !endpoints.length;
      notice.textContent = !canRun()
        ? "평가 실행에는 프로젝트 developer 또는 manager 권한이 필요합니다."
        : permission?.message ||
          "DAG 실행 권한을 확인할 수 없습니다. 새로고침 후 다시 확인하세요.";
    };
    form
      .querySelector("[name=system]")
      .closest("label")
      .insertAdjacentHTML(
        "afterend",
        `<label class="check"><input type="checkbox" name="json_mode">JSON 출력 모드</label><label>고급 생성 옵션 JSON<textarea name="options" rows="3" class="mono">{}</textarea></label><p class="helper">Ollama: top_p·top_k·num_ctx·repeat_penalty·seed. OpenAI 호환: top_p·seed·presence_penalty·frequency_penalty. Gemini: topP·topK·seed.</p>`,
      );
    restoreDraft(form);
    if (!Object.hasOwn(lab.drafts[lab.project + ":experiment"] || {}, "system"))
      form.elements.system.value = exp.default_system || "";
    const support = () => {
      updateEvaluationPermission();
      const data = list("dataset").find(
        (d) => d.id === form.elements.dataset_id.value,
      );
      for (const key of [
        "temperature",
        "max_tokens",
        "system",
        "options",
        "json_mode",
      ])
        form.elements[key].closest("label").hidden =
          data?.spec.kind === "embedding";
      form.elements.k.closest("label").hidden = data?.spec.kind !== "embedding";
    };
    support();
    on("#lab-form", "change", () => {
      form.elements.request_id.value = crypto.randomUUID();
      support();
    });
    on("#fetch-catalog", "click", async () => {
      const result = await api(
        `lab/endpoints/${encodeURIComponent(form.elements.endpoint_id.value)}/models`,
      );
      $("#lab-model-list").innerHTML = result.models
        .map((m) => option(m.name, m.name))
        .join("");
      $("#catalog-status").textContent =
        result.models.map((m) => m.name).join(" · ") ||
        "등록된 모델 목록이 없습니다. 연결 설정의 모델 목록을 확인하세요.";
      if (!form.elements.model.value && result.models[0])
        form.elements.model.value = result.models[0].name;
    });
    on("#open-training", "click", () => navigate("training"));
    on("#lab-form", "submit", async (e) => {
      e.preventDefault();
      updateEvaluationPermission();
      if (evaluateButton.disabled) return;
      const f = Object.fromEntries(new FormData(e.target));
      const embedding =
        list("dataset").find((d) => d.id === f.dataset_id)?.spec.kind ===
        "embedding";
      const payload = {
        ...f,
        experiment_id: lab.experiment,
        temperature: Number(f.temperature),
        max_tokens: Number(f.max_tokens),
        k: Number(f.k),
        options: embedding ? {} : JSON.parse(f.options || "{}"),
        json_mode: !embedding && f.json_mode === "on",
      };
      const button = e.submitter;
      button.disabled = true;
      try {
        const run = await api(path("evaluate"), "POST", payload);
        capture();
        lab.run = run.id;
        await navigate("run");
      } finally {
        if (button.isConnected) updateEvaluationPermission();
      }
    });
    bindRuns();
    wireNavigation();
  }
  function newExperiment(existing) {
    if (!existing?.id) existing = null;
    $("#experiment-workspace").innerHTML = card(
      existing ? "실험 설정" : "새 실험",
      `<form id="lab-form"><label>이름<input name="name" required maxlength="100" placeholder="질문 해석 모델 비교"></label><label>개선 목적과 가설<textarea name="hypothesis" rows="4" placeholder="기준 모델에서 어떤 실패를 줄이고 싶은지 기록하세요."></textarea></label><button class="primary" ${canRun() ? "" : "disabled"}>실험 만들기</button></form>`,
    );
    $("#lab-form")
      .querySelector("button")
      .insertAdjacentHTML(
        "beforebegin",
        '<label>기본 시스템 프롬프트<textarea name="default_system" rows="5" placeholder="이 실험에서 비교할 후보들의 기본 역할과 출력 규칙"></textarea></label>',
      );
    if (existing)
      for (const key of ["name", "hypothesis", "default_system"])
        $("#lab-form").elements[key].value = existing[key] || "";
    on("#lab-form", "submit", async (e) => {
      e.preventDefault();
      const exp = await api(
        path(
          existing
            ? `experiment/${existing.id}?version=${existing.version}`
            : "experiment",
        ),
        existing ? "PATCH" : "POST",
        Object.fromEntries(new FormData(e.target)),
      );
      lab.experiment = exp.id;
      await navigate("experiment");
    });
  }
  function runTable(values) {
    return table(
      ["실행", "종류", "상태", "기록 시각", ""],
      values.map((r) => [
        esc(r.name),
        r.kind === "training" ? "학습" : "평가",
        mark(r.status),
        formatDate(r.created),
        `<button data-run="${r.id}">상세</button>`,
      ]),
    );
  }
  function bindRuns() {
    document.querySelectorAll("[data-run]").forEach(
      (b) =>
        (b.onclick = guarded(async () => {
          lab.run = b.dataset.run;
          await navigate("run");
        })),
    );
  }
  function runs() {
    $("#lab-content").innerHTML =
      card(
        "실행 이력",
        `<p class="helper">상세를 열면 Airflow와 외부 결과를 다시 확인합니다. 입력·결과는 실행 ID별로 보존됩니다.</p>${runTable(list("run"))}`,
        `<button id="refresh-runs">새로고침</button>`,
      ) + `<div id="run-detail"></div>`;
    on("#refresh-runs", "click", () => navigate("run"));
    bindRuns();
    if (lab.run) guarded(() => showRun(lab.run))();
  }
  async function showRun(id, silent = false) {
    const run = await api(path(`run/${id}`));
    if (lab.tab !== "run" || lab.run !== id) return;
    lab.data.run = list("run").map((r) =>
      r.id === id ? { ...r, status: run.status } : r,
    );
    document.querySelectorAll(`[data-run="${id}"]`).forEach((button) => {
      const row = button.closest("tr");
      if (row) row.cells[2].innerHTML = mark(run.status);
    });
    const result = run.result;
    const summary = result?.summary || run.summary || {};
    const running = [
      "queued",
      "running",
      "submitting",
      "submission_unknown",
    ].includes(run.status);
    const target = $("#run-detail");
    target.innerHTML = card(
      run.name,
      `<div class="actions">${mark(run.status)}<a target="_top" href="/dags/${encodeURIComponent(run.dag_id)}/runs/${encodeURIComponent(run.dag_run_id)}">Airflow DAG 실행 ↗</a><span class="muted mono">${esc(run.id)}</span></div>${run.message ? `<p class="notice">${esc(run.message)}</p>` : ""}${result?.error ? `<p class="notice">${esc(result.error)}</p>` : ""}${run.kind === "evaluation" ? `<div class="lab-metrics"><div><span>처리 / 전체</span><strong>${summary.completed ?? 0} / ${summary.total ?? "—"}</strong></div><div><span>실패 사례</span><strong>${summary.failed ?? 0}</strong></div><div><span>채점 사례</span><strong>${summary.scored || summary.retrieval_scored || 0}</strong></div><div><span>${summary.recall != null ? "Recall@K" : "조건 통과율"}</span><strong>${pct(summary.recall ?? summary.pass_rate)}</strong></div></div><div class="actions">${run.status === "success" ? `<button id="set-baseline">기준선으로 지정</button><button id="compare-run">기준선과 비교</button><button id="candidate-run">설정으로 다음 후보 평가</button><button id="register-model" class="primary">모델 버전 등록</button>` : ""}${running ? `<button id="cancel-run" class="danger" ${run.cancel_requested ? "disabled" : ""}>${run.cancel_requested ? "취소 확인 중" : "취소 요청"}</button>` : ""}<button id="export-run">결과 내보내기</button></div><p class="helper">조건 통과율은 검토 완료된 자동 채점 조건의 결과입니다. 답변의 종합 품질을 의미하지 않습니다. 미라벨·실행 실패 사례를 함께 확인하세요.</p><div id="comparison-result"></div>${caseResults(result?.rows || [])}` : `<p>학습 결과를 추론 환경에서 로드한 뒤 이 실험의 평가셋으로 재평가하세요.</p><pre class="code">${asJSON(run.training_result || { next: "산출물 준비 상태는 DAG 실행에서 확인하세요." })}</pre><button id="training-evaluate">이 실험에서 후보 평가</button>${running ? '<button id="cancel-run" class="danger">취소 요청</button>' : ""}`}<details><summary>고정된 입력 설정</summary><pre class="code">${asJSON(run.config)}</pre></details>`,
    );
    if (run.status === "submission_rejected" || run.dag_run_created === false)
      target.querySelector('a[href^="/dags/"]')?.remove();
    if (!silent) target.scrollIntoView({ behavior: "smooth", block: "start" });
    on("#set-baseline", "click", async () => {
      await api(path(`run/${id}/baseline`), "POST");
      toast("이 실행을 실험 기준선으로 지정했습니다.");
      await load();
    });
    on("#compare-run", "click", async () => {
      clearInterval(lab.poll);
      const c = await api(path(`compare/${id}`));
      $("#comparison-result").innerHTML = compareResults(c);
    });
    on("#candidate-run", "click", async () => {
      lab.experiment = run.experiment_id;
      lab.drafts[lab.project + ":experiment"] = {
        ...run.config,
        request_id: crypto.randomUUID(),
      };
      await navigate("experiment");
    });
    on("#training-evaluate", "click", async () => {
      lab.experiment = run.experiment_id;
      await navigate("experiment");
    });
    on("#register-model", "click", () => registerForm(run));
    on("#cancel-run", "click", async () => {
      await api(path(`run/${id}/cancel`), "POST");
      await showRun(id, true);
    });
    on("#export-run", "click", () => download(`model-lab-${id}.json`, run));
    if (result?.rows?.length) {
      for (const details of document.querySelectorAll(".lab-case")) {
        const caseId = details.dataset.case;
        const row = result.rows.find((r) => r.id === caseId);
        const reviewed = run.case_reviews?.[caseId];
        if (row.reference && Object.keys(row.reference).length)
          details.insertAdjacentHTML(
            "beforeend",
            `<details><summary>검토 기준 · 모델 입력에 포함되지 않음</summary><pre class="code">${asJSON(row.reference)}</pre></details>`,
          );
        if (reviewed)
          details.insertAdjacentHTML(
            "beforeend",
            `<p class="notice info">사람 검토: ${reviewed.score === 1 ? "통과" : reviewed.score === 0.5 ? "부분 통과" : "실패"} · ${esc(reviewed.reason)}</p>`,
          );
        if (canRun() && !row.error) {
          details.insertAdjacentHTML(
            "beforeend",
            `<form class="case-review"><div class="fields"><label>사람 검토<select name="score"><option value="1">통과</option><option value="0.5">부분 통과</option><option value="0">실패</option></select></label><label>검토 사유<input name="reason" required minlength="3" maxlength="2000"></label></div><button>사례 검토 저장</button></form>`,
          );
          const form = details.querySelector(".case-review");
          if (reviewed) {
            form.elements.score.value = reviewed.score;
            form.elements.reason.value = reviewed.reason;
          }
          form.onsubmit = guarded(async (e) => {
            e.preventDefault();
            await api(
              path(`run/${id}/cases/${encodeURIComponent(caseId)}/review`),
              "POST",
              {
                score: Number(form.elements.score.value),
                reason: form.elements.reason.value,
              },
            );
            toast("사례 검토를 저장했습니다.");
          });
          form.addEventListener("input", () => clearInterval(lab.poll));
        }
      }
    }
    clearInterval(lab.poll);
    if (running)
      lab.poll = setInterval(() => showRun(id, true).catch(() => {}), 5000);
  }
  function caseResults(rows) {
    return `<div class="lab-cases">${rows.map((r) => `<details class="lab-case" data-case="${esc(r.id)}"><summary><span>${esc(r.id)} · ${esc(r.category)}</span><span>${r.error ? "실행 실패" : r.score == null ? (r.metrics?.recall != null ? `Recall ${pct(r.metrics.recall)}` : "미채점") : r.score === 1 ? "조건 통과" : "조건 미달"} · ${Math.round(r.latency_ms)}ms</span></summary><p>${esc(r.input)}</p>${r.error ? `<p class="notice">${esc(r.error)}</p>` : `<pre class="lab-output">${esc(r.output ?? JSON.stringify(r.ranking, null, 2))}</pre>`}${r.metrics ? `<pre class="code">${asJSON(r.metrics)}</pre>` : ""}${r.trace ? `<details><summary>에이전트 단계 결과</summary><pre class="code">${asJSON(r.trace)}</pre></details>` : ""}</details>`).join("")}</div>`;
  }
  function compareResults(c) {
    const byId = new Map((c.baseline.result?.rows || []).map((r) => [r.id, r]));
    return `<h3>기준선 ↔ 후보 · 동일 평가셋</h3><p class="helper">${c.same_system ? "시스템 프롬프트 동일" : "시스템 프롬프트가 변경되었습니다."} 모델·파라미터 변경은 각 실행 입력에서 확인하세요.</p>${table(
      ["사례", "기준선", "후보", "조건 변화"],
      (c.candidate.result?.rows || []).map((r) => {
        const b = byId.get(r.id);
        return [
          esc(r.id),
          `<pre class="lab-output">${esc(b?.output ?? JSON.stringify(b?.metrics || {}))}</pre>`,
          `<pre class="lab-output">${esc(r.output ?? JSON.stringify(r.metrics || {}))}</pre>`,
          b?.score != null && r.score != null
            ? r.score > b.score
              ? "개선"
              : r.score < b.score
                ? "회귀"
                : "동일"
            : "미산정",
        ];
      }),
    )}`;
  }
  function registerForm(run) {
    clearInterval(lab.poll);
    $("#run-detail").innerHTML = card(
      "평가한 모델 버전 등록",
      `<form id="lab-form"><label>버전 이름<input name="name" required value="${esc(run.config.model)} · candidate" maxlength="100"></label><label>연결할 학습 실행 · 선택<select name="training_run_id">${option("", "기본 모델 / 별도 준비 모델", "")}${list(
        "run",
      )
        .filter(
          (r) =>
            r.kind === "training" &&
            r.status === "success" &&
            r.experiment_id === run.experiment_id,
        )
        .map((r) => option(r.id, r.name))
        .join(
          "",
        )}</select></label><label>산출물 참조 · 선택<input name="artifact_uri" placeholder="학습 결과 저장소 URI 또는 실행기 artifact ID"></label><label>검토 메모<textarea name="notes" rows="4" placeholder="변환·양자화·서빙 준비와 평가 결과를 기록하세요."></textarea></label><button class="primary">후보 모델 등록</button></form>`,
    );
    on("#lab-form", "submit", async (e) => {
      e.preventDefault();
      await api(path("model"), "POST", {
        ...Object.fromEntries(new FormData(e.target)),
        evaluation_run_id: run.id,
      });
      await navigate("model");
    });
  }
  function modelsPage() {
    $("#lab-content").innerHTML =
      card(
        "모델 버전과 품질 검토",
        table(
          ["버전", "추론 모델", "품질", ""],
          list("model").map((m) => [
            `${esc(m.name)}<small class="lab-sub mono">${esc(m.id.slice(0, 12))}</small>`,
            `${esc(m.model)}<small class="lab-sub">${esc(m.endpoint_id)} · ${esc(m.kind)}</small>`,
            mark(m.quality),
            `<button data-model="${m.id}">검토·적용 구성</button>`,
          ]),
        ),
      ) +
      card(
        "적용 구성 이력",
        `<p class="helper">검증한 모델·프롬프트·검색 구성을 내보내 프로젝트 배포 과정에 사용합니다. 서비스가 실제로 적용했는지는 해당 배포 환경에서 확인하세요. 이전 구성도 다시 내려받을 수 있습니다.</p>${table(
          ["대상", "상태", "생성 시각", ""],
          list("release").map((r) => [
            esc(r.target),
            mark(r.status),
            formatDate(r.created),
            `<button data-release="${r.id}">구성 내려받기</button>`,
          ]),
        )}`,
      ) +
      `<div id="model-detail"></div>`;
    document
      .querySelectorAll("[data-model]")
      .forEach(
        (b) =>
          (b.onclick = () =>
            modelDetail(list("model").find((m) => m.id === b.dataset.model))),
      );
    document.querySelectorAll("[data-release]").forEach(
      (b) =>
        (b.onclick = () => {
          const r = list("release").find((r) => r.id === b.dataset.release);
          download(`model-release-${r.id}.json`, r.manifest);
        }),
    );
  }
  function modelDetail(m) {
    $("#model-detail").innerHTML = card(
      m.name,
      `<p>${esc(m.notes)}</p><button data-run="${m.evaluation_run_id}">평가 근거 확인</button><pre class="code">${asJSON({ model_revision: m.model_revision, artifact: m.artifact_uri, training_run: m.training_run_id, review: m.review })}</pre><form id="model-review"><label>검토 사유<textarea name="reason" required minlength="3" rows="3"></textarea></label><div class="actions"><button name="decision" value="approved" ${canManage() ? "" : "disabled"}>품질 검토 승인</button><button name="decision" value="rejected" ${canManage() ? "" : "disabled"}>반려</button></div></form><div class="divider"></div><form id="release-form"><h3>서비스 적용 구성 만들기</h3><label>적용 대상 이름<input name="target" required placeholder="chatbot-development"></label>${m.kind === "embedding" ? `<label>검색 구성 JSON<textarea name="search_config" rows="8" class="mono">${asJSON({ index_version: "", corpus_version: "", preprocessing: "", dimension: 1024, distance: "cosine" })}</textarea></label><p class="helper">새 모델로 생성한 인덱스와 질문 임베딩을 함께 전환하세요.</p>` : ""}<label>적용 메모<textarea name="notes" rows="2"></textarea></label><button class="primary" ${canManage() && m.quality === "approved" ? "" : "disabled"}>검증 구성 저장·내보내기</button></form>`,
    );
    bindRuns();
    on("#model-review", "submit", async (e) => {
      e.preventDefault();
      await api(path(`model/${m.id}/review`), "POST", {
        reason: e.target.elements.reason.value,
        decision: e.submitter.value,
      });
      await navigate("model");
    });
    on("#release-form", "submit", async (e) => {
      e.preventDefault();
      const f = Object.fromEntries(new FormData(e.target));
      const r = await api(path("release"), "POST", {
        ...f,
        model_version_id: m.id,
        search_config: f.search_config ? JSON.parse(f.search_config) : {},
      });
      download(`model-release-${r.id}.json`, r.manifest);
      await navigate("model");
    });
  }
  function training() {
    state.tab = "training";
    renderTraining();
    $("#lab-content").insertAdjacentHTML(
      "afterbegin",
      `<div class="lab-toolbar"><span>실험: <strong>${esc(selectedExperiment()?.name || "실험 선택 필요")}</strong></span><button data-go="experiment">평가 작업 공간으로 돌아가기</button></div>`,
    );
    const form = $("#training-form");
    form.elements.environment_id
      .closest("label")
      .insertAdjacentHTML(
        "afterend",
        `<label>저장소 프로필<select name="storage_profile_id" id="training-storage"></select></label><input name="storage_profile_version" type="hidden"><div id="training-storage-summary"></div>`,
      );
    refreshStorageChoices();
    restoreDraft(form);
    refreshStorageChoices(
      lab.drafts[lab.project + ":training"]?.storage_profile_id || "",
    );
    storageSummary();
    on("#training-storage", "change", () => {
      const selected = list("storage").find(
        (s) => s.id === form.elements.storage_profile_id.value,
      );
      form.elements.storage_profile_version.value = selected?.version || "";
      if (selected) {
        if (selected.default_model) {
          form.elements.base_model.value = selected.default_model;
          form.elements.revision.value = selected.default_revision;
        }
        for (const key of ["train_dataset", "validation_dataset"]) {
          const filename =
            form.elements[key].value.split("/").pop() ||
            (key === "train_dataset" ? "train.jsonl" : "validation.jsonl");
          form.elements[key].value =
            selected.locations.mode === "object"
              ? selected.locations.data_root + "/" + filename
              : filename;
        }
        form.elements.artifact_uri.value =
          selected.locations.mode === "object"
            ? selected.locations.artifact_root
            : "";
      }
      storageSummary();
      invalidateTraining();
      capture();
    });
    form.elements.environment_id.addEventListener("change", () => {
      refreshStorageChoices();
      form.elements.storage_profile_version.value = "";
      storageSummary();
    });
    form.addEventListener("input", storageSummary);
    $("#validate-training").disabled = !canRun();
    if (window.syncTrainingCapabilities) window.syncTrainingCapabilities();
    wireNavigation();
  }

  function refreshStorageChoices(selected = "") {
    const form = $("#training-form");
    form.elements.storage_profile_id.innerHTML =
      option("", "직접 입력 · 실행기 기본 설정", selected) +
      list("storage")
        .filter((s) => s.environment_id === form.elements.environment_id.value)
        .map((s) => option(s.id, `${s.name} · v${s.version}`, selected))
        .join("");
  }

  function storageSummary() {
    const form = $("#training-form"),
      selected = list("storage").find(
        (s) => s.id === form.elements.storage_profile_id.value,
      );
    form.elements.artifact_uri.readOnly = !!selected;
    form.elements.artifact_uri.closest("label").hidden =
      selected?.locations.mode === "mounted";
    if (!selected) {
      $("#training-storage-summary").innerHTML =
        `<p class="helper">데이터·모델·결과 경로를 반복 사용하려면 프로젝트 저장소를 등록하세요.</p><button type="button" data-go="storage">저장소 설정</button>`;
    } else {
      const s = selected.locations,
        mounted = s.mode === "mounted";
      $("#training-storage-summary").innerHTML =
        `<div class="lab-storage-summary"><strong>이번 실행의 저장 위치 · ${esc(selected.name)}</strong>${Number(form.elements.storage_profile_version.value) !== selected.version ? '<p class="notice">프로필 버전이 변경되었습니다. 프로필을 다시 선택하세요.</p>' : ""}<dl class="lab-facts"><dt>학습 데이터</dt><dd>${esc(mounted ? s.data_root + "/" + form.elements.train_dataset.value : form.elements.train_dataset.value)}</dd><dt>검증 데이터</dt><dd>${esc(mounted ? s.data_root + "/" + form.elements.validation_dataset.value : form.elements.validation_dataset.value)}</dd><dt>기본 모델</dt><dd>${esc(mounted ? s.model_root + "/" + (form.elements.base_model.value.replaceAll("/", "--") || "{모델 ID}") + "/" + (form.elements.revision.value || "{revision}") : "Hugging Face · " + form.elements.base_model.value + " @ " + (form.elements.revision.value || "{revision}"))}</dd><dt>학습 결과</dt><dd>${esc(s.artifact_root)}/{실행 ID}/model</dd></dl><p class="helper">경로와 프로필 버전이 실행에 고정됩니다. 모델 가중치·데이터 검증은 아래 설정 검증에서 진행합니다.</p></div>`;
    }
    wireNavigation();
  }

  function recipeStorage(recipe) {
    const id = recipe.storage_profile_id,
      version = Number(recipe.storage_profile_version);
    delete recipe.storage_profile_id;
    delete recipe.storage_profile_version;
    if (!id) return recipe;
    const selected = list("storage").find((s) => s.id === id);
    if (
      !selected ||
      selected.version !== version ||
      selected.environment_id !== recipe.environment_id
    )
      throw Error("저장소 프로필을 다시 선택하고 설정을 검증하세요.");
    return { ...recipe, storage: { ...selected.locations } };
  }

  function restoreStoragePreset(config) {
    const form = $("#training-form");
    refreshStorageChoices(config.storage?.profile_id || "");
    form.elements.storage_profile_version.value =
      config.storage?.profile_version || "";
    if (config.storage && !form.elements.storage_profile_id.value)
      throw Error("이 프로젝트·환경에 해당 저장소 프로필이 없습니다.");
    storageSummary();
    window.syncTrainingCapabilities?.();
  }

  function storage() {
    $("#lab-content").innerHTML =
      card(
        "프로젝트 저장소",
        `<p>학습 데이터, 기본 모델, 학습 결과가 위치할 곳을 실행 환경별로 관리합니다. 설정은 Airflow PostgreSQL에 저장되고 파일은 선택한 저장소에 남습니다.</p>${table(
          ["이름 / 실행 환경", "학습 데이터", "기본 모델", "학습 결과", ""],
          list("storage").map((s) => [
            `<strong>${esc(s.name)}</strong><br>${esc(s.environment_id)} · v${s.version}`,
            `<span class="mono lab-path">${esc(s.locations.data_root)}</span>`,
            `<span class="mono lab-path">${esc(s.locations.model_root || "Hugging Face · 고정 revision")}</span>`,
            `<span class="mono lab-path">${esc(s.locations.artifact_root)}</span>`,
            `<button data-storage-edit="${esc(s.id)}">설정</button><button data-storage-check="${esc(s.id)}" ${canRun() ? "" : "disabled"}>접근 확인</button>`,
          ]),
        )}<button id="new-storage" class="primary" ${canManage() ? "" : "disabled"}>저장소 프로필 등록</button><div id="storage-check-result" aria-live="polite"></div><div id="storage-editor"></div>`,
      ) +
      card(
        "경로를 지정하는 기준",
        `<ul><li><strong>HTTP 워커:</strong> 워커 컨테이너 안의 마운트 경로입니다. 개발 PC의 D: 경로는 운영자가 먼저 워커에 마운트해야 합니다.</li><li><strong>Kubernetes:</strong> 데이터·결과는 S3 / GCS 등의 저장소 URI, 기본 모델은 Hugging Face 모델 ID와 commit SHA를 사용합니다.</li><li><strong>인증:</strong> 워커 연결은 Airflow Connections, 객체 저장소 권한은 실행 주체의 IAM·Service Account로 관리합니다.</li></ul><p class="helper">접근 확인은 학습을 시작하거나 대용량 파일을 옮기지 않습니다. 마운트·버킷·권한 자체를 생성하는 기능은 아닙니다.</p>${goButton("experiment", "실험에서 저장소 사용")}`,
      );
    on("#new-storage", "click", () => storageForm());
    document
      .querySelectorAll("[data-storage-edit]")
      .forEach(
        (b) =>
          (b.onclick = () =>
            storageForm(
              list("storage").find((s) => s.id === b.dataset.storageEdit),
            )),
      );
    document.querySelectorAll("[data-storage-check]").forEach(
      (b) =>
        (b.onclick = guarded(async () => {
          b.disabled = true;
          $("#storage-check-result").innerHTML =
            '<p class="muted">선택한 실행 환경에서 저장소 접근을 확인하고 있습니다…</p>';
          try {
            const r = await api(
              path(
                `storage/${encodeURIComponent(b.dataset.storageCheck)}/check`,
              ),
              "POST",
            );
            $("#storage-check-result").innerHTML =
              `<div class="divider"></div><h3>${r.ready === true ? "경로 접근 확인 완료" : r.ready === null ? "실행 시 접근 확인 필요" : "경로 접근 확인 필요"}</h3><p>${esc(r.scope || "")}</p>${(r.blockers || []).map((v) => `<p class="notice">${esc(v)}</p>`).join("")}${table(
                ["대상", "경로", "접근", "여유 공간"],
                Object.entries(r.checks).map(([key, c]) => [
                  esc(
                    {
                      data_root: "학습·검증 데이터",
                      model_root: "기본 모델",
                      artifact_root: "학습 결과",
                    }[key] || key,
                  ),
                  `<span class="mono lab-path">${esc(c.path)}</span>`,
                  c.writable
                    ? "쓰기 가능" +
                      (c.exists === false ? " · 실행 시 폴더 생성" : "")
                    : c.readable
                      ? "읽기 가능"
                      : "확인 필요",
                  c.free_mib != null
                    ? (c.free_mib / 1024).toFixed(1) + " GiB"
                    : "—",
                ]),
              )}<p class="helper">프로필 v${r.profile_version} · ${new Date(r.checked_at * 1000).toLocaleString()}</p>`;
          } finally {
            b.disabled = false;
          }
        })),
    );
    wireNavigation();
  }

  function storageForm(existing) {
    const s = existing || {
      id: "",
      name: "",
      environment_id: state.config.environments[0]?.id || "",
      description: "",
      locations: { data_root: "", model_root: "", artifact_root: "" },
      default_model: "",
      default_revision: "",
      version: 0,
    };
    $("#storage-editor").innerHTML =
      `<div class="divider"></div><form id="storage-form"><h3>${existing ? "저장소 프로필 수정" : "저장소 프로필 등록"}</h3><div class="fields"><label>ID<input name="id" value="${esc(s.id)}" required ${existing ? "readonly" : ""} pattern="[a-zA-Z0-9_-]{1,80}"></label><label>이름<input name="name" value="${esc(s.name)}" maxlength="100" required></label><label class="full">실행 환경<select name="environment_id" required>${state.config.environments.map((e) => option(e.id, `${e.name} · ${e.backend}`, s.environment_id)).join("")}</select></label></div><button id="storage-defaults" type="button" ${canRun() ? "" : "disabled"}>실행기 경로 설정 불러오기</button><div id="storage-options"></div><h3>파일 저장 위치</h3><label>학습·검증 데이터 루트<input name="data_root" value="${esc(s.locations.data_root)}" required maxlength="800"></label><label id="storage-model-root">기본 모델 루트<input name="model_root" value="${esc(s.locations.model_root)}" maxlength="800"></label><label>학습 결과 루트<input name="artifact_root" value="${esc(s.locations.artifact_root)}" required maxlength="800"></label><p id="storage-path-help" class="helper"></p><details><summary>기본 모델 입력값 · 선택</summary><p class="helper">학습 레시피에서 프로필을 선택할 때 채워집니다. 생성 모델용·임베딩 모델용 프로필을 각각 둘 수 있습니다.</p><div class="fields"><label>기본 모델 ID<input name="default_model" value="${esc(s.default_model)}" placeholder="조직/모델 ID"></label><label>기본 revision<input name="default_revision" value="${esc(s.default_revision)}" pattern="[a-fA-F0-9]{40}" placeholder="40자리 commit SHA"></label></div></details><label>설명<textarea name="description" rows="2" maxlength="2000">${esc(s.description)}</textarea></label><p class="helper">저장은 설정을 보관합니다. 저장 후 접근 확인을 진행하세요. 변경 사항은 새 실행부터 적용됩니다.</p><button class="primary" ${canManage() ? "" : "disabled"}>프로필 저장</button></form>`;
    const form = $("#storage-form");
    function syncMode() {
      const mounted =
        state.config.environments.find(
          (e) => e.id === form.elements.environment_id.value,
        )?.backend === "http";
      $("#storage-model-root").hidden = !mounted;
      form.elements.model_root.required = mounted;
      $("#storage-path-help").textContent = mounted
        ? "워커의 절대 경로를 입력하세요. 기본 모델은 {모델 루트}/{조직--모델 ID}/{revision} 구조로 읽으며 결과는 {결과 루트}/{실행 ID}/model에 저장합니다."
        : "데이터·결과 루트에 버킷과 경로를 포함한 URI를 입력하세요. 기본 모델은 Hugging Face의 지정 revision에서 Pod로 내려받습니다.";
    }
    syncMode();
    on("#storage-form select", "change", () => {
      syncMode();
      $("#storage-options").innerHTML = "";
    });
    on("#storage-defaults", "click", async () => {
      const r = await api(
        path(
          `storage-options/${encodeURIComponent(form.elements.environment_id.value)}`,
        ),
      );
      if (r.defaults?.mode === "mounted")
        for (const key of ["data_root", "model_root", "artifact_root"])
          form.elements[key].value = r.defaults[key];
      $("#storage-options").innerHTML = r.allowed_roots
        ? `<p class="helper">실행기가 허용하는 경로와 그 하위 폴더를 사용할 수 있습니다.</p><dl class="lab-facts">${Object.entries(
            r.allowed_roots,
          )
            .map(
              ([key, values]) =>
                `<dt>${esc({ data_root: "학습·검증 데이터", model_root: "기본 모델", artifact_root: "학습 결과" }[key] || key)}</dt><dd class="mono lab-path">${values.map(esc).join("<br>")}</dd>`,
            )
            .join("")}</dl>`
        : `<p class="helper">${esc(r.message)}</p>`;
    });
    on("#storage-form", "submit", async (e) => {
      e.preventDefault();
      const v = Object.fromEntries(new FormData(form)),
        mounted =
          state.config.environments.find((env) => env.id === v.environment_id)
            ?.backend === "http";
      const locations = {
        mode: mounted ? "mounted" : "object",
        data_root: v.data_root,
        model_root: mounted ? v.model_root : "",
        artifact_root: v.artifact_root,
      };
      delete v.data_root;
      delete v.model_root;
      delete v.artifact_root;
      await api(path(`storage/${encodeURIComponent(v.id)}`), "PUT", {
        ...v,
        locations,
        version: s.version,
      });
      await navigate("storage");
      toast(
        "저장소 프로필을 저장했습니다. 접근 확인 후 학습 레시피에서 선택하세요.",
      );
    });
  }
  async function launchTraining(recipe, requestId) {
    if (!lab.experiment) throw Error("학습을 연결할 실험을 먼저 만드세요.");
    const run = await api(path("train"), "POST", {
      recipe,
      request_id: requestId,
      experiment_id: lab.experiment,
    });
    lab.run = run.id;
    await navigate("run");
    return run;
  }
  const evaluationDagId = (id) => "model_lab_eval_" + id.replaceAll("-", "_");
  async function refreshEndpoint(element, spec) {
    if (!element?.isConnected || element.dataset.loading === "1") return;
    element.dataset.loading = "1";
    element.innerHTML = '<p class="helper" role="status">접속 정보와 DAG 설정을 확인하고 있습니다…</p>';
    try {
      const report = await api(`lab/endpoints/${encodeURIComponent(spec.id)}/status`);
      if (!element.isConnected) return;
      const dag = report.dag, check = report.settings;
      element.innerHTML = `<dl class="lab-connection-status"><div><dt>접속 정보 출처</dt><dd>${esc(report.connection.source_label)}</dd></div><div><dt>DAG 운영 상태</dt><dd>${dag.exists === false ? "아직 반영되지 않음" : dag.is_paused == null ? "조회 불가" : dag.is_paused ? "일시중지" : "활성"}</dd></div><div><dt>평가 설정 점검</dt><dd>${check.state === "ready" ? "설정 일치" : check.state === "blocked" ? "설정 확인 필요" : "조회 불가"}</dd></div></dl><p class="helper">${esc(check.message)}</p><div class="actions">${report.can_change_state ? `<button data-endpoint-state>${dag.is_paused ? "활성화" : "일시중지"}</button>` : dag.exists && !report.can_edit ? '<span class="helper">운영 상태 변경 권한이 없습니다.</span>' : ""}<button data-check-again>상태 새로고침</button><span class="helper">확인 ${esc(formatDate(report.checked_at))}</span></div>`;
      $("[data-endpoint-state]", element)?.addEventListener("click", guarded(async () => {
        await WorkbenchUI.changeDagState(dag.id);
        await refreshEndpoint(element, spec);
      }));
      $("[data-check-again]", element).onclick = () => refreshEndpoint(element, spec);
    } catch (error) {
      if (element.isConnected) {
        element.innerHTML = `<p class="notice" role="alert">${esc(error.message)}</p><button data-check-again>다시 확인</button>`;
        $("[data-check-again]", element).onclick = () => refreshEndpoint(element, spec);
      }
    } finally {
      delete element.dataset.loading;
    }
  }
  function settings() {
    $("#lab-content").innerHTML =
      card(
        "추론 연결",
        `<p>사용할 서버의 접속 정보와 평가 DAG를 연결합니다. DAG를 활성화한 뒤에도 평가 요청 시 설정을 다시 점검합니다.</p><div class="actions"><button id="new-endpoint" class="primary" ${lab.bootstrap.can_admin ? "" : "disabled"}>추론 연결 등록</button>${lab.bootstrap.can_admin ? '<a target="_top" href="/connections">Airflow 접속 정보 관리 ↗</a>' : ""}</div><p class="helper">환경변수·외부 Secrets Backend에서 공급한 접속 정보는 Airflow Connections 목록에 나타나지 않을 수 있습니다.</p><div class="lab-endpoints">${endpointList().map((e) => `<article class="lab-endpoint"><div class="section-heading"><h3>${esc(e.name)}</h3><span class="helper">${esc(e.provider)} · ${esc(e.capabilities.join(", "))}</span></div><dl class="lab-connection-status"><div><dt>추론 연결 ID</dt><dd class="mono">${esc(e.id)}</dd></div><div><dt>Airflow 접속 정보 ID</dt><dd class="mono">${esc(e.connection_id || "기본 Ollama 환경 설정")}</dd></div><div><dt>평가 DAG</dt><dd><a class="mono" target="_top" href="/dags/${encodeURIComponent(evaluationDagId(e.id))}">${esc(evaluationDagId(e.id))} ↗</a></dd></div></dl><details><summary>파일·자원 설정</summary><p class="helper">DAG 파일: <code>evaluation_${esc(e.id)}.py</code><br>Pool: ${esc(e.pool)} · 동시 실행 ${e.capacity}개</p></details><div data-endpoint-status="${esc(e.id)}"><button data-initial-check="${esc(e.id)}">상태 확인</button></div><div class="actions"><button data-edit-endpoint="${esc(e.id)}" ${lab.bootstrap.can_admin ? "" : "disabled"}>연결 설정</button><button data-prepare="${esc(e.id)}" ${lab.bootstrap.can_admin ? "" : "disabled"}>DAG 생성·갱신</button><button data-endpoint-models="${esc(e.id)}">모델 목록 확인</button></div><div data-model-feedback="${esc(e.id)}" class="helper" role="status"></div></article>`).join("") || '<p class="empty">추론 연결을 등록하면 연결된 DAG와 상태를 여기서 확인할 수 있습니다.</p>'}</div><div id="endpoint-editor"></div>`,
      ) +
      card(
        "학습 실행 환경",
        `<p>HTTP Job API 또는 Kubernetes 환경을 등록하고 실행기 준비 상태를 확인합니다.</p><button id="manage-environments">실행 환경 관리 열기</button>`,
      ) +
      (lab.project
        ? card(
            "프로젝트 접근 권한",
            `<p class="helper">Airflow 계정 ID에 viewer / developer / manager 역할을 부여합니다. 실험 실행에는 대상 DAG의 Airflow 권한도 필요합니다.</p><form id="members-form"><label>프로젝트 구성원 JSON<textarea name="members" rows="4" class="mono">${asJSON(project().members)}</textarea></label><button ${canManage() ? "" : "disabled"}>구성원 저장</button></form>`,
          )
        : "") +
      card(
        "이전 실험 기록",
        `<p>이전 생성·임베딩 테스트와 프리셋은 원본을 유지합니다. 신규 프로젝트 실험과 구분해 조회할 수 있습니다.</p><button id="legacy-history">이전 기록 조회</button>`,
      );
    on("#new-endpoint", "click", () => endpointForm());
    document
      .querySelectorAll("[data-edit-endpoint]")
      .forEach(
        (b) =>
          (b.onclick = guarded(() =>
            endpointForm(
              endpointList().find((e) => e.id === b.dataset.editEndpoint),
            ))),
      );
    const statusElements = [...document.querySelectorAll("[data-endpoint-status]")];
    for (const element of statusElements) {
      const spec = endpointList().find((e) => e.id === element.dataset.endpointStatus);
      $("[data-initial-check]", element).onclick = () => refreshEndpoint(element, spec);
    }
    // Bound initial requests; remaining connections can be checked explicitly.
    void (async () => {
      for (const element of statusElements.slice(0, 8)) {
        if (!element.isConnected) break;
        await refreshEndpoint(element, endpointList().find((e) => e.id === element.dataset.endpointStatus));
      }
    })();
    document.querySelectorAll("[data-endpoint-models]").forEach((button) => {
      button.onclick = guarded(async () => {
        button.disabled = true;
        const output = [...document.querySelectorAll("[data-model-feedback]")].find((el) => el.dataset.modelFeedback === button.dataset.endpointModels);
        try {
          const result = await api(`lab/endpoints/${encodeURIComponent(button.dataset.endpointModels)}/models`);
          output.textContent = result.models.length ? result.models.map((m) => typeof m === "string" ? m : m.name || m.id || "모델").join(", ") : "서버에 등록된 모델이 없습니다.";
        } catch (error) { output.textContent = error.message; }
        finally { button.disabled = false; }
      });
    });
    for (const action of ["prepare"])
      document.querySelectorAll(`[data-${action}]`).forEach(
        (b) =>
          (b.onclick = guarded(async () => {
            b.disabled = true;
            try {
              const r = await api(
                `lab/endpoints/${encodeURIComponent(b.dataset[action])}/${action}`,
                "POST",
              );
              toast(r.message || "DAG 파일을 게시했습니다. 목록 반영 후 상태를 확인하세요.");
              const element = statusElements.find((el) => el.dataset.endpointStatus === b.dataset[action]);
              await refreshEndpoint(element, endpointList().find((e) => e.id === b.dataset[action]));
            } finally {
              b.disabled = false;
            }
          })),
      );
    on("#manage-environments", "click", async () => {
      state.tab = "operations";
      await MLOpsUI.render();
    });
    on("#legacy-history", "click", renderHistory);
    on("#members-form", "submit", async (e) => {
      e.preventDefault();
      const p = project();
      await api(`lab/projects/${encodeURIComponent(p.id)}`, "PUT", {
        id: p.id,
        name: p.name,
        description: p.description,
        members: JSON.parse(e.target.elements.members.value),
        version: p.version,
      });
      await navigate("settings");
    });
  }
  async function endpointForm(existing) {
    const editor = $("#endpoint-editor"), request = ++lab.endpointEditorRequest;
    editor.innerHTML = '<p class="helper">선택할 수 있는 접속 정보를 불러오고 있습니다…</p>';
    let catalog;
    try { catalog = await api("lab/connection-options"); }
    catch (error) { catalog = { connections: [], message: error.message + " 접속 정보 ID를 직접 입력할 수 있습니다." }; }
    if (!editor.isConnected || request !== lab.endpointEditorRequest) return;
    const value = existing || {
      id: "",
      name: "",
      provider: "ollama",
      connection_id: "",
      capabilities: ["generation", "embedding"],
      models: [],
      pool: "model_lab_gpu",
      capacity: 1,
      timeout_seconds: 600,
      reservation_environment: "",
      version: 0,
    };
    $("#endpoint-editor").innerHTML =
      `<div class="divider"></div><form id="endpoint-form"><h3>${existing ? "추론 연결 설정" : "추론 연결 등록"}</h3><p class="helper">평가 설정의 이름과 서버 접속 정보는 역할이 다릅니다. 하나의 접속 정보를 여러 평가 DAG가 함께 사용할 수 있습니다.</p><div class="fields"><label>표시 이름<input name="name" value="${esc(value.name)}" required placeholder="예: 개발용 Ollama"></label><label>제공자<select name="provider">${["ollama", "openai", "gemini", "evaluation_api"].map((v) => option(v, v, value.provider)).join("")}</select></label><label>등록된 접속 정보 선택<select id="connection-choice">${option("", "직접 입력 / 목록에 없는 접속 정보", "")}${catalog.connections.map((c) => option(c.connection_id, `${c.connection_id} · ${c.source_label}`, value.connection_id)).join("")}</select></label><label>Airflow 접속 정보 ID<input name="connection_id" value="${esc(value.connection_id)}" placeholder="예: model_lab_ollama" pattern="[a-zA-Z0-9_-]{0,128}"></label></div><p class="helper">${esc(catalog.message)}</p><p id="connection-choice-note" class="helper" role="status"></p><div class="fields"><label>추론 연결 ID<input name="id" value="${esc(value.id)}" required ${existing ? "readonly" : ""} pattern="[a-zA-Z0-9_-]{1,80}" placeholder="예: ollama"></label></div><p class="helper">이 평가 설정을 구분하는 고유 이름입니다. 접속 정보 ID와 같아도 됩니다. 저장 후에는 변경할 수 없습니다.</p><div id="endpoint-dag-preview" class="notice info" aria-live="polite"></div><details><summary>평가 기능·자원 설정</summary><div class="fields"><label>지원 기능 · 쉼표 구분<input name="capabilities" value="${esc(value.capabilities.join(","))}" required></label><label>모델 목록 · 선택<input name="models" value="${esc(value.models.join(","))}" placeholder="API에서 목록을 제공하지 않는 경우"></label><label>자원 Pool<input name="pool" value="${esc(value.pool)}" required></label><label>동시 실행 수<input name="capacity" type="number" min="1" max="16" value="${value.capacity}"></label><label>요청 제한 시간 · 초<input name="timeout_seconds" type="number" min="10" max="1200" value="${value.timeout_seconds}"></label><label>공유 GPU 예약 · 실행기 Connection ID<input name="reservation_environment" value="${esc(value.reservation_environment)}" placeholder="공유 GPU에만 설정"></label></div><p class="helper">같은 GPU를 공유하는 학습·평가는 같은 Pool과 실행기 예약을 사용하세요. 이 예약에 참여하지 않는 챗봇 요청은 자동 제한되지 않습니다.</p></details><p class="helper">서버 주소와 인증 정보는 Airflow Connections 또는 설치 환경에서 관리합니다. OpenAI 호환 주소에는 /v1, Gemini는 /v1beta를 포함하세요.</p><button class="primary" ${lab.bootstrap.can_admin ? "" : "disabled"}>연결 저장</button></form>`;
    const form = $("#endpoint-form");
    const updatePreview = () => {
      const id = form.elements.id.value;
      $("#endpoint-dag-preview").textContent = id ? `연결될 DAG: ${evaluationDagId(id)} · 파일: evaluation_${id}.py` : "추론 연결 ID를 입력하면 생성할 DAG 이름이 표시됩니다.";
      const found = catalog.connections.find((c) => c.connection_id === form.elements.connection_id.value);
      $("#connection-choice-note").textContent = found ? `접속 정보 출처: ${found.source_label}. 서버 주소·인증 정보는 이 ID로 조회합니다.` : !form.elements.connection_id.value && existing?.id === "default-ollama" ? "기존 기본 Ollama 환경 설정을 사용합니다." : "직접 입력한 접속 정보 ID는 저장할 때 확인합니다. 외부 Secrets Backend의 ID도 입력할 수 있습니다.";
    };
    on("#connection-choice", "change", (event) => {
      if (!event.target.value) { form.elements.connection_id.focus(); return; }
      form.elements.connection_id.value = event.target.value;
      if (!existing && !form.elements.id.value) form.elements.id.value = event.target.value.replace(/^model_lab_/, "").slice(0, 80);
      updatePreview();
    });
    form.elements.id.addEventListener("input", updatePreview);
    form.elements.connection_id.addEventListener("input", () => {
      $("#connection-choice").value = catalog.connections.some((c) => c.connection_id === form.elements.connection_id.value) ? form.elements.connection_id.value : "";
      updatePreview();
    });
    updatePreview();
    editor.scrollIntoView({ behavior: "smooth", block: "start" });
    on("#endpoint-form", "submit", async (e) => {
      e.preventDefault();
      const f = Object.fromEntries(new FormData(e.target));
      await api(`lab/endpoints/${encodeURIComponent(f.id)}`, "PUT", {
        ...f,
        capabilities: f.capabilities
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean),
        models: f.models
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean),
        capacity: Number(f.capacity),
        timeout_seconds: Number(f.timeout_seconds),
        version: value.version,
      });
      toast("연결을 저장했습니다. DAG 생성·갱신 후 상태를 확인하세요.");
      await navigate("settings");
    });
  }
  async function start() {
    lab.active = true;
    document.addEventListener("input", (e) => {
      if (
        !e.target.closest("#lab-form, #training-form") ||
        !["dataset", "experiment", "training"].includes(lab.tab) ||
        !lab.project ||
        !canRun()
      )
        return;
      clearTimeout(lab.saveTimer);
      const savedProject = lab.project,
        savedTab = lab.tab;
      lab.saveTimer = setTimeout(() => {
        if (lab.project !== savedProject || lab.tab !== savedTab) return;
        capture();
        api(path(`draft/${savedTab}`), "PUT", {
          values: lab.drafts[savedProject + ":" + savedTab],
        }).catch(() => {});
      }, 800);
    });
    await load();
    render();
  }
  return {
    start,
    render,
    launchTraining,
    recipeStorage,
    restoreStoragePreset,
    presetPath: () => path("preset"),
    validateTraining: (recipe) =>
      api(path("training/validate"), "POST", recipe),
    get active() {
      return lab.active;
    },
  };
})();
