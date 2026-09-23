"use strict";
const {
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
} = WorkbenchUI;
const state = {
  config: null,
  models: [],
  presets: [],
  tab: "operations",
  busy: false,
  trainingReady: false,
  launchId: null,
  online: false,
};
const api = (route, ...args) =>
  WorkbenchUI.api(
    route === "presets" && typeof LabUI !== "undefined" && LabUI.active
      ? LabUI.presetPath()
      : route,
    ...args,
  );
function experimentTable(rows) {
  return table(
    ["실험", "유형", "상태", "시작", "실행자"],
    rows.map((r) => [
      `<button class="text-button experiment-detail" data-id="${esc(r.id)}">${esc(r.name)}</button>`,
      esc(r.kind),
      badge(r.status),
      formatDate(r.created),
      esc(r.actor),
    ]),
  );
}
function setConnection(online) {
  state.online = online;
}
function modelOptions(embedding = false, optional = false) {
  const names = state.models.map((m) => m.name),
    preferred = names.find((n) =>
      embedding ? /bge|embed|e5|gte/i.test(n) : !/bge|embed|e5|gte/i.test(n),
    );
  return (
    (optional
      ? '<option value="">비교 모델 없음</option>'
      : names.length
        ? ""
        : '<option value="">설치 모델 없음</option>') +
    names
      .map(
        (n) =>
          `<option value="${esc(n)}" ${!optional && n === preferred ? "selected" : ""}>${esc(n)}</option>`,
      )
      .join("")
  );
}
function numeric(name, label, value, min, max, step = 1) {
  return `<label>${label}<input name="${name}" type="number" value="${value}" min="${min}" max="${max}" step="${step}" required></label>`;
}
function presetSelect(kind) {
  return `<label>저장된 프리셋<select id="preset-select"><option value="">프리셋 불러오기</option>${state.presets
    .filter((p) => p.kind === kind)
    .map((p) => `<option value="${esc(p.id)}">${esc(p.name)}</option>`)
    .join("")}</select></label>`;
}
function renderLab() {
  if (typeof LabUI !== "undefined" && LabUI.active) return LabUI.render();
  $("#app").innerHTML =
    `<div class="page-heading"><div><p class="eyebrow">AIRFLOW WORKBENCH</p><h1>Model Lab <span id="connection" class="pill ${state.online ? "online" : ""}">${state.online ? "● Ollama connected" : "○ Ollama offline"}</span></h1><p class="description">실행 환경을 연결하고 학습을 오케스트레이션하며 모델 성능을 비교하세요.</p></div><button id="reload-models">모델 목록 새로고침</button></div><div class="model-strip">${state.models.length ? state.models.map((m) => `<div class="model-chip">◈ ${esc(m.name)}<small>${(m.size / 1e9).toFixed(2)} GB · ${esc(m.details?.quantization_level || "—")}</small></div>`).join("") : "<div class='notice'>Ollama 서버에 연결하거나 모델을 설치한 뒤 목록을 새로고침하세요.</div>"}</div><div class="tabs" role="tablist" aria-label="모델 실험 메뉴">${[
      ["generation", "생성 모델 비교"],
      ["embedding", "임베딩 평가"],
      ["training", "파인튜닝 설정"],
      ["operations", "실행·리소스"],
      ["history", "실험 이력"],
    ]
      .map(
        ([id, label]) =>
          `<button role="tab" aria-selected="${state.tab === id}" class="${state.tab === id ? "active" : ""}" data-tab="${id}" ${state.busy ? "disabled" : ""}>${label}</button>`,
      )
      .join("")}</div><div id="lab-content"></div>`;
  document.querySelectorAll("[data-tab]").forEach((b) =>
    b.addEventListener("click", () => {
      state.tab = b.dataset.tab;
      renderLab();
    }),
  );
  on("#reload-models", "click", async () => {
    const r = await api("models");
    state.models = r.models;
    setConnection(r.online);
    renderLab();
  });
  const showModels = ["generation", "embedding"].includes(state.tab);
  for (const selector of ["#connection", "#reload-models", ".model-strip"])
    $(selector).hidden = !showModels;
  if (state.tab === "generation") renderGeneration();
  if (state.tab === "embedding") renderEmbedding();
  if (state.tab === "training") renderTraining();
  if (state.tab === "operations") guarded(MLOpsUI.render)();
  if (state.tab === "history") guarded(renderHistory)();
}
function renderGeneration() {
  $("#lab-content").innerHTML =
    `<form id="experiment-form" class="lab-layout"><aside class="card"><div class="section-heading"><h2>실험 설정</h2><span class="pill">OLLAMA</span></div>${presetSelect("generation")}<label>실험 이름<input name="name" value="모델 기준선 비교" maxlength="100" required></label><label>기본 모델<select name="model_a">${modelOptions()}</select></label><label>비교 모델 · 선택<select name="model_b">${modelOptions(false, true)}</select></label><p class="helper">Ollama에 등록한 파인튜닝 모델도 선택할 수 있습니다.</p><div class="divider"></div><h3>생성 파라미터</h3><div class="fields">${numeric("temperature", "Temperature", 0.2, 0, 2, 0.05)}${numeric("top_p", "Top P", 0.9, 0.01, 1, 0.01)}${numeric("top_k", "Top K", 40, 1, 100)}${numeric("num_ctx", "Context length", 4096, 512, 16384, 512)}${numeric("num_predict", "Max tokens", 256, 16, 2048, 16)}${numeric("repeat_penalty", "Repeat penalty", 1.1, 0.5, 2, 0.05)}${numeric("seed", "Seed", 42, 0, 2147483647)}</div><label class="check"><input type="checkbox" name="json_mode">JSON 출력 모드</label><button type="button" id="save-preset">현재 설정을 프리셋으로 저장</button></aside><section class="card"><div class="section-heading"><h2>프롬프트 플레이그라운드</h2><span class="muted">동일 입력 · 순차 실행</span></div><label>시스템 프롬프트<textarea name="system" rows="3" maxlength="8000">한국어로 정확하고 간결하게 답하세요. 모르는 사실은 추측하지 마세요.</textarea></label><label>사용자 질문<textarea name="prompt" rows="6" maxlength="12000" required placeholder="모델에 보낼 질문을 입력하세요.">검색 증강 생성(RAG)의 장단점을 두 문장으로 설명하세요.</textarea></label><div class="actions"><button class="primary" type="submit" id="run-experiment">▶ 비교 실행</button><span class="muted">설정과 응답은 실험 이력에 저장됩니다.</span></div><div id="experiment-result">${empty("실행하면 모델별 응답·지연시간·생성 속도를 비교할 수 있습니다.")}</div></section></form>`;
  bindExperimentForm("generation");
}
function renderEmbedding() {
  const examples = [
    "* 비밀번호를 잊었다면 로그인 화면에서 비밀번호 재설정을 선택하고 이메일 인증을 진행하세요.",
    "알림 수신 방법은 계정 설정의 알림 메뉴에서 변경할 수 있습니다.",
    "* 비밀번호 재설정 이메일이 도착하지 않으면 스팸함과 등록한 이메일 주소를 확인하세요.",
    "작업 내역은 실행 이력 메뉴에서 기간별로 조회하고 내려받을 수 있습니다.",
  ].join("\n");
  $("#lab-content").innerHTML =
    `<form id="experiment-form" class="lab-layout"><aside class="card"><div class="section-heading"><h2>검색 실험 설정</h2><span class="pill">EMBEDDING</span></div>${presetSelect("embedding")}<label>실험 이름<input name="name" value="임베딩 검색 기준선" maxlength="100" required></label><label>임베딩 모델<select name="model_a">${modelOptions(true)}</select></label><label>비교 임베딩 모델 · 선택<select name="model_b">${modelOptions(true, true)}</select></label>${numeric("k", "검색 Top K", 3, 1, 40)}<div class="notice info">각 모델이 질문과 문서를 함께 임베딩합니다. 서로 다른 모델의 벡터를 섞지 않습니다.</div><button type="button" id="save-preset">현재 설정을 프리셋으로 저장</button></aside><section class="card"><h2>검색 품질 플레이그라운드</h2><p class="muted">문서마다 한 줄을 사용하고, 정답 문서는 줄 앞에 <strong>* </strong>를 붙이세요.</p><label>검색 질문<textarea name="query" rows="2" maxlength="2000" required>비밀번호를 잊었는데 어떻게 재설정하나요?</textarea></label><label>후보 문서 · 한 줄에 한 문서<textarea name="documents" rows="9" required>${esc(examples)}</textarea></label><p class="helper">위 문서는 기능 확인용 가상 예시입니다. 정답을 지정하지 않으면 유사도 순위만 표시합니다.</p><div class="actions"><button class="primary" type="submit" id="run-experiment">▶ 검색 평가 실행</button></div><div id="experiment-result">${empty("코사인 유사도·벡터 차원과 Recall@K / nDCG@K / MRR@K를 확인하세요.")}</div></section></form>`;
  bindExperimentForm("embedding");
}
function readExperiment(kind) {
  const v = Object.fromEntries(new FormData($("#experiment-form"))),
    s = {
      name: v.name,
      models: [v.model_a, ...(v.model_b ? [v.model_b] : [])],
    };
  if (kind === "generation")
    return {
      ...s,
      prompt: v.prompt,
      system: v.system,
      json_mode: !!v.json_mode,
      options: Object.fromEntries(
        [
          "temperature",
          "top_p",
          "top_k",
          "num_ctx",
          "num_predict",
          "repeat_penalty",
          "seed",
        ].map((k) => [k, Number(v[k])]),
      ),
    };
  const lines = v.documents
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter(Boolean);
  return {
    ...s,
    query: v.query,
    documents: lines.map((s) => (s.startsWith("* ") ? s.slice(2).trim() : s)),
    relevant_indices: lines
      .map((s, i) => (s.startsWith("* ") ? i : -1))
      .filter((i) => i >= 0),
    k: Number(v.k),
  };
}
function loadPreset(kind, config) {
  const form = $(kind === "training" ? "#training-form" : "#experiment-form");
  let values = { ...config };
  if (kind !== "training")
    values = {
      ...config,
      ...config.options,
      model_a: config.models[0],
      model_b: config.models[1] || "",
    };
  if (kind === "embedding")
    values.documents = config.documents
      .map((s, i) => (config.relevant_indices.includes(i) ? "* " : "") + s)
      .join("\n");
  for (const [key, value] of Object.entries(values)) {
    const f = form.elements.namedItem(key);
    if (!f) continue;
    if (f.type === "checkbox") f.checked = !!value;
    else f.value = value;
  }
  if (kind === "training") {
    invalidateTraining();
    if (typeof LabUI !== "undefined" && LabUI.active)
      LabUI.restoreStoragePreset(config);
  }
  toast("프리셋을 불러왔습니다.");
}
function bindPresets(kind, reader) {
  on("#preset-select", "change", (e) => {
    const p = state.presets.find((p) => p.id === e.target.value);
    if (p) loadPreset(kind, p.config);
  });
  on("#save-preset", "click", async () => {
    const f = $(kind === "training" ? "#training-form" : "#experiment-form");
    if (!f.reportValidity()) return;
    const config = reader(),
      saved = await api("presets", "POST", { name: config.name, kind, config });
    state.presets.unshift(saved);
    const o = document.createElement("option");
    o.value = saved.id;
    o.textContent = saved.name;
    $("#preset-select").append(o);
    toast("프리셋을 저장했습니다.");
  });
  if (!state.config.can_edit) $("#save-preset").disabled = true;
}
function bindExperimentForm(kind) {
  bindPresets(kind, () => readExperiment(kind));
  if (!state.config.can_edit || !state.models.length)
    $("#run-experiment").disabled = true;
  on("#experiment-form", "submit", async (e) => {
    e.preventDefault();
    if (state.busy) return;
    const spec = readExperiment(kind);
    state.busy = true;
    const b = $("#run-experiment");
    b.disabled = true;
    document
      .querySelectorAll("[data-tab],#reload-models")
      .forEach((b) => (b.disabled = true));
    $("#experiment-result").innerHTML =
      '<div class="notice info busy" role="status">모델 실행 중… 최초 로딩은 수 분 걸릴 수 있습니다. 결과가 나오면 자동으로 저장됩니다.</div>';
    try {
      showResult(
        await api(`experiments/${kind}`, "POST", spec),
        $("#experiment-result"),
      );
    } catch (error) {
      $("#experiment-result").innerHTML =
        `<div class="notice">${esc(error.message)}<br>연결이 중단된 경우 실험 이력에서 상태를 확인하세요.</div>`;
    } finally {
      state.busy = false;
      b.disabled = false;
      document
        .querySelectorAll("[data-tab],#reload-models")
        .forEach((b) => (b.disabled = false));
    }
  });
}
function showResult(record, target) {
  if (record.status !== "success") {
    target.innerHTML = `<div class="notice">${badge(record.status)} ${esc(record.error || "실행 중입니다. 이력을 새로고침하세요.")}</div>`;
    return;
  }
  target.innerHTML = `<div class="output-grid">${record.result.models
    .map(
      (m) =>
        `<article class="output"><h3>${esc(m.model)}</h3><div class="mono muted">${esc(m.digest?.slice(0, 16))}…</div><div class="metric-row"><div><strong>${m.latency_seconds}s</strong><span>전체 지연시간</span></div>${record.kind === "generation" ? `<div><strong>${m.tokens_per_second ?? "—"}</strong><span>tokens / sec</span></div><div><strong>${m.tokens}</strong><span>출력 tokens</span></div>` : `<div><strong>${m.dimension}</strong><span>벡터 차원</span></div>`}</div>${
          record.kind === "generation"
            ? `<pre>${esc(m.output)}</pre>${m.done_reason === "length" ? '<p class="notice">최대 토큰에 도달해 응답이 잘렸습니다.</p>' : ""}`
            : `${
                m.metrics
                  ? `<div class="metric-row">${Object.entries(m.metrics)
                      .map(
                        ([k, v]) =>
                          `<div><strong>${v.toFixed(3)}</strong><span>${esc(k.replaceAll("_at_k", `@${m.k}`))}</span></div>`,
                      )
                      .join("")}</div>`
                  : '<p class="muted">정답 미지정 · 품질 점수 없음</p>'
              }${table(
                ["순위", "문서", "유사도"],
                m.ranking
                  .slice(0, m.k)
                  .map((d, i) => [
                    String(i + 1),
                    `${d.relevant ? "★ " : ""}${esc(d.text)}`,
                    d.score.toFixed(4),
                  ]),
              )}`
        }</article>`,
    )
    .join(
      "",
    )}</div><p class="helper">${esc(record.result.note)}</p><button class="download-result">실험 JSON 내보내기</button>`;
  $(".download-result", target).addEventListener("click", () =>
    download(`experiment-${record.id}.json`, record),
  );
}
function readRecipe() {
  const v = Object.fromEntries(new FormData($("#training-form")));
  for (const k of [
    "epochs",
    "learning_rate",
    "batch_size",
    "gradient_accumulation_steps",
    "max_seq_length",
    "lora_rank",
    "lora_alpha",
    "lora_dropout",
    "warmup_ratio",
    "weight_decay",
    "seed",
  ])
    v[k] = Number(v[k]);
  return typeof LabUI !== "undefined" && LabUI.active
    ? LabUI.recipeStorage(v)
    : v;
}
function invalidateTraining() {
  state.trainingReady = false;
  state.launchId = null;
  $("#launch-training").disabled = true;
  $("#training-report").innerHTML = empty(
    "설정 검증을 실행하면 데이터와 학습 DAG 준비 상태를 확인할 수 있습니다.",
  );
}
function renderTraining() {
  const datasets = [];
  const environments = state.config.environments;
  $("#lab-content").innerHTML =
    `<div class="notice info">선택한 실행 환경에서 데이터·가중치·자원을 검사하고 해당 DAG에 학습을 요청합니다. Airflow 서버에 학습 데이터나 GPU를 둘 필요가 없습니다.</div><div class="training-grid"><form class="card" id="training-form"><div class="section-heading"><h2>파인튜닝 레시피</h2><span class="pill">RECIPE v2</span></div>${presetSelect("training")}<label>실행 환경<select name="environment_id" required>${environments.map((e) => `<option value="${e.id}" ${e.id === state.environmentId ? "selected" : ""}>${esc(e.name)} · ${e.backend}</option>`).join("")}</select></label><label>레시피 이름<input name="name" maxlength="100" value="모델 파인튜닝" required></label><div class="fields"><label>학습 대상<select name="task"><option value="llm_sft">생성 모델 · SFT</option><option value="embedding_contrastive">임베딩 · Contrastive</option></select></label><label>학습 방식<select name="method"><option value="qlora">QLoRA · 4-bit</option><option value="lora">LoRA</option><option value="full">Full · Embedding</option></select></label><label class="full">Hugging Face 원본 모델<input name="base_model" placeholder="조직/모델 ID" maxlength="160" required></label><label class="full">원본 revision · 고정 commit SHA<input name="revision" placeholder="원본 모델의 40자리 commit SHA" pattern="[a-fA-F0-9]{40}" required></label><label>학습 데이터<input name="train_dataset" list="dataset-list" value="train.jsonl" required></label><label>검증 데이터<input name="validation_dataset" list="dataset-list" value="validation.jsonl" required></label><datalist id="dataset-list">${datasets.map((d) => `<option value="${esc(d.name)}">`).join("")}</datalist></div>${datasets.length ? "" : '<p class="notice">Job API: 실행기 데이터 이름 또는 지원하는 저장소 URI. Kubernetes: 이미지가 지원하는 저장소 URI와 SHA256을 입력하세요. 데이터는 실행 대상이 직접 읽고 검증합니다.</p>'}<div class="fields"><label>학습 SHA256 · 원격 데이터<input name="train_sha256" pattern="[a-f0-9]{64}" placeholder="64자리 SHA256"></label><label>검증 SHA256 · 원격 데이터<input name="validation_sha256" pattern="[a-f0-9]{64}" placeholder="64자리 SHA256"></label><label class="full">결과 저장 URI<input name="artifact_uri" placeholder="s3://bucket/models/experiment/"></label></div><div class="divider"></div><h3>학습 파라미터</h3><div class="fields">${numeric("epochs", "Epochs", 1, 0.1, 20, 0.1)}${numeric("learning_rate", "Learning rate", 0.0002, 0.000001, 0.01, 0.000001)}${numeric("batch_size", "Batch size", 1, 1, 64)}${numeric("gradient_accumulation_steps", "Gradient accumulation", 16, 1, 128)}${numeric("max_seq_length", "Max sequence length", 1024, 128, 8192, 128)}${numeric("seed", "Seed", 42, 0, 2147483647)}${numeric("lora_rank", "LoRA rank", 16, 4, 128)}${numeric("lora_alpha", "LoRA alpha", 32, 4, 256)}${numeric("lora_dropout", "LoRA dropout", 0.05, 0, 0.5, 0.01)}${numeric("warmup_ratio", "Warmup ratio", 0.03, 0, 0.3, 0.01)}${numeric("weight_decay", "Weight decay", 0.01, 0, 1, 0.01)}</div><label>결과 모델 이름<input name="output_name" value="model-adapter-v1" pattern="[a-zA-Z0-9_-]{1,80}" required></label><div class="actions"><button type="submit" class="primary" id="validate-training">설정 검증</button><button type="button" id="save-preset">프리셋 저장</button><button type="button" id="export-recipe">레시피 내보내기</button></div></form><div class="card"><div class="section-heading"><h2>학습 준비 상태</h2><span class="pill">PREFLIGHT</span></div><p class="muted">선택한 환경의 DAG와 Pool, 실행기 준비 상태를 검증합니다. 실행 환경 설정을 바꾸면 DAG도 다시 배포해야 합니다.</p><div id="training-report"></div><div class="divider"></div><button id="launch-training" class="primary" disabled>학습 DAG 실행 요청</button><p class="helper">실행 전 데이터를 다시 검증합니다. DAG 로그와 학습 결과는 Airflow 실행 화면에서 확인합니다.</p><div class="divider"></div><h3>모델 개선 순서</h3><p class="muted">01 &nbsp; 기본 모델 기준선 기록<br>02 &nbsp; 학습·검증 데이터 분리<br>03 &nbsp; 파인튜닝 및 adapter 산출<br>04 &nbsp; 추론 환경에 맞춰 후보 모델 준비<br>05 &nbsp; 동일 평가 입력으로 기본 모델과 비교</p><p class="helper">이 화면의 실험은 챗봇 서비스의 모델 설정이나 기존 문서 벡터를 바꾸지 않습니다.</p></div></div>`;
  invalidateTraining();
  bindPresets("training", readRecipe);
  const syncTrainingCapabilities = () => {
    const form = $("#training-form");
    const environment = state.config.environments.find(
      (e) => e.id === form.elements.environment_id.value,
    );
    for (const option of form.elements.task.options)
      option.disabled = !environment?.workloads.includes(option.value);
    if (form.elements.task.selectedOptions[0]?.disabled)
      form.elements.task.value =
        Array.from(form.elements.task.options).find((o) => !o.disabled)
          ?.value || "";
    const embedding = form.elements.task.value === "embedding_contrastive";
    for (const option of form.elements.method.options)
      option.disabled = embedding
        ? option.value !== "full"
        : option.value === "full";
    if (form.elements.method.selectedOptions[0]?.disabled)
      form.elements.method.value = embedding ? "full" : "qlora";
    for (const key of ["lora_rank", "lora_alpha", "lora_dropout"])
      form.elements[key].closest("label").hidden = embedding;
  };
  window.syncTrainingCapabilities = syncTrainingCapabilities;
  on("#training-form", "change", syncTrainingCapabilities);
  syncTrainingCapabilities();
  on("#training-form", "input", invalidateTraining);
  on("#training-form", "change", invalidateTraining);
  on("#training-form", "submit", async (e) => {
    e.preventDefault();
    const r =
      typeof LabUI !== "undefined" && LabUI.active
        ? await LabUI.validateTraining(readRecipe())
        : await api("training/validate", "POST", readRecipe());
    state.trainingReady = r.ready;
    state.launchId = crypto.randomUUID();
    $("#launch-training").disabled = !r.ready;
    $("#training-report").innerHTML =
      `<div class="notice ${r.ready ? "info" : ""}">${r.ready ? "검증 완료 · 학습 DAG에 실행을 요청할 수 있습니다." : r.blockers.map(esc).join("<br>")}</div><p class="muted">실효 batch size: ${r.effective_batch_size}<br>recipe SHA: <span class="mono">${esc(r.recipe_sha256.slice(0, 16))}…</span></p><pre class="code">${esc(JSON.stringify(r.recipe, null, 2))}</pre>`;
  });
  on("#export-recipe", "click", () => {
    if (!$("#training-form").reportValidity()) return;
    download("training-recipe.json", {
      contract_version: 2,
      recipe: readRecipe(),
    });
  });
  on("#launch-training", "click", async () => {
    if (!state.trainingReady) return;
    const b = $("#launch-training");
    b.disabled = true;
    try {
      if (typeof LabUI !== "undefined" && LabUI.active) {
        await LabUI.launchTraining(readRecipe(), state.launchId);
        state.trainingReady = false;
        return;
      }
      const r = await api("training/launch", "POST", {
        recipe: readRecipe(),
        request_id: state.launchId,
      });
      $("#training-report").innerHTML =
        `<div class="notice info">학습 실행을 요청했습니다. ${badge(r.state)}<br><a target="_top" href="/dags/${encodeURIComponent(r.dag_id)}/runs/${encodeURIComponent(r.dag_run_id)}">Airflow 실행 확인 →</a></div>`;
      state.trainingReady = false;
    } catch (e) {
      b.disabled = false;
      throw e;
    }
  });
  if (!state.config.can_edit) $("#validate-training").disabled = true;
}
async function renderHistory() {
  $("#lab-content").innerHTML =
    `<div class="card"><div class="section-heading"><h2>실험 이력</h2><button id="refresh-history">새로고침</button></div><p class="muted">최근 100건 · 실험을 선택하면 입력 설정과 결과를 다시 확인할 수 있습니다.</p><div id="history-list">${empty("실험 기록을 불러오는 중입니다…")}</div><div id="history-detail"></div></div>`;
  on("#refresh-history", "click", renderHistory);
  $("#history-list").innerHTML = experimentTable(await api("experiments"));
  bindExperimentDetails();
}
function bindExperimentDetails() {
  document.querySelectorAll(".experiment-detail").forEach((b) =>
    b.addEventListener(
      "click",
      guarded(async () => {
        const r = await api(`experiments/${b.dataset.id}`);
        const target = $("#history-detail");
        target.innerHTML = `<div class="divider"></div><h2>${esc(r.name)}</h2><details><summary>실험 입력과 파라미터</summary><pre class="code">${esc(JSON.stringify(r.config, null, 2))}</pre></details><div id="history-result"></div>`;
        showResult(r, $("#history-result"));
      }),
    ),
  );
}
