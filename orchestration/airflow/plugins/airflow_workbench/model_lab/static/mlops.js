"use strict";
const MLOpsUI = (() => {
  let timer,
    serial = 0;
  const labels = {
    diagnostic: "자원 진단",
    llm_sft: "생성 모델 SFT",
    embedding_contrastive: "임베딩 학습",
  };
  const selected = () =>
    state.config.environments.find((e) => e.id === state.environmentId);
  const endpoint = (path) =>
    `${path}?environment_id=${encodeURIComponent(state.environmentId || "")}`;
  async function refresh() {
    if (state.tab !== "operations" || !selected()) return;
    const turn = ++serial,
      s = await api(endpoint("mlops/status"));
    if (turn !== serial || state.tab !== "operations") return;
    const e = s.environment,
      w = s.worker;
    $("#runner-status").innerHTML = w.error
      ? `<div class="notice">${esc(w.error)}</div>`
      : e.backend === "kubernetes"
        ? `<p>Kubernetes · ${esc(e.namespace)} · CPU ${esc(e.cpu)} · RAM ${esc(e.memory)} · GPU ${e.gpu}</p><p class="mono">${esc(e.image)}</p><p class="helper">Kubernetes scheduler가 자원을 할당하고 Pod를 배치합니다.</p>`
        : `<div class="runner-metrics"><div class="card"><span>외부 GPU</span><h3>${esc(w.gpus?.map((g) => g.name).join(", ") || "정보 없음")}</h3><p>여유 VRAM ${w.gpus?.[0]?.free_mib ?? "—"} MiB</p></div><div class="card"><span>실행기 자원</span><h3>RAM ${w.ram_available_mib ?? "—"} MiB</h3><p>CPU ${esc(w.cpu_limit ?? "—")} · 디스크 ${w.disk_free_mib ?? "—"} MiB</p></div><div class="card"><span>실행 상태</span><h3>${w.active_job ? "외부 작업 실행 중" : "활성 작업 없음"}</h3></div></div><details><summary>실행기가 보고한 자원·프로필</summary><pre>${esc(JSON.stringify(w, null, 2))}</pre></details>`;
    $("#runner-pool").innerHTML = s.pool.error
      ? `<div class="notice">${esc(s.pool.error)}</div>`
      : `<p>Pool ${esc(s.pool.name)} · ${s.pool.slots} slots · deferred 포함 ${s.pool.include_deferred ? "ON" : "OFF"}</p>`;
    $("#runner-dags").innerHTML = table(
      ["작업 / DAG", "설정 검증", "상태", "관리"],
      s.dags.map((d) => [
        d.error
          ? `${esc(labels[d.kind])}<br>${esc(d.error)}`
          : `<strong>${esc(labels[d.kind])}</strong><br><a target="_top" href="/dags/${encodeURIComponent(d.dag_id)}">${esc(d.dag_id)}</a>`,
        d.error
          ? "미배포 / 접근 불가"
          : d.contract_valid
            ? "최신 설정 일치"
            : esc(d.blockers.join(" · ")),
        d.error ? "—" : d.is_paused ? "일시중지" : "활성",
        !d.error
          ? `<button data-runner-state="${esc(d.dag_id)}">${d.is_paused ? "활성화" : "일시중지"}</button>`
          : "—",
      ]),
    );
    document.querySelectorAll("[data-runner-state]").forEach(
      (b) =>
        (b.onclick = guarded(async () => {
          await WorkbenchUI.changeDagState(b.dataset.runnerState);
          await refresh();
        })),
    );
    $("#runner-jobs").innerHTML =
      Array.isArray(s.jobs) && s.jobs.length
        ? table(
            ["외부 작업 ID", "상태", "시작", "결과 / 제어"],
            s.jobs.map((j) => [
              `<span class="mono">${esc(j.id)}</span>`,
              badge(j.status),
              formatDate(j.created),
              ["queued", "running", "cancelling"].includes(j.status) &&
              state.config.can_edit
                ? `<button data-cancel="${esc(j.id)}">취소</button>`
                : `<details><summary>실행 결과</summary><pre>${esc(JSON.stringify(j.result || { error: j.error }, null, 2))}</pre></details>`,
            ]),
          )
        : `<p class="helper">${e.backend === "kubernetes" ? "Pod 실행·로그·결과 참조는 위 DAG의 실행 이력에서 확인합니다." : esc(s.jobs.error || "실행 이력이 없습니다.")}</p>`;
    document.querySelectorAll("[data-cancel]").forEach(
      (b) =>
        (b.onclick = guarded(async () => {
          await api(endpoint(`mlops/jobs/${b.dataset.cancel}/cancel`), "POST");
          await refresh();
        })),
    );
    $("#runner-checked").textContent = new Date().toLocaleTimeString("ko-KR");
  }
  function editor(existing) {
    const e = existing || {
      id: "",
      name: "",
      version: 0,
      backend: "http",
      connection_id: "",
      dag_prefix: "model_lab",
      pool: "model_lab_gpu",
      pool_slots: 1,
      capacity: 1,
      max_active_runs: 1,
      timeout_seconds: 21600,
      poll_seconds: 10,
      namespace: "default",
      service_account: "default",
      image: "",
      cpu: "2",
      memory: "8Gi",
      gpu: 1,
    };
    const field = (key, label, type = "text") =>
      `<label>${label}<input name="${key}" type="${type}" value="${esc(e[key] ?? "")}" required></label>`;
    const d = document.createElement("dialog");
    d.className = "environment-dialog";
    d.innerHTML = `<div class="section-heading"><h2>${existing ? "실행 환경 편집" : "실행 환경 등록"}</h2><button id="environment-close">닫기</button></div><p>자격 증명은 Airflow Connections에서 관리합니다. 저장 후 DAG 설정을 배포하세요.</p><form><div class="fields">${field("id", "환경 ID")}${field("name", "표시 이름")}<label>실행 방식<select name="backend"><option value="http">원격 Job API</option><option value="kubernetes">Kubernetes Pod</option></select></label>${field("connection_id", "Airflow Connection ID")}${field("dag_prefix", "DAG 이름 prefix")}${field("pool", "Airflow Pool")}${field("capacity", "Pool 전체 slots", "number")}${field("pool_slots", "작업당 slots", "number")}${field("max_active_runs", "DAG당 동시 실행", "number")}${field("timeout_seconds", "실행 제한 · 초", "number")}</div><div id="kubernetes-settings"><h3>Kubernetes 실행 설정</h3><div class="fields">${field("image", "실행 이미지 · 고정 tag/digest")}${field("namespace", "Namespace")}${field("service_account", "Service account")}${field("cpu", "CPU 요청·한도")}${field("memory", "RAM 요청·한도")}${field("gpu", "GPU 수", "number")}</div><p class="helper">이미지는 WORKBENCH_REQUEST 입력과 /airflow/xcom/return.json 결과 계약을 구현해야 합니다.</p></div><details><summary>Workload · 리소스 프로필 · 노드 선택</summary><label>Workloads · JSON<textarea name="workloads">${esc(JSON.stringify(e.workloads || ["diagnostic", "llm_sft", "embedding_contrastive"]))}</textarea></label><label>Job API resource profiles · JSON<textarea name="resource_profiles">${esc(JSON.stringify(e.resource_profiles || { diagnostic: "diagnostic", qlora: "qlora_8b", lora: "lora_small", full: "embedding_small" }))}</textarea></label><label>Kubernetes node selector · JSON<textarea name="node_selector">${esc(JSON.stringify(e.node_selector || {}))}</textarea></label></details><p id="environment-error" class="notice" hidden></p><button class="primary">환경 저장</button></form>`;
    document.body.append(d);
    d.showModal();
    const form = d.querySelector("form");
    form.elements.backend.value = e.backend;
    if (existing) form.elements.id.readOnly = true;
    const toggle = () => {
      const k = form.elements.backend.value === "kubernetes";
      d.querySelector("#kubernetes-settings").hidden = !k;
      form.elements.image.required = k;
    };
    form.elements.backend.onchange = toggle;
    toggle();
    d.querySelector("#environment-close").onclick = () => d.close();
    d.onclose = () => d.remove();
    form.onsubmit = async (event) => {
      event.preventDefault();
      try {
        const v = { ...e, ...Object.fromEntries(new FormData(form)) };
        for (const k of [
          "capacity",
          "pool_slots",
          "max_active_runs",
          "timeout_seconds",
          "gpu",
        ])
          v[k] = Number(v[k]);
        for (const k of ["workloads", "resource_profiles", "node_selector"])
          v[k] = JSON.parse(v[k]);
        const saved = await api(
          `mlops/environments/${encodeURIComponent(v.id)}`,
          "PUT",
          v,
        );
        state.config.environments = await api("mlops/environments");
        state.environmentId = saved.id;
        d.close();
        render();
        toast("실행 환경을 저장했습니다. DAG 설정을 배포하세요.");
      } catch (error) {
        const el = d.querySelector("#environment-error");
        el.hidden = false;
        el.textContent = error.message;
      }
    };
  }
  async function render() {
    clearInterval(timer);
    serial++;
    state.config.environments = await api("mlops/environments");
    if (state.tab !== "operations") return;
    if (!selected())
      state.environmentId = state.config.environments[0]?.id || "";
    const e = selected();
    $("#lab-content").innerHTML =
      `<div class="notice info">실행 환경을 등록하고 DAG를 배포한 뒤 학습을 요청합니다. Airflow는 제출·대기·결과 확인을 맡고 학습은 선택한 실행 대상에서 수행합니다.</div><section class="card"><div class="section-heading"><h2>실행 환경</h2><button id="environment-new" ${state.config.can_edit ? "" : "disabled"}>환경 등록</button></div><div class="toolbar"><label>실행 대상<select id="environment-select">${state.config.environments.map((v) => `<option value="${v.id}" ${v.id === e?.id ? "selected" : ""}>${esc(v.name)} · ${v.backend}</option>`).join("")}</select></label>${e ? `<button id="environment-edit" ${state.config.can_edit ? "" : "disabled"}>환경 편집</button><button id="environment-bundle">DAG 파일 내려받기</button><button id="environment-prepare" class="primary" ${state.config.can_edit ? "" : "disabled"}>Pool·DAG 설정 배포</button>` : ""}</div><p class="helper">${e ? `Connection ${esc(e.connection_id)} · 설정 v${e.version} · Airflow PostgreSQL에 저장` : "등록된 실행 환경이 없습니다. 원격 Job API 또는 Kubernetes 환경을 등록하세요."}</p><div id="environment-result"></div></section>${e ? `<section class="card"><div class="section-heading"><h2>실행 대상 상태</h2><span id="runner-checked"></span><button id="runner-refresh">새로고침</button></div><div id="runner-status"></div></section><section class="card"><h2>환경별 DAG</h2><p class="helper">model-lab · mlops · external-executor · contract:v2 · 환경/설정 버전 태그 / 수동 실행 / owner mlops / 자동 재시도 0</p><div id="runner-pool"></div><div id="runner-dags"></div>${e.workloads.includes("diagnostic") ? `<form id="diagnostic-form" class="toolbar"><label>진단 VRAM · MiB<input name="required_gpu_mib" type="number" value="256" min="256" max="100000"></label><button class="primary" ${state.config.can_edit ? "" : "disabled"}>진단 DAG 실행</button></form>` : ""}<div id="diagnostic-result"></div></section><section class="card"><h2>외부 실행 이력</h2><div id="runner-jobs"></div></section>` : ""}`;
    on("#environment-new", "click", () => editor());
    on("#environment-select", "change", (event) => {
      state.environmentId = event.target.value;
      render();
    });
    if (!e) return;
    on("#environment-edit", "click", () => editor(selected()));
    on("#environment-bundle", "click", async () => {
      const b = await api(`mlops/environments/${e.id}/bundle`),
        url = URL.createObjectURL(
          new Blob([b.source], { type: "text/x-python" }),
        ),
        a = document.createElement("a");
      a.href = url;
      a.download = b.filename;
      a.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    });
    on("#environment-prepare", "click", async () => {
      const r = await api(`mlops/environments/${e.id}/prepare`, "POST");
      $("#environment-result").textContent =
        `배포됨: ${Object.values(r.dag_ids).join(", ")} · DAG processor 반영 후 새로고침하세요.`;
      await refresh();
    });
    on("#runner-refresh", "click", refresh);
    on("#diagnostic-form", "submit", async (event) => {
      event.preventDefault();
      const r = await api("mlops/diagnostic", "POST", {
        environment_id: e.id,
        request_id: crypto.randomUUID(),
        required_gpu_mib: Number(event.target.elements.required_gpu_mib.value),
      });
      $("#diagnostic-result").innerHTML =
        `<a target="_top" href="/dags/${encodeURIComponent(r.dag_id)}/runs/${encodeURIComponent(r.dag_run_id)}">진단 실행 보기</a>`;
      await refresh();
    });
    await refresh();
    timer = setInterval(() => {
      if (state.tab !== "operations") clearInterval(timer);
      else if (!document.hidden) guarded(refresh)();
    }, 10000);
  }
  return { render };
})();
