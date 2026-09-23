# Phase 1 실제 테이블 목록 — 2026-09-23

> 삭제 전 조사 스냅샷입니다. 이후 구형 17개를 삭제하고 신규 분석 객체를 생성했습니다. 현재 상태는 [실행 안내](phase1-pipeline.md)를 참고합니다.

플랫폼 DB와 Airflow DB에서 읽기 전용 트랜잭션으로 조회한 카탈로그와 정확한 행 수입니다. 서비스 DB는 중지 상태여서 포함하지 않습니다. 운영 중인 Airflow 행 수는 이후 달라질 수 있습니다.

분류 이유·서비스 DB의 잠정 분류·신규 개발 대상은 [테이블 계획](phase1-table-plan.md)을 참고합니다. 이번 조사는 어떤 DB 객체도 변경하지 않았습니다.

## 플랫폼 DB: pop_talk_platform

| 테이블 | 행 수 | 분류 |
|---|---:|---|
| dw_control.asset_deliveries | 0 | 삭제 후보 |
| dw_control.asset_inbox | 0 | 삭제 후보 |
| dw_control.asset_outbox | 0 | 삭제 후보 |
| dw_control.dbt_invocation_artifacts | 0 | 삭제 후보 |
| dw_control.dbt_invocation_registries | 0 | 삭제 후보 |
| dw_control.dbt_invocation_reservations | 0 | 삭제 후보 |
| dw_control.dbt_model_invocation_specs | 0 | 삭제 후보 |
| dw_control.generation_gate_receipts | 0 | 삭제 후보 |
| dw_control.generation_plans | 0 | 삭제 후보 |
| dw_control.model_build_attempts | 0 | 삭제 후보 |
| dw_control.model_build_slots | 0 | 삭제 후보 |
| dw_control.model_cohorts | 0 | 삭제 후보 |
| dw_control.model_execution_receipts | 0 | 삭제 후보 |
| dw_control.model_results | 0 | 삭제 후보 |
| dw_control.release_delivery_registries | 0 | 삭제 후보 |
| dw_control.release_delivery_specs | 0 | 삭제 후보 |
| dw_control.schema_migrations | 7 | 삭제 후보 |

## Airflow DB: airflow

| 테이블 | 행 수 | 분류 |
|---|---:|---|
| public.ab_group | 0 | 보존: Airflow/FAB 관리 |
| public.ab_group_role | 0 | 보존: Airflow/FAB 관리 |
| public.ab_permission | 5 | 보존: Airflow/FAB 관리 |
| public.ab_permission_view | 195 | 보존: Airflow/FAB 관리 |
| public.ab_permission_view_role | 257 | 보존: Airflow/FAB 관리 |
| public.ab_register_user | 0 | 보존: Airflow/FAB 관리 |
| public.ab_role | 5 | 보존: Airflow/FAB 관리 |
| public.ab_user | 1 | 보존: Airflow/FAB 관리 |
| public.ab_user_group | 0 | 보존: Airflow/FAB 관리 |
| public.ab_user_role | 1 | 보존: Airflow/FAB 관리 |
| public.ab_view_menu | 79 | 보존: Airflow/FAB 관리 |
| public.alembic_version | 1 | 보존: Airflow/FAB 관리 |
| public.alembic_version_fab | 1 | 보존: Airflow/FAB 관리 |
| public.asset | 1 | 보존: Airflow/FAB 관리 |
| public.asset_active | 1 | 보존: Airflow/FAB 관리 |
| public.asset_alias | 0 | 보존: Airflow/FAB 관리 |
| public.asset_alias_asset | 0 | 보존: Airflow/FAB 관리 |
| public.asset_alias_asset_event | 0 | 보존: Airflow/FAB 관리 |
| public.asset_dag_run_queue | 0 | 보존: Airflow/FAB 관리 |
| public.asset_event | 0 | 보존: Airflow/FAB 관리 |
| public.asset_partition_dag_run | 0 | 보존: Airflow/FAB 관리 |
| public.asset_state_store | 0 | 보존: Airflow/FAB 관리 |
| public.asset_watcher | 0 | 보존: Airflow/FAB 관리 |
| public.backfill | 0 | 보존: Airflow/FAB 관리 |
| public.backfill_dag_run | 0 | 보존: Airflow/FAB 관리 |
| public.callback | 0 | 보존: Airflow/FAB 관리 |
| public.connection | 0 | 보존: Airflow/FAB 관리 |
| public.connection_test_request | 0 | 보존: Airflow/FAB 관리 |
| public.dag | 5 | 보존: Airflow/FAB 관리 |
| public.dag_bundle | 1 | 보존: Airflow/FAB 관리 |
| public.dag_bundle_team | 0 | 보존: Airflow/FAB 관리 |
| public.dag_code | 5 | 보존: Airflow/FAB 관리 |
| public.dag_favorite | 0 | 보존: Airflow/FAB 관리 |
| public.dag_owner_attributes | 0 | 보존: Airflow/FAB 관리 |
| public.dag_priority_parsing_request | 0 | 보존: Airflow/FAB 관리 |
| public.dag_run | 0 | 보존: Airflow/FAB 관리 |
| public.dag_run_note | 0 | 보존: Airflow/FAB 관리 |
| public.dag_schedule_asset_alias_reference | 0 | 보존: Airflow/FAB 관리 |
| public.dag_schedule_asset_name_reference | 0 | 보존: Airflow/FAB 관리 |
| public.dag_schedule_asset_reference | 0 | 보존: Airflow/FAB 관리 |
| public.dag_schedule_asset_uri_reference | 0 | 보존: Airflow/FAB 관리 |
| public.dag_tag | 15 | 보존: Airflow/FAB 관리 |
| public.dag_version | 5 | 보존: Airflow/FAB 관리 |
| public.dag_warning | 0 | 보존: Airflow/FAB 관리 |
| public.dagrun_asset_event | 0 | 보존: Airflow/FAB 관리 |
| public.deadline | 0 | 보존: Airflow/FAB 관리 |
| public.deadline_alert | 0 | 보존: Airflow/FAB 관리 |
| public.hitl_detail | 0 | 보존: Airflow/FAB 관리 |
| public.hitl_detail_history | 0 | 보존: Airflow/FAB 관리 |
| public.import_error | 0 | 보존: Airflow/FAB 관리 |
| public.job | 9 | 보존: Airflow/FAB 관리 |
| public.log | 36 | 보존: Airflow/FAB 관리 |
| public.log_template | 2 | 보존: Airflow/FAB 관리 |
| public.partitioned_asset_key_log | 0 | 보존: Airflow/FAB 관리 |
| public.rendered_task_instance_fields | 0 | 보존: Airflow/FAB 관리 |
| public.revoked_token | 0 | 보존: Airflow/FAB 관리 |
| public.serialized_dag | 5 | 보존: Airflow/FAB 관리 |
| public.session | 2 | 보존: Airflow/FAB 관리 |
| public.slot_pool | 2 | 보존: Airflow/FAB 관리 |
| public.task_inlet_asset_reference | 0 | 보존: Airflow/FAB 관리 |
| public.task_instance | 0 | 보존: Airflow/FAB 관리 |
| public.task_instance_history | 0 | 보존: Airflow/FAB 관리 |
| public.task_instance_note | 0 | 보존: Airflow/FAB 관리 |
| public.task_map | 0 | 보존: Airflow/FAB 관리 |
| public.task_outlet_asset_reference | 1 | 보존: Airflow/FAB 관리 |
| public.task_reschedule | 0 | 보존: Airflow/FAB 관리 |
| public.task_state_store | 0 | 보존: Airflow/FAB 관리 |
| public.team | 0 | 보존: Airflow/FAB 관리 |
| public.trigger | 0 | 보존: Airflow/FAB 관리 |
| public.variable | 0 | 보존: Airflow/FAB 관리 |
| public.xcom | 0 | 보존: Airflow/FAB 관리 |
| workbench.dashboard_actions | 0 | 보존: Workbench 공용 |
| workbench.dashboard_cache | 1 | 보존: Workbench 공용 |
| workbench.documents | 0 | 보존: Workbench 공용 |
| workbench.executor_jobs | 0 | 조건부 삭제: 코드 참조 분리 후 |
| workbench.executor_reservations | 0 | 조건부 삭제: 코드 참조 분리 후 |
| workbench.experiments | 0 | 조건부 삭제: 코드 참조 분리 후 |
| workbench.lab_records | 0 | 조건부 삭제: 코드 참조 분리 후 |
| workbench.leases | 1 | 보존: Workbench 공용 |
| workbench.schema_migrations | 5 | 보존: Workbench 공용 |
| workbench.studio_boards | 0 | 보존: Workbench 공용 |
| workbench.studio_preferences | 0 | 보존: Workbench 공용 |
| workbench.studio_revisions | 0 | 보존: Workbench 공용 |
| workbench.studio_templates | 0 | 보존: Workbench 공용 |

합계: 101개 — 삭제 후보 17개, 보존 80개, 조건부 삭제 후보 4개. 신규 분석용 테이블과 서비스 DB 객체는 이 합계에 포함하지 않습니다.
