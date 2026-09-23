with fixture as (
    select column1::timestamp_tz as source_observed_at,
           column2::integer as publication_revision,
           column3::timestamp_tz as publication_completed_at,
           column4::string as source_run_id,
           column5::string as artifact_version
    from values
      ('2026-09-09 01:00:00 +00:00', 1, '2026-09-09 03:00:00 +00:00', 'run-1', 'artifact-1'),
      ('2026-09-09 01:00:00 +00:00', 2, '2026-09-09 02:00:00 +00:00', 'run-1', 'artifact-2'),
      -- 나중에 적재한 초기 스냅샷이 최신 일일 관측을 밀어내면 안 된다.
      (null, 99, '2026-09-11 02:00:00 +00:00', 'legacy:source', 'artifact-legacy')
), ranked as (
    select *, row_number() over (order by {{ observation_recency_order() }}) as recency_rank
    from fixture
)
select * from ranked where recency_rank = 1 and publication_revision <> 2
