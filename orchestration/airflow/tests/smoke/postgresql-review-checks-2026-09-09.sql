-- Read-only audit. No user text, credentials, tokens, or account identifiers.
BEGIN READ ONLY;
SET LOCAL search_path = dev, public, cdb_admin;
SET LOCAL statement_timeout = '20s';
SELECT format('SELECT %L AS table_name, count(*) AS row_count FROM %I.%I;', schemaname || '.' || tablename, schemaname, tablename)
FROM pg_tables WHERE schemaname IN ('dev','dw_serving') ORDER BY schemaname,tablename
\gexec
SELECT service_status,approval_status,count(*) FROM popcorn_movies GROUP BY 1,2 ORDER BY 1,2;
SELECT coalesce(source_system,'LOCAL') AS source,status,count(*) FROM reviews GROUP BY 1,2 ORDER BY 1,2;
SELECT embedding_model,status,count(*) FROM popcorn_movie_embeddings GROUP BY 1,2 ORDER BY 1,2;
SELECT embedding_model,status,count(*) FROM movie_embedding_jobs GROUP BY 1,2 ORDER BY 1,2;
SELECT count(*) AS hidden_but_catalog_eligible FROM popcorn_movies m LEFT JOIN movie_editorial e ON e.movie_id=m.id WHERE m.service_status <> 'PUBLISHED' AND NOT coalesce(e.is_removed,false);
SELECT count(*) AS orphan_preference_ids FROM users u CROSS JOIN LATERAL unnest(u.onboarding_movie_category_ids) p(id) LEFT JOIN movie_categories c ON c.id=p.id WHERE c.id IS NULL;
SELECT count(*) AS orphan_display_codes FROM display_categories d CROSS JOIN LATERAL unnest(d.category_codes) p(code) LEFT JOIN movie_categories c ON c.code=p.code WHERE c.id IS NULL;
SELECT count(*) AS dw_rows,count(m.id) AS matched_by_kofic,count(*) FILTER(WHERE m.id IS NOT NULL AND m.id <> d.source_movie_id) AS source_id_mismatch FROM dw_serving.movie_catalog d LEFT JOIN popcorn_movies m ON m.kofic_movie_cd=d.kofic_movie_cd;
SELECT count(*) AS pre2020,count(*) FILTER(WHERE service_status='PUBLISHED') AS published_pre2020 FROM popcorn_movies WHERE release_date < DATE '2020-01-01';
SELECT status,count(*) FROM dw_serving.publish_runs GROUP BY 1;
SELECT extname,extversion FROM pg_extension;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT m.id,m.title_ko,count(*) OVER() FROM popcorn_movies m LEFT JOIN movie_editorial e ON e.movie_id=m.id WHERE NOT coalesce(e.is_removed,false) ORDER BY m.release_date DESC,m.id DESC LIMIT 20;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT m.id FROM popcorn_movies m WHERE title_ko ILIKE '%사랑%' OR title_en ILIKE '%사랑%' OR title_original ILIKE '%사랑%' ORDER BY release_date DESC,id DESC LIMIT 20;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT s.id,r.average_score FROM popcorn_movies_service s LEFT JOIN LATERAL (SELECT round(avg(rating),1) AS average_score,count(*) AS rating_count FROM reviews WHERE movie_id=s.id AND status='ACTIVE' AND deleted_at IS NULL) r ON true WHERE s.service_status='PUBLISHED' ORDER BY coalesce(r.average_score,0) DESC,s.release_date DESC NULLS LAST LIMIT 10;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT u.id,(SELECT count(*) FROM reviews r WHERE r.user_id=u.id AND r.deleted_at IS NULL) FROM users u ORDER BY u.created_at DESC;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT * FROM display_categories_service;
COMMIT;
