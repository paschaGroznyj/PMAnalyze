-- PMAnalyze schema (process_mining)
-- Все флаги обнуляются: ни одна статья не имеет ревью на старте.

CREATE SCHEMA IF NOT EXISTS process_mining;

-- ============================================================
-- Приведение существующей papers_metadata к целевому виду.
-- Дропаем бесполезные колонки, category ОСТАВЛЯЕМ (LLM проставит).
-- ============================================================
ALTER TABLE process_mining.papers_metadata DROP COLUMN IF EXISTS file_article;
ALTER TABLE process_mining.papers_metadata DROP COLUMN IF EXISTS pdf_downloaded;
ALTER TABLE process_mining.papers_metadata DROP COLUMN IF EXISTS text_review;   -- ревью выносим в отдельную таблицу

-- id: делаем нормальный автоинкремент (в дампе секвенса не было)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='process_mining' AND table_name='papers_metadata' AND column_name='id'
  ) THEN
    ALTER TABLE process_mining.papers_metadata ADD COLUMN id BIGINT;
  END IF;
END$$;

CREATE SEQUENCE IF NOT EXISTS process_mining.papers_metadata_id_seq OWNED BY process_mining.papers_metadata.id;
SELECT setval('process_mining.papers_metadata_id_seq',
              COALESCE((SELECT MAX(id) FROM process_mining.papers_metadata), 0) + 1, false);
ALTER TABLE process_mining.papers_metadata
  ALTER COLUMN id SET DEFAULT nextval('process_mining.papers_metadata_id_seq');
UPDATE process_mining.papers_metadata
  SET id = nextval('process_mining.papers_metadata_id_seq') WHERE id IS NULL;
ALTER TABLE process_mining.papers_metadata ALTER COLUMN id SET NOT NULL;

-- Тайм-колонки
ALTER TABLE process_mining.papers_metadata ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now();
ALTER TABLE process_mining.papers_metadata ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();
ALTER TABLE process_mining.papers_metadata ADD COLUMN IF NOT EXISTS llm_status TEXT DEFAULT 'new';

-- БОЕВЫЕ ДАННЫЕ СОХРАНЯЕМ: is_relevant / relevance_score / category / processed_at не трогаем.
-- Уже обработанным (processed_at IS NOT NULL) выставляем llm_status='done',
-- чтобы пайплайн не переоценивал их заново.
UPDATE process_mining.papers_metadata
  SET llm_status = 'done'
  WHERE processed_at IS NOT NULL;

-- Уникальность источника
CREATE UNIQUE INDEX IF NOT EXISTS uq_papers_source_extid
  ON process_mining.papers_metadata (source, external_id);

CREATE INDEX IF NOT EXISTS idx_papers_llm_status ON process_mining.papers_metadata (llm_status);
CREATE INDEX IF NOT EXISTS idx_papers_is_relevant ON process_mining.papers_metadata (is_relevant);
CREATE INDEX IF NOT EXISTS idx_papers_date_sub ON process_mining.papers_metadata (date_sub);

-- ============================================================
-- id должен быть UNIQUE, иначе FK reviews.article_id -> papers_metadata(id) не создастся.
-- PK стоит на external_id, поэтому добавляем отдельное уникальное ограничение на id.
-- ============================================================
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'uq_papers_id' AND conrelid = 'process_mining.papers_metadata'::regclass
  ) THEN
    ALTER TABLE process_mining.papers_metadata ADD CONSTRAINT uq_papers_id UNIQUE (id);
  END IF;
END$$;

-- ============================================================
-- Ревью: только markdown (review_html не храним, рендерим md на фронте)
-- ============================================================
CREATE TABLE IF NOT EXISTS process_mining.reviews (
    id           BIGSERIAL PRIMARY KEY,
    article_id   BIGINT NOT NULL REFERENCES process_mining.papers_metadata(id) ON DELETE CASCADE,
    review_md    TEXT NOT NULL,
    model_name   TEXT,
    created_at   TIMESTAMPTZ DEFAULT now(),
    UNIQUE (article_id)
);
CREATE INDEX IF NOT EXISTS idx_reviews_article ON process_mining.reviews (article_id);

-- ============================================================
-- Журнал запусков пайплайна (еженедельный крон)
-- ============================================================
CREATE TABLE IF NOT EXISTS process_mining.parser_runs (
    id           BIGSERIAL PRIMARY KEY,
    started_at   TIMESTAMPTZ DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    status       TEXT DEFAULT 'running',   -- running|done|error
    fetched      INT DEFAULT 0,
    inserted     INT DEFAULT 0,
    relevant     INT DEFAULT 0,
    reviewed     INT DEFAULT 0,
    errors       INT DEFAULT 0,
    detail       JSONB
);
