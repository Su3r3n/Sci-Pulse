CREATE TYPE project_status AS ENUM ('active', 'paused');

CREATE TABLE institutions (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ror_id      text UNIQUE,
  name        text NOT NULL,
  country     char(2),
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tracked_projects (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  slug        text NOT NULL UNIQUE CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,62}$'),
  name        text NOT NULL,
  description text,
  keywords    text[] NOT NULL DEFAULT '{}',
  topics      text[] NOT NULL DEFAULT '{}',
  priority    smallint NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 5),
  status      project_status NOT NULL DEFAULT 'active',
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE project_repos (
  id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id       bigint NOT NULL REFERENCES tracked_projects(id) ON DELETE CASCADE,
  github_full_name text NOT NULL UNIQUE
                   CHECK (github_full_name ~ '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$'),
  is_primary       boolean NOT NULL DEFAULT false,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX project_repos_one_primary ON project_repos (project_id) WHERE is_primary;
CREATE INDEX project_repos_by_project ON project_repos (project_id);

CREATE TABLE project_institutions (
  project_id     bigint NOT NULL REFERENCES tracked_projects(id) ON DELETE CASCADE,
  institution_id bigint NOT NULL REFERENCES institutions(id) ON DELETE CASCADE,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (project_id, institution_id)
);

CREATE FUNCTION set_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at := now(); RETURN NEW; END $$;

CREATE TRIGGER trg_projects_upd     BEFORE UPDATE ON tracked_projects     FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_repos_upd        BEFORE UPDATE ON project_repos        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_institutions_upd BEFORE UPDATE ON institutions         FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_proj_inst_upd    BEFORE UPDATE ON project_institutions FOR EACH ROW EXECUTE FUNCTION set_updated_at();
