DROP TRIGGER IF EXISTS trg_proj_inst_upd ON project_institutions;
DROP TRIGGER IF EXISTS trg_institutions_upd ON institutions;
DROP TRIGGER IF EXISTS trg_repos_upd ON project_repos;
DROP TRIGGER IF EXISTS trg_projects_upd ON tracked_projects;
DROP FUNCTION IF EXISTS set_updated_at();

DROP TABLE IF EXISTS project_institutions;
DROP INDEX IF EXISTS project_repos_by_project;
DROP INDEX IF EXISTS project_repos_one_primary;
DROP TABLE IF EXISTS project_repos;
DROP TABLE IF EXISTS tracked_projects;
DROP TABLE IF EXISTS institutions;
DROP TYPE IF EXISTS project_status;
