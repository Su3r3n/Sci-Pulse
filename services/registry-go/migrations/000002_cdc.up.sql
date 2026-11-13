ALTER TABLE tracked_projects     REPLICA IDENTITY FULL;
ALTER TABLE project_repos        REPLICA IDENTITY FULL;
ALTER TABLE institutions         REPLICA IDENTITY FULL;
ALTER TABLE project_institutions REPLICA IDENTITY FULL;
CREATE PUBLICATION dbz_registry
  FOR TABLE tracked_projects, project_repos, institutions, project_institutions;
