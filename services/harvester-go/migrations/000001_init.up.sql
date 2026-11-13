CREATE TABLE IF NOT EXISTS harvest_cursors (
  source          text NOT NULL,
  scope           text NOT NULL,
  cursor_value    text,
  last_success_at timestamptz,
  last_error      text,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, scope)
);

CREATE TABLE IF NOT EXISTS http_etags (
  url         text PRIMARY KEY,
  etag        text NOT NULL,
  last_status smallint NOT NULL,
  fetched_at  timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS source_budget (
  source  text NOT NULL,
  day     date NOT NULL,
  spent   integer NOT NULL DEFAULT 0,
  budget  integer,
  PRIMARY KEY (source, day)
);
