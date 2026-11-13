"""Spark job: Enrich lake.silver.papers with project_slugs and repo_links from ClickHouse registry."""

import os

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from common import ch_jdbc_props, ch_jdbc_url, ensure_tables, get_spark
from repo_names import GH_REPO_PATTERN, normalize_repo

# M4: generic JDBC не мапит Array(String) из ClickHouse (java.sql.Types.ARRAY).
# Подзапрос сворачивает массивы в строку с разделителем char(31), а разбор в массивы
# делает Spark (split + filter пустых элементов).
PROJECTS_QUERY = """(
  SELECT id, slug,
         arrayStringConcat(keywords, char(31)) AS keywords_s,
         arrayStringConcat(topics, char(31))   AS topics_s
  FROM v_projects_current
  WHERE status = 'active'
) AS projects_q"""

SEP = "\x1f"  # то же значение, что char(31) в запросе


def _split_lower(col_name: str):
    """Строка с разделителем -> массив непустых строк в нижнем регистре."""
    parts = F.split(F.coalesce(F.col(col_name), F.lit("")), SEP)
    return F.transform(
        F.filter(parts, lambda x: x != ""),
        lambda x: F.lower(x),
    )


def load_projects(spark, jdbc_url: str, props: dict):
    return spark.read.jdbc(jdbc_url, PROJECTS_QUERY, properties=props).select(
        "id",
        "slug",
        _split_lower("keywords_s").alias("keywords"),
        _split_lower("topics_s").alias("topics"),
    )


def main() -> None:
    spark = get_spark("scipulse-enrich-projects")
    # UDF выполняются на executor-ах: модуль repo_names должен быть доступен там.
    spark.sparkContext.addPyFile(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "repo_names.py")
    )
    ensure_tables(spark)

    jdbc_url = ch_jdbc_url()
    props = ch_jdbc_props()

    projects = [
        {
            "id": r["id"],
            "slug": r["slug"],
            "keywords": list(r["keywords"] or []),
            "topics": list(r["topics"] or []),
        }
        for r in load_projects(spark, jdbc_url, props).collect()
    ]
    repos_df = spark.read.jdbc(jdbc_url, "v_repos_current", properties=props)

    repo_to_slug = {}
    proj_id_to_slug = {p["id"]: p["slug"] for p in projects}
    for r in repos_df.collect():
        slug = proj_id_to_slug.get(r["project_id"])
        if slug and r["github_full_name"]:
            repo_to_slug[r["github_full_name"].lower()] = (r["github_full_name"], slug)

    if not projects and not repo_to_slug:
        print("No active projects or repos in ClickHouse registry; skipping enrichment.")
        spark.stop()
        return

    @F.udf(ArrayType(StringType()))
    def match_repos(links, comments, abstract):
        found = set()

        def add_from(text):
            for m in GH_REPO_PATTERN.findall(text or ""):
                entry = repo_to_slug.get(normalize_repo(m).lower())
                if entry:
                    found.add(entry[0])

        for lnk in links or []:
            url = (lnk["url"] or "") if isinstance(lnk, dict) else (getattr(lnk, "url", "") or "")
            add_from(url)
        add_from(comments)
        add_from(abstract)
        return sorted(found)

    @F.udf(ArrayType(StringType()))
    def match_projects(title, abstract, topics, matched_repos):
        slugs = set()
        for repo in matched_repos or []:
            entry = repo_to_slug.get(repo.lower())
            if entry:
                slugs.add(entry[1])

        haystack = f"{title or ''} {abstract or ''}".lower()
        paper_topics = {t.lower() for t in (topics or []) if t}
        for p in projects:
            if any(kw in haystack for kw in p["keywords"]):
                slugs.add(p["slug"])
            elif any(tp in paper_topics for tp in p["topics"]):
                slugs.add(p["slug"])
        return sorted(slugs)

    papers = spark.table("lake.silver.papers")
    enriched = (
        papers.withColumn(
            "new_repos", match_repos(F.col("links"), F.col("comments"), F.col("abstract"))
        )
        .withColumn(
            "new_slugs",
            match_projects(F.col("title"), F.col("abstract"), F.col("topics"), F.col("new_repos")),
        )
        .filter((F.size("new_repos") > 0) | (F.size("new_slugs") > 0))
        .select(
            "paper_key",
            F.array_distinct(F.concat(F.coalesce("repo_links", F.array()), "new_repos")).alias(
                "repo_links"
            ),
            F.array_distinct(F.concat(F.coalesce("project_slugs", F.array()), "new_slugs")).alias(
                "project_slugs"
            ),
        )
    )

    if enriched.rdd.isEmpty():
        print("No papers matched registry projects.")
        spark.stop()
        return

    enriched.createOrReplaceTempView("enriched_updates")
    spark.sql(
        """
        MERGE INTO lake.silver.papers AS t
        USING enriched_updates AS s
        ON t.paper_key = s.paper_key
        WHEN MATCHED THEN UPDATE SET
          t.repo_links    = s.repo_links,
          t.project_slugs = s.project_slugs
        """
    )

    print("Project enrichment completed.")
    spark.stop()


if __name__ == "__main__":
    main()
