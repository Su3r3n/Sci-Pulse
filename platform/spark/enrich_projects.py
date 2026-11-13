"""Spark job: Enrich lake.silver.papers with project_slugs and repo_links from ClickHouse registry."""

import re
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from common import ch_jdbc_props, ch_jdbc_url, ensure_tables, get_spark


def main() -> None:
    spark = get_spark("scipulse-enrich-projects")
    ensure_tables(spark)

    jdbc_url = ch_jdbc_url()
    props = ch_jdbc_props()

    projects_df = spark.read.jdbc(jdbc_url, "v_projects_current", properties=props).filter(
        F.col("status") == "active"
    )
    repos_df = spark.read.jdbc(jdbc_url, "v_repos_current", properties=props)

    projects = [
        {
            "id": r["id"],
            "slug": r["slug"],
            "keywords": [k.lower() for k in (r["keywords"] or []) if k],
            "topics": [t.lower() for t in (r["topics"] or []) if t],
        }
        for r in projects_df.collect()
    ]
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

    gh_pattern = re.compile(r"github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)")

    @F.udf(ArrayType(StringType()))
    def match_repos(links, comments, abstract):
        found = set()
        for lnk in links or []:
            url = (lnk["url"] or "") if isinstance(lnk, dict) else (getattr(lnk, "url", "") or "")
            for m in gh_pattern.findall(url):
                clean = m.rstrip(".git/")
                if clean.lower() in repo_to_slug:
                    found.add(repo_to_slug[clean.lower()][0])
        for txt in (comments, abstract):
            if txt:
                for m in gh_pattern.findall(txt):
                    clean = m.rstrip(".git/")
                    if clean.lower() in repo_to_slug:
                        found.add(repo_to_slug[clean.lower()][0])
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
