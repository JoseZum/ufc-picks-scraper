# Retired scripts

Nothing here runs on a schedule, is imported by the spiders, or is referenced by
a workflow. They are kept rather than deleted because each one wrote to
production at some point, and the writer inventory has to be able to account for
every file that ever could — deleting them would erase that record.

| Script | Why it is here |
|---|---|
| `backfill_2026.py` | One-time 2026 completed-card migration. Already executed; its `workflow_dispatch` job was removed with it. |
| `backfill_2026_timing.py` | One-time staged section-timing migration for 2026 cards. Already executed. |
| `fix_nationality_data.py` | One-off repair of malformed fighter nationalities. The bad shape it fixed is no longer produced. |
| `fix_ranking_data.py` | One-off repair of `ranking` stored as an int instead of a dict. Same reason. |
| `sync_image_keys.py` | One-off reconciliation of S3 image keys into Mongo, from before the ESPN headshot pipeline set them directly. |
| `scheduler.py` | The Tapology-era scrape-window loop. GitHub Actions owns scheduling now; no Dockerfile, deployment or workflow referenced it. |

They remain classified in `tapology_scraper/writer_precedence_inventory.py` under
their new paths. If one is ever needed again, run it deliberately and by hand —
none of them is safe to put back on a schedule without re-reviewing what it
writes.
