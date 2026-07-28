# UFC Picks ETL

The primary card, result, fighter-stat, and fighter-photo source is ESPN's UFC
JSON feed. Event posters are resolved separately from Wikipedia/original
sources and official UFC event pages.

## Setup

```bash
pip install -r requirements.txt
```

Required for every ESPN mode:

```dotenv
MONGODB_URI=mongodb://...
```

Required when the selected mode uploads fighter photos:

```dotenv
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_S3_BUCKET=...
AWS_REGION=us-east-1
IMAGE_SOURCE_MODE=s3
```

## ESPN modes

### General

Refreshes cards, source mappings, fighter profiles, records, physical stats,
available results, scoring, and missing S3 headshots.

```bash
scrapy crawl espn -a MODE=general -a DAYS_BACK=14 -a DAYS_AHEAD=60
```

Full-season backfill:

```bash
scrapy crawl espn -a MODE=general -a SEASON=2026
```

One event, using either the existing UFC Picks ID or ESPN event ID:

```bash
scrapy crawl espn -a MODE=general -a EVENT_ID=600059339
```

### Results

Finds every local UFC card with a bout still missing its result (through
tomorrow), requests those exact ESPN dates, and recalculates pick/user scores.
Existing results are not overwritten.

```bash
scrapy crawl espn -a MODE=results
```

### Photos

Links fighters on upcoming cards, downloads ESPN's 350x254 PNG headshots,
uploads them to S3, and stores the resulting `image_key`.

```bash
scrapy crawl espn -a MODE=photos -a DAYS_AHEAD=60
```

Use `-a FORCE_PHOTOS=true` to replace already-mirrored ESPN headshots.

## Event images

```bash
scrapy crawl event_images
```

- Card poster: Wikipedia's credited original source, with the Wikipedia file
  as fallback.
- Wide hero: official UFC `background_image_xl_2x`.

## Source-ID policy

Existing UFC Picks event and bout IDs remain canonical. The ETL matches ESPN
cards by source ID or by date/name/fighter matchup, then stores
`espn_event_id`, `espn_competition_id`, and fighter `espn_id`. This prevents
existing picks and frontend URLs from being invalidated.

New ESPN-only cards use ESPN's numeric event and competition IDs as their
canonical IDs.

## Automation

- ESPN results: every 2 hours.
- ESPN general ETL: Monday and Thursday.
- Event posters/heroes: daily.
- ESPN upcoming-card photos: daily.

All jobs can also be triggered manually from the GitHub Actions workflow.
