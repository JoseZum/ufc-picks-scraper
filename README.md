# UFC Scraper

This scraper extracts UFC fight data from Tapology.com and stores it in MongoDB.

## Features

- Scrapes UFC events (name, date, location, etc.)
- Extracts detailed fight information (fighters, weight, category, etc.)
- Accesses individual fight pages to extract comparative data:
  - UFC Rankings
  - Fight records
  - Last 5 fights
  - Betting odds
  - Nationality
  - Fighting out of
  - Age at the fight
  - Weight, height, reach
  - Gyms

## Local Installation

1. Install dependencies:
```bash
cd scraper
pip install -r requirements.txt
```

2. Configure environment variables:
Create a `.env` file in the `scraper/` directory with:
```
MONGODB_URI=your_mongodb_connection_here
```

## Usage

### Discovery Mode (automatic scraper for upcoming events)
```bash
cd scraper
python -m scrapy crawl ufc
```

### Scraper for a specific event
```bash
python -m scrapy crawl ufc -a EVENT_ID=136026
```

### Scraper for event results
```bash
python -m scrapy crawl ufc -a EVENT_ID=136026 -a MODE=results
```

### Skip fight details scraping (faster)
```bash
python -m scrapy crawl ufc -a SKIP_BOUT_DETAILS=true
```

## GitHub Actions

The scraper runs automatically every day at 00:00 UTC via GitHub Actions.

### Configure secrets in GitHub:

1. Go to your repository on GitHub
2. Settings → Secrets and variables → Actions
3. Click "New repository secret"
4. Add:
   - Name: `MONGODB_URI`
   - Secret: Your MongoDB connection URI

### Manual Execution:

1. Go to the "Actions" tab on GitHub
2. Select "UFC Scraper Workflow"
3. Click "Run workflow"
4. Optional: specify an EVENT_ID or MODE

## MongoDB Data Structure

### Collection: `events`
```json
{
  "id": 136026,
  "name": "UFC 325",
  "event_date": "2026-02-08",
  "location": "Las Vegas, Nevada"
}
```

### Collection: `bouts`
```json
{
  "id": 1074233,
  "event_id": 136026,
  "fighters": {
    "red": {
      "fighter_name": "Alexander Volkanovski",
      "ufc_ranking": {"position": 1, "division": "Featherweight"},
      "record_at_fight": {"wins": 27, "losses": 4, "draws": 0},
      "height": {"cm": 168},
      "reach": {"cm": 182},
      "betting_odds": {"line": "-160", "description": "Slight Favorite"}
    },
    "blue": { /* ... */ }
  }
}
```

## Notes

- The scraper respects robots.txt and uses delays between requests
- Events prior to 2026-01-01 are automatically ignored
- After 10 consecutive old events, the scraper stops pagination
