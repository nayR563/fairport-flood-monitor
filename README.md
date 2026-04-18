# Fairport Flood Monitor

Automated flood monitoring for a property on the Mississippi River near Fairport, Iowa.
Runs free on GitHub Actions. No credits, no subscriptions, no home computer required.

## What It Does

**Every 6 hours** — checks all three gauges and sends an ntfy.sh alert if any
threshold is crossed.

**Every Thursday 6 PM CDT** — sends a full weekly briefing with 10-day forecast,
garage decision timeline, and a plain-English recommendation.

## Gauges Monitored

| Gauge | Location | Role |
|-------|----------|------|
| ILNI2 | Mississippi at Illinois City | PRIMARY — has NWS forecast |
| WAPI4 | Iowa River at Wapello | EARLY WARNING — 2-5 day lead time |
| FAII4 | Mississippi above Fairport | OBSERVED ONLY — dam gap analysis |

## Alert Thresholds

| Level | Condition |
|-------|-----------|
| 🚨 CRITICAL | ILNI2 forecast peak ≥ 15 ft, OR Wapello hits/forecast ≥ 22 ft, OR < 3 days to 15 ft |
| ⚠️ WARNING | ILNI2 forecast peak ≥ 14 ft, OR dam gap < 0.6 ft, OR 3-5 days to 15 ft |
| 👀 WATCH | Fairport ≥ 13 ft, OR ILNI2 ≥ 13.5 ft, OR Wapello forecast ≥ 20 ft |

## Setup (One Time)

### 1. Create the GitHub repository
- Go to github.com → click the **+** → **New repository**
- Name it `fairport-flood-monitor`
- Set to **Private**
- Click **Create repository**

### 2. Upload the files
In your new repo, click **Add file → Upload files** and upload:
- `flood_monitor.py`
- `.github/workflows/flood_monitor.yml`

### 3. Done
GitHub Actions will automatically run on schedule. No setup needed.

### Manual trigger
Go to your repo → **Actions** tab → **Fairport Flood Monitor** → **Run workflow**
Choose `check`, `weekly`, or `both`.

## Notifications

Delivered via ntfy.sh to topic `fairport-flood-alerts-563`.
Install the ntfy app on your phone and subscribe to that topic.

## Data Source

NOAA National Water Prediction Service (NWPS) API — free, no key required.
- https://api.water.noaa.gov/nwps/v1/gauges/ILNI2/stageflow
- https://api.water.noaa.gov/nwps/v1/gauges/WAPI4/stageflow
- https://api.water.noaa.gov/nwps/v1/gauges/FAII4/stageflow
