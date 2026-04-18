"""
Fairport Flood Monitor
======================
Monitors Mississippi River flood risk for a property in Fairport, Iowa.

Gauges:
  ILNI2  - Mississippi at Illinois City (PRIMARY - has NWS forecast)
  WAPI4  - Iowa River at Wapello (EARLY WARNING - 2-5 day lead time)
  FAII4  - Mississippi above Fairport (OBSERVED ONLY - no forecast)

Notification: ntfy.sh push notifications
Platform: GitHub Actions (runs on GitHub's servers, free)
"""

import requests
import json
import os
import sys
from datetime import datetime, timezone, timedelta

# ─── CONFIGURATION ────────────────────────────────────────────────────────────

NTFY_TOPIC = "fairport-flood-alerts-563"
NTFY_SERVER = "https://ntfy.sh"

NOAA_BASE = "https://api.water.noaa.gov/nwps/v1"

# Personal thresholds
GARAGE_FLOODS_FT       = 15.0   # Garage takes water at this Fairport level
PERSONAL_ALERT_FT      = 13.0   # Start paying close attention (Fairport observed)
ILNI2_ALERT_FT         = 13.5   # ILNI2 personal alert threshold
ILNI2_FLOOD_FT         = 15.0   # ILNI2 minor flood / garage equivalent
ILNI2_ACTION_FT        = 14.0   # ILNI2 action stage
ILNI2_MODERATE_FT      = 16.0
ILNI2_MAJOR_FT         = 18.0
WAPELLO_ALARM_FT       = 22.0   # 100% historical correlation with Fairport 15ft
WAPELLO_WATCH_FT       = 20.0   # Approaching alarm - elevated watch
DAM_GAP_WARNING_FT     = 0.6    # FAII4 - ILNI2 gap below this = dam losing pool control
GARAGE_PREP_DAYS       = 3      # Days needed to empty garage

# ─── NOAA API ─────────────────────────────────────────────────────────────────

def fetch_stageflow(gauge_id):
    """Fetch current observations and NWS forecast for a gauge."""
    url = f"{NOAA_BASE}/gauges/{gauge_id}/stageflow"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"WARNING: Could not fetch {gauge_id}: {e}")
        return None


def fetch_gauge_meta(gauge_id):
    """Fetch gauge metadata including flood categories and crest history."""
    url = f"{NOAA_BASE}/gauges/{gauge_id}"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"WARNING: Could not fetch metadata for {gauge_id}: {e}")
        return None


def get_current_stage(stageflow):
    """Extract the most recent observed stage in feet."""
    try:
        obs = stageflow.get("observed", {}).get("data", [])
        # Walk backwards to find the last non-null reading
        for entry in reversed(obs):
            val = entry.get("primary")
            if val is not None:
                return float(val), entry.get("validTime")
        return None, None
    except Exception:
        return None, None


def get_forecast_series(stageflow):
    """
    Return forecast as list of (datetime, stage_ft) tuples.
    Returns empty list if no forecast available.
    """
    try:
        fc = stageflow.get("forecast", {}).get("data", [])
        result = []
        for entry in fc:
            val = entry.get("primary")
            t   = entry.get("validTime")
            if val is not None and t is not None:
                dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
                result.append((dt, float(val)))
        return result
    except Exception:
        return []


def forecast_peak(series):
    """Return (peak_stage, peak_datetime) from a forecast series."""
    if not series:
        return None, None
    peak = max(series, key=lambda x: x[1])
    return peak[1], peak[0]


def days_until_stage(series, target_ft):
    """
    Find when the forecast first crosses target_ft (rising).
    Returns fractional days from now, or None if never crossed.
    """
    now = datetime.now(timezone.utc)
    for i in range(len(series) - 1):
        t0, s0 = series[i]
        t1, s1 = series[i + 1]
        if s0 < target_ft <= s1:
            # Linear interpolation
            frac = (target_ft - s0) / (s1 - s0)
            cross_time = t0 + (t1 - t0) * frac
            delta = cross_time - now
            return delta.total_seconds() / 86400, cross_time
    return None, None


def trend_6hr(stageflow):
    """Calculate stage change over last 6 hours."""
    try:
        obs = [(e["validTime"], float(e["primary"]))
               for e in stageflow.get("observed", {}).get("data", [])
               if e.get("primary") is not None]
        if len(obs) < 2:
            return None
        # Most recent
        latest_t = datetime.fromisoformat(obs[-1][0].replace("Z", "+00:00"))
        latest_s = obs[-1][1]
        # Find reading ~6 hours ago
        target_t = latest_t - timedelta(hours=6)
        closest = min(obs, key=lambda x: abs(
            datetime.fromisoformat(x[0].replace("Z", "+00:00")) - target_t
        ))
        return latest_s - closest[1]
    except Exception:
        return None


def rise_rate_ftperday(stageflow):
    """Estimate current rise rate in ft/day from recent observations."""
    try:
        obs = [(datetime.fromisoformat(e["validTime"].replace("Z", "+00:00")),
                float(e["primary"]))
               for e in stageflow.get("observed", {}).get("data", [])
               if e.get("primary") is not None]
        if len(obs) < 6:
            return None
        # Use last 12 hours of data
        recent = obs[-12:]
        dt_hours = (recent[-1][0] - recent[0][0]).total_seconds() / 3600
        if dt_hours < 1:
            return None
        ds = recent[-1][1] - recent[0][1]
        return (ds / dt_hours) * 24  # ft/day
    except Exception:
        return None


# ─── NTFY NOTIFICATIONS ───────────────────────────────────────────────────────

def send_ntfy(title, message, priority="default", tags=None):
    """Send a push notification via ntfy.sh."""
    headers = {
        "Title": title,
        "Priority": priority,
        "Content-Type": "text/plain",
    }
    if tags:
        headers["Tags"] = ",".join(tags)

    try:
        r = requests.post(
            f"{NTFY_SERVER}/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers=headers,
            timeout=10
        )
        r.raise_for_status()
        print(f"✓ Notification sent: {title}")
        return True
    except Exception as e:
        print(f"✗ Failed to send notification: {e}")
        return False


# ─── FORMATTING HELPERS ───────────────────────────────────────────────────────

def fmt_ft(val):
    return f"{val:.2f} ft" if val is not None else "N/A"


def fmt_dt(dt):
    if dt is None:
        return "unknown"
    # Convert to CDT (UTC-5 in April)
    cdt = dt - timedelta(hours=5)
    return cdt.strftime("%a %b %-d %-I:%M %p CDT")


def fmt_days(d):
    if d is None:
        return None
    if d < 1:
        hours = d * 24
        return f"{hours:.0f} hours"
    return f"{d:.1f} days"


def dam_mode_assessment(faii4_stage, ilni2_stage):
    """
    Assess Lock & Dam 16 pool status from gauge gap.
    Normal pool: FAII4 reads 1.1-1.6 ft higher than ILNI2
    Flood/open gates: gap closes to 0.3-0.5 ft
    """
    if faii4_stage is None or ilni2_stage is None:
        return None, "unknown"
    gap = faii4_stage - ilni2_stage
    if gap >= 1.0:
        return gap, "normal pool (dam holding)"
    elif gap >= 0.6:
        return gap, "pool reducing (watch closely)"
    elif gap >= 0.3:
        return gap, "⚠️ FLOOD MODE — dam gates opening"
    else:
        return gap, "🔴 GATES WIDE OPEN — full flood flow"


# ─── ALERT LOGIC ──────────────────────────────────────────────────────────────

def check_alerts():
    """
    6-hour check. Fires alerts if any threshold is crossed.
    Returns True if a critical alert was sent.
    """
    print(f"\n{'='*60}")
    print(f"FAIRPORT FLOOD MONITOR — Alert Check")
    print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    # Fetch all gauges
    ilni2_data = fetch_stageflow("ILNI2")
    wapi4_data = fetch_stageflow("WAPI4")
    faii4_data = fetch_stageflow("FAII4")

    if ilni2_data is None:
        send_ntfy("⚠️ Flood Monitor Error",
                  "Could not reach NOAA API. Check water.noaa.gov manually.",
                  priority="high", tags=["warning"])
        return False

    # Current stages
    ilni2_stage, ilni2_time  = get_current_stage(ilni2_data)
    wapi4_stage, wapi4_time  = get_current_stage(wapi4_data) if wapi4_data else (None, None)
    faii4_stage, faii4_time  = get_current_stage(faii4_data) if faii4_data else (None, None)

    # Forecasts
    ilni2_fc   = get_forecast_series(ilni2_data)
    wapi4_fc   = get_forecast_series(wapi4_data) if wapi4_data else []

    # Peak forecasts
    ilni2_peak, ilni2_peak_dt   = forecast_peak(ilni2_fc)
    wapi4_peak, wapi4_peak_dt   = forecast_peak(wapi4_fc)

    # Days until ILNI2 hits key thresholds
    days_to_alert, cross_alert_dt   = days_until_stage(ilni2_fc, ILNI2_ALERT_FT)
    days_to_flood, cross_flood_dt   = days_until_stage(ilni2_fc, ILNI2_FLOOD_FT)
    days_to_action, cross_action_dt = days_until_stage(ilni2_fc, ILNI2_ACTION_FT)

    # Days until Wapello alarm threshold in forecast
    days_wap_alarm, _ = days_until_stage(wapi4_fc, WAPELLO_ALARM_FT)
    days_wap_watch, _ = days_until_stage(wapi4_fc, WAPELLO_WATCH_FT)

    # Rise rates
    ilni2_rate = rise_rate_ftperday(ilni2_data)
    faii4_rate = rise_rate_ftperday(faii4_data) if faii4_data else None

    # Dam gap assessment
    dam_gap, dam_status = dam_mode_assessment(faii4_stage, ilni2_stage)

    # Trend
    ilni2_trend = trend_6hr(ilni2_data)

    print(f"ILNI2: {fmt_ft(ilni2_stage)} | Peak forecast: {fmt_ft(ilni2_peak)}")
    print(f"WAPI4: {fmt_ft(wapi4_stage)} | Peak forecast: {fmt_ft(wapi4_peak)}")
    print(f"FAII4: {fmt_ft(faii4_stage)} (observed only)")
    print(f"Dam gap: {fmt_ft(dam_gap)} — {dam_status}")
    print(f"Days to 15ft (ILNI2): {fmt_days(days_to_flood)}")

    # ── DETERMINE ALERT LEVEL ────────────────────────────────────────────────

    alert_level = 0   # 0=none, 1=watch, 2=warning, 3=critical
    reasons = []

    # Critical: forecast hits garage flood level
    if ilni2_peak is not None and ilni2_peak >= ILNI2_FLOOD_FT:
        alert_level = max(alert_level, 3)
        reasons.append(f"ILNI2 forecast peak {fmt_ft(ilni2_peak)} — GARAGE FLOOD EXPECTED")

    # Critical: Wapello hits or forecast to hit 22ft alarm
    if wapi4_stage is not None and wapi4_stage >= WAPELLO_ALARM_FT:
        alert_level = max(alert_level, 3)
        reasons.append(f"Wapello AT {fmt_ft(wapi4_stage)} — 100% historical flood correlation triggered")

    if days_wap_alarm is not None:
        alert_level = max(alert_level, 3)
        reasons.append(f"Wapello FORECAST to hit 22 ft in {fmt_days(days_wap_alarm)}")

    # Critical: less than GARAGE_PREP_DAYS until flood stage
    if days_to_flood is not None and days_to_flood <= GARAGE_PREP_DAYS:
        alert_level = max(alert_level, 3)
        reasons.append(f"Only {fmt_days(days_to_flood)} until 15 ft — LESS THAN 3-DAY GARAGE WINDOW")

    # Warning: forecast hits action stage or dam losing pool control
    if ilni2_peak is not None and ilni2_peak >= ILNI2_ACTION_FT:
        alert_level = max(alert_level, 2)
        reasons.append(f"ILNI2 forecast peak {fmt_ft(ilni2_peak)} — approaching flood stage")

    if dam_gap is not None and dam_gap < DAM_GAP_WARNING_FT:
        alert_level = max(alert_level, 2)
        reasons.append(f"Dam gap only {fmt_ft(dam_gap)} — Lock & Dam 16 losing pool control")

    if days_to_flood is not None and days_to_flood <= 5:
        alert_level = max(alert_level, 2)
        reasons.append(f"{fmt_days(days_to_flood)} until 15 ft at ILNI2")

    # Watch: Fairport above personal alert, or Wapello approaching 22ft
    if faii4_stage is not None and faii4_stage >= PERSONAL_ALERT_FT:
        alert_level = max(alert_level, 1)
        reasons.append(f"Fairport at {fmt_ft(faii4_stage)} — above personal alert threshold")

    if ilni2_stage is not None and ilni2_stage >= ILNI2_ALERT_FT:
        alert_level = max(alert_level, 1)
        reasons.append(f"ILNI2 at {fmt_ft(ilni2_stage)} — above personal alert threshold")

    if wapi4_peak is not None and wapi4_peak >= WAPELLO_WATCH_FT:
        alert_level = max(alert_level, 1)
        reasons.append(f"Wapello forecast peak {fmt_ft(wapi4_peak)} — approaching 22 ft alarm")

    if alert_level == 0:
        print("✓ No alert conditions met.")
        return False

    # ── BUILD AND SEND ALERT ─────────────────────────────────────────────────

    # Garage prep countdown language
    def garage_line():
        if days_to_flood is None:
            if ilni2_peak and ilni2_peak >= ILNI2_FLOOD_FT:
                return "⏰ Garage prep: START NOW (crossing imminent)"
            return ""
        if days_to_flood <= 1:
            return f"🚨 Garage prep: START IMMEDIATELY ({fmt_days(days_to_flood)} to 15 ft)"
        elif days_to_flood <= GARAGE_PREP_DAYS:
            return f"🚨 Garage prep: START NOW ({fmt_days(days_to_flood)} to 15 ft — inside 3-day window)"
        else:
            return f"⏰ Garage prep: {fmt_days(days_to_flood)} until 15 ft (3 days needed)"

    if alert_level == 3:
        emoji = "🚨"
        title = "🚨 FLOOD ALERT — Garage Flooding Likely"
        priority = "urgent"
        tags = ["rotating_light", "droplet"]
    elif alert_level == 2:
        emoji = "⚠️"
        title = "⚠️ Flood WARNING — Action Required"
        priority = "high"
        tags = ["warning", "droplet"]
    else:
        emoji = "👀"
        title = "👀 Flood WATCH — Monitor Closely"
        priority = "default"
        tags = ["eyes", "droplet"]

    lines = [
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "── TRIGGER CONDITIONS ──",
    ]
    for r in reasons:
        lines.append(f"• {r}")

    lines += [
        "",
        "── CURRENT CONDITIONS ──",
        f"ILNI2 (Illinois City): {fmt_ft(ilni2_stage)}",
        f"  Forecast peak: {fmt_ft(ilni2_peak)} around {fmt_dt(ilni2_peak_dt)}",
        f"  Rise rate: {f'{ilni2_rate:+.2f} ft/day' if ilni2_rate else 'N/A'}",
        f"  6hr trend: {f'{ilni2_trend:+.2f} ft' if ilni2_trend else 'N/A'}",
        "",
        f"Fairport (FAII4): {fmt_ft(faii4_stage)} (observed only)",
        f"  Dam gap vs ILNI2: {fmt_ft(dam_gap)} — {dam_status}",
        "",
        f"Wapello (WAPI4): {fmt_ft(wapi4_stage)}",
        f"  Forecast peak: {fmt_ft(wapi4_peak)} around {fmt_dt(wapi4_peak_dt)}",
        f"  22 ft alarm: {'TRIGGERED' if wapi4_stage and wapi4_stage >= WAPELLO_ALARM_FT else 'Not triggered'}",
        "",
        "── GARAGE DECISION ──",
        garage_line(),
    ]

    if days_to_action is not None:
        lines.append(f"Action stage (14 ft): {fmt_days(days_to_action)}")
    if days_to_flood is not None:
        lines.append(f"Flood stage (15 ft):  {fmt_days(days_to_flood)} → ~{fmt_dt(cross_flood_dt)}")

    message = "\n".join(lines)
    send_ntfy(title, message, priority=priority, tags=tags)
    return alert_level >= 2


# ─── WEEKLY REPORT ────────────────────────────────────────────────────────────

def weekly_report():
    """
    Full weekly briefing with 10-day forecast table,
    all gauge status, dam analysis, and plain-English recommendation.
    """
    print(f"\n{'='*60}")
    print(f"FAIRPORT FLOOD MONITOR — Weekly Report")
    print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    ilni2_data = fetch_stageflow("ILNI2")
    wapi4_data = fetch_stageflow("WAPI4")
    faii4_data = fetch_stageflow("FAII4")
    ilni2_meta = fetch_gauge_meta("ILNI2")

    if ilni2_data is None:
        send_ntfy("⚠️ Weekly Report Failed",
                  "Could not reach NOAA API for weekly report.",
                  priority="high", tags=["warning"])
        return

    ilni2_stage, _ = get_current_stage(ilni2_data)
    wapi4_stage, _ = get_current_stage(wapi4_data) if wapi4_data else (None, None)
    faii4_stage, _ = get_current_stage(faii4_data) if faii4_data else (None, None)

    ilni2_fc = get_forecast_series(ilni2_data)
    wapi4_fc = get_forecast_series(wapi4_data) if wapi4_data else []

    ilni2_peak, ilni2_peak_dt = forecast_peak(ilni2_fc)
    wapi4_peak, wapi4_peak_dt = forecast_peak(wapi4_fc)

    days_to_flood, cross_flood_dt   = days_until_stage(ilni2_fc, ILNI2_FLOOD_FT)
    days_to_action, cross_action_dt = days_until_stage(ilni2_fc, ILNI2_ACTION_FT)
    days_to_alert, _                = days_until_stage(ilni2_fc, ILNI2_ALERT_FT)
    days_wap_alarm, _               = days_until_stage(wapi4_fc, WAPELLO_ALARM_FT)

    ilni2_rate = rise_rate_ftperday(ilni2_data)
    ilni2_trend = trend_6hr(ilni2_data)
    dam_gap, dam_status = dam_mode_assessment(faii4_stage, ilni2_stage)

    # 10-day forecast table (daily max per day)
    from collections import defaultdict
    daily_max = defaultdict(float)
    for dt, stage in ilni2_fc:
        cdt = dt - timedelta(hours=5)  # CDT
        day_key = cdt.strftime("%Y-%m-%d %a")
        daily_max[day_key] = max(daily_max[day_key], stage)

    # Plain English recommendation
    def recommendation():
        if days_to_flood is not None and days_to_flood <= GARAGE_PREP_DAYS:
            return (f"🚨 MOVE YOUR GARAGE NOW. 15 ft expected in {fmt_days(days_to_flood)}. "
                    f"You need 3 days — you're inside that window.")
        elif days_to_flood is not None and days_to_flood <= 5:
            return (f"⚠️ START GARAGE PREP. {fmt_days(days_to_flood)} until 15 ft. "
                    f"You have time but do not wait.")
        elif ilni2_peak is not None and ilni2_peak >= ILNI2_FLOOD_FT:
            return (f"⚠️ Flood stage expected (peak {fmt_ft(ilni2_peak)}) but timing uncertain. "
                    f"Monitor daily and be ready to move garage on short notice.")
        elif wapi4_peak is not None and wapi4_peak >= WAPELLO_ALARM_FT:
            return (f"⚠️ Wapello forecast to hit 22 ft — historically 100% correlation with "
                    f"your garage flooding. Watch ILNI2 daily.")
        elif ilni2_peak is not None and ilni2_peak >= ILNI2_ACTION_FT:
            return (f"👀 Elevated risk. Peak forecast {fmt_ft(ilni2_peak)} — below flood stage "
                    f"but monitor closely. No garage action yet.")
        elif dam_gap is not None and dam_gap < DAM_GAP_WARNING_FT:
            return (f"👀 Dam gap narrowing ({fmt_ft(dam_gap)}) — river rising fast toward Lock & Dam 16. "
                    f"Watch daily.")
        else:
            return (f"✅ No action needed. Peak forecast {fmt_ft(ilni2_peak)} — well below 15 ft. "
                    f"Normal weekly monitoring.")

    # Build message
    lines = [
        f"📅 Fairport Weekly Flood Report — {datetime.now(timezone.utc).strftime('%B %-d, %Y')}",
        f"Generated: {datetime.now(timezone.utc).strftime('%H:%M UTC')}",
        "",
        "━━ CURRENT CONDITIONS ━━",
        f"ILNI2 (Illinois City): {fmt_ft(ilni2_stage)}",
        f"  Thresholds: Alert 13.5 | Action 14 | Flood/Garage 15 | Mod 16 | Major 18",
        f"  Rise rate: {f'{ilni2_rate:+.2f} ft/day' if ilni2_rate else 'N/A'}",
        f"  6hr trend: {f'{ilni2_trend:+.2f} ft' if ilni2_trend else 'N/A'}",
        "",
        f"Fairport (FAII4): {fmt_ft(faii4_stage)} (observed only, no NWS forecast)",
        f"  Dam gap vs ILNI2: {fmt_ft(dam_gap)}",
        f"  Dam status: {dam_status}",
        f"  Note: Normal gap 1.1-1.6 ft | Flood gap 0.3-0.5 ft",
        "",
        f"Wapello (WAPI4): {fmt_ft(wapi4_stage)}",
        f"  Forecast peak: {fmt_ft(wapi4_peak)} around {fmt_dt(wapi4_peak_dt)}",
        f"  22 ft alarm threshold: {'🔴 TRIGGERED' if wapi4_stage and wapi4_stage >= WAPELLO_ALARM_FT else 'Not triggered (need ' + (fmt_ft(WAPELLO_ALARM_FT - wapi4_stage) if wapi4_stage else 'N/A') + ' more)'}",
        f"  20 ft watch threshold: {'⚠️ EXCEEDED' if wapi4_stage and wapi4_stage >= WAPELLO_WATCH_FT else 'Not reached'}",
        "",
WAPELLO_WATCH_FT       = 20.0   # Approaching alarm - elevated watch
DAM_GAP_WARNING_FT     = 0.6    # FAII4 - ILNI2 gap below this = dam losing pool control
GARAGE_PREP_DAYS       = 3      # Days needed to empty garage

# ─── NOAA API ─────────────────────────────────────────────────────────────────

def fetch_stageflow(gauge_id):
    """Fetch current observations and NWS forecast for a gauge."""
    url = f"{NOAA_BASE}/gauges/{gauge_id}/stageflow"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"WARNING: Could not fetch {gauge_id}: {e}")
        return None


def fetch_gauge_meta(gauge_id):
    """Fetch gauge metadata including flood categories and crest history."""
    url = f"{NOAA_BASE}/gauges/{gauge_id}"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"WARNING: Could not fetch metadata for {gauge_id}: {e}")
        return None


def get_current_stage(stageflow):
    """Extract the most recent observed stage in feet."""
    try:
        obs = stageflow.get("observed", {}).get("data", [])
        # Walk backwards to find the last non-null reading
        for entry in reversed(obs):
            val = entry.get("primary")
            if val is not None:
                return float(val), entry.get("validTime")
        return None, None
    except Exception:
        return None, None


def get_forecast_series(stageflow):
    """
    Return forecast as list of (datetime, stage_ft) tuples.
    Returns empty list if no forecast available.
    """
    try:
        fc = stageflow.get("forecast", {}).get("data", [])
        result = []
        for entry in fc:
            val = entry.get("primary")
            t   = entry.get("validTime")
            if val is not None and t is not None:
                dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
                result.append((dt, float(val)))
        return result
    except Exception:
        return []


def forecast_peak(series):
    """Return (peak_stage, peak_datetime) from a forecast series."""
    if not series:
        return None, None
    peak = max(series, key=lambda x: x[1])
    return peak[1], peak[0]


def days_until_stage(series, target_ft):
    """
    Find when the forecast first crosses target_ft (rising).
    Returns fractional days from now, or None if never crossed.
    """
    now = datetime.now(timezone.utc)
    for i in range(len(series) - 1):
        t0, s0 = series[i]
        t1, s1 = series[i + 1]
        if s0 < target_ft <= s1:
            # Linear interpolation
            frac = (target_ft - s0) / (s1 - s0)
            cross_time = t0 + (t1 - t0) * frac
            delta = cross_time - now
            return delta.total_seconds() / 86400, cross_time
    return None, None


def trend_6hr(stageflow):
    """Calculate stage change over last 6 hours."""
    try:
        obs = [(e["validTime"], float(e["primary"]))
               for e in stageflow.get("observed", {}).get("data", [])
               if e.get("primary") is not None]
        if len(obs) < 2:
            return None
        # Most recent
        latest_t = datetime.fromisoformat(obs[-1][0].replace("Z", "+00:00"))
        latest_s = obs[-1][1]
        # Find reading ~6 hours ago
        target_t = latest_t - timedelta(hours=6)
        closest = min(obs, key=lambda x: abs(
            datetime.fromisoformat(x[0].replace("Z", "+00:00")) - target_t
        ))
        return latest_s - closest[1]
    except Exception:
        return None


def rise_rate_ftperday(stageflow):
    """Estimate current rise rate in ft/day from recent observations."""
    try:
        obs = [(datetime.fromisoformat(e["validTime"].replace("Z", "+00:00")),
                float(e["primary"]))
               for e in stageflow.get("observed", {}).get("data", [])
               if e.get("primary") is not None]
        if len(obs) < 6:
            return None
        # Use last 12 hours of data
        recent = obs[-12:]
        dt_hours = (recent[-1][0] - recent[0][0]).total_seconds() / 3600
        if dt_hours < 1:
            return None
        ds = recent[-1][1] - recent[0][1]
        return (ds / dt_hours) * 24  # ft/day
    except Exception:
        return None


# ─── NTFY NOTIFICATIONS ───────────────────────────────────────────────────────

def send_ntfy(title, message, priority="default", tags=None):
    """Send a push notification via ntfy.sh."""
    headers = {
        "Title": title,
        "Priority": priority,
        "Content-Type": "text/plain",
    }
    if tags:
        headers["Tags"] = ",".join(tags)

    try:
        r = requests.post(
            f"{NTFY_SERVER}/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers=headers,
            timeout=10
        )
        r.raise_for_status()
        print(f"✓ Notification sent: {title}")
        return True
    except Exception as e:
        print(f"✗ Failed to send notification: {e}")
        return False


# ─── FORMATTING HELPERS ───────────────────────────────────────────────────────

def fmt_ft(val):
    return f"{val:.2f} ft" if val is not None else "N/A"


def fmt_dt(dt):
    if dt is None:
        return "unknown"
    # Convert to CDT (UTC-5 in April)
    cdt = dt - timedelta(hours=5)
    return cdt.strftime("%a %b %-d %-I:%M %p CDT")


def fmt_days(d):
    if d is None:
        return None
    if d < 1:
        hours = d * 24
        return f"{hours:.0f} hours"
    return f"{d:.1f} days"


def dam_mode_assessment(faii4_stage, ilni2_stage):
    """
    Assess Lock & Dam 16 pool status from gauge gap.
    Normal pool: FAII4 reads 1.1-1.6 ft higher than ILNI2
    Flood/open gates: gap closes to 0.3-0.5 ft
    """
    if faii4_stage is None or ilni2_stage is None:
        return None, "unknown"
    gap = faii4_stage - ilni2_stage
    if gap >= 1.0:
        return gap, "normal pool (dam holding)"
    elif gap >= 0.6:
        return gap, "pool reducing (watch closely)"
    elif gap >= 0.3:
        return gap, "⚠️ FLOOD MODE — dam gates opening"
    else:
        return gap, "🔴 GATES WIDE OPEN — full flood flow"


# ─── ALERT LOGIC ──────────────────────────────────────────────────────────────

def check_alerts():
    """
    6-hour check. Fires alerts if any threshold is crossed.
    Returns True if a critical alert was sent.
    """
    print(f"\n{'='*60}")
    print(f"FAIRPORT FLOOD MONITOR — Alert Check")
    print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    # Fetch all gauges
    ilni2_data = fetch_stageflow("ILNI2")
    wapi4_data = fetch_stageflow("WAPI4")
    faii4_data = fetch_stageflow("FAII4")

    if ilni2_data is None:
        send_ntfy("⚠️ Flood Monitor Error",
                  "Could not reach NOAA API. Check water.noaa.gov manually.",
                  priority="high", tags=["warning"])
        return False

    # Current stages
    ilni2_stage, ilni2_time  = get_current_stage(ilni2_data)
    wapi4_stage, wapi4_time  = get_current_stage(wapi4_data) if wapi4_data else (None, None)
    faii4_stage, faii4_time  = get_current_stage(faii4_data) if faii4_data else (None, None)

    # Forecasts
    ilni2_fc   = get_forecast_series(ilni2_data)
    wapi4_fc   = get_forecast_series(wapi4_data) if wapi4_data else []

    # Peak forecasts
    ilni2_peak, ilni2_peak_dt   = forecast_peak(ilni2_fc)
    wapi4_peak, wapi4_peak_dt   = forecast_peak(wapi4_fc)

    # Days until ILNI2 hits key thresholds
    days_to_alert, cross_alert_dt   = days_until_stage(ilni2_fc, ILNI2_ALERT_FT)
    days_to_flood, cross_flood_dt   = days_until_stage(ilni2_fc, ILNI2_FLOOD_FT)
    days_to_action, cross_action_dt = days_until_stage(ilni2_fc, ILNI2_ACTION_FT)

    # Days until Wapello alarm threshold in forecast
    days_wap_alarm, _ = days_until_stage(wapi4_fc, WAPELLO_ALARM_FT)
    days_wap_watch, _ = days_until_stage(wapi4_fc, WAPELLO_WATCH_FT)

    # Rise rates
    ilni2_rate = rise_rate_ftperday(ilni2_data)
    faii4_rate = rise_rate_ftperday(faii4_data) if faii4_data else None

    # Dam gap assessment
    dam_gap, dam_status = dam_mode_assessment(faii4_stage, ilni2_stage)

    # Trend
    ilni2_trend = trend_6hr(ilni2_data)

    print(f"ILNI2: {fmt_ft(ilni2_stage)} | Peak forecast: {fmt_ft(ilni2_peak)}")
    print(f"WAPI4: {fmt_ft(wapi4_stage)} | Peak forecast: {fmt_ft(wapi4_peak)}")
    print(f"FAII4: {fmt_ft(faii4_stage)} (observed only)")
    print(f"Dam gap: {fmt_ft(dam_gap)} — {dam_status}")
    print(f"Days to 15ft (ILNI2): {fmt_days(days_to_flood)}")

    # ── DETERMINE ALERT LEVEL ────────────────────────────────────────────────

    alert_level = 0   # 0=none, 1=watch, 2=warning, 3=critical
    reasons = []

    # Critical: forecast hits garage flood level
    if ilni2_peak is not None and ilni2_peak >= ILNI2_FLOOD_FT:
        alert_level = max(alert_level, 3)
        reasons.append(f"ILNI2 forecast peak {fmt_ft(ilni2_peak)} — GARAGE FLOOD EXPECTED")

    # Critical: Wapello hits or forecast to hit 22ft alarm
    if wapi4_stage is not None and wapi4_stage >= WAPELLO_ALARM_FT:
        alert_level = max(alert_level, 3)
        reasons.append(f"Wapello AT {fmt_ft(wapi4_stage)} — 100% historical flood correlation triggered")

    if days_wap_alarm is not None:
        alert_level = max(alert_level, 3)
        reasons.append(f"Wapello FORECAST to hit 22 ft in {fmt_days(days_wap_alarm)}")

    # Critical: less than GARAGE_PREP_DAYS until flood stage
    if days_to_flood is not None and days_to_flood <= GARAGE_PREP_DAYS:
        alert_level = max(alert_level, 3)
        reasons.append(f"Only {fmt_days(days_to_flood)} until 15 ft — LESS THAN 3-DAY GARAGE WINDOW")

    # Warning: forecast hits action stage or dam losing pool control
    if ilni2_peak is not None and ilni2_peak >= ILNI2_ACTION_FT:
        alert_level = max(alert_level, 2)
        reasons.append(f"ILNI2 forecast peak {fmt_ft(ilni2_peak)} — approaching flood stage")

    if dam_gap is not None and dam_gap < DAM_GAP_WARNING_FT:
        alert_level = max(alert_level, 2)
        reasons.append(f"Dam gap only {fmt_ft(dam_gap)} — Lock & Dam 16 losing pool control")

    if days_to_flood is not None and days_to_flood <= 5:
        alert_level = max(alert_level, 2)
        reasons.append(f"{fmt_days(days_to_flood)} until 15 ft at ILNI2")

    # Watch: Fairport above personal alert, or Wapello approaching 22ft
    if faii4_stage is not None and faii4_stage >= PERSONAL_ALERT_FT:
        alert_level = max(alert_level, 1)
        reasons.append(f"Fairport at {fmt_ft(faii4_stage)} — above personal alert threshold")

    if ilni2_stage is not None and ilni2_stage >= ILNI2_ALERT_FT:
        alert_level = max(alert_level, 1)
        reasons.append(f"ILNI2 at {fmt_ft(ilni2_stage)} — above personal alert threshold")

    if wapi4_peak is not None and wapi4_peak >= WAPELLO_WATCH_FT:
        alert_level = max(alert_level, 1)
        reasons.append(f"Wapello forecast peak {fmt_ft(wapi4_peak)} — approaching 22 ft alarm")

    if alert_level == 0:
        print("✓ No alert conditions met.")
        return False

    # ── BUILD AND SEND ALERT ─────────────────────────────────────────────────

    # Garage prep countdown language
    def garage_line():
        if days_to_flood is None:
            if ilni2_peak and ilni2_peak >= ILNI2_FLOOD_FT:
                return "⏰ Garage prep: START NOW (crossing imminent)"
            return ""
        if days_to_flood <= 1:
            return f"🚨 Garage prep: START IMMEDIATELY ({fmt_days(days_to_flood)} to 15 ft)"
        elif days_to_flood <= GARAGE_PREP_DAYS:
            return f"🚨 Garage prep: START NOW ({fmt_days(days_to_flood)} to 15 ft — inside 3-day window)"
        else:
            return f"⏰ Garage prep: {fmt_days(days_to_flood)} until 15 ft (3 days needed)"

    if alert_level == 3:
        emoji = "🚨"
        title = "🚨 FLOOD ALERT — Garage Flooding Likely"
        priority = "urgent"
        tags = ["rotating_light", "droplet"]
    elif alert_level == 2:
        emoji = "⚠️"
        title = "⚠️ Flood WARNING — Action Required"
        priority = "high"
        tags = ["warning", "droplet"]
    else:
        emoji = "👀"
        title = "👀 Flood WATCH — Monitor Closely"
        priority = "default"
        tags = ["eyes", "droplet"]

    lines = [
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "── TRIGGER CONDITIONS ──",
    ]
    for r in reasons:
        lines.append(f"• {r}")

    lines += [
        "",
        "── CURRENT CONDITIONS ──",
        f"ILNI2 (Illinois City): {fmt_ft(ilni2_stage)}",
        f"  Forecast peak: {fmt_ft(ilni2_peak)} around {fmt_dt(ilni2_peak_dt)}",
        f"  Rise rate: {f'{ilni2_rate:+.2f} ft/day' if ilni2_rate else 'N/A'}",
        f"  6hr trend: {f'{ilni2_trend:+.2f} ft' if ilni2_trend else 'N/A'}",
        "",
        f"Fairport (FAII4): {fmt_ft(faii4_stage)} (observed only)",
        f"  Dam gap vs ILNI2: {fmt_ft(dam_gap)} — {dam_status}",
        "",
        f"Wapello (WAPI4): {fmt_ft(wapi4_stage)}",
        f"  Forecast peak: {fmt_ft(wapi4_peak)} around {fmt_dt(wapi4_peak_dt)}",
        f"  22 ft alarm: {'TRIGGERED' if wapi4_stage and wapi4_stage >= WAPELLO_ALARM_FT else 'Not triggered'}",
        "",
        "── GARAGE DECISION ──",
        garage_line(),
    ]

    if days_to_action is not None:
        lines.append(f"Action stage (14 ft): {fmt_days(days_to_action)}")
    if days_to_flood is not None:
        lines.append(f"Flood stage (15 ft):  {fmt_days(days_to_flood)} → ~{fmt_dt(cross_flood_dt)}")

    message = "\n".join(lines)
    send_ntfy(title, message, priority=priority, tags=tags)
    return alert_level >= 2


# ─── WEEKLY REPORT ────────────────────────────────────────────────────────────

def weekly_report():
    """
    Full weekly briefing with 10-day forecast table,
    all gauge status, dam analysis, and plain-English recommendation.
    """
    print(f"\n{'='*60}")
    print(f"FAIRPORT FLOOD MONITOR — Weekly Report")
    print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    ilni2_data = fetch_stageflow("ILNI2")
    wapi4_data = fetch_stageflow("WAPI4")
    faii4_data = fetch_stageflow("FAII4")
    ilni2_meta = fetch_gauge_meta("ILNI2")

    if ilni2_data is None:
        send_ntfy("⚠️ Weekly Report Failed",
                  "Could not reach NOAA API for weekly report.",
                  priority="high", tags=["warning"])
        return

    ilni2_stage, _ = get_current_stage(ilni2_data)
    wapi4_stage, _ = get_current_stage(wapi4_data) if wapi4_data else (None, None)
    faii4_stage, _ = get_current_stage(faii4_data) if faii4_data else (None, None)

    ilni2_fc = get_forecast_series(ilni2_data)
    wapi4_fc = get_forecast_series(wapi4_data) if wapi4_data else []

    ilni2_peak, ilni2_peak_dt = forecast_peak(ilni2_fc)
    wapi4_peak, wapi4_peak_dt = forecast_peak(wapi4_fc)

    days_to_flood, cross_flood_dt   = days_until_stage(ilni2_fc, ILNI2_FLOOD_FT)
    days_to_action, cross_action_dt = days_until_stage(ilni2_fc, ILNI2_ACTION_FT)
    days_to_alert, _                = days_until_stage(ilni2_fc, ILNI2_ALERT_FT)
    days_wap_alarm, _               = days_until_stage(wapi4_fc, WAPELLO_ALARM_FT)

    ilni2_rate = rise_rate_ftperday(ilni2_data)
    ilni2_trend = trend_6hr(ilni2_data)
    dam_gap, dam_status = dam_mode_assessment(faii4_stage, ilni2_stage)

    # 10-day forecast table (daily max per day)
    from collections import defaultdict
    daily_max = defaultdict(float)
    for dt, stage in ilni2_fc:
        cdt = dt - timedelta(hours=5)  # CDT
        day_key = cdt.strftime("%Y-%m-%d %a")
        daily_max[day_key] = max(daily_max[day_key], stage)

    # Plain English recommendation
    def recommendation():
        if days_to_flood is not None and days_to_flood <= GARAGE_PREP_DAYS:
            return (f"🚨 MOVE YOUR GARAGE NOW. 15 ft expected in {fmt_days(days_to_flood)}. "
                    f"You need 3 days — you're inside that window.")
        elif days_to_flood is not None and days_to_flood <= 5:
            return (f"⚠️ START GARAGE PREP. {fmt_days(days_to_flood)} until 15 ft. "
                    f"You have time but do not wait.")
        elif ilni2_peak is not None and ilni2_peak >= ILNI2_FLOOD_FT:
            return (f"⚠️ Flood stage expected (peak {fmt_ft(ilni2_peak)}) but timing uncertain. "
                    f"Monitor daily and be ready to move garage on short notice.")
        elif wapi4_peak is not None and wapi4_peak >= WAPELLO_ALARM_FT:
            return (f"⚠️ Wapello forecast to hit 22 ft — historically 100% correlation with "
                    f"your garage flooding. Watch ILNI2 daily.")
        elif ilni2_peak is not None and ilni2_peak >= ILNI2_ACTION_FT:
            return (f"👀 Elevated risk. Peak forecast {fmt_ft(ilni2_peak)} — below flood stage "
                    f"but monitor closely. No garage action yet.")
        elif dam_gap is not None and dam_gap < DAM_GAP_WARNING_FT:
            return (f"👀 Dam gap narrowing ({fmt_ft(dam_gap)}) — river rising fast toward Lock & Dam 16. "
                    f"Watch daily.")
        else:
            return (f"✅ No action needed. Peak forecast {fmt_ft(ilni2_peak)} — well below 15 ft. "
                    f"Normal weekly monitoring.")

    # Build message
    lines = [
        f"📅 Fairport Weekly Flood Report — {datetime.now(timezone.utc).strftime('%B %-d, %Y')}",
        f"Generated: {datetime.now(timezone.utc).strftime('%H:%M UTC')}",
        "",
        "━━ CURRENT CONDITIONS ━━",
        f"ILNI2 (Illinois City): {fmt_ft(ilni2_stage)}",
        f"  Thresholds: Alert 13.5 | Action 14 | Flood/Garage 15 | Mod 16 | Major 18",
        f"  Rise rate: {f'{ilni2_rate:+.2f} ft/day' if ilni2_rate else 'N/A'}",
        f"  6hr trend: {f'{ilni2_trend:+.2f} ft' if ilni2_trend else 'N/A'}",
        "",
        f"Fairport (FAII4): {fmt_ft(faii4_stage)} (observed only, no NWS forecast)",
        f"  Dam gap vs ILNI2: {fmt_ft(dam_gap)}",
        f"  Dam status: {dam_status}",
        f"  Note: Normal gap 1.1-1.6 ft | Flood gap 0.3-0.5 ft",
        "",
        f"Wapello (WAPI4): {fmt_ft(wapi4_stage)}",
        f"  Forecast peak: {fmt_ft(wapi4_peak)} around {fmt_dt(wapi4_peak_dt)}",
        f"  22 ft alarm threshold: {'🔴 TRIGGERED' if wapi4_stage and wapi4_stage >= WAPELLO_ALARM_FT else 'Not triggered (need ' + (fmt_ft(WAPELLO_ALARM_FT - wapi4_stage) if wapi4_stage else 'N/A') + ' more)'}",
        f"  20 ft watch threshold: {'⚠️ EXCEEDED' if wapi4_stage and wapi4_stage >= WAPELLO_WATCH_FT else 'Not reached'}",
        "",
        "━━ 10-DAY FORECAST (ILNI2) ━━",
    ]

    for day, peak in sorted(daily_max.items()):
        flag = ""
        if peak >= ILNI2_FLOOD_FT:
            flag = " 🚨 GARAGE FLOODS"
        elif peak >= ILNI2_ACTION_FT:
            flag = " ⚠️ ACTION STAGE"
        elif peak >= ILNI2_ALERT_FT:
            flag = " 👀 PERSONAL ALERT"
        lines.append(f"  {day}: {peak:.2f} ft{flag}")

    lines += [
        "",
        "━━ GARAGE DECISION TIMELINE ━━",
    ]

    if days_to_alert is not None:
        lines.append(f"  13.5 ft (personal alert): {fmt_days(days_to_alert)}")
    if days_to_action is not None:
        lines.append(f"  14.0 ft (action stage):   {fmt_days(days_to_action)} → {fmt_dt(cross_action_dt)}")
    if days_to_flood is not None:
        lines.append(f"  15.0 ft (garage floods):  {fmt_days(days_to_flood)} → {fmt_dt(cross_flood_dt)}")
        lines.append(f"  Garage prep needed by:    {fmt_dt(cross_flood_dt - timedelta(days=GARAGE_PREP_DAYS) if cross_flood_dt else None)}")
    else:
        lines.append(f"  15.0 ft (garage floods):  Not forecast in current outlook")

    if days_wap_alarm is not None:
        lines.append(f"  Wapello 22 ft alarm:      forecast in {fmt_days(days_wap_alarm)}")

    lines += [
        "",
        "━━ RECOMMENDATION ━━",
        recommendation(),
        "",
        f"Next weekly report: {(datetime.now(timezone.utc) + timedelta(days=7)).strftime('%B %-d')}",
        "Monitor: water.noaa.gov/gauges/ilni2",
    ]

    message = "\n".join(lines)

    # Set priority based on situation
    if days_to_flood is not None and days_to_flood <= GARAGE_PREP_DAYS:
        priority, tags = "urgent", ["rotating_light", "house"]
    elif days_to_flood is not None and days_to_flood <= 5:
        priority, tags = "high", ["warning", "house"]
    elif ilni2_peak and ilni2_peak >= ILNI2_FLOOD_FT:
        priority, tags = "high", ["warning", "droplet"]
    else:
        priority, tags = "default", ["calendar", "droplet"]

    send_ntfy("📅 Fairport Weekly Flood Report", message, priority=priority, tags=tags)
    print("✓ Weekly report sent.")


# ─── ENTRY POINT ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "auto"

    # Strip leading dashes in case old-style args are passed
    mode = mode.lstrip("-")

    if mode == "auto":
        # Always run the alert check
        check_alerts()
        # Also run weekly report on Thursdays (UTC)
        if datetime.now(timezone.utc).weekday() == 3:  # 3 = Thursday
            weekly_report()

    elif mode == "weekly":
        weekly_report()

    elif mode == "check":
        check_alerts()

    elif mode == "both":
        check_alerts()
        weekly_report()

    else:
        print(f"Usage: python flood_monitor.py [auto | check | weekly | both]")
        sys.exit(1)
WAPELLO_WATCH_FT       = 20.0   # Approaching alarm - elevated watch
DAM_GAP_WARNING_FT     = 0.6    # FAII4 - ILNI2 gap below this = dam losing pool control
GARAGE_PREP_DAYS       = 3      # Days needed to empty garage

# ─── NOAA API ─────────────────────────────────────────────────────────────────

def fetch_stageflow(gauge_id):
    """Fetch current observations and NWS forecast for a gauge."""
    url = f"{NOAA_BASE}/gauges/{gauge_id}/stageflow"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"WARNING: Could not fetch {gauge_id}: {e}")
        return None


def fetch_gauge_meta(gauge_id):
    """Fetch gauge metadata including flood categories and crest history."""
    url = f"{NOAA_BASE}/gauges/{gauge_id}"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"WARNING: Could not fetch metadata for {gauge_id}: {e}")
        return None


def get_current_stage(stageflow):
    """Extract the most recent observed stage in feet."""
    try:
        obs = stageflow.get("observed", {}).get("data", [])
        # Walk backwards to find the last non-null reading
        for entry in reversed(obs):
            val = entry.get("primary")
            if val is not None:
                return float(val), entry.get("validTime")
        return None, None
    except Exception:
        return None, None


def get_forecast_series(stageflow):
    """
    Return forecast as list of (datetime, stage_ft) tuples.
    Returns empty list if no forecast available.
    """
    try:
        fc = stageflow.get("forecast", {}).get("data", [])
        result = []
        for entry in fc:
            val = entry.get("primary")
            t   = entry.get("validTime")
            if val is not None and t is not None:
                dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
                result.append((dt, float(val)))
        return result
    except Exception:
        return []


def forecast_peak(series):
    """Return (peak_stage, peak_datetime) from a forecast series."""
    if not series:
        return None, None
    peak = max(series, key=lambda x: x[1])
    return peak[1], peak[0]


def days_until_stage(series, target_ft):
    """
    Find when the forecast first crosses target_ft (rising).
    Returns fractional days from now, or None if never crossed.
    """
    now = datetime.now(timezone.utc)
    for i in range(len(series) - 1):
        t0, s0 = series[i]
        t1, s1 = series[i + 1]
        if s0 < target_ft <= s1:
            # Linear interpolation
            frac = (target_ft - s0) / (s1 - s0)
            cross_time = t0 + (t1 - t0) * frac
            delta = cross_time - now
            return delta.total_seconds() / 86400, cross_time
    return None, None


def trend_6hr(stageflow):
    """Calculate stage change over last 6 hours."""
    try:
        obs = [(e["validTime"], float(e["primary"]))
               for e in stageflow.get("observed", {}).get("data", [])
               if e.get("primary") is not None]
        if len(obs) < 2:
            return None
        # Most recent
        latest_t = datetime.fromisoformat(obs[-1][0].replace("Z", "+00:00"))
        latest_s = obs[-1][1]
        # Find reading ~6 hours ago
        target_t = latest_t - timedelta(hours=6)
        closest = min(obs, key=lambda x: abs(
            datetime.fromisoformat(x[0].replace("Z", "+00:00")) - target_t
        ))
        return latest_s - closest[1]
    except Exception:
        return None


def rise_rate_ftperday(stageflow):
    """Estimate current rise rate in ft/day from recent observations."""
    try:
        obs = [(datetime.fromisoformat(e["validTime"].replace("Z", "+00:00")),
                float(e["primary"]))
               for e in stageflow.get("observed", {}).get("data", [])
               if e.get("primary") is not None]
        if len(obs) < 6:
            return None
        # Use last 12 hours of data
        recent = obs[-12:]
        dt_hours = (recent[-1][0] - recent[0][0]).total_seconds() / 3600
        if dt_hours < 1:
            return None
        ds = recent[-1][1] - recent[0][1]
        return (ds / dt_hours) * 24  # ft/day
    except Exception:
        return None


# ─── NTFY NOTIFICATIONS ───────────────────────────────────────────────────────

def send_ntfy(title, message, priority="default", tags=None):
    """Send a push notification via ntfy.sh."""
    headers = {
    "Title": title.encode("utf-8"),
        "Priority": priority,
        "Content-Type": "text/plain",
    }
    if tags:
        headers["Tags"] = ",".join(tags)

    try:
        r = requests.post(
            f"{NTFY_SERVER}/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers=headers,
            timeout=10
        )
        r.raise_for_status()
        print(f"✓ Notification sent: {title}")
        return True
    except Exception as e:
        print(f"✗ Failed to send notification: {e}")
        return False


# ─── FORMATTING HELPERS ───────────────────────────────────────────────────────

def fmt_ft(val):
    return f"{val:.2f} ft" if val is not None else "N/A"


def fmt_dt(dt):
    if dt is None:
        return "unknown"
    # Convert to CDT (UTC-5 in April)
    cdt = dt - timedelta(hours=5)
    return cdt.strftime("%a %b %-d %-I:%M %p CDT")


def fmt_days(d):
    if d is None:
        return None
    if d < 1:
        hours = d * 24
        return f"{hours:.0f} hours"
    return f"{d:.1f} days"


def dam_mode_assessment(faii4_stage, ilni2_stage):
    """
    Assess Lock & Dam 16 pool status from gauge gap.
    Normal pool: FAII4 reads 1.1-1.6 ft higher than ILNI2
    Flood/open gates: gap closes to 0.3-0.5 ft
    """
    if faii4_stage is None or ilni2_stage is None:
        return None, "unknown"
    gap = faii4_stage - ilni2_stage
    if gap >= 1.0:
        return gap, "normal pool (dam holding)"
    elif gap >= 0.6:
        return gap, "pool reducing (watch closely)"
    elif gap >= 0.3:
        return gap, "⚠️ FLOOD MODE — dam gates opening"
    else:
        return gap, "🔴 GATES WIDE OPEN — full flood flow"


# ─── ALERT LOGIC ──────────────────────────────────────────────────────────────

def check_alerts():
    """
    6-hour check. Fires alerts if any threshold is crossed.
    Returns True if a critical alert was sent.
    """
    print(f"\n{'='*60}")
    print(f"FAIRPORT FLOOD MONITOR — Alert Check")
    print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    # Fetch all gauges
    ilni2_data = fetch_stageflow("ILNI2")
    wapi4_data = fetch_stageflow("WAPI4")
    faii4_data = fetch_stageflow("FAII4")

    if ilni2_data is None:
        send_ntfy("⚠️ Flood Monitor Error",
                  "Could not reach NOAA API. Check water.noaa.gov manually.",
                  priority="high", tags=["warning"])
        return False

    # Current stages
    ilni2_stage, ilni2_time  = get_current_stage(ilni2_data)
    wapi4_stage, wapi4_time  = get_current_stage(wapi4_data) if wapi4_data else (None, None)
    faii4_stage, faii4_time  = get_current_stage(faii4_data) if faii4_data else (None, None)

    # Forecasts
    ilni2_fc   = get_forecast_series(ilni2_data)
    wapi4_fc   = get_forecast_series(wapi4_data) if wapi4_data else []

    # Peak forecasts
    ilni2_peak, ilni2_peak_dt   = forecast_peak(ilni2_fc)
    wapi4_peak, wapi4_peak_dt   = forecast_peak(wapi4_fc)

    # Days until ILNI2 hits key thresholds
    days_to_alert, cross_alert_dt   = days_until_stage(ilni2_fc, ILNI2_ALERT_FT)
    days_to_flood, cross_flood_dt   = days_until_stage(ilni2_fc, ILNI2_FLOOD_FT)
    days_to_action, cross_action_dt = days_until_stage(ilni2_fc, ILNI2_ACTION_FT)

    # Days until Wapello alarm threshold in forecast
    days_wap_alarm, _ = days_until_stage(wapi4_fc, WAPELLO_ALARM_FT)
    days_wap_watch, _ = days_until_stage(wapi4_fc, WAPELLO_WATCH_FT)

    # Rise rates
    ilni2_rate = rise_rate_ftperday(ilni2_data)
    faii4_rate = rise_rate_ftperday(faii4_data) if faii4_data else None

    # Dam gap assessment
    dam_gap, dam_status = dam_mode_assessment(faii4_stage, ilni2_stage)

    # Trend
    ilni2_trend = trend_6hr(ilni2_data)

    print(f"ILNI2: {fmt_ft(ilni2_stage)} | Peak forecast: {fmt_ft(ilni2_peak)}")
    print(f"WAPI4: {fmt_ft(wapi4_stage)} | Peak forecast: {fmt_ft(wapi4_peak)}")
    print(f"FAII4: {fmt_ft(faii4_stage)} (observed only)")
    print(f"Dam gap: {fmt_ft(dam_gap)} — {dam_status}")
    print(f"Days to 15ft (ILNI2): {fmt_days(days_to_flood)}")

    # ── DETERMINE ALERT LEVEL ────────────────────────────────────────────────

    alert_level = 0   # 0=none, 1=watch, 2=warning, 3=critical
    reasons = []

    # Critical: forecast hits garage flood level
    if ilni2_peak is not None and ilni2_peak >= ILNI2_FLOOD_FT:
        alert_level = max(alert_level, 3)
        reasons.append(f"ILNI2 forecast peak {fmt_ft(ilni2_peak)} — GARAGE FLOOD EXPECTED")

    # Critical: Wapello hits or forecast to hit 22ft alarm
    if wapi4_stage is not None and wapi4_stage >= WAPELLO_ALARM_FT:
        alert_level = max(alert_level, 3)
        reasons.append(f"Wapello AT {fmt_ft(wapi4_stage)} — 100% historical flood correlation triggered")

    if days_wap_alarm is not None:
        alert_level = max(alert_level, 3)
        reasons.append(f"Wapello FORECAST to hit 22 ft in {fmt_days(days_wap_alarm)}")

    # Critical: less than GARAGE_PREP_DAYS until flood stage
    if days_to_flood is not None and days_to_flood <= GARAGE_PREP_DAYS:
        alert_level = max(alert_level, 3)
        reasons.append(f"Only {fmt_days(days_to_flood)} until 15 ft — LESS THAN 3-DAY GARAGE WINDOW")

    # Warning: forecast hits action stage or dam losing pool control
    if ilni2_peak is not None and ilni2_peak >= ILNI2_ACTION_FT:
        alert_level = max(alert_level, 2)
        reasons.append(f"ILNI2 forecast peak {fmt_ft(ilni2_peak)} — approaching flood stage")

    if dam_gap is not None and dam_gap < DAM_GAP_WARNING_FT:
        alert_level = max(alert_level, 2)
        reasons.append(f"Dam gap only {fmt_ft(dam_gap)} — Lock & Dam 16 losing pool control")

    if days_to_flood is not None and days_to_flood <= 5:
        alert_level = max(alert_level, 2)
        reasons.append(f"{fmt_days(days_to_flood)} until 15 ft at ILNI2")

    # Watch: Fairport above personal alert, or Wapello approaching 22ft
    if faii4_stage is not None and faii4_stage >= PERSONAL_ALERT_FT:
        alert_level = max(alert_level, 1)
        reasons.append(f"Fairport at {fmt_ft(faii4_stage)} — above personal alert threshold")

    if ilni2_stage is not None and ilni2_stage >= ILNI2_ALERT_FT:
        alert_level = max(alert_level, 1)
        reasons.append(f"ILNI2 at {fmt_ft(ilni2_stage)} — above personal alert threshold")

    if wapi4_peak is not None and wapi4_peak >= WAPELLO_WATCH_FT:
        alert_level = max(alert_level, 1)
        reasons.append(f"Wapello forecast peak {fmt_ft(wapi4_peak)} — approaching 22 ft alarm")

    if alert_level == 0:
        print("✓ No alert conditions met.")
        return False

    # ── BUILD AND SEND ALERT ─────────────────────────────────────────────────

    # Garage prep countdown language
    def garage_line():
        if days_to_flood is None:
            if ilni2_peak and ilni2_peak >= ILNI2_FLOOD_FT:
                return "⏰ Garage prep: START NOW (crossing imminent)"
            return ""
        if days_to_flood <= 1:
            return f"🚨 Garage prep: START IMMEDIATELY ({fmt_days(days_to_flood)} to 15 ft)"
        elif days_to_flood <= GARAGE_PREP_DAYS:
            return f"🚨 Garage prep: START NOW ({fmt_days(days_to_flood)} to 15 ft — inside 3-day window)"
        else:
            return f"⏰ Garage prep: {fmt_days(days_to_flood)} until 15 ft (3 days needed)"

    if alert_level == 3:
        emoji = "🚨"
        title = "🚨 FLOOD ALERT — Garage Flooding Likely"
        priority = "urgent"
        tags = ["rotating_light", "droplet"]
    elif alert_level == 2:
        emoji = "⚠️"
        title = "⚠️ Flood WARNING — Action Required"
        priority = "high"
        tags = ["warning", "droplet"]
    else:
        emoji = "👀"
        title = "👀 Flood WATCH — Monitor Closely"
        priority = "default"
        tags = ["eyes", "droplet"]

    lines = [
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "── TRIGGER CONDITIONS ──",
    ]
    for r in reasons:
        lines.append(f"• {r}")

    lines += [
        "",
        "── CURRENT CONDITIONS ──",
        f"ILNI2 (Illinois City): {fmt_ft(ilni2_stage)}",
        f"  Forecast peak: {fmt_ft(ilni2_peak)} around {fmt_dt(ilni2_peak_dt)}",
        f"  Rise rate: {f'{ilni2_rate:+.2f} ft/day' if ilni2_rate else 'N/A'}",
        f"  6hr trend: {f'{ilni2_trend:+.2f} ft' if ilni2_trend else 'N/A'}",
        "",
        f"Fairport (FAII4): {fmt_ft(faii4_stage)} (observed only)",
        f"  Dam gap vs ILNI2: {fmt_ft(dam_gap)} — {dam_status}",
        "",
        f"Wapello (WAPI4): {fmt_ft(wapi4_stage)}",
        f"  Forecast peak: {fmt_ft(wapi4_peak)} around {fmt_dt(wapi4_peak_dt)}",
        f"  22 ft alarm: {'TRIGGERED' if wapi4_stage and wapi4_stage >= WAPELLO_ALARM_FT else 'Not triggered'}",
        "",
        "── GARAGE DECISION ──",
        garage_line(),
    ]

    if days_to_action is not None:
        lines.append(f"Action stage (14 ft): {fmt_days(days_to_action)}")
    if days_to_flood is not None:
        lines.append(f"Flood stage (15 ft):  {fmt_days(days_to_flood)} → ~{fmt_dt(cross_flood_dt)}")

    message = "\n".join(lines)
    send_ntfy(title, message, priority=priority, tags=tags)
    return alert_level >= 2


# ─── WEEKLY REPORT ────────────────────────────────────────────────────────────

def weekly_report():
    """
    Full weekly briefing with 10-day forecast table,
    all gauge status, dam analysis, and plain-English recommendation.
    """
    print(f"\n{'='*60}")
    print(f"FAIRPORT FLOOD MONITOR — Weekly Report")
    print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    ilni2_data = fetch_stageflow("ILNI2")
    wapi4_data = fetch_stageflow("WAPI4")
    faii4_data = fetch_stageflow("FAII4")
    ilni2_meta = fetch_gauge_meta("ILNI2")

    if ilni2_data is None:
        send_ntfy("⚠️ Weekly Report Failed",
                  "Could not reach NOAA API for weekly report.",
                  priority="high", tags=["warning"])
        return

    ilni2_stage, _ = get_current_stage(ilni2_data)
    wapi4_stage, _ = get_current_stage(wapi4_data) if wapi4_data else (None, None)
    faii4_stage, _ = get_current_stage(faii4_data) if faii4_data else (None, None)

    ilni2_fc = get_forecast_series(ilni2_data)
    wapi4_fc = get_forecast_series(wapi4_data) if wapi4_data else []

    ilni2_peak, ilni2_peak_dt = forecast_peak(ilni2_fc)
    wapi4_peak, wapi4_peak_dt = forecast_peak(wapi4_fc)

    days_to_flood, cross_flood_dt   = days_until_stage(ilni2_fc, ILNI2_FLOOD_FT)
    days_to_action, cross_action_dt = days_until_stage(ilni2_fc, ILNI2_ACTION_FT)
    days_to_alert, _                = days_until_stage(ilni2_fc, ILNI2_ALERT_FT)
    days_wap_alarm, _               = days_until_stage(wapi4_fc, WAPELLO_ALARM_FT)

    ilni2_rate = rise_rate_ftperday(ilni2_data)
    ilni2_trend = trend_6hr(ilni2_data)
    dam_gap, dam_status = dam_mode_assessment(faii4_stage, ilni2_stage)

    # 10-day forecast table (daily max per day)
    from collections import defaultdict
    daily_max = defaultdict(float)
    for dt, stage in ilni2_fc:
        cdt = dt - timedelta(hours=5)  # CDT
        day_key = cdt.strftime("%Y-%m-%d %a")
        daily_max[day_key] = max(daily_max[day_key], stage)

    # Plain English recommendation
    def recommendation():
        if days_to_flood is not None and days_to_flood <= GARAGE_PREP_DAYS:
            return (f"🚨 MOVE YOUR GARAGE NOW. 15 ft expected in {fmt_days(days_to_flood)}. "
                    f"You need 3 days — you're inside that window.")
        elif days_to_flood is not None and days_to_flood <= 5:
            return (f"⚠️ START GARAGE PREP. {fmt_days(days_to_flood)} until 15 ft. "
                    f"You have time but do not wait.")
        elif ilni2_peak is not None and ilni2_peak >= ILNI2_FLOOD_FT:
            return (f"⚠️ Flood stage expected (peak {fmt_ft(ilni2_peak)}) but timing uncertain. "
                    f"Monitor daily and be ready to move garage on short notice.")
        elif wapi4_peak is not None and wapi4_peak >= WAPELLO_ALARM_FT:
            return (f"⚠️ Wapello forecast to hit 22 ft — historically 100% correlation with "
                    f"your garage flooding. Watch ILNI2 daily.")
        elif ilni2_peak is not None and ilni2_peak >= ILNI2_ACTION_FT:
            return (f"👀 Elevated risk. Peak forecast {fmt_ft(ilni2_peak)} — below flood stage "
                    f"but monitor closely. No garage action yet.")
        elif dam_gap is not None and dam_gap < DAM_GAP_WARNING_FT:
            return (f"👀 Dam gap narrowing ({fmt_ft(dam_gap)}) — river rising fast toward Lock & Dam 16. "
                    f"Watch daily.")
        else:
            return (f"✅ No action needed. Peak forecast {fmt_ft(ilni2_peak)} — well below 15 ft. "
                    f"Normal weekly monitoring.")

    # Build message
    lines = [
        f"📅 Fairport Weekly Flood Report — {datetime.now(timezone.utc).strftime('%B %-d, %Y')}",
        f"Generated: {datetime.now(timezone.utc).strftime('%H:%M UTC')}",
        "",
        "━━ CURRENT CONDITIONS ━━",
        f"ILNI2 (Illinois City): {fmt_ft(ilni2_stage)}",
        f"  Thresholds: Alert 13.5 | Action 14 | Flood/Garage 15 | Mod 16 | Major 18",
        f"  Rise rate: {f'{ilni2_rate:+.2f} ft/day' if ilni2_rate else 'N/A'}",
        f"  6hr trend: {f'{ilni2_trend:+.2f} ft' if ilni2_trend else 'N/A'}",
        "",
        f"Fairport (FAII4): {fmt_ft(faii4_stage)} (observed only, no NWS forecast)",
        f"  Dam gap vs ILNI2: {fmt_ft(dam_gap)}",
        f"  Dam status: {dam_status}",
        f"  Note: Normal gap 1.1-1.6 ft | Flood gap 0.3-0.5 ft",
        "",
        f"Wapello (WAPI4): {fmt_ft(wapi4_stage)}",
        f"  Forecast peak: {fmt_ft(wapi4_peak)} around {fmt_dt(wapi4_peak_dt)}",
        f"  22 ft alarm threshold: {'🔴 TRIGGERED' if wapi4_stage and wapi4_stage >= WAPELLO_ALARM_FT else 'Not triggered (need ' + (fmt_ft(WAPELLO_ALARM_FT - wapi4_stage) if wapi4_stage else 'N/A') + ' more)'}",
        f"  20 ft watch threshold: {'⚠️ EXCEEDED' if wapi4_stage and wapi4_stage >= WAPELLO_WATCH_FT else 'Not reached'}",
        "",
        "━━ 10-DAY FORECAST (ILNI2) ━━",
    ]

    for day, peak in sorted(daily_max.items()):
        flag = ""
        if peak >= ILNI2_FLOOD_FT:
            flag = " 🚨 GARAGE FLOODS"
        elif peak >= ILNI2_ACTION_FT:
            flag = " ⚠️ ACTION STAGE"
        elif peak >= ILNI2_ALERT_FT:
            flag = " 👀 PERSONAL ALERT"
        lines.append(f"  {day}: {peak:.2f} ft{flag}")

    lines += [
        "",
        "━━ GARAGE DECISION TIMELINE ━━",
    ]

    if days_to_alert is not None:
        lines.append(f"  13.5 ft (personal alert): {fmt_days(days_to_alert)}")
    if days_to_action is not None:
        lines.append(f"  14.0 ft (action stage):   {fmt_days(days_to_action)} → {fmt_dt(cross_action_dt)}")
    if days_to_flood is not None:
        lines.append(f"  15.0 ft (garage floods):  {fmt_days(days_to_flood)} → {fmt_dt(cross_flood_dt)}")
        lines.append(f"  Garage prep needed by:    {fmt_dt(cross_flood_dt - timedelta(days=GARAGE_PREP_DAYS) if cross_flood_dt else None)}")
    else:
        lines.append(f"  15.0 ft (garage floods):  Not forecast in current outlook")

    if days_wap_alarm is not None:
        lines.append(f"  Wapello 22 ft alarm:      forecast in {fmt_days(days_wap_alarm)}")

    lines += [
        "",
        "━━ RECOMMENDATION ━━",
        recommendation(),
        "",
        f"Next weekly report: {(datetime.now(timezone.utc) + timedelta(days=7)).strftime('%B %-d')}",
        "Monitor: water.noaa.gov/gauges/ilni2",
    ]

    message = "\n".join(lines)

    # Set priority based on situation
    if days_to_flood is not None and days_to_flood <= GARAGE_PREP_DAYS:
        priority, tags = "urgent", ["rotating_light", "house"]
    elif days_to_flood is not None and days_to_flood <= 5:
        priority, tags = "high", ["warning", "house"]
    elif ilni2_peak and ilni2_peak >= ILNI2_FLOOD_FT:
        priority, tags = "high", ["warning", "droplet"]
    else:
        priority, tags = "default", ["calendar", "droplet"]

    send_ntfy("📅 Fairport Weekly Flood Report", message, priority=priority, tags=tags)
    print("✓ Weekly report sent.")


# ─── ENTRY POINT ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "--check"

    if mode == "--weekly":
        weekly_report()
    elif mode == "--check":
        check_alerts()
    elif mode == "--both":
        check_alerts()
        weekly_report()
    else:
        print(f"Usage: python flood_monitor.py [--check | --weekly | --both]")
        sys.exit(1)
