#!/usr/bin/env python3
"""
transform_to_giq_brain.py
─────────────────────────
Transforms raw data files → GIQ Brain data files for the Lovable frontend.
Run automatically via GitHub Actions after every n8n sync.

Sources consumed:
  dashboard_data.json         — SevenRooms reservations/guests
  google_analytics_data.json  — GA4 traffic + Search Console (optional)
  meta_social_data.json       — Instagram + Facebook insights (optional)
  mailchimp_data.json         — Mailchimp campaigns + list size (optional)
  eventbrite_data.json        — Eventbrite events + ticket sales (optional)

Output files:
  data/kpis/pulse.json
  data/alerts.json
  data/reservations/upcoming.json
  data/guests/profiles.json
  data/sales/items.json
  data/sales/revenue_trend.json
  data/staff/performance.json
  data/marketing/email_metrics.json
  data/marketing/social_metrics.json    ← updated from Meta live data when available
  data/marketing/campaigns.json
  data/marketing/google_analytics.json  ← GA4 traffic + devices + conversions
  data/marketing/search_console.json    ← GSC keywords + pages
  data/marketing/meta_social.json       ← full Meta/IG breakdown
  data/events/eventbrite.json           ← NEW: Eventbrite events + ticket sales
"""

import json, os, sys
from datetime import datetime, timezone

# ── Helpers ───────────────────────────────────────────────────────────────────
def load_json(path):
    """Load a JSON file, return empty dict if missing or malformed."""
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}

def write_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  ✓  {path}")

def pct_change(cur, prv):
    if not prv: return 0.0, "flat"
    delta = ((cur - prv) / prv) * 100
    return round(delta, 1), ("up" if delta > 0 else ("down" if delta < 0 else "flat"))

def safe_int(v, default=0):
    try: return int(v)
    except Exception: return default

def safe_float(v, default=0.0):
    try: return float(v)
    except Exception: return default


# ── Load source data ──────────────────────────────────────────────────────────
src   = load_json("dashboard_data.json")
ga    = load_json("google_analytics_data.json")
meta  = load_json("meta_social_data.json")
mc    = load_json("mailchimp_data.json")
eb    = load_json("eventbrite_data.json")

sr   = src.get("sevenrooms", {})
res  = sr.get("reservations", {})
prev = sr.get("previous_period", {})
comp = sr.get("comparison", {})
ts   = src.get("timestamp", datetime.now(timezone.utc).isoformat())

ga_has_data   = bool(ga and ga.get("traffic"))
meta_has_data = bool(meta and (meta.get("ig_account") or meta.get("ig_media")))
mc_has_data   = bool(mc and mc.get("total_members"))
eb_has_data   = bool(eb and eb.get("last_updated"))

print(f"\n🔄  Transforming data → GIQ Brain files")
print(f"    SevenRooms:  {'LIVE' if res else 'empty'}")
print(f"    GA4 / GSC:   {'LIVE' if ga_has_data else 'not connected'}")
print(f"    Meta/IG:     {'LIVE' if meta_has_data else 'not connected'}")
print(f"    Mailchimp:   {'LIVE' if mc_has_data else 'not connected'}")
print(f"    Eventbrite:  {'LIVE' if eb_has_data else 'not connected'}")
print(f"    Timestamp:   {ts}\n")


# ══════════════════════════════════════════════════════════════════════════════
# 1. data/kpis/pulse.json  — 8 KPI metric cards
# ══════════════════════════════════════════════════════════════════════════════
by_day = res.get("by_day", {})
sorted_days = sorted(by_day.keys())[-12:]

covers_cur  = res.get("total_covers", 1323)
covers_prev = prev.get("total_covers", 654)
covers_chg, covers_trend = pct_change(covers_cur, covers_prev)
covers_spark = [by_day[d]["covers"] for d in sorted_days] if sorted_days else [100]*12

rev_cur  = res.get("total_revenue", 69850)
rev_prev = prev.get("total_revenue", 45191)
rev_chg, rev_trend = pct_change(rev_cur, rev_prev)
rev_spark = [round(by_day[d]["revenue"]) for d in sorted_days] if sorted_days else [10000]*12

party_cur  = res.get("avg_party_size", 3.4)
party_prev = prev.get("avg_party_size", 3.4)
party_chg, party_trend = pct_change(party_cur, party_prev)

completed  = res.get("completed", 164)
total_res  = res.get("total_reservations", 206)
conv_cur   = round((completed / total_res) * 100, 1) if total_res else 72.4
conv_prev  = round((prev.get("completed", 98) / prev.get("total_reservations", 123)) * 100, 1) \
             if prev.get("total_reservations") else 70.0
conv_chg, conv_trend = pct_change(conv_cur, conv_prev)

# Social reach: use live Meta data if available, else fallback
if meta_has_data:
    ig_acc = meta.get("ig_account", {})
    ig_data = ig_acc.get("data", [{}])[0] if isinstance(ig_acc.get("data"), list) else ig_acc
    ig_followers = safe_int(ig_data.get("followers_count", 0))
    fb_page = meta.get("fb_page_insights", {})
    fb_data = fb_page.get("data", [{}])[0] if isinstance(fb_page.get("data"), list) else {}
    fb_fans = safe_int(fb_data.get("page_fans", fb_data.get("value", 0)))
    social_reach = ig_followers + fb_fans
    social_reach_chg = 18.2  # will be computed dynamically when we have historical data
    social_reach_trend = "up" if social_reach > 89400 else "flat"
    social_spark = [social_reach] * 12  # will improve with time-series data
else:
    social_reach = 89400
    social_reach_chg = 18.2
    social_reach_trend = "up"
    social_spark = [62000,65000,68000,71000,74000,76000,79000,81000,83000,85000,87000,89400]

# Website sessions: use live GA4 data if available
if ga_has_data:
    ga_traffic = ga.get("traffic", {})
    ga_rows = ga_traffic.get("rows", [])
    total_sessions = sum(safe_int(row.get("metricValues", [{}])[0].get("value", 0)) for row in ga_rows)
    total_users = sum(safe_int(row.get("metricValues", [{}])[1].get("value", 0)) for row in ga_rows if len(row.get("metricValues", [])) > 1)
    web_sessions = total_sessions or 0
else:
    web_sessions = 0

pulse = {
    "period": "30d",
    "updated": ts,
    "dataSources": {
        "sevenrooms": bool(res),
        "googleAnalytics": ga_has_data,
        "meta": meta_has_data,
        "mailchimp": mc_has_data,
        "eventbrite": eb_has_data
    },
    "metrics": [
        {"id": "total_covers",          "label": "Total Covers",
         "value": covers_cur,           "change": covers_chg,  "trend": covers_trend,
         "sparkline": covers_spark},
        {"id": "total_revenue",         "label": "Total Revenue",
         "value": round(rev_cur),       "format": "currency",
         "change": rev_chg,             "trend": rev_trend,
         "sparkline": rev_spark},
        {"id": "avg_party_size",        "label": "Avg Party Size",
         "value": party_cur,            "change": party_chg,   "trend": party_trend,
         "sparkline": [party_cur] * 12},
        {"id": "google_rating",         "label": "Google Rating",
         "value": 4.7,                  "change": 0.1,         "trend": "up",
         "sparkline": [4.5,4.5,4.6,4.6,4.6,4.7,4.6,4.7,4.7,4.7,4.7,4.7]},
        {"id": "email_list",            "label": "Email List Size",
         "value": mc.get("total_members", 12847) if mc_has_data else 12847,
         "change": 5.4,                "trend": "up",
         "live": mc_has_data,
         "sparkline": [11800,11950,12050,12200,12300,12400,12500,12550,12650,12700,12780,
                       mc.get("total_members", 12847) if mc_has_data else 12847]},
        {"id": "social_reach",          "label": "Social Reach",
         "value": social_reach,         "change": social_reach_chg, "trend": social_reach_trend,
         "live": meta_has_data,
         "sparkline": social_spark},
        {"id": "reservation_conversion","label": "Reservation Conversion",
         "value": conv_cur,             "format": "percent",
         "change": conv_chg,            "trend": conv_trend,
         "sparkline": [65,66,68,69,70,70,71,71,72,71,72,conv_cur]},
        {"id": "guest_satisfaction",    "label": "Guest Satisfaction",
         "value": 94.2,                 "format": "percent",
         "change": 1.5,                 "trend": "up",
         "sparkline": [90,91,91,92,92,93,93,93,94,94,94,94.2]},
    ]
}

# Inject web sessions KPI if GA is live
if ga_has_data and web_sessions > 0:
    pulse["metrics"].append({
        "id": "website_sessions", "label": "Website Sessions",
        "value": web_sessions, "change": 0, "trend": "up",
        "live": True,
        "sparkline": [web_sessions] * 12
    })

write_json("data/kpis/pulse.json", pulse)


# ══════════════════════════════════════════════════════════════════════════════
# 2. data/alerts.json  — dynamic 3-alert feed from real data
# ══════════════════════════════════════════════════════════════════════════════
dow      = res.get("by_day_of_week", {})
dow_prev = prev.get("by_day_of_week", {}) if prev else {}

thu_cur  = dow.get("Thursday", {}).get("covers", 46)
thu_prev = dow_prev.get("Thursday", {}).get("covers", 60)
thu_pct, thu_dir = pct_change(thu_cur, thu_prev) if thu_prev else (0, "flat")

sat_covers = dow.get("Saturday", {}).get("covers", 0)
vip_count  = res.get("vip_count", 2)
rev_pct    = comp.get("total_revenue", {}).get("pct_change", 0)

alerts_out = []

# Alert 1: Thursday cover performance
if thu_pct < -5:
    alerts_out.append({
        "id": "alert_thu_covers", "severity": "critical",
        "headline": f"Thursday covers down {abs(thu_pct):.0f}% vs prior period",
        "explanation": f"Thursday dropped from {thu_prev} to {thu_cur} covers. "
                       "Weeknight volume has softened — monitor competitor activity.",
        "action": "Launch counter-promo: '$12 Negroni + Bruschetta Board' Thu 5–7 PM "
                  "via Instagram Stories + email to 10-mile radius list.",
        "source": "SevenRooms"
    })
elif thu_pct > 10:
    alerts_out.append({
        "id": "alert_thu_covers", "severity": "success",
        "headline": f"Thursday covers up {thu_pct:.0f}% — weeknight momentum building",
        "explanation": f"Thursday covers rose from {thu_prev} to {thu_cur}. Strategy is working.",
        "action": "Push a mid-week social post and consider expanding the Thursday event program.",
        "source": "SevenRooms"
    })
else:
    alerts_out.append({
        "id": "alert_revenue", "severity": "success",
        "headline": f"Revenue up {rev_pct:.0f}% vs prior period",
        "explanation": f"Total net revenue reached ${rev_cur:,.0f} — up {rev_pct:.0f}% period-over-period.",
        "action": "Review top-performing days and server assignments to sustain the momentum.",
        "source": "SevenRooms"
    })

# Alert 2: Food cost (static until Toast reconnects)
alerts_out.append({
    "id": "alert_food_cost", "severity": "warning",
    "headline": "Food cost up 2.1 points to 34.6%",
    "explanation": "Ribeye (cost +11%) and Halibut (cost +14%) are primary drivers. "
                   "Both high-volume items are pushing food cost above the 32.5% target.",
    "action": "Renegotiate Sysco protein pricing. Consider Branzino swap for Halibut "
              "(22% lower cost) and add a Ribeye supplement charge.",
    "source": "Toast POS + Inventory"
})

# Alert 3: GA4 insight if live, otherwise Saturday strength
if ga_has_data and web_sessions > 0:
    ga_conversions = ga.get("conversions", {})
    conv_rows = ga_conversions.get("rows", [])
    total_conversions = sum(safe_int(r.get("metricValues", [{}])[0].get("value", 0)) for r in conv_rows)
    conv_rate = round((total_conversions / web_sessions) * 100, 2) if web_sessions else 0
    alerts_out.append({
        "id": "alert_web_traffic", "severity": "success" if total_sessions > 2000 else "warning",
        "headline": f"Website: {web_sessions:,} sessions, {conv_rate}% conversion rate",
        "explanation": f"GA4 live: {web_sessions:,} sessions, {total_users:,} users, "
                       f"{total_conversions} goal completions in the last 30 days.",
        "action": "Review top landing pages in Search Console. Optimize reservation funnel page speed.",
        "source": "Google Analytics"
    })
elif sat_covers > 100:
    alerts_out.append({
        "id": "alert_sat_vip", "severity": "success",
        "headline": f"Strong Saturday confirmed — {sat_covers} covers this period",
        "explanation": f"{vip_count} VIP guests tracked. Ensure top server coverage for peak night.",
        "action": "Pre-assign top servers (Fab, Daniel) to VIP tables. Brief kitchen on timing. "
                  "Confirm celebration packages are prepped.",
        "source": "SevenRooms"
    })
else:
    alerts_out.append({
        "id": "alert_sat_vip", "severity": "warning",
        "headline": "Saturday covers below 100 — review weekend strategy",
        "explanation": "Weekend volume lower than expected. Consider targeted weekend promotions.",
        "action": "Push weekend event promotions via email and Instagram by Wednesday.",
        "source": "SevenRooms"
    })

write_json("data/alerts.json", {"alerts": alerts_out[:3]})


# ══════════════════════════════════════════════════════════════════════════════
# 3. data/reservations/upcoming.json  — forecast + shift breakdown + guests
# ══════════════════════════════════════════════════════════════════════════════
day_labels   = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
full_day_map = dict(zip(day_labels,["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]))

forecast = []
for d in day_labels:
    full     = full_day_map[d]
    covers   = dow.get(full, {}).get("covers", 50)
    per_week = max(int(covers / 4), 10)
    booked   = int(per_week * 0.78)
    walkin   = per_week - booked
    forecast.append({"date": d,       "booked": booked,             "walkin": walkin})
    forecast.append({"date": f"{d}+1","booked": int(booked * 0.92), "walkin": int(walkin * 0.92)})

by_shift  = res.get("by_shift", {})
tot_shift = sum(v.get("covers", 0) for v in by_shift.values()) or 1
shift_colors = {"DINNER": "hsl(160 60% 45%)", "BRUNCH": "hsl(270 60% 55%)",
                "LUNCH": "hsl(42 52% 54%)", "LATE NIGHT": "hsl(30 80% 55%)"}
shift_breakdown = [
    {"name": k.title(), "value": round(v.get("covers", 0) / tot_shift * 100),
     "color": shift_colors.get(k.upper(), "hsl(200 60% 50%)")}
    for k, v in by_shift.items()
]

upcoming_guests = [
    {"id":1,"guest":"Anthony Morello","party":6,"time":"7:30 PM","date":"Saturday","vip":True,
     "lastVisit":"Mar 8","visits":24,"spend":14200,
     "notes":"Prefers booth 4. Anniversary dinner. Allergic to shellfish. Always orders the Barolo.",
     "phone":"(914) 555-0142","email":"a.morello@gmail.com",
     "recommendation":"Anniversary visit — prepare complimentary prosecco toast. Seat at booth 4. Alert sommelier for 2019 Barolo Riserva."},
    {"id":2,"guest":"Patricia Chen","party":4,"time":"8:00 PM","date":"Saturday","vip":True,
     "lastVisit":"Feb 22","visits":18,"spend":9800,
     "notes":"Local real estate developer. Brings clients. Gluten-free.",
     "phone":"(914) 555-0287","email":"pchen@chenhomes.com",
     "recommendation":"High-value client entertainer — assign top server. Prepare GF menu options. Follow up re: private event."},
    {"id":3,"guest":"Michael DeLuca","party":2,"time":"6:30 PM","date":"Saturday","vip":False,
     "lastVisit":"Mar 15","visits":8,"spend":2400,
     "notes":"Birthday on Saturday! Turning 40. Wife confirmed cake arranged.",
     "phone":"(914) 555-0391","email":"mdeluca77@yahoo.com",
     "recommendation":"Birthday dinner — coordinate surprise dessert + candle at 8:15 PM. Consider complimentary limoncello."},
    {"id":4,"guest":"Sarah Goldstein","party":3,"time":"7:00 PM","date":"Friday","vip":False,
     "lastVisit":"First visit","visits":0,"spend":0,
     "notes":"Referred by Patricia Chen. Vegetarian preferences noted.",
     "phone":"(914) 555-0455","email":"sgold@outlook.com",
     "recommendation":"First-time VIP referral — strong impression critical. Offer seasonal vegetarian tasting menu. Manager greet table."},
    {"id":5,"guest":"Robert Marino","party":8,"time":"8:30 PM","date":"Saturday","vip":False,
     "lastVisit":"Jan 12","visits":5,"spend":3200,
     "notes":"Returning after 2 months. Large party — likely a celebration.",
     "phone":"(914) 555-0518","email":"rmarino@aol.com",
     "recommendation":"Large party — suggest family-style. Pre-set for 8. Upsell Antipasto Grande as starter."},
    {"id":6,"guest":"Jennifer Walsh","party":2,"time":"6:00 PM","date":"Friday","vip":False,
     "lastVisit":"Mar 1","visits":12,"spend":4100,
     "notes":"Regular weeknight diner. Works at WP Hospital. Loves the pappardelle.",
     "phone":"(914) 555-0622","email":"jwalsh.rn@gmail.com",
     "recommendation":"Loyal regular — send thank-you note with check. Mention new spring pappardelle variation."},
    {"id":7,"guest":"David Okafor","party":4,"time":"7:45 PM","date":"Saturday","vip":False,
     "lastVisit":"Feb 14","visits":3,"spend":1800,
     "notes":"Wine enthusiast — asked about wine club. No dietary restrictions.",
     "phone":"(914) 555-0734","email":"dokafor@techstart.io",
     "recommendation":"Wine enthusiast — have sommelier present new Brunello. Mention wine dinner April 12."},
    {"id":8,"guest":"Angela Ricci","party":2,"time":"5:30 PM","date":"Friday","vip":False,
     "lastVisit":"Mar 10","visits":15,"spend":5600,
     "notes":"Scarsdale local. Bi-weekly Friday visitor. Husband is Tony.",
     "phone":"(914) 555-0849","email":"angela.ricci@me.com",
     "recommendation":"Approaching VIP threshold (15 visits) — recognize loyalty. Have Aperol Spritz ready at bar."},
]

write_json("data/reservations/upcoming.json", {
    "forecast": forecast[:14],
    "shiftBreakdown": shift_breakdown,
    "upcoming": upcoming_guests,
    "updated": ts
})


# ══════════════════════════════════════════════════════════════════════════════
# 4. data/guests/profiles.json
# ══════════════════════════════════════════════════════════════════════════════
write_json("data/guests/profiles.json", upcoming_guests)


# ══════════════════════════════════════════════════════════════════════════════
# 5. data/sales/items.json
# ══════════════════════════════════════════════════════════════════════════════
items = [
    {"name":"Pappardelle Bolognese","volume":842,"revenue":25260,"margin":78,"trend":"up",  "category":"star"},
    {"name":"Grilled Branzino",     "volume":624,"revenue":24960,"margin":72,"trend":"up",  "category":"star"},
    {"name":"Margherita Pizza",     "volume":1180,"revenue":20060,"margin":82,"trend":"up", "category":"star"},
    {"name":"Ribeye Steak",         "volume":486,"revenue":26730,"margin":42,"trend":"down","category":"plowhorse"},
    {"name":"Chicken Parmigiana",   "volume":920,"revenue":23000,"margin":55,"trend":"flat","category":"plowhorse"},
    {"name":"Caesar Salad",         "volume":1040,"revenue":14560,"margin":85,"trend":"flat","category":"plowhorse"},
    {"name":"Lobster Ravioli",      "volume":210,"revenue":8400, "margin":68,"trend":"up",  "category":"puzzle"},
    {"name":"Osso Buco",            "volume":145,"revenue":7250, "margin":58,"trend":"down","category":"puzzle"},
    {"name":"Halibut Special",      "volume":180,"revenue":9000, "margin":38,"trend":"down","category":"dog"},
    {"name":"Veal Chop",            "volume":98, "revenue":5390, "margin":35,"trend":"down","category":"dog"},
]
matrix = items + [
    {"name":"Tiramisu",       "volume":680,"revenue":10200,"margin":88,"category":"star"},
    {"name":"Truffle Risotto","volume":175,"revenue":7000, "margin":72,"category":"puzzle"},
    {"name":"Garlic Bread",   "volume":1400,"revenue":8400,"margin":45,"category":"plowhorse"},
    {"name":"Lamb Shank",     "volume":120,"revenue":5400, "margin":32,"category":"dog"},
]
write_json("data/sales/items.json", {"topItems": items, "menuMatrix": matrix})


# ══════════════════════════════════════════════════════════════════════════════
# 6. data/sales/revenue_trend.json  — REAL SevenRooms daily revenue
# ══════════════════════════════════════════════════════════════════════════════
revenue_trend = []
for date_str in sorted(by_day.keys()):
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        label = dt.strftime("%b %-d")
    except Exception:
        label = date_str
    revenue_trend.append({"date": label, "revenue": round(by_day[date_str].get("revenue", 0))})

write_json("data/sales/revenue_trend.json", revenue_trend)


# ══════════════════════════════════════════════════════════════════════════════
# 7. data/staff/performance.json
# ══════════════════════════════════════════════════════════════════════════════
by_server = res.get("by_server", {})
staff = []
for name, d in sorted(by_server.items(), key=lambda x: x[1].get("revenue", 0), reverse=True):
    covers  = d.get("covers", 0)
    revenue = d.get("revenue", 0)
    staff.append({
        "name":          name,
        "avgCheck":      round(revenue / covers, 2) if covers else 0,
        "feedbackScore": None,
        "shift":         "Dinner",
        "covers":        covers,
        "revenue":       round(revenue, 2),
        "reservations":  d.get("reservations", 0)
    })
write_json("data/staff/performance.json", staff)


# ══════════════════════════════════════════════════════════════════════════════
# 8. data/marketing/email_metrics.json  — Mailchimp live or fallback
# ══════════════════════════════════════════════════════════════════════════════
if mc_has_data:
    mc_agg = mc.get("aggregate", {})
    mc_campaigns = mc.get("recent_campaigns", [])
    avg_open  = round(safe_float(mc_agg.get("avg_open_rate", 0)) * 100, 1)
    avg_click = round(safe_float(mc_agg.get("avg_click_rate", 0)) * 100, 1)
    # Per-campaign open rates for sparkline
    open_spark  = [round(safe_float(c.get("open_rate", 0)) * 100, 1) for c in mc_campaigns[-10:]] or [avg_open] * 10
    click_spark = [round(safe_float(c.get("click_rate", 0)) * 100, 1) for c in mc_campaigns[-10:]] or [avg_click] * 10
    write_json("data/marketing/email_metrics.json", {
        "live": True,
        "totalMembers": mc.get("total_members", 0),
        "openRate":  {"value": avg_open,  "change": 0, "trend": "flat", "sparkline": open_spark},
        "clickRate": {"value": avg_click, "change": 0, "trend": "flat", "sparkline": click_spark},
        "unsubRate": {"value": 0.12, "change": -0.03, "trend": "down",
                      "sparkline": [0.18,0.16,0.15,0.14,0.15,0.14,0.13,0.13,0.12,0.12]}
    })
    print(f"      Mailchimp: {mc.get('total_members', 0):,} members, {len(mc_campaigns)} campaigns (LIVE)")
else:
    write_json("data/marketing/email_metrics.json", {
        "live": False,
        "totalMembers": 12847,
        "openRate":  {"value": 21.4, "change": -2.8, "trend": "down",
                      "sparkline": [24,23,22.5,23,22,21.8,22.1,21.5,21.2,21.4]},
        "clickRate": {"value": 3.8,  "change":  0.5, "trend": "up",
                      "sparkline": [3,3.1,3.2,3.4,3.3,3.5,3.6,3.5,3.7,3.8]},
        "unsubRate": {"value": 0.12, "change": -0.03,"trend": "down",
                      "sparkline": [0.18,0.16,0.15,0.14,0.15,0.14,0.13,0.13,0.12,0.12]}
    })


# ══════════════════════════════════════════════════════════════════════════════
# 9. data/marketing/social_metrics.json  — IG monthly trend (live from Meta if available)
# ══════════════════════════════════════════════════════════════════════════════
if meta_has_data:
    # Build monthly social metrics from Meta live data
    ig_acc  = meta.get("ig_account", {})
    ig_data = ig_acc.get("data", [{}])[0] if isinstance(ig_acc.get("data"), list) else ig_acc
    ig_media = meta.get("ig_media", {})
    media_items = ig_media.get("data", []) if isinstance(ig_media.get("data"), list) else []

    ig_followers = safe_int(ig_data.get("followers_count", 26200))

    # Compute avg engagement from recent posts
    total_engagement = 0
    post_count = 0
    for item in media_items[:20]:  # last 20 posts
        likes = safe_int(item.get("like_count", 0))
        comments = safe_int(item.get("comments_count", 0))
        total_engagement += likes + comments
        post_count += 1
    avg_eng_rate = round((total_engagement / post_count / ig_followers) * 100, 2) if (post_count and ig_followers) else 4.2

    # Build monthly trend (most recent month live, prior months historical)
    social_metrics_live = [
        {"date":"Apr","igFollowers":15800,"igEngagement":4.0, "tiktokEngagement":5.8},
        {"date":"May","igFollowers":16500,"igEngagement":4.5, "tiktokEngagement":8.3},
        {"date":"Jun","igFollowers":17400,"igEngagement":4.2, "tiktokEngagement":7.5},
        {"date":"Jul","igFollowers":18200,"igEngagement":3.9, "tiktokEngagement":9.1},
        {"date":"Aug","igFollowers":19800,"igEngagement":4.6, "tiktokEngagement":8.8},
        {"date":"Sep","igFollowers":21200,"igEngagement":4.8, "tiktokEngagement":10.2},
        {"date":"Oct","igFollowers":22100,"igEngagement":4.4, "tiktokEngagement":9.4},
        {"date":"Nov","igFollowers":23500,"igEngagement":4.1, "tiktokEngagement":8.7},
        {"date":"Dec","igFollowers":24800,"igEngagement":3.8, "tiktokEngagement":7.9},
        {"date":"Jan","igFollowers":25200,"igEngagement":4.0, "tiktokEngagement":8.2},
        {"date":"Feb","igFollowers":25800,"igEngagement":4.1, "tiktokEngagement":8.9},
        {"date":"Mar","igFollowers":ig_followers, "igEngagement":avg_eng_rate, "tiktokEngagement":8.5,
         "live": True}
    ]
    write_json("data/marketing/social_metrics.json", social_metrics_live)
else:
    write_json("data/marketing/social_metrics.json", [
        {"date":"Apr","igFollowers":15800,"igEngagement":4.0, "tiktokEngagement":5.8},
        {"date":"May","igFollowers":16500,"igEngagement":4.5, "tiktokEngagement":8.3},
        {"date":"Jun","igFollowers":17400,"igEngagement":4.2, "tiktokEngagement":7.5},
        {"date":"Jul","igFollowers":18200,"igEngagement":3.9, "tiktokEngagement":9.1},
        {"date":"Aug","igFollowers":19800,"igEngagement":4.6, "tiktokEngagement":8.8},
        {"date":"Sep","igFollowers":21200,"igEngagement":4.8, "tiktokEngagement":10.2},
        {"date":"Oct","igFollowers":22100,"igEngagement":4.4, "tiktokEngagement":9.4},
        {"date":"Nov","igFollowers":23500,"igEngagement":4.1, "tiktokEngagement":8.7},
        {"date":"Dec","igFollowers":24800,"igEngagement":3.8, "tiktokEngagement":7.9},
        {"date":"Jan","igFollowers":25200,"igEngagement":4.0, "tiktokEngagement":8.2},
        {"date":"Feb","igFollowers":25800,"igEngagement":4.1, "tiktokEngagement":8.9},
        {"date":"Mar","igFollowers":26200,"igEngagement":4.2, "tiktokEngagement":11.4},
    ])


# ══════════════════════════════════════════════════════════════════════════════
# 10. data/marketing/campaigns.json  — Mailchimp live or fallback mock
# ══════════════════════════════════════════════════════════════════════════════
if mc_has_data:
    mc_campaigns = mc.get("recent_campaigns", [])
    campaigns_out = []
    for c in mc_campaigns[:10]:
        send_time = c.get("send_time", "") or c.get("create_time", "")
        try:
            dt = datetime.strptime(send_time[:10], "%Y-%m-%d")
            date_label = dt.strftime("%b %-d")
        except Exception:
            date_label = send_time[:10] if send_time else "—"
        open_rate  = round(safe_float(c.get("open_rate", 0)) * 100, 1)
        click_rate = round(safe_float(c.get("click_rate", 0)) * 100, 1)
        # Benchmark: open rate > 20% and click rate > 3% is above industry avg
        above_benchmark = (open_rate > 20) and (click_rate > 3)
        campaigns_out.append({
            "name":       c.get("subject_line", c.get("title", "Campaign"))[:60],
            "type":       "Email",
            "date":       date_label,
            "openRate":   open_rate,
            "clickRate":  click_rate,
            "revenue":    None,       # Mailchimp doesn't expose revenue attribution directly
            "benchmark":  above_benchmark,
            "emails_sent": safe_int(c.get("emails_sent", 0)),
            "live":        True,
        })
    write_json("data/marketing/campaigns.json", campaigns_out)
    print(f"      Campaigns: {len(campaigns_out)} live Mailchimp campaigns")
else:
    write_json("data/marketing/campaigns.json", [
        {"name":"Spring Menu Launch",  "type":"Email",          "date":"Mar 15","openRate":28.4,"clickRate":5.2,"revenue":12400,"benchmark":True},
        {"name":"St. Patrick's Event", "type":"Email + Social", "date":"Mar 14","openRate":32.1,"clickRate":6.8,"revenue":18700,"benchmark":True},
        {"name":"Wine Wednesday",      "type":"Email",          "date":"Mar 12","openRate":16.2,"clickRate":2.1,"revenue":3200, "benchmark":False},
        {"name":"Weekend Brunch Promo","type":"Instagram",      "date":"Mar 9", "openRate":None, "clickRate":4.5,"revenue":8900, "benchmark":True},
        {"name":"Valentine's Recap",   "type":"Email",          "date":"Feb 20","openRate":24.5,"clickRate":3.9,"revenue":6100, "benchmark":True},
        {"name":"Super Bowl Special",  "type":"Email + Social", "date":"Feb 9", "openRate":19.8,"clickRate":2.8,"revenue":4500, "benchmark":False},
    ])


# ══════════════════════════════════════════════════════════════════════════════
# 11. NEW: data/marketing/google_analytics.json  — GA4 + Search Console
# ══════════════════════════════════════════════════════════════════════════════
if ga_has_data:
    ga_traffic    = ga.get("traffic", {})
    ga_convs      = ga.get("conversions", {})
    ga_devices    = ga.get("devices", {})
    gsc_keywords  = ga.get("gsc_keywords", {})
    gsc_pages     = ga.get("gsc_pages", {})

    # Traffic by day — rows: [{dimensionValues:[{value:date}], metricValues:[{value:sessions},{value:users}]}]
    traffic_rows = ga_traffic.get("rows", [])
    traffic_trend = []
    for row in traffic_rows:
        dims = row.get("dimensionValues", [{}])
        mets = row.get("metricValues", [{},{},{}])
        date_str = dims[0].get("value", "") if dims else ""
        try:
            dt = datetime.strptime(date_str, "%Y%m%d")
            label = dt.strftime("%b %-d")
        except Exception:
            label = date_str
        traffic_trend.append({
            "date":     label,
            "sessions": safe_int(mets[0].get("value", 0) if len(mets) > 0 else 0),
            "users":    safe_int(mets[1].get("value", 0) if len(mets) > 1 else 0),
            "newUsers": safe_int(mets[2].get("value", 0) if len(mets) > 2 else 0),
        })

    # Conversions
    conv_rows = ga_convs.get("rows", [])
    conversions_by_type = []
    for row in conv_rows:
        dims = row.get("dimensionValues", [{}])
        mets = row.get("metricValues", [{}])
        event_name = dims[0].get("value", "unknown") if dims else "unknown"
        conversions_by_type.append({
            "event":       event_name,
            "conversions": safe_int(mets[0].get("value", 0) if mets else 0)
        })

    # Devices
    device_rows = ga_devices.get("rows", [])
    device_breakdown = []
    for row in device_rows:
        dims = row.get("dimensionValues", [{}])
        mets = row.get("metricValues", [{},{},{}])
        device = dims[0].get("value", "unknown") if dims else "unknown"
        device_breakdown.append({
            "device":    device,
            "sessions":  safe_int(mets[0].get("value", 0) if len(mets) > 0 else 0),
            "users":     safe_int(mets[1].get("value", 0) if len(mets) > 1 else 0),
            "bounceRate": safe_float(mets[2].get("value", 0) if len(mets) > 2 else 0),
        })

    # GSC Keywords  — rows: [{keys:[query,country,device], clicks, impressions, ctr, position}]
    kw_rows = gsc_keywords.get("rows", [])
    top_keywords = []
    for row in kw_rows[:20]:
        keys = row.get("keys", ["","",""])
        top_keywords.append({
            "query":       keys[0] if keys else "",
            "clicks":      safe_int(row.get("clicks", 0)),
            "impressions": safe_int(row.get("impressions", 0)),
            "ctr":         round(safe_float(row.get("ctr", 0)) * 100, 2),
            "position":    round(safe_float(row.get("position", 0)), 1),
        })

    # GSC Top Pages
    page_rows = gsc_pages.get("rows", [])
    top_pages = []
    for row in page_rows[:20]:
        keys = row.get("keys", [""])
        top_pages.append({
            "page":        keys[0] if keys else "",
            "clicks":      safe_int(row.get("clicks", 0)),
            "impressions": safe_int(row.get("impressions", 0)),
            "ctr":         round(safe_float(row.get("ctr", 0)) * 100, 2),
            "position":    round(safe_float(row.get("position", 0)), 1),
        })

    google_analytics_out = {
        "updated":          ts,
        "live":             True,
        "period":           "30d",
        "summary": {
            "totalSessions":   total_sessions,
            "totalUsers":      total_users,
            "totalNewUsers":   sum(r.get("newUsers", 0) for r in traffic_trend),
        },
        "trafficTrend":      traffic_trend,
        "deviceBreakdown":   device_breakdown,
        "conversionsByType": conversions_by_type,
        "topKeywords":       top_keywords,
        "topPages":          top_pages,
    }
    write_json("data/marketing/google_analytics.json", google_analytics_out)
    print(f"      GA4: {total_sessions:,} sessions, {total_users:,} users (LIVE)")
    print(f"      GSC: {len(top_keywords)} keywords, {len(top_pages)} pages (LIVE)")
else:
    # Write a placeholder so the frontend doesn't 404
    write_json("data/marketing/google_analytics.json", {
        "updated": ts, "live": False, "period": "30d",
        "summary": {"totalSessions": 0, "totalUsers": 0, "totalNewUsers": 0},
        "trafficTrend": [], "deviceBreakdown": [],
        "conversionsByType": [], "topKeywords": [], "topPages": [],
        "_note": "Google Analytics not yet connected. Complete OAuth in n8n credential gPUKhw3TIaqjFxaQ."
    })
    print(f"      GA4: not connected — placeholder written")


# ══════════════════════════════════════════════════════════════════════════════
# 12. NEW: data/marketing/meta_social.json  — full Meta/IG data
# ══════════════════════════════════════════════════════════════════════════════
if meta_has_data:
    ig_acc     = meta.get("ig_account", {})
    ig_data    = ig_acc.get("data", [{}])[0] if isinstance(ig_acc.get("data"), list) else ig_acc
    ig_media   = meta.get("ig_media", {})
    ig_profile = meta.get("ig_profile", {})
    fb_page    = meta.get("fb_page_insights", {})
    fb_posts   = meta.get("fb_posts", {})

    media_items = ig_media.get("data", []) if isinstance(ig_media.get("data"), list) else []
    fb_post_items = fb_posts.get("data", []) if isinstance(fb_posts.get("data"), list) else []

    # Process IG media items
    ig_posts_out = []
    for item in media_items[:30]:
        ig_posts_out.append({
            "id":          item.get("id", ""),
            "type":        item.get("media_type", "IMAGE"),
            "caption":     (item.get("caption", "") or "")[:120],
            "timestamp":   item.get("timestamp", ""),
            "permalink":   item.get("permalink", ""),
            "likes":       safe_int(item.get("like_count", 0)),
            "comments":    safe_int(item.get("comments_count", 0)),
            "mediaUrl":    item.get("media_url", ""),
            "engagementRate": round(
                (safe_int(item.get("like_count", 0)) + safe_int(item.get("comments_count", 0)))
                / ig_followers * 100, 2
            ) if ig_followers else 0
        })

    # FB page insights
    fb_data = fb_page.get("data", [{}])[0] if isinstance(fb_page.get("data"), list) else {}
    fb_fans = safe_int(fb_data.get("page_fans", fb_data.get("value", 0)))

    # IG account insights from API response
    ig_insights = ig_acc.get("data", [{}])
    ig_reach = 0
    ig_impressions = 0
    ig_profile_views = 0
    if isinstance(ig_insights, list):
        for metric_obj in ig_insights:
            name = metric_obj.get("name", "")
            vals = metric_obj.get("values", [])
            total = sum(safe_int(v.get("value", 0)) for v in vals) if isinstance(vals, list) else safe_int(metric_obj.get("value", 0))
            if name == "reach": ig_reach = total
            elif name == "impressions": ig_impressions = total
            elif name == "profile_views": ig_profile_views = total

    meta_social_out = {
        "updated":    ts,
        "live":       True,
        "period":     "30d",
        "instagram": {
            "followers":      safe_int(ig_data.get("followers_count", ig_followers)),
            "following":      safe_int(ig_data.get("follows_count", 0)),
            "mediaCount":     safe_int(ig_data.get("media_count", 0)),
            "reach":          ig_reach,
            "impressions":    ig_impressions,
            "profileViews":   ig_profile_views,
            "avgEngagement":  avg_eng_rate,
            "recentPosts":    ig_posts_out,
        },
        "facebook": {
            "pageFans":       fb_fans,
            "recentPosts":    [
                {
                    "id":        p.get("id", ""),
                    "message":   (p.get("message", "") or "")[:120],
                    "createdAt": p.get("created_time", ""),
                    "likes":     safe_int(p.get("likes", {}).get("summary", {}).get("total_count", 0) if isinstance(p.get("likes"), dict) else p.get("likes", 0)),
                    "comments":  safe_int(p.get("comments", {}).get("summary", {}).get("total_count", 0) if isinstance(p.get("comments"), dict) else p.get("comments", 0)),
                }
                for p in fb_post_items[:20]
            ]
        }
    }
    write_json("data/marketing/meta_social.json", meta_social_out)
    print(f"      IG:  {safe_int(ig_data.get('followers_count', ig_followers)):,} followers, "
          f"{len(ig_posts_out)} posts analyzed (LIVE)")
    print(f"      FB:  {fb_fans:,} fans (LIVE)")
else:
    write_json("data/marketing/meta_social.json", {
        "updated": ts, "live": False, "period": "30d",
        "instagram": {"followers": 0, "recentPosts": []},
        "facebook":  {"pageFans": 0, "recentPosts": []},
        "_note": "Meta not yet connected. Add META_ACCESS_TOKEN, META_PAGE_ID, META_INSTAGRAM_ID to n8n variables."
    })
    print(f"      Meta: not connected — placeholder written")


# ══════════════════════════════════════════════════════════════════════════════
# 13. NEW: data/events/eventbrite.json  — Eventbrite events + ticket sales
# ══════════════════════════════════════════════════════════════════════════════
if eb_has_data:
    upcoming_events = eb.get("upcoming_events", [])
    past_events     = eb.get("past_events", [])
    stats           = eb.get("stats", {})

    def format_eb_event(e):
        """Normalize an Eventbrite event object for the frontend."""
        start = e.get("start", {})
        start_local = start.get("local", "") if isinstance(start, dict) else str(start)
        try:
            dt = datetime.strptime(start_local[:16], "%Y-%m-%dT%H:%M")
            date_label = dt.strftime("%b %-d, %Y")
            time_label = dt.strftime("%-I:%M %p")
        except Exception:
            date_label = start_local[:10]
            time_label = ""
        capacity   = safe_int(e.get("capacity", 0))
        sold       = safe_int(e.get("tickets_sold", e.get("quantity_sold", 0)))
        revenue    = safe_float(e.get("gross_revenue", e.get("revenue", 0)))
        return {
            "id":           e.get("id", ""),
            "name":         (e.get("name", {}).get("text", "") or e.get("name", ""))[:80],
            "date":         date_label,
            "time":         time_label,
            "status":       e.get("status", ""),
            "capacity":     capacity,
            "ticketsSold":  sold,
            "ticketsAvail": max(capacity - sold, 0) if capacity else None,
            "fillRate":     round(sold / capacity * 100, 1) if capacity else None,
            "grossRevenue": round(revenue, 2),
            "url":          e.get("url", ""),
            "isFree":       e.get("is_free", False),
        }

    upcoming_out = [format_eb_event(e) for e in upcoming_events[:10]]
    past_out     = [format_eb_event(e) for e in past_events[:10]]

    eventbrite_out = {
        "updated":        eb.get("last_updated", ts),
        "live":           True,
        "organizerId":    eb.get("organizer_id", ""),
        "summary": {
            "totalUpcoming":   safe_int(stats.get("total_upcoming", len(upcoming_events))),
            "totalPast30d":    safe_int(stats.get("total_past_30d", len(past_events))),
            "totalTicketsSold": safe_int(stats.get("total_tickets_sold", 0)),
            "totalGrossRevenue": safe_float(stats.get("total_gross_revenue", 0)),
        },
        "upcomingEvents": upcoming_out,
        "pastEvents":     past_out,
    }
    write_json("data/events/eventbrite.json", eventbrite_out)
    print(f"      Eventbrite: {len(upcoming_out)} upcoming, {len(past_out)} past events (LIVE)")
else:
    write_json("data/events/eventbrite.json", {
        "updated":  ts,
        "live":     False,
        "summary": {"totalUpcoming": 0, "totalPast30d": 0, "totalTicketsSold": 0, "totalGrossRevenue": 0},
        "upcomingEvents": [],
        "pastEvents":     [],
        "_note": "Eventbrite not yet connected or no events found."
    })
    print(f"      Eventbrite: not connected — placeholder written")


# ── Summary ───────────────────────────────────────────────────────────────────
file_count = 13  # updated from 12 (added data/events/eventbrite.json)
print(f"\n✅  GIQ Brain — {file_count} data files generated")
print(f"    SevenRooms:       {'LIVE' if res else 'no data'} (covers={covers_cur}, revenue=${rev_cur:,.0f})")
print(f"    Google Analytics: {'LIVE' if ga_has_data else 'not connected (OAuth needed)'}")
print(f"    Meta / Instagram: {'LIVE' if meta_has_data else 'not connected (token needed)'}")
print(f"    Mailchimp:        {'LIVE' if mc_has_data else 'not connected'}")
print(f"    Eventbrite:       {'LIVE' if eb_has_data else 'not connected'}\n")
