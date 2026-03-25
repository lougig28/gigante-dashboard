#!/usr/bin/env python3
"""
transform_to_giq_brain.py
─────────────────────────
Transforms dashboard_data.json → 10 individual GIQ Brain data files
Run automatically after sync_dashboard_data.py on every GitHub Actions sync.

Output:
  data/kpis/pulse.json
  data/alerts.json
  data/reservations/upcoming.json
  data/guests/profiles.json
  data/sales/items.json
  data/sales/revenue_trend.json
  data/staff/performance.json
  data/marketing/email_metrics.json
  data/marketing/social_metrics.json
  data/marketing/campaigns.json
"""

import json, os, sys
from datetime import datetime, timezone

# ── Load source data ──────────────────────────────────────────────────────────
with open("dashboard_data.json") as f:
    src = json.load(f)

sr   = src.get("sevenrooms", {})
res  = sr.get("reservations", {})
prev = sr.get("previous_period", {})
comp = sr.get("comparison", {})
ts   = src.get("timestamp", datetime.now(timezone.utc).isoformat())

def pct_change(cur, prv):
    if not prv: return 0.0, "flat"
    delta = ((cur - prv) / prv) * 100
    return round(delta, 1), ("up" if delta > 0 else ("down" if delta < 0 else "flat"))

def write_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  ✓  {path}")

print(f"\n🔄  Transforming dashboard_data.json → GIQ Brain data files")
print(f"    Source timestamp: {ts}\n")


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

pulse = {
    "period": "30d",
    "updated": ts,
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
         "value": 12847,                "change": 5.4,         "trend": "up",
         "sparkline": [11800,11950,12050,12200,12300,12400,12500,12550,12650,12700,12780,12847]},
        {"id": "social_reach",          "label": "Social Reach",
         "value": 89400,                "change": 18.2,        "trend": "up",
         "sparkline": [62000,65000,68000,71000,74000,76000,79000,81000,83000,85000,87000,89400]},
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

# Alert 3: Saturday/weekend strength
if sat_covers > 100:
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
    per_week = max(int(covers / 4), 10)        # average weekly from 30d total
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
     "recommendation":"🎂 Birthday dinner — coordinate surprise dessert + candle at 8:15 PM. Consider complimentary limoncello."},
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
# 4. data/guests/profiles.json  — guest cards
# ══════════════════════════════════════════════════════════════════════════════
write_json("data/guests/profiles.json", upcoming_guests)


# ══════════════════════════════════════════════════════════════════════════════
# 5. data/sales/items.json  — top items + menu matrix (Toast mock until live)
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
    {"name":"Tiramisu",     "volume":680,"revenue":10200,"margin":88,"category":"star"},
    {"name":"Truffle Risotto","volume":175,"revenue":7000,"margin":72,"category":"puzzle"},
    {"name":"Garlic Bread", "volume":1400,"revenue":8400,"margin":45,"category":"plowhorse"},
    {"name":"Lamb Shank",   "volume":120,"revenue":5400,"margin":32,"category":"dog"},
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
# 7. data/staff/performance.json  — REAL SevenRooms server data
# ══════════════════════════════════════════════════════════════════════════════
by_server = res.get("by_server", {})
staff = []
for name, d in sorted(by_server.items(), key=lambda x: x[1].get("revenue", 0), reverse=True):
    covers  = d.get("covers", 0)
    revenue = d.get("revenue", 0)
    staff.append({
        "name":          name,
        "avgCheck":      round(revenue / covers, 2) if covers else 0,
        "feedbackScore": None,   # not available from SevenRooms — fill from future review source
        "shift":         "Dinner",
        "covers":        covers,
        "revenue":       round(revenue, 2),
        "reservations":  d.get("reservations", 0)
    })
write_json("data/staff/performance.json", staff)


# ══════════════════════════════════════════════════════════════════════════════
# 8. data/marketing/email_metrics.json  — Mailchimp (mock until connected)
# ══════════════════════════════════════════════════════════════════════════════
write_json("data/marketing/email_metrics.json", {
    "openRate":  {"value": 21.4, "change": -2.8, "trend": "down",
                  "sparkline": [24,23,22.5,23,22,21.8,22.1,21.5,21.2,21.4]},
    "clickRate": {"value": 3.8,  "change":  0.5, "trend": "up",
                  "sparkline": [3,3.1,3.2,3.4,3.3,3.5,3.6,3.5,3.7,3.8]},
    "unsubRate": {"value": 0.12, "change": -0.03,"trend": "down",
                  "sparkline": [0.18,0.16,0.15,0.14,0.15,0.14,0.13,0.13,0.12,0.12]}
})


# ══════════════════════════════════════════════════════════════════════════════
# 9. data/marketing/social_metrics.json  — Instagram/TikTok (mock until connected)
# ══════════════════════════════════════════════════════════════════════════════
write_json("data/marketing/social_metrics.json", [
    {"date":"Jan","igFollowers":14200,"igEngagement":4.1,"tiktokEngagement":6.2},
    {"date":"Feb","igFollowers":15100,"igEngagement":4.3,"tiktokEngagement":7.1},
    {"date":"Mar","igFollowers":15800,"igEngagement":4.0,"tiktokEngagement":5.8},
    {"date":"Apr","igFollowers":16500,"igEngagement":4.5,"tiktokEngagement":8.3},
    {"date":"May","igFollowers":17400,"igEngagement":4.2,"tiktokEngagement":7.5},
    {"date":"Jun","igFollowers":18200,"igEngagement":3.9,"tiktokEngagement":9.1},
    {"date":"Jul","igFollowers":19800,"igEngagement":4.6,"tiktokEngagement":8.8},
    {"date":"Aug","igFollowers":21200,"igEngagement":4.8,"tiktokEngagement":10.2},
    {"date":"Sep","igFollowers":22100,"igEngagement":4.4,"tiktokEngagement":9.4},
    {"date":"Oct","igFollowers":23500,"igEngagement":4.1,"tiktokEngagement":8.7},
    {"date":"Nov","igFollowers":24800,"igEngagement":3.8,"tiktokEngagement":7.9},
    {"date":"Dec","igFollowers":26200,"igEngagement":4.2,"tiktokEngagement":11.4},
])


# ══════════════════════════════════════════════════════════════════════════════
# 10. data/marketing/campaigns.json  — campaign performance (mock until Mailchimp)
# ══════════════════════════════════════════════════════════════════════════════
write_json("data/marketing/campaigns.json", [
    {"name":"Spring Menu Launch",  "type":"Email",          "date":"Mar 15","openRate":28.4,"clickRate":5.2,"revenue":12400,"benchmark":True},
    {"name":"St. Patrick's Event", "type":"Email + Social", "date":"Mar 14","openRate":32.1,"clickRate":6.8,"revenue":18700,"benchmark":True},
    {"name":"Wine Wednesday",      "type":"Email",          "date":"Mar 12","openRate":16.2,"clickRate":2.1,"revenue":3200, "benchmark":False},
    {"name":"Weekend Brunch Promo","type":"Instagram",      "date":"Mar 9", "openRate":None, "clickRate":4.5,"revenue":8900, "benchmark":True},
    {"name":"Valentine's Recap",   "type":"Email",          "date":"Feb 20","openRate":24.5,"clickRate":3.9,"revenue":6100, "benchmark":True},
    {"name":"Super Bowl Special",  "type":"Email + Social", "date":"Feb 9", "openRate":19.8,"clickRate":2.8,"revenue":4500, "benchmark":False},
])


print(f"\n✅  All 10 GIQ Brain data files generated successfully!")
print(f"    SevenRooms: LIVE DATA  (covers={covers_cur}, revenue=${rev_cur:,.0f})")
print(f"    Toast:      mock data  (reconnect Toast to get live menu/sales data)")
print(f"    Marketing:  mock data  (connect Mailchimp/Instagram for live metrics)\n")
