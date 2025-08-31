import os, json, asyncio, pytz, httpx
from datetime import datetime
from dateutil import parser as dateparser
from bs4 import BeautifulSoup
from icalendar import Calendar

BACKEND = os.getenv("BACKEND_API_URL", "").rstrip("/")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")
HEADERS = {"Authorization": f"Bearer {ADMIN_TOKEN}"} if ADMIN_TOKEN else {}

def to_iso(dt):
    if isinstance(dt, datetime):
        if dt.tzinfo is None: return dt.replace(tzinfo=pytz.UTC).isoformat()
        return dt.isoformat()
    return str(dt)

def norm_event(e: dict):
    if not e.get("title") or not e.get("starts_at"): return None
    return {
        "title": e["title"].strip(),
        "description": e.get("description"),
        "starts_at": to_iso(e["starts_at"]),
        "ends_at": to_iso(e["ends_at"]) if e.get("ends_at") else None,
        "timezone": e.get("timezone"),
        "format": e.get("format","in_person"),
        "audience": e.get("audience", ["parents"]),
        "language": e.get("language","en"),
        "cost_min": e.get("cost_min", 0),
        "cost_max": e.get("cost_max", 0),
        "organizer_name": e.get("organizer_name"),
        "organizer_email": e.get("organizer_email"),
        "venue_name": e.get("venue_name"),
        "address": e.get("address"),
        "city": e.get("city"),
        "state": e.get("state"),
        "postal_code": e.get("postal_code"),
        "lat": e.get("lat"),
        "lng": e.get("lng"),
        "badges": e.get("badges", []),
        "verified": True,
    }

async def fetch_text(client, url):
    r = await client.get(url, timeout=40.0)
    r.raise_for_status()
    return r.text

def parse_ics(text, meta_city=None, meta_state=None):
    cal = Calendar.from_ical(text); out=[]
    for comp in cal.walk("vevent"):
        title = str(comp.get("summary","")).strip()
        start = comp.get("dtstart").dt if comp.get("dtstart") else None
        if not title or not start: continue
        end = comp.get("dtend").dt if comp.get("dtend") else None
        loc = str(comp.get("location") or "").strip()
        out.append({
            "title": title,
            "description": str(comp.get("description") or ""),
            "starts_at": start if isinstance(start, datetime) else dateparser.parse(str(start)),
            "ends_at": end if isinstance(end, datetime) else (dateparser.parse(str(end)) if end else None),
            "format": "in_person" if loc else "virtual",
            "venue_name": loc or None,
            "city": meta_city, "state": meta_state,
        })
    return out

def parse_jsonld_page(html, meta_city=None, meta_state=None):
    soup = BeautifulSoup(html, "html.parser"); events=[]
    for tag in soup.find_all("script", {"type":"application/ld+json"}):
        try: data = json.loads(tag.string)
        except Exception: continue
        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            if node.get("@type") not in ["Event","event"]: continue
            name=node.get("name"); start=node.get("startDate")
            if not name or not start: continue
            try:
                starts_at=dateparser.parse(start)
                ends_at=dateparser.parse(node.get("endDate")) if node.get("endDate") else None
            except Exception: continue
            loc=node.get("location") or {}; venue=None; address=None; city=None; state=None; postal=None
            if isinstance(loc, dict):
                venue=loc.get("name"); addr=loc.get("address")
                if isinstance(addr, dict):
                    address=addr.get("streetAddress"); city=addr.get("addressLocality")
                    state=addr.get("addressRegion"); postal=addr.get("postalCode")
            events.append({
                "title": name,"description": node.get("description"),
                "starts_at": starts_at,"ends_at": ends_at,
                "format": "in_person" if venue or address else "virtual",
                "venue_name": venue,"address": address,
                "city": city or meta_city,"state": state or meta_state,"postal_code": postal
            })
    return events

async def scan_once():
    import yaml
    with open("providers.yaml","r",encoding="utf-8") as f: providers=yaml.safe_load(f) or {}
    out=[]
    async with httpx.AsyncClient(timeout=40.0) as client:
        for src in providers.get("ics",[]):
            try:
                txt=await fetch_text(client,src["url"])
                out+=parse_ics(txt,src.get("city"),src.get("state"))
            except Exception as ex: print("ICS error",src.get("name"),ex)
        for src in providers.get("jsonld",[]):
            try:
                html=await fetch_text(client,src["url"])
                out+=parse_jsonld_page(html,src.get("city"),src.get("state"))
            except Exception as ex: print("JSONLD error",src.get("name"),ex)
    normalized=[e for e in (norm_event(e) for e in out) if e]
    if not normalized: print("No events found"); return
    url=f"{BACKEND}/admin/ingest_bulk"
    r=await httpx.AsyncClient(timeout=60.0).post(url,headers=HEADERS,json={"events":normalized})
    print("Ingest status:",r.status_code,r.text)

if __name__=="__main__":
    asyncio.run(scan_once())
