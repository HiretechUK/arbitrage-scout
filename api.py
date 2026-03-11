"""
api.py - Flask REST API with SSE live feed, watchlist, top deals, new listing alerts
"""

import os
import json
import queue
import threading
import uuid
import logging
from datetime import datetime
from flask import Flask, request, jsonify, Response, stream_with_context, send_file
from flask_cors import CORS

from database import Database
from scraper import run_search, ACTORS
from alerts import AlertManager

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

app = Flask(__name__)
CORS(app)

db = Database()

APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "")

ALERT_CONFIG = {
    "email_enabled": os.environ.get("EMAIL_ENABLED", "false").lower() == "true",
    "smtp_host":     os.environ.get("SMTP_HOST", "smtp.gmail.com"),
    "smtp_port":     int(os.environ.get("SMTP_PORT", "587")),
    "smtp_user":     os.environ.get("SMTP_USER", ""),
    "smtp_password": os.environ.get("SMTP_PASSWORD", ""),
    "alert_email":   os.environ.get("ALERT_EMAIL", ""),
    "webhook_url":   os.environ.get("WEBHOOK_URL", ""),
}

alert_manager = AlertManager(db=db, config=ALERT_CONFIG)

# ---------------------------------------------------------------------------
# SSE event bus — a simple pub/sub for live dashboard updates
# ---------------------------------------------------------------------------

class EventBus:
    def __init__(self):
        self._subscribers: list[queue.Queue] = []
        self._lock = threading.Lock()

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=100)
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q: queue.Queue):
        with self._lock:
            self._subscribers = [s for s in self._subscribers if s is not q]

    def publish(self, event_type: str, data: dict):
        payload = json.dumps({"type": event_type, "data": data,
                              "ts": datetime.utcnow().isoformat()})
        with self._lock:
            dead = []
            for q in self._subscribers:
                try:
                    q.put_nowait(payload)
                except queue.Full:
                    dead.append(q)
            for q in dead:
                self._subscribers.remove(q)


bus = EventBus()

# ---------------------------------------------------------------------------
# Background jobs tracker
# ---------------------------------------------------------------------------
_jobs: dict = {}


def _alert_callback(alert_type: str, keyword: str, listing: dict, threshold):
    """Called from scraper for every triggered alert."""
    db.save_alert(alert_type, keyword, listing, threshold)

    # Push live to dashboard
    bus.publish("alert", {
        "alert_type": alert_type,
        "keyword": keyword,
        "listing": {
            "title":      listing.get("title","")[:80],
            "price_gbp":  listing.get("price_gbp"),
            "deal_score": listing.get("deal_score"),
            "pct_below":  listing.get("pct_below_avg"),
            "marketplace": listing.get("marketplace"),
            "country":    listing.get("country"),
            "url":        listing.get("url"),
            "image_url":  listing.get("image_url"),
        },
        "threshold": threshold,
    })

    # Also call email/webhook
    if alert_type in ("price", "deal"):
        alert_manager.fire_single(keyword, listing, threshold, alert_type)


def _run_search_bg(job_id: str, keyword: str, markets: list,
                   max_items: int, watchlist_config: dict):
    _jobs[job_id] = {"status": "running", "keyword": keyword}
    try:
        result = run_search(
            keyword=keyword,
            apify_token=APIFY_TOKEN,
            db=db,
            markets=markets or None,
            max_items=max_items,
            alert_callback=_alert_callback,
            watchlist_config=watchlist_config,
        )
        _jobs[job_id] = {"status": "done", **result}

        # Push live events to connected dashboards
        bus.publish("search_done", {
            "keyword": keyword,
            "total": result["total"],
            "new": result["new"],
            "deals": result["deals"],
        })

        # Push each new listing individually
        for l in result.get("new_listings", [])[:10]:
            bus.publish("new_listing", {
                "keyword":    keyword,
                "title":      l.get("title","")[:80],
                "price_gbp":  l.get("price_gbp"),
                "marketplace": l.get("marketplace"),
                "country":    l.get("country"),
                "url":        l.get("url"),
                "image_url":  l.get("image_url"),
            })

        # Push top deals
        if result.get("top_deals"):
            bus.publish("top_deals", {
                "keyword": keyword,
                "deals": [
                    {
                        "id":          d.get("id"),
                        "title":       d.get("title","")[:80],
                        "price_gbp":   d.get("price_gbp"),
                        "deal_score":  d.get("deal_score"),
                        "pct_below":   d.get("pct_below_avg"),
                        "marketplace": d.get("marketplace"),
                        "country":     d.get("country"),
                        "url":         d.get("url"),
                        "image_url":   d.get("image_url"),
                    }
                    for d in result["top_deals"][:20]
                ],
            })

        # Refresh live stats
        bus.publish("stats", db.get_live_stats())

    except Exception as e:
        log.error(f"Job {job_id} failed: {e}", exc_info=True)
        _jobs[job_id] = {"status": "error", "error": str(e), "keyword": keyword}
        bus.publish("error", {"job_id": job_id, "message": str(e)})


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """Serve the dashboard HTML."""
    import os
    dashboard = os.path.join(os.path.dirname(__file__), "dashboard.html")
    return send_file(dashboard)


@app.route("/api/health")
def health():
    return jsonify({"ok": True, "apify_configured": bool(APIFY_TOKEN)})


@app.route("/api/markets")
def markets():
    return jsonify([
        {"key": k, "marketplace": v["marketplace"],
         "country": v["country"], "currency": v["currency"]}
        for k, v in ACTORS.items()
    ])


# -- Live feed (SSE) --

@app.route("/api/live")
def live_feed():
    """
    Server-Sent Events endpoint.
    Dashboard connects once and receives real-time updates for all events.
    """
    q = bus.subscribe()

    def generate():
        # Send current stats immediately on connect
        try:
            stats = db.get_live_stats()
            yield f"data: {json.dumps({'type': 'stats', 'data': stats})}\n\n"
        except Exception:
            pass

        while True:
            try:
                payload = q.get(timeout=25)
                yield f"data: {payload}\n\n"
            except queue.Empty:
                yield ": heartbeat\n\n"   # keep connection alive

    def stream():
        try:
            yield from generate()
        finally:
            bus.unsubscribe(q)

    return Response(
        stream_with_context(stream()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# -- Search --

@app.route("/api/search", methods=["POST"])
def search():
    body = request.get_json() or {}
    keyword = (body.get("keyword") or "").strip()
    if not keyword:
        return jsonify({"error": "keyword required"}), 400
    if not APIFY_TOKEN:
        return jsonify({"error": "APIFY_TOKEN not configured in .env"}), 500

    markets       = body.get("markets", [])
    max_items     = int(body.get("max_items", 30))
    alert_price   = body.get("alert_price_gbp")
    alert_score   = body.get("alert_deal_score", 70)
    alert_new     = body.get("alert_new_listings", True)

    watchlist_config = {
        "alert_new_listings": alert_new,
        "alert_price_gbp":    alert_price,
        "alert_deal_score":   alert_score,
    }

    job_id = str(uuid.uuid4())[:8]
    t = threading.Thread(
        target=_run_search_bg,
        args=(job_id, keyword, markets, max_items, watchlist_config),
        daemon=True,
    )
    t.start()
    return jsonify({"job_id": job_id, "status": "running", "keyword": keyword})


@app.route("/api/jobs/<job_id>")
def job_status(job_id):
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "not found"}), 404
    return jsonify(job)


# -- Top deals --

@app.route("/api/top-deals")
def top_deals():
    keyword   = request.args.get("keyword")
    limit     = request.args.get("limit", 20, type=int)
    min_score = request.args.get("min_score", 10, type=float)
    deals = db.get_top_deals(limit=limit, keyword=keyword, min_score=min_score)
    return jsonify(deals)


# -- New listings --

@app.route("/api/new-listings")
def new_listings():
    keyword = request.args.get("keyword")
    since   = request.args.get("since_minutes", 60, type=int)
    limit   = request.args.get("limit", 50, type=int)
    return jsonify(db.get_new_listings(keyword=keyword, since_minutes=since, limit=limit))


# -- Listings --

@app.route("/api/listings")
def listings():
    return jsonify(db.get_listings(
        keyword        = request.args.get("keyword"),
        country        = request.args.get("country"),
        marketplace    = request.args.get("marketplace"),
        max_price_gbp  = request.args.get("max_price_gbp", type=float),
        min_price_gbp  = request.args.get("min_price_gbp", type=float),
        limit          = request.args.get("limit", 200, type=int),
        order_by       = request.args.get("order", "price_gbp ASC"),
    ))


# -- Summary / baselines --

@app.route("/api/summary")
def summary():
    return jsonify(db.get_price_summary(request.args.get("keyword", "")))


# -- Price history --

@app.route("/api/price-history")
def price_history():
    return jsonify(db.get_price_history(
        keyword     = request.args.get("keyword", ""),
        marketplace = request.args.get("marketplace"),
        days        = request.args.get("days", 30, type=int),
    ))


# -- Arbitrage --

@app.route("/api/arbitrage")
def arbitrage():
    return jsonify(db.get_arbitrage_opportunities(
        keyword        = request.args.get("keyword", ""),
        min_margin_gbp = request.args.get("min_margin_gbp", 20.0, type=float),
    ))


# -- Alerts --

@app.route("/api/alerts")
def alerts():
    return jsonify(db.get_recent_alerts())


# -- Stats --

@app.route("/api/stats")
def stats():
    return jsonify(db.get_live_stats())


# -- Search history --

@app.route("/api/history")
def history():
    return jsonify(db.get_search_history())


# -- Watchlist CRUD --

@app.route("/api/watchlist", methods=["GET"])
def watchlist_get():
    return jsonify(db.get_watchlist())


@app.route("/api/watchlist", methods=["POST"])
def watchlist_add():
    body = request.get_json() or {}
    keyword = (body.get("keyword") or "").strip()
    if not keyword:
        return jsonify({"error": "keyword required"}), 400
    item = db.add_to_watchlist(
        keyword          = keyword,
        markets          = body.get("markets"),
        alert_new        = body.get("alert_new_listings", True),
        alert_price      = body.get("alert_price_gbp"),
        alert_deal_score = body.get("alert_deal_score", 70),
        max_items        = body.get("max_items", 30),
        poll_mins        = body.get("poll_interval_mins", 30),
    )
    bus.publish("watchlist_updated", {"action": "added", "keyword": keyword})
    return jsonify(item)


@app.route("/api/watchlist/<keyword>", methods=["DELETE"])
def watchlist_remove(keyword):
    db.remove_from_watchlist(keyword)
    bus.publish("watchlist_updated", {"action": "removed", "keyword": keyword})
    return jsonify({"ok": True})


# -- Keywords / marketplaces --

@app.route("/api/keywords")
def keywords():
    return jsonify(db.get_all_keywords())


if __name__ == "__main__":
    # Import and start scheduler in background
    from scheduler import Scheduler
    sched = Scheduler(db=db, apify_token=APIFY_TOKEN,
                      alert_callback=_alert_callback, bus=bus)
    sched.start()

    port = int(os.environ.get("PORT", 5050))
    log.info(f"Arbitrage Scout API → http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
