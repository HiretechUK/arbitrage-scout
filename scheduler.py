"""
scheduler.py - Background polling engine for watchlisted products
Checks every minute which items are due, fires searches automatically.
"""

import threading
import logging
import time
from datetime import datetime

log = logging.getLogger(__name__)


class Scheduler:
    """
    Runs in a daemon thread. Every 60 seconds it checks the watchlist
    for items that are due a refresh and kicks off a background search.
    """

    def __init__(self, db, apify_token: str, alert_callback, bus):
        self.db = db
        self.apify_token = apify_token
        self.alert_callback = alert_callback
        self.bus = bus
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="Scheduler")

    def start(self):
        if not self.apify_token:
            log.warning("Scheduler: no APIFY_TOKEN — auto-polling disabled.")
            return
        self._thread.start()
        log.info("Scheduler started.")

    def stop(self):
        self._stop.set()

    def _loop(self):
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception as e:
                log.error(f"Scheduler tick error: {e}", exc_info=True)
            self._stop.wait(60)   # check every 60 seconds

    def _tick(self):
        due = self.db.get_due_watchlist()
        if not due:
            return

        log.info(f"Scheduler: {len(due)} item(s) due for refresh.")
        for item in due:
            keyword = item["keyword"]
            log.info(f"  Auto-polling: '{keyword}'")

            # Run in its own thread so we don't block the scheduler
            t = threading.Thread(
                target=self._run_item,
                args=(item,),
                daemon=True,
                name=f"poll-{keyword[:20]}",
            )
            t.start()

    def _run_item(self, item: dict):
        from scraper import run_search
        import json

        keyword = item["keyword"]
        try:
            markets_raw = item.get("markets") or "[]"
            markets = json.loads(markets_raw) if isinstance(markets_raw, str) else markets_raw
            markets = markets or None

            watchlist_config = {
                "alert_new_listings": bool(item.get("alert_new_listings", 1)),
                "alert_price_gbp":    item.get("alert_price_gbp"),
                "alert_deal_score":   item.get("alert_deal_score", 70),
            }

            result = run_search(
                keyword=keyword,
                apify_token=self.apify_token,
                db=self.db,
                markets=markets,
                max_items=item.get("max_items", 30),
                alert_callback=self.alert_callback,
                watchlist_config=watchlist_config,
            )

            # Broadcast to live dashboard
            self.bus.publish("poll_done", {
                "keyword": keyword,
                "total":   result["total"],
                "new":     result["new"],
                "deals":   result["deals"],
                "auto":    True,
            })

            for l in result.get("new_listings", [])[:10]:
                self.bus.publish("new_listing", {
                    "keyword":     keyword,
                    "title":       l.get("title", "")[:80],
                    "price_gbp":   l.get("price_gbp"),
                    "deal_score":  l.get("deal_score"),
                    "marketplace": l.get("marketplace"),
                    "country":     l.get("country"),
                    "url":         l.get("url"),
                    "image_url":   l.get("image_url"),
                    "auto":        True,
                })

            if result.get("top_deals"):
                self.bus.publish("top_deals", {
                    "keyword": keyword,
                    "deals":   result["top_deals"][:20],
                    "auto":    True,
                })

            self.bus.publish("stats", self.db.get_live_stats())

        except Exception as e:
            log.error(f"Scheduler poll failed for '{keyword}': {e}", exc_info=True)
            self.bus.publish("error", {"keyword": keyword, "message": str(e), "auto": True})
