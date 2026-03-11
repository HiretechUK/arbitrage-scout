"""
scraper.py - Apify orchestrator with live deal detection + new listing alerts
"""

import os
import time
import requests
import logging
from datetime import datetime
from typing import Optional
from database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

APIFY_BASE = "https://api.apify.com/v2"

# ---------------------------------------------------------------------------
# Actor registry  (verified working actors, pay-per-use, no monthly rental)
# ---------------------------------------------------------------------------
import urllib.parse

def _fb_url(query, city):
    q = urllib.parse.quote_plus(query)
    return f"https://www.facebook.com/marketplace/{city}/search?query={q}"

ACTORS = {
    "ebay_uk": {
        "id": "automation-lab/ebay-scraper",
        "country": "UK", "currency": "GBP", "marketplace": "eBay UK",
        "input_builder": lambda kw, n: {"searchQueries": [kw], "ebayDomain": "ebay.co.uk", "maxItems": n},
        "type": "ebay",
    },
    "ebay_uae": {
        "id": "automation-lab/ebay-scraper",
        "country": "UAE", "currency": "USD", "marketplace": "eBay UAE",
        "input_builder": lambda kw, n: {"searchQueries": [kw], "ebayDomain": "ebay.com", "maxItems": n},
        "type": "ebay",
    },
    "facebook_uk": {
        "id": "curious_coder/facebook-marketplace",
        "country": "UK", "currency": "GBP", "marketplace": "Facebook UK",
        "input_builder": lambda kw, n: {"urls": [_fb_url(kw, "london")], "maxItems": n},
        "type": "facebook",
    },
    "facebook_uae": {
        "id": "curious_coder/facebook-marketplace",
        "country": "UAE", "currency": "AED", "marketplace": "Facebook UAE",
        "input_builder": lambda kw, n: {"urls": [_fb_url(kw, "dubai")], "maxItems": n},
        "type": "facebook",
    },
    "facebook_norway": {
        "id": "curious_coder/facebook-marketplace",
        "country": "Norway", "currency": "NOK", "marketplace": "Facebook Norway",
        "input_builder": lambda kw, n: {"urls": [_fb_url(kw, "oslo")], "maxItems": n},
        "type": "facebook",
    },
}

FALLBACK_RATES_TO_GBP = {"GBP": 1.0, "USD": 0.79, "AED": 0.215, "NOK": 0.074, "EUR": 0.86}


def get_exchange_rates() -> dict:
    try:
        resp = requests.get("https://api.exchangerate-api.com/v4/latest/GBP", timeout=8)
        data = resp.json()
        rates = {c: 1.0 / r for c, r in data["rates"].items() if r > 0}
        rates["GBP"] = 1.0
        log.info("Live FX rates loaded.")
        return rates
    except Exception as e:
        log.warning(f"FX rate fetch failed ({e}), using fallback.")
        return FALLBACK_RATES_TO_GBP


# ---------------------------------------------------------------------------
# Apify runner
# ---------------------------------------------------------------------------

class ApifyRunner:
    def __init__(self, api_token: str):
        self.token = api_token
        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"

    def _run_actor(self, actor_id: str, input_data: dict, timeout_secs: int = 180) -> list:
        actor_path = actor_id.replace("/", "~")
        url = f"{APIFY_BASE}/acts/{actor_path}/run-sync-get-dataset-items"
        params = {"token": self.token, "timeout": timeout_secs}
        try:
            log.info(f"  Running {actor_id}...")
            resp = self.session.post(url, json=input_data, params=params, timeout=timeout_secs + 30)
            if resp.status_code == 200:
                items = resp.json()
                log.info(f"  -> {len(items)} items from {actor_id}")
                return items
            log.warning(f"  -> {actor_id} HTTP {resp.status_code}: {resp.text[:150]}")
            return []
        except Exception as e:
            log.error(f"  -> {actor_id} failed: {e}")
            return []

    def search_market(self, market_key: str, keyword: str, max_items: int,
                      rates: dict) -> list:
        cfg = ACTORS[market_key]
        raw = self._run_actor(cfg["id"], cfg["input_builder"](keyword, max_items))
        results = []
        for item in raw:
            l = self._normalise(item, cfg, rates)
            if l:
                l["keyword"] = keyword
                results.append(l)
        return results

    def search_all(self, keyword: str, markets: Optional[list], max_items: int,
                   rates: dict) -> list:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        keys = [k for k in (markets or list(ACTORS.keys())) if k in ACTORS]
        all_results = []
        with ThreadPoolExecutor(max_workers=len(keys)) as executor:
            futures = {executor.submit(self.search_market, k, keyword, max_items, rates): k for k in keys}
            for future in as_completed(futures):
                try:
                    all_results.extend(future.result())
                except Exception as e:
                    log.error(f"Market {futures[future]} failed: {e}")
        return all_results

    def _normalise(self, item: dict, cfg: dict, rates: dict) -> Optional[dict]:
        actor_type = cfg.get("type", "generic")

        # --- Extract fields based on actor type ---
        if actor_type == "facebook":
            title = item.get("marketplace_listing_title") or item.get("custom_title") or "Unknown"
            url   = item.get("listingUrl") or ""
            img   = item.get("primary_listing_photo_url") or ""
            cond  = "Unknown"
            # Price is a nested dict: {"formatted_amount": "£12,000", "amount": "12000.00"}
            lp = item.get("listing_price") or {}
            if isinstance(lp, dict):
                price_raw = lp.get("amount") or lp.get("amount_with_offset")
            else:
                price_raw = lp
            # Location is also nested
            loc_raw = item.get("location") or {}
            if isinstance(loc_raw, dict):
                rg = loc_raw.get("reverse_geocode") or {}
                loc = rg.get("city") or rg.get("state") or cfg["country"]
            else:
                loc = str(loc_raw) if loc_raw else cfg["country"]

        elif actor_type == "ebay":
            title = item.get("title") or "Unknown"
            url   = item.get("url") or item.get("itemUrl") or ""
            img   = item.get("thumbnail") or item.get("image") or ""
            cond  = item.get("condition") or "Unknown"
            price_raw = item.get("price") or item.get("priceValue")
            loc   = item.get("location") or item.get("itemLocation") or cfg["country"]

        else:
            price_raw = (item.get("price") or item.get("priceValue") or item.get("price_value")
                         or item.get("listingPrice") or item.get("Price"))
            title = (item.get("title") or item.get("name") or item.get("itemName")
                     or item.get("Title") or "Unknown")
            url   = (item.get("url") or item.get("link") or item.get("itemUrl")
                     or item.get("listingUrl") or "")
            loc   = item.get("location") or item.get("city") or item.get("region") or cfg["country"]
            cond  = item.get("condition") or item.get("itemCondition") or "Unknown"
            img   = item.get("image") or item.get("imageUrl") or item.get("thumbnail") or ""

        # --- Convert price to GBP ---
        price_gbp = None
        if price_raw is not None:
            try:
                price_num = float(
                    str(price_raw).replace(",","").replace("£","").replace("$","")
                    .replace("AED","").replace("kr","").replace("NOK","").strip()
                )
                rate = rates.get(cfg["currency"], FALLBACK_RATES_TO_GBP.get(cfg["currency"], 1.0))
                price_gbp = round(price_num * rate, 2)
            except (ValueError, TypeError):
                pass

        if not url:                   # skip items with no URL — can't deduplicate
            return None

        return {
            "marketplace": cfg["marketplace"],
            "country": cfg["country"],
            "currency": cfg["currency"],
            "title": str(title)[:250],
            "price_local": price_raw,
            "price_gbp": price_gbp,
            "url": str(url)[:500],
            "location": str(loc)[:100],
            "condition": str(cond)[:50],
            "image_url": str(img)[:500],
            "scraped_at": datetime.utcnow().isoformat(),
            "deal_score": 0.0,
            "pct_below_avg": 0.0,
            "is_new": 0,
        }


# ---------------------------------------------------------------------------
# High-level pipeline
# ---------------------------------------------------------------------------

def run_search(
    keyword: str,
    apify_token: str,
    db: Database,
    markets: Optional[list] = None,
    max_items: int = 50,
    alert_callback=None,       # fn(alert_type, keyword, listing, threshold)
    watchlist_config: dict = None,
) -> dict:
    """
    Full pipeline: scrape → save → score deals → fire alerts.
    Returns summary dict.
    """
    rates = get_exchange_rates()
    runner = ApifyRunner(apify_token)

    log.info(f"Starting search: '{keyword}' | markets={markets or 'ALL'} | max={max_items}")
    raw_listings = runner.search_all(keyword, markets, max_items, rates)

    # Save + detect new listings
    saved, new_listings = db.save_listings(raw_listings)

    # Compute deal scores (needs at least 2 markets worth of data to be meaningful)
    db.update_deal_scores(keyword)

    # Re-fetch with updated scores
    all_scored = db.get_listings(keyword=keyword, limit=10000, order_by="deal_score DESC")
    deals = [l for l in all_scored if l.get("deal_score", 0) >= 30]

    log.info(f"  -> {len(saved)} saved, {len(new_listings)} new, {len(deals)} deals")

    # --------------- Alerts ---------------
    if alert_callback and watchlist_config:
        cfg = watchlist_config

        # 1. New listing alerts
        if cfg.get("alert_new_listings") and new_listings:
            for l in new_listings:
                alert_callback("new_listing", keyword, l, None)

        # 2. Price threshold alerts
        price_threshold = cfg.get("alert_price_gbp")
        if price_threshold:
            for l in all_scored:
                p = l.get("price_gbp")
                if p and p <= price_threshold:
                    alert_callback("price", keyword, l, price_threshold)

        # 3. Deal score alerts
        score_threshold = cfg.get("alert_deal_score", 70)
        for l in deals:
            if l.get("deal_score", 0) >= score_threshold:
                alert_callback("deal", keyword, l, score_threshold)

    db.log_search(keyword, markets or [], max_items,
                  watchlist_config.get("alert_price_gbp") if watchlist_config else None,
                  len(saved), len(new_listings), len(deals))

    db.update_watchlist_polled(keyword)

    return {
        "keyword": keyword,
        "total": len(saved),
        "new": len(new_listings),
        "deals": len(deals),
        "new_listings": new_listings,
        "top_deals": sorted(deals, key=lambda x: x.get("deal_score", 0), reverse=True)[:20],
    }
