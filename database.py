"""
database.py - SQLite storage with price history, deal scoring, watchlist, new-listing tracking
"""

import sqlite3
import json
import logging
import statistics
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)
DB_PATH = Path(__file__).parent / "arbitrage.db"


class Database:
    def __init__(self, path: Path = DB_PATH):
        self.path = path
        self.conn = sqlite3.connect(str(path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")   # concurrent reads
        self._migrate()
        log.info(f"Database ready at {path}")

    def _migrate(self):
        c = self.conn.cursor()

        # Core listings table
        c.execute("""
            CREATE TABLE IF NOT EXISTS listings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword         TEXT NOT NULL,
                marketplace     TEXT NOT NULL,
                country         TEXT NOT NULL,
                currency        TEXT NOT NULL,
                title           TEXT,
                price_local     REAL,
                price_gbp       REAL,
                url             TEXT,
                location        TEXT,
                condition_text  TEXT,
                image_url       TEXT,
                scraped_at      TEXT NOT NULL,
                first_seen_at   TEXT NOT NULL,
                is_new          INTEGER DEFAULT 1,    -- 1 = brand new listing
                deal_score      REAL DEFAULT 0,       -- 0-100, higher = better deal
                pct_below_avg   REAL DEFAULT 0,       -- % below market average
                UNIQUE(url)                           -- deduplicate by URL
            )
        """)

        # Price history — one row per (url, scrape run) so we track price changes
        c.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id  INTEGER NOT NULL,
                keyword     TEXT NOT NULL,
                marketplace TEXT NOT NULL,
                country     TEXT NOT NULL,
                price_gbp   REAL,
                recorded_at TEXT NOT NULL,
                FOREIGN KEY(listing_id) REFERENCES listings(id)
            )
        """)

        # Market baselines — rolling avg/median per (keyword, marketplace)
        c.execute("""
            CREATE TABLE IF NOT EXISTS market_baselines (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword         TEXT NOT NULL,
                marketplace     TEXT NOT NULL,
                country         TEXT NOT NULL,
                avg_price_gbp   REAL,
                median_price_gbp REAL,
                min_price_gbp   REAL,
                max_price_gbp   REAL,
                sample_count    INTEGER,
                updated_at      TEXT NOT NULL,
                UNIQUE(keyword, marketplace)
            )
        """)

        # Watchlist — products being actively monitored
        c.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword             TEXT NOT NULL UNIQUE,
                markets             TEXT,           -- JSON array, NULL = all
                alert_new_listings  INTEGER DEFAULT 1,
                alert_price_gbp     REAL,           -- alert if price <= this
                alert_deal_score    REAL DEFAULT 70,-- alert if deal_score >= this
                max_items           INTEGER DEFAULT 30,
                poll_interval_mins  INTEGER DEFAULT 30,
                last_polled_at      TEXT,
                active              INTEGER DEFAULT 1,
                created_at          TEXT NOT NULL
            )
        """)

        # Alerts log
        c.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_type  TEXT NOT NULL,   -- 'price', 'deal', 'new_listing'
                keyword     TEXT NOT NULL,
                listing_id  INTEGER,
                price_gbp   REAL,
                threshold   REAL,
                deal_score  REAL,
                pct_below   REAL,
                marketplace TEXT,
                title       TEXT,
                url         TEXT,
                fired_at    TEXT NOT NULL,
                FOREIGN KEY(listing_id) REFERENCES listings(id)
            )
        """)

        # Search run log
        c.execute("""
            CREATE TABLE IF NOT EXISTS searches (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword     TEXT NOT NULL,
                markets     TEXT,
                max_items   INTEGER,
                alert_gbp   REAL,
                run_at      TEXT NOT NULL,
                result_count INTEGER,
                new_listings INTEGER DEFAULT 0,
                deals_found  INTEGER DEFAULT 0
            )
        """)

        # Indices
        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_kw       ON listings(keyword)",
            "CREATE INDEX IF NOT EXISTS idx_price    ON listings(price_gbp)",
            "CREATE INDEX IF NOT EXISTS idx_deal     ON listings(deal_score DESC)",
            "CREATE INDEX IF NOT EXISTS idx_new      ON listings(is_new)",
            "CREATE INDEX IF NOT EXISTS idx_scraped  ON listings(scraped_at)",
            "CREATE INDEX IF NOT EXISTS idx_ph_lid   ON price_history(listing_id)",
            "CREATE INDEX IF NOT EXISTS idx_ph_kw    ON price_history(keyword)",
        ]:
            c.execute(idx)

        self.conn.commit()

    # ------------------------------------------------------------------
    # Baseline calculation
    # ------------------------------------------------------------------

    def _compute_baselines(self, keyword: str):
        """
        Recalculate market average/median for this keyword using all
        historical price data. Called after each scrape batch.
        """
        rows = self.conn.execute("""
            SELECT marketplace, country, price_gbp
            FROM listings
            WHERE keyword LIKE ? AND price_gbp IS NOT NULL AND price_gbp > 0
        """, (f"%{keyword}%",)).fetchall()

        # Group by marketplace
        by_market: dict = {}
        for r in rows:
            key = (r["marketplace"], r["country"])
            by_market.setdefault(key, []).append(r["price_gbp"])

        now = datetime.utcnow().isoformat()
        for (marketplace, country), prices in by_market.items():
            if len(prices) < 2:
                continue
            avg = sum(prices) / len(prices)
            med = statistics.median(prices)
            self.conn.execute("""
                INSERT INTO market_baselines
                    (keyword, marketplace, country, avg_price_gbp, median_price_gbp,
                     min_price_gbp, max_price_gbp, sample_count, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT(keyword, marketplace) DO UPDATE SET
                    avg_price_gbp    = excluded.avg_price_gbp,
                    median_price_gbp = excluded.median_price_gbp,
                    min_price_gbp    = excluded.min_price_gbp,
                    max_price_gbp    = excluded.max_price_gbp,
                    sample_count     = excluded.sample_count,
                    updated_at       = excluded.updated_at
            """, (keyword, marketplace, country, round(avg,2), round(med,2),
                  round(min(prices),2), round(max(prices),2), len(prices), now))
        self.conn.commit()

    def _score_deal(self, price_gbp: float, keyword: str, marketplace: str) -> tuple:
        """
        Return (deal_score 0-100, pct_below_avg).
        Uses cross-market median so you can compare UK vs UAE vs Norway.
        """
        # Get global median across ALL markets for this keyword
        row = self.conn.execute("""
            SELECT AVG(median_price_gbp) as global_median,
                   AVG(avg_price_gbp)    as global_avg
            FROM market_baselines
            WHERE keyword LIKE ?
        """, (f"%{keyword}%",)).fetchone()

        if not row or not row["global_median"]:
            return 0.0, 0.0

        global_median = row["global_median"]
        if global_median <= 0:
            return 0.0, 0.0

        pct_below = ((global_median - price_gbp) / global_median) * 100
        # Score: 0 = at or above median, 100 = free
        score = max(0.0, min(100.0, pct_below * 2))   # 50% below median = 100 score
        return round(score, 1), round(pct_below, 1)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def save_listings(self, listings: list) -> tuple:
        """
        Upsert listings. Returns (all_saved, new_listings).
        New = URL never seen before.
        """
        saved, new_listings = [], []
        now = datetime.utcnow().isoformat()
        c = self.conn.cursor()

        for l in listings:
            url = l.get("url", "")
            price_gbp = l.get("price_gbp")

            # Check if we've seen this URL before
            existing = c.execute(
                "SELECT id, price_gbp, first_seen_at FROM listings WHERE url=?", (url,)
            ).fetchone()

            is_new = 0 if existing else 1
            first_seen = existing["first_seen_at"] if existing else now

            if existing:
                # Update price + scraped_at; keep first_seen_at
                c.execute("""
                    UPDATE listings SET price_gbp=?, price_local=?, scraped_at=?, is_new=0
                    WHERE url=?
                """, (price_gbp, l.get("price_local"), now, url))
                listing_id = existing["id"]
            else:
                c.execute("""
                    INSERT OR IGNORE INTO listings
                        (keyword, marketplace, country, currency, title,
                         price_local, price_gbp, url, location, condition_text,
                         image_url, scraped_at, first_seen_at, is_new)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    l.get("keyword",""), l.get("marketplace",""),
                    l.get("country",""), l.get("currency",""),
                    l.get("title",""), l.get("price_local"),
                    price_gbp, url, l.get("location",""),
                    l.get("condition",""), l.get("image_url",""),
                    now, first_seen, 1,
                ))
                listing_id = c.lastrowid

            if listing_id and price_gbp:
                # Always record price history
                c.execute("""
                    INSERT INTO price_history (listing_id, keyword, marketplace, country, price_gbp, recorded_at)
                    VALUES (?,?,?,?,?,?)
                """, (listing_id, l.get("keyword",""), l.get("marketplace",""),
                      l.get("country",""), price_gbp, now))

            l["id"] = listing_id
            l["is_new"] = is_new
            l["first_seen_at"] = first_seen
            saved.append(l)
            if is_new:
                new_listings.append(l)

        self.conn.commit()
        return saved, new_listings

    def update_deal_scores(self, keyword: str):
        """Recalculate deal scores for all listings of this keyword."""
        self._compute_baselines(keyword)
        rows = self.conn.execute(
            "SELECT id, price_gbp, marketplace FROM listings WHERE keyword LIKE ? AND price_gbp > 0",
            (f"%{keyword}%",)
        ).fetchall()
        for r in rows:
            score, pct = self._score_deal(r["price_gbp"], keyword, r["marketplace"])
            self.conn.execute(
                "UPDATE listings SET deal_score=?, pct_below_avg=? WHERE id=?",
                (score, pct, r["id"])
            )
        self.conn.commit()

    def log_search(self, keyword, markets, max_items, alert_gbp, result_count, new_count=0, deals_count=0):
        self.conn.execute("""
            INSERT INTO searches (keyword, markets, max_items, alert_gbp, run_at, result_count, new_listings, deals_found)
            VALUES (?,?,?,?,?,?,?,?)
        """, (keyword, json.dumps(markets or []), max_items, alert_gbp,
              datetime.utcnow().isoformat(), result_count, new_count, deals_count))
        self.conn.commit()

    def save_alert(self, alert_type: str, keyword: str, listing: dict,
                   threshold: float = None):
        self.conn.execute("""
            INSERT INTO alerts (alert_type, keyword, listing_id, price_gbp, threshold,
                                deal_score, pct_below, marketplace, title, url, fired_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            alert_type, keyword, listing.get("id"), listing.get("price_gbp"),
            threshold, listing.get("deal_score"), listing.get("pct_below_avg"),
            listing.get("marketplace"), listing.get("title","")[:150],
            listing.get("url",""), datetime.utcnow().isoformat()
        ))
        self.conn.commit()

    # ------------------------------------------------------------------
    # Watchlist
    # ------------------------------------------------------------------

    def add_to_watchlist(self, keyword: str, markets: list = None,
                         alert_new: bool = True, alert_price: float = None,
                         alert_deal_score: float = 70, max_items: int = 30,
                         poll_mins: int = 30) -> dict:
        now = datetime.utcnow().isoformat()
        self.conn.execute("""
            INSERT INTO watchlist
                (keyword, markets, alert_new_listings, alert_price_gbp,
                 alert_deal_score, max_items, poll_interval_mins, active, created_at)
            VALUES (?,?,?,?,?,?,?,1,?)
            ON CONFLICT(keyword) DO UPDATE SET
                markets=excluded.markets,
                alert_new_listings=excluded.alert_new_listings,
                alert_price_gbp=excluded.alert_price_gbp,
                alert_deal_score=excluded.alert_deal_score,
                max_items=excluded.max_items,
                poll_interval_mins=excluded.poll_interval_mins,
                active=1
        """, (keyword, json.dumps(markets or []), int(alert_new), alert_price,
              alert_deal_score, max_items, poll_mins, now))
        self.conn.commit()
        return self.get_watchlist_item(keyword)

    def get_watchlist_item(self, keyword: str) -> Optional[dict]:
        r = self.conn.execute(
            "SELECT * FROM watchlist WHERE keyword=?", (keyword,)
        ).fetchone()
        return dict(r) if r else None

    def get_watchlist(self, active_only: bool = True) -> list:
        q = "SELECT * FROM watchlist"
        if active_only:
            q += " WHERE active=1"
        q += " ORDER BY created_at DESC"
        return [dict(r) for r in self.conn.execute(q).fetchall()]

    def update_watchlist_polled(self, keyword: str):
        self.conn.execute(
            "UPDATE watchlist SET last_polled_at=? WHERE keyword=?",
            (datetime.utcnow().isoformat(), keyword)
        )
        self.conn.commit()

    def remove_from_watchlist(self, keyword: str):
        self.conn.execute("UPDATE watchlist SET active=0 WHERE keyword=?", (keyword,))
        self.conn.commit()

    def get_due_watchlist(self) -> list:
        """Return watchlist items that are due for a poll."""
        now = datetime.utcnow()
        items = self.get_watchlist(active_only=True)
        due = []
        for item in items:
            lp = item.get("last_polled_at")
            if not lp:
                due.append(item)
                continue
            last = datetime.fromisoformat(lp)
            interval = timedelta(minutes=item.get("poll_interval_mins", 30))
            if now - last >= interval:
                due.append(item)
        return due

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_top_deals(self, limit: int = 20, keyword: str = None, min_score: float = 10) -> list:
        q = """
            SELECT * FROM listings
            WHERE deal_score >= ? AND price_gbp IS NOT NULL
        """
        params = [min_score]
        if keyword:
            q += " AND keyword LIKE ?"
            params.append(f"%{keyword}%")
        q += " ORDER BY deal_score DESC LIMIT ?"
        params.append(limit)
        return [dict(r) for r in self.conn.execute(q, params).fetchall()]

    def get_new_listings(self, keyword: str = None, since_minutes: int = 60, limit: int = 50) -> list:
        since = (datetime.utcnow() - timedelta(minutes=since_minutes)).isoformat()
        q = "SELECT * FROM listings WHERE is_new=1 AND first_seen_at >= ?"
        params = [since]
        if keyword:
            q += " AND keyword LIKE ?"
            params.append(f"%{keyword}%")
        q += " ORDER BY first_seen_at DESC LIMIT ?"
        params.append(limit)
        return [dict(r) for r in self.conn.execute(q, params).fetchall()]

    def get_listings(self, keyword=None, country=None, marketplace=None,
                     max_price_gbp=None, min_price_gbp=None,
                     limit=200, order_by="price_gbp ASC") -> list:
        q = "SELECT * FROM listings WHERE 1=1"
        params = []
        if keyword:
            q += " AND keyword LIKE ?"; params.append(f"%{keyword}%")
        if country:
            q += " AND country=?"; params.append(country)
        if marketplace:
            q += " AND marketplace=?"; params.append(marketplace)
        if max_price_gbp is not None:
            q += " AND price_gbp<=?"; params.append(max_price_gbp)
        if min_price_gbp is not None:
            q += " AND price_gbp>=?"; params.append(min_price_gbp)
        q += f" ORDER BY {order_by} LIMIT ?"
        params.append(limit)
        return [dict(r) for r in self.conn.execute(q, params).fetchall()]

    def get_price_summary(self, keyword: str) -> list:
        rows = self.conn.execute("""
            SELECT mb.marketplace, mb.country, mb.avg_price_gbp, mb.median_price_gbp,
                   mb.min_price_gbp, mb.max_price_gbp, mb.sample_count, mb.updated_at
            FROM market_baselines mb
            WHERE mb.keyword LIKE ?
            ORDER BY mb.avg_price_gbp ASC
        """, (f"%{keyword}%",)).fetchall()
        return [dict(r) for r in rows]

    def get_price_history(self, keyword: str, marketplace: str = None, days: int = 30) -> list:
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        q = """
            SELECT ph.*, l.title FROM price_history ph
            JOIN listings l ON l.id = ph.listing_id
            WHERE ph.keyword LIKE ? AND ph.recorded_at >= ?
        """
        params = [f"%{keyword}%", since]
        if marketplace:
            q += " AND ph.marketplace=?"; params.append(marketplace)
        q += " ORDER BY ph.recorded_at ASC LIMIT 1000"
        return [dict(r) for r in self.conn.execute(q, params).fetchall()]

    def get_arbitrage_opportunities(self, keyword: str, min_margin_gbp: float = 20.0) -> list:
        rows = self.conn.execute("""
            SELECT
                cheap.id            as buy_id,
                cheap.title         as buy_title,
                cheap.marketplace   as buy_market,
                cheap.country       as buy_country,
                cheap.price_gbp     as buy_price_gbp,
                cheap.url           as buy_url,
                cheap.image_url     as buy_image,
                cheap.deal_score    as buy_deal_score,
                exp.marketplace     as sell_market,
                exp.country         as sell_country,
                exp.price_gbp       as sell_price_gbp,
                (exp.price_gbp - cheap.price_gbp) as margin_gbp
            FROM listings cheap
            JOIN listings exp
                ON exp.keyword LIKE ?
                AND exp.country != cheap.country
                AND exp.price_gbp > cheap.price_gbp
            WHERE cheap.keyword LIKE ?
              AND cheap.price_gbp IS NOT NULL
              AND (exp.price_gbp - cheap.price_gbp) >= ?
            ORDER BY margin_gbp DESC
            LIMIT 100
        """, (f"%{keyword}%", f"%{keyword}%", min_margin_gbp)).fetchall()
        return [dict(r) for r in rows]

    def get_recent_alerts(self, limit: int = 100) -> list:
        rows = self.conn.execute(
            "SELECT * FROM alerts ORDER BY fired_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_search_history(self, limit: int = 30) -> list:
        rows = self.conn.execute(
            "SELECT * FROM searches ORDER BY run_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_all_keywords(self) -> list:
        return [r[0] for r in self.conn.execute(
            "SELECT DISTINCT keyword FROM listings ORDER BY keyword"
        ).fetchall()]

    def get_live_stats(self) -> dict:
        now = datetime.utcnow()
        since_1h = (now - timedelta(hours=1)).isoformat()
        since_24h = (now - timedelta(hours=24)).isoformat()
        return {
            "total_listings": self.conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0],
            "new_last_hour": self.conn.execute(
                "SELECT COUNT(*) FROM listings WHERE first_seen_at >= ?", (since_1h,)
            ).fetchone()[0],
            "new_last_24h": self.conn.execute(
                "SELECT COUNT(*) FROM listings WHERE first_seen_at >= ?", (since_24h,)
            ).fetchone()[0],
            "deals_found": self.conn.execute(
                "SELECT COUNT(*) FROM listings WHERE deal_score >= 30"
            ).fetchone()[0],
            "alerts_today": self.conn.execute(
                "SELECT COUNT(*) FROM alerts WHERE fired_at >= ?", (since_24h,)
            ).fetchone()[0],
            "watchlist_count": self.conn.execute(
                "SELECT COUNT(*) FROM watchlist WHERE active=1"
            ).fetchone()[0],
        }

    def close(self):
        self.conn.close()
