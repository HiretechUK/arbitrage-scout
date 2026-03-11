"""
cli.py - Command-line interface for running searches without the dashboard
Usage:
  python cli.py search "iPhone 14" --markets ebay_uk finn_no --alert 150
  python cli.py list "iPhone 14"
  python cli.py arbitrage "iPhone 14" --margin 30
  python cli.py summary "iPhone 14"
"""

import argparse
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from database import Database
from scraper import run_search, ACTORS
from alerts import AlertManager


def cmd_search(args):
    token = os.environ.get("APIFY_TOKEN", "")
    if not token:
        print("ERROR: APIFY_TOKEN not set. Add it to .env or export it.")
        sys.exit(1)

    db = Database()
    config = {
        "email_enabled": os.environ.get("EMAIL_ENABLED", "false").lower() == "true",
        "smtp_host": os.environ.get("SMTP_HOST", "smtp.gmail.com"),
        "smtp_port": int(os.environ.get("SMTP_PORT", "587")),
        "smtp_user": os.environ.get("SMTP_USER", ""),
        "smtp_password": os.environ.get("SMTP_PASSWORD", ""),
        "alert_email": os.environ.get("ALERT_EMAIL", ""),
        "webhook_url": os.environ.get("WEBHOOK_URL", ""),
    }
    alert_manager = AlertManager(db=db, config=config)

    print(f"\n🔍 Searching: '{args.keyword}'")
    print(f"   Markets : {', '.join(args.markets) if args.markets else 'ALL'}")
    print(f"   Items   : up to {args.max_items} per market")
    if args.alert:
        print(f"   Alert   : ≤ £{args.alert:.2f}")
    print()

    listings = run_search(
        keyword=args.keyword,
        apify_token=token,
        db=db,
        markets=args.markets or None,
        max_items=args.max_items,
        alert_threshold_gbp=args.alert,
        alert_callback=alert_manager.fire if args.alert else None,
    )
    db.log_search(args.keyword, args.markets or [], args.max_items, args.alert, len(listings))

    if listings:
        print(f"✅ {len(listings)} listings saved.\n")
        print(f"{'Title':<50} {'Market':<22} {'Country':<8} {'Price GBP':>10}")
        print("-" * 95)
        for l in sorted(listings, key=lambda x: x.get("price_gbp") or 9999)[:30]:
            p = f"£{l['price_gbp']:.2f}" if l.get("price_gbp") else "—"
            print(f"{str(l.get('title',''))[:49]:<50} {str(l.get('marketplace',''))[:21]:<22} {str(l.get('country',''))[:7]:<8} {p:>10}")
    else:
        print("No listings returned. Check your APIFY_TOKEN and actor availability.")


def cmd_list(args):
    db = Database()
    listings = db.get_listings(keyword=args.keyword, limit=100, order_by="price_gbp ASC")
    if not listings:
        print(f"No listings for '{args.keyword}' in DB.")
        return
    print(f"\n{'Title':<50} {'Market':<22} {'Country':<8} {'Price GBP':>10}")
    print("-" * 95)
    for l in listings:
        p = f"£{l['price_gbp']:.2f}" if l.get("price_gbp") else "—"
        print(f"{str(l.get('title',''))[:49]:<50} {str(l.get('marketplace',''))[:21]:<22} {str(l.get('country',''))[:7]:<8} {p:>10}")
    print(f"\n{len(listings)} listings shown.")


def cmd_arbitrage(args):
    db = Database()
    opps = db.get_arbitrage_opportunities(args.keyword, args.margin)
    if not opps:
        print(f"No opportunities with ≥ £{args.margin} margin for '{args.keyword}'.")
        return
    print(f"\n⚡ Arbitrage opportunities for '{args.keyword}' (min margin £{args.margin})\n")
    print(f"{'BUY (market)':<25} {'BUY £':>8}  {'SELL (market)':<25} {'SELL £':>8}  {'MARGIN':>8}")
    print("-" * 80)
    for o in opps[:30]:
        print(f"{str(o['buy_market'])[:24]:<25} £{o['buy_price_gbp']:>7.2f}  {str(o['sell_market'])[:24]:<25} £{o['sell_price_gbp']:>7.2f}  £{o['margin_gbp']:>7.2f}")
        print(f"  └ {o['buy_url'][:80]}" if o.get('buy_url') else "")


def cmd_summary(args):
    db = Database()
    rows = db.get_price_summary(args.keyword)
    if not rows:
        print(f"No data for '{args.keyword}'.")
        return
    print(f"\n📊 Price summary for '{args.keyword}'\n")
    print(f"{'Marketplace':<25} {'Country':<8} {'Curr':<5} {'Min £':>8} {'Avg £':>8} {'Max £':>8} {'Count':>6}")
    print("-" * 75)
    for r in rows:
        print(f"{str(r['marketplace'])[:24]:<25} {str(r['country'])[:7]:<8} {r['currency']:<5} £{r['min_gbp']:>6.2f} £{r['avg_gbp']:>6.2f} £{r['max_gbp']:>6.2f} {r['count']:>6}")


def main():
    parser = argparse.ArgumentParser(description="Arbitrage Scout CLI")
    sub = parser.add_subparsers(dest="cmd")

    # search
    p_search = sub.add_parser("search", help="Scrape marketplaces for a product")
    p_search.add_argument("keyword")
    p_search.add_argument("--markets", nargs="*", choices=list(ACTORS.keys()), help="Which markets to search (default: all)")
    p_search.add_argument("--max-items", type=int, default=30)
    p_search.add_argument("--alert", type=float, default=None, help="Alert price threshold in GBP")

    # list
    p_list = sub.add_parser("list", help="List stored results for a keyword")
    p_list.add_argument("keyword")

    # arbitrage
    p_arb = sub.add_parser("arbitrage", help="Show buy-low/sell-high opportunities")
    p_arb.add_argument("keyword")
    p_arb.add_argument("--margin", type=float, default=20.0, help="Minimum margin in GBP (default: 20)")

    # summary
    p_sum = sub.add_parser("summary", help="Price stats by marketplace")
    p_sum.add_argument("keyword")

    args = parser.parse_args()

    if args.cmd == "search":    cmd_search(args)
    elif args.cmd == "list":    cmd_list(args)
    elif args.cmd == "arbitrage": cmd_arbitrage(args)
    elif args.cmd == "summary": cmd_summary(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
