"""Synthetic e-commerce data generator.

Produces a realistic, *behaviour-driven* dataset for an Indian online
marketplace so that every analytics / ML module has genuine signal to learn:

* customers  (CSV)        - demographics, city, acquisition channel
* products   (JSON lines) - 8 categories, ~40 sub-categories, brands, prices
* orders     (CSV)        - order header: time, payment, device, status
* order_items(CSV)        - line items with discounts and ratings
* events     (JSON lines) - clickstream: sessions, views, carts, checkouts

Behaviour is simulated with hidden *personas* (loyal, regular, bargain
hunter, occasional, one-time, browser) whose purchase rate, basket size,
coupon usage and lifetime differ. Seasonality (weekends, salary days and
Indian sale festivals), year-on-year growth, product co-purchase bundles,
pre-churn engagement decay and a handful of anomalous days are built in.

A small amount of *dirty data* (duplicates, bad casing, missing values,
invalid quantities) is injected deliberately so that the Spark cleaning
stage has real work to do.

Usage:  python -m shopsense.datagen.generator --scale medium
"""

import argparse
import csv
import json
import math
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np

from shopsense import config
from shopsense.datagen.catalog import (
    ACQUISITION_CHANNELS, BRAND_SYLLABLES_A, BRAND_SYLLABLES_B, CATEGORIES, CITIES,
    COMPLEMENTS, DEVICES, FIRST_NAMES_F, FIRST_NAMES_M, LAST_NAMES, PAYMENT_METHODS,
    PRODUCT_TAGS, SALE_EVENTS, TRAFFIC_SOURCES,
)

START = date(2024, 7, 1)
END = date(2026, 6, 30)
N_DAYS = (END - START).days + 1

SCALES = {
    # customers, products per sub-category
    "small": (2500, 10),
    "medium": (10000, 16),
    "large": (25000, 24),
}

# persona: share, orders/month, lifetime mean (days), extra items, coupon prob,
#          premium affinity, browse sessions per order
PERSONAS = {
    "loyal":      (0.10, 2.0, 1100, 0.9, 0.25, 0.8, 2.0),
    "regular":    (0.25, 0.8, 650, 0.6, 0.35, 0.3, 2.5),
    "bargain":    (0.20, 0.7, 480, 0.7, 0.85, -0.4, 3.5),
    "occasional": (0.25, 0.25, 420, 0.4, 0.40, 0.0, 3.0),
    "one_time":   (0.12, 1.3, 28, 0.3, 0.50, 0.0, 2.0),
    "browser":    (0.08, 0.05, 360, 0.2, 0.30, 0.0, 12.0),
}

HOUR_WEIGHTS = np.array([1.2, 0.7, 0.4, 0.3, 0.3, 0.5, 1.0, 1.8, 2.6, 3.2, 3.6, 4.0,
                         4.6, 4.4, 3.8, 3.6, 3.7, 4.0, 4.6, 5.4, 6.2, 6.6, 5.4, 2.8])
HOUR_P = HOUR_WEIGHTS / HOUR_WEIGHTS.sum()

# Planted anomalies (offset from START in days, multiplier, reason) - the
# anomaly-detection module should rediscover these.
PLANTED_ANOMALIES = [
    (140, 0.25, "Payment gateway outage"),
    (301, 3.2, "Viral influencer flash sale"),
    (455, 0.3, "Website downtime"),
    (610, 2.8, "Bulk corporate gifting orders"),
]


class Generator:
    def __init__(self, scale: str = "medium", seed: int = 42, out_dir: Path = None):
        if scale not in SCALES:
            raise ValueError(f"scale must be one of {list(SCALES)}")
        self.scale = scale
        self.n_customers, self.per_subcat = SCALES[scale]
        self.rng = np.random.default_rng(seed)
        self.out = Path(out_dir or config.RAW_DIR)
        self.days = [START + timedelta(days=i) for i in range(N_DAYS)]

    # ------------------------------------------------------------ helpers
    def _log(self, msg):
        print(f"  [datagen] {msg}", flush=True)

    def _brand_names(self, n):
        names = set()
        while len(names) < n:
            names.add(self.rng.choice(BRAND_SYLLABLES_A) + self.rng.choice(BRAND_SYLLABLES_B))
        return sorted(names)

    # ------------------------------------------------------------ calendar
    def build_calendar(self):
        """Daily demand multiplier: trend x weekly x salary-day x sale events x noise."""
        mult = np.ones(N_DAYS)
        sale_name = [""] * N_DAYS
        sale_disc = np.zeros(N_DAYS)
        for i, d in enumerate(self.days):
            m = 1.0 + 0.38 * (i / N_DAYS)                       # ~38% growth over 2 years
            m *= 1.18 if d.weekday() >= 5 else (0.94 if d.weekday() in (1, 2) else 1.0)
            if d.day <= 5:
                m *= 1.10                                        # salary days
            for name, mo, day, length, boost, disc in SALE_EVENTS:
                s = date(d.year, mo, day)
                if s <= d < s + timedelta(days=length):
                    # sales peak on the first days and fade out
                    k = (d - s).days
                    m *= 1 + (boost - 1) * (1.0 - 0.45 * k / max(length - 1, 1))
                    sale_name[i] = name
                    sale_disc[i] = disc
            mult[i] = m
        mult *= self.rng.lognormal(0, 0.07, N_DAYS)
        for off, factor, _ in PLANTED_ANOMALIES:
            if off < N_DAYS:
                mult[off] *= factor
        self.day_mult, self.sale_name, self.sale_disc = mult, sale_name, sale_disc

    # ------------------------------------------------------------ products
    def build_products(self):
        rng = self.rng
        cats = list(CATEGORIES)
        self.cat_pop = np.array([CATEGORIES[c][0] for c in cats])
        self.categories = cats
        products = []
        self.subcat_products = {}    # subcat -> list of product indexes
        self.cat_subcats = {}
        self.subcat_w = {}           # cheaper sub-categories are bought more often
        for cat in cats:
            _, margin, ret_rate, subcats = CATEGORIES[cat]
            brands = self._brand_names(6)
            self.cat_subcats[cat] = list(subcats)
            w = np.array([(lo * hi) ** -0.22 for lo, hi, _ in subcats.values()])
            self.subcat_w[cat] = w / w.sum()
            for sub, (pmin, pmax, nouns) in subcats.items():
                idxs = []
                for k in range(self.per_subcat):
                    price = math.exp(rng.uniform(math.log(pmin), math.log(pmax)))
                    price = int(round(price / 10.0) * 10 - 1) if price > 100 else int(round(price))
                    brand = brands[int(rng.integers(len(brands)))]
                    noun = nouns[k % len(nouns)]
                    tag = rng.choice(PRODUCT_TAGS)
                    model = f"{chr(65 + int(rng.integers(26)))}{int(rng.integers(1, 99))}"
                    quality = float(np.clip(rng.beta(6, 2), 0.2, 0.99))
                    pid = f"P{len(products) + 1:05d}"
                    products.append({
                        "product_id": pid,
                        "product_name": f"{brand} {noun} {tag} {model}",
                        "category": cat,
                        "sub_category": sub,
                        "brand": brand,
                        "list_price": float(price),
                        "cost_price": round(price * (1 - margin * rng.uniform(0.75, 1.25)), 2),
                        "launch_date": str(START - timedelta(days=int(rng.integers(30, 900)))),
                        "_quality": quality,
                        "_return": ret_rate * (1.6 - quality),
                    })
                    idxs.append(len(products) - 1)
                self.subcat_products[sub] = idxs
        # popularity: Zipf-like inside each sub-category
        pop = np.zeros(len(products))
        for sub, idxs in self.subcat_products.items():
            ranks = rng.permutation(len(idxs)) + 1
            pop[idxs] = 1.0 / ranks ** 0.9
        self.pop = pop
        self.prices = np.array([p["list_price"] for p in products])
        # companion product ("frequently bought together")
        for p in products:
            comp_subs = COMPLEMENTS.get(p["sub_category"])
            if comp_subs and rng.random() < 0.8:
                sub = comp_subs[int(rng.integers(len(comp_subs)))]
                cands = self.subcat_products[sub]
                w = pop[cands] / pop[cands].sum()
                p["_companion"] = int(rng.choice(cands, p=w))
            else:
                p["_companion"] = None
        self.products = products
        self._log(f"products: {len(products)}")

    def _subcat_sampler(self, sub, premium):
        idxs = self.subcat_products[sub]
        w = self.pop[idxs].copy()
        if premium:
            pr = self.prices[idxs]
            w *= (pr / np.median(pr)) ** premium
        return idxs, w / w.sum()

    # ------------------------------------------------------------ customers
    def build_customers(self):
        rng = self.rng
        n = self.n_customers
        names = list(PERSONAS)
        shares = np.array([PERSONAS[p][0] for p in names])
        persona = rng.choice(len(names), size=n, p=shares / shares.sum())
        city_w = np.array([c[6] for c in CITIES])
        city_idx = rng.choice(len(CITIES), size=n, p=city_w / city_w.sum())
        gender = rng.choice(["Male", "Female"], size=n, p=[0.54, 0.46])
        age = np.clip(rng.normal(31, 8.5, n), 18, 68).astype(int)
        chan = rng.choice([c[0] for c in ACQUISITION_CHANNELS], size=n,
                          p=[c[1] for c in ACQUISITION_CHANNELS])

        # signup: 45% existing base before START, the rest join over time
        existing = rng.random(n) < 0.45
        pre = rng.integers(30, 700, n)
        # mildly increasing sign-up density -> steady business growth
        post = (rng.random(n) ** 0.85 * (N_DAYS - 40)).astype(int)
        signup_off = np.where(existing, -pre, post)

        customers = []
        self.cust = []
        for i in range(n):
            pname = names[persona[i]]
            _, rate, life, extra, coupon, premium, browse = PERSONAS[pname]
            life_days = int(rng.exponential(life)) + 7
            s_off = int(signup_off[i])
            active_until = s_off + life_days
            g = gender[i]
            first = rng.choice(FIRST_NAMES_M if g == "Male" else FIRST_NAMES_F)
            last = rng.choice(LAST_NAMES)
            city = CITIES[city_idx[i]]
            # category preference (Dirichlet around global popularity)
            pref = rng.dirichlet(self.cat_pop * 0.9)
            if g == "Female":
                pref[self.categories.index("Beauty & Personal Care")] *= 1.8
                pref[self.categories.index("Fashion")] *= 1.3
            else:
                pref[self.categories.index("Electronics")] *= 1.4
                pref[self.categories.index("Sports & Fitness")] *= 1.3
            pref /= pref.sum()
            dev_p = np.array([0.62, 0.23, 0.15]) if age[i] < 35 else np.array([0.45, 0.2, 0.35])
            self.cust.append({
                "persona": pname, "rate": rate * rng.lognormal(0, 0.35), "extra": extra,
                "coupon": coupon, "premium": premium, "browse": browse,
                "signup": s_off, "until": active_until, "pref": pref, "gender": g,
                "device": DEVICES[int(rng.choice(3, p=dev_p))], "tier": city[3],
            })
            customers.append({
                "customer_id": f"C{i + 1:06d}",
                "first_name": first, "last_name": last,
                "email": f"{first.lower()}.{last.lower()}{int(rng.integers(10, 999))}@example.com",
                "gender": g, "age": int(age[i]),
                "city": city[0], "state": city[1], "region": city[2], "city_tier": city[3],
                "signup_date": str(START + timedelta(days=s_off)),
                "acquisition_channel": chan[i],
            })
        self.customers = customers
        self._log(f"customers: {n}")

    # ------------------------------------------------------------ orders
    def build_orders(self):
        rng = self.rng
        orders, items = [], []
        self.order_sessions = []   # (customer_idx, ts, [product idx], order_id, device)
        n = self.n_customers
        chunk = 2000
        day_idx = np.arange(N_DAYS)
        oid = 0
        for c0 in range(0, n, chunk):
            c1 = min(n, c0 + chunk)
            rate = np.array([self.cust[i]["rate"] for i in range(c0, c1)]) / 30.0
            start = np.array([max(self.cust[i]["signup"], 0) for i in range(c0, c1)])
            until = np.array([self.cust[i]["until"] for i in range(c0, c1)])
            active = (day_idx[None, :] >= start[:, None]) & (day_idx[None, :] <= until[:, None])
            # engagement decays in the ~60 days before a customer churns
            decay = np.where((until[:, None] - day_idx[None, :]) < 60, 0.45, 1.0)
            lam = rate[:, None] * self.day_mult[None, :] * active * decay
            counts = rng.poisson(lam)
            cs, ds = np.nonzero(counts)
            for ci, di in zip(cs, ds):
                for _ in range(counts[ci, di]):
                    oid += 1
                    self._make_order(c0 + ci, di, oid, orders, items)
        self.orders, self.items = orders, items
        self._log(f"orders: {len(orders)}  order_items: {len(items)}")

    def _make_order(self, ci, di, oid, orders, items):
        rng = self.rng
        c = self.cust[ci]
        cust = self.customers[ci]
        d = self.days[di]
        hour = int(rng.choice(24, p=HOUR_P))
        ts = datetime(d.year, d.month, d.day, hour, int(rng.integers(60)), int(rng.integers(60)))
        n_items = 1 + int(rng.poisson(c["extra"]))
        chosen = []
        for _ in range(n_items):
            cat = self.categories[int(rng.choice(len(self.categories), p=c["pref"]))]
            subs = self.cat_subcats[cat]
            if cat == "Fashion":
                wanted = "Women's Clothing" if c["gender"] == "Female" else "Men's Clothing"
                sub = wanted if rng.random() < 0.45 else subs[int(rng.integers(len(subs)))]
            else:
                sub = subs[int(rng.choice(len(subs), p=self.subcat_w[cat]))]
            idxs, w = self._subcat_sampler(sub, c["premium"])
            chosen.append(int(rng.choice(idxs, p=w)))
            comp = self.products[chosen[-1]]["_companion"]
            if comp is not None and rng.random() < 0.38:
                chosen.append(comp)
        chosen = list(dict.fromkeys(chosen))  # unique, keep order

        on_sale = self.sale_disc[di] > 0
        uses_coupon = rng.random() < c["coupon"] * (1.3 if on_sale else 0.8)
        order_id = f"ORD{oid:07d}"
        total = 0.0
        max_ret = 0.0
        lines = []
        for ln, pi in enumerate(chosen, start=1):
            p = self.products[pi]
            qty = 1
            if p["category"] in ("Grocery & Gourmet", "Beauty & Personal Care") and rng.random() < 0.35:
                qty = int(rng.integers(2, 4))
            disc = 0.0
            if on_sale:
                disc = max(0.0, rng.normal(self.sale_disc[di], 0.06))
            if uses_coupon:
                disc += rng.choice([0.05, 0.1, 0.15])
            disc = float(np.clip(disc, 0, 0.6))
            unit = round(p["list_price"] * (1 - disc), 2)
            total += unit * qty
            max_ret = max(max_ret, p["_return"])
            lines.append([order_id, ln, p["product_id"], qty, p["list_price"], unit, round(disc, 3), pi])

        # payment method
        pw = np.array([0.42, 0.14, 0.10, 0.18, 0.06, 0.07, 0.03])
        if c["persona"] == "bargain" or c["tier"] == 3:
            pw[3] *= 1.9
        if total > 20000:
            pw[6] *= 12
            pw[1] *= 2
        payment = PAYMENT_METHODS[int(rng.choice(len(pw), p=pw / pw.sum()))]
        device = c["device"] if rng.random() < 0.72 else DEVICES[int(rng.integers(3))]

        # status
        days_to_end = (END - d).days
        r = rng.random()
        cancel_p = 0.035 + (0.04 if payment == "Cash on Delivery" else 0)
        if days_to_end < 3:
            status = "Processing"
        elif days_to_end < 7:
            status = "Shipped"
        elif r < cancel_p:
            status = "Cancelled"
        elif r < cancel_p + max_ret:
            status = "Returned"
        else:
            status = "Delivered"

        for line in lines:
            pi = line.pop()
            rating = ""
            if status == "Delivered" and rng.random() < 0.55:
                q = self.products[pi]["_quality"]
                rating = int(np.clip(round(rng.normal(1 + 4 * q, 0.8)), 1, 5))
            items.append(line + [rating])

        orders.append([
            order_id, cust["customer_id"], ts.isoformat(sep=" "), status, payment, device,
            f"SAVE{int(rng.integers(5, 30))}" if uses_coupon else "",
            0.0 if total >= 499 else 40.0,
        ])
        self.order_sessions.append((ci, ts, [l[2] for l in lines], order_id, device))

    # ------------------------------------------------------------ clickstream
    def build_events(self):
        rng = self.rng
        pid_index = {p["product_id"]: i for i, p in enumerate(self.products)}
        events_by_month = {}
        sid = 0
        eid = 0

        def emit(ts, sess, cust_id, etype, pid, device, source, order_id=""):
            nonlocal eid
            eid += 1
            key = ts.strftime("%Y-%m")
            events_by_month.setdefault(key, []).append({
                "event_id": f"E{eid:09d}", "session_id": sess, "customer_id": cust_id,
                "event_time": ts.isoformat(sep=" "), "event_type": etype,
                "product_id": pid, "device": device, "traffic_source": source,
                "order_id": order_id,
            })

        def pick_source():
            return TRAFFIC_SOURCES[int(rng.choice(6, p=[0.3, 0.18, 0.2, 0.07, 0.08, 0.17]))]

        def browse_products(ci, k):
            c = self.cust[ci]
            out = []
            for _ in range(k):
                cat = self.categories[int(rng.choice(len(self.categories), p=c["pref"]))]
                subs = self.cat_subcats[cat]
                sub = subs[int(rng.choice(len(subs), p=self.subcat_w[cat]))]
                idxs, w = self._subcat_sampler(sub, c["premium"])
                out.append(self.products[int(rng.choice(idxs, p=w))]["product_id"])
            return out

        # 1) purchase sessions (one per order)
        for ci, ts, pids, order_id, device in self.order_sessions:
            sid += 1
            sess = f"S{sid:08d}"
            cust_id = self.customers[ci]["customer_id"]
            src = pick_source()
            t = ts - timedelta(minutes=int(rng.integers(4, 40)), seconds=int(rng.integers(60)))
            emit(t, sess, cust_id, "page_view", "", device, src)
            extra = browse_products(ci, int(rng.integers(0, 4)))
            for pid in extra + pids:
                t += timedelta(seconds=int(rng.integers(15, 180)))
                emit(t, sess, cust_id, "product_view", pid, device, src)
            for pid in pids:
                t += timedelta(seconds=int(rng.integers(5, 60)))
                emit(t, sess, cust_id, "add_to_cart", pid, device, src)
            if extra and rng.random() < 0.2:
                t += timedelta(seconds=int(rng.integers(5, 40)))
                emit(t, sess, cust_id, "add_to_cart", extra[0], device, src)
                t += timedelta(seconds=int(rng.integers(5, 40)))
                emit(t, sess, cust_id, "remove_from_cart", extra[0], device, src)
            t = max(t + timedelta(seconds=20), ts - timedelta(seconds=45))
            emit(t, sess, cust_id, "checkout", "", device, src)
            emit(ts, sess, cust_id, "purchase", "", device, src, order_id)

        # 2) browsing sessions (no purchase) - funnel drop-offs
        n_orders = np.zeros(self.n_customers)
        for ci, *_ in self.order_sessions:
            n_orders[ci] += 1
        for ci, c in enumerate(self.cust):
            s = max(c["signup"], 0)
            u = min(c["until"], N_DAYS - 1)
            if u < s:
                continue
            window = u - s + 1
            expected = c["browse"] * max(n_orders[ci], 0.6) + window / 120.0
            k = int(rng.poisson(expected))
            # some churned customers still window-shop after leaving
            late = int(rng.poisson(1.2)) if (c["until"] < N_DAYS - 1 and rng.random() < 0.25) else 0
            if k + late == 0:
                continue
            w = self.day_mult[s:u + 1]
            days = list(rng.choice(np.arange(s, u + 1), size=k, p=w / w.sum())) if k else []
            if late:
                lo = min(c["until"] + 1, N_DAYS - 1)
                days += list(rng.integers(lo, N_DAYS, size=late))
            cust_id = self.customers[ci]["customer_id"]
            for di in days:
                sid += 1
                sess = f"S{sid:08d}"
                d = self.days[int(di)]
                device = c["device"] if rng.random() < 0.7 else DEVICES[int(rng.integers(3))]
                src = pick_source()
                t = datetime(d.year, d.month, d.day, int(rng.choice(24, p=HOUR_P)),
                             int(rng.integers(60)), int(rng.integers(60)))
                emit(t, sess, cust_id, "page_view", "", device, src)
                bounce = {"Paid Search": 0.42, "Social Media": 0.48}.get(src, 0.32)
                if device == "Mobile Web":
                    bounce += 0.08
                if rng.random() < bounce:
                    continue
                viewed = browse_products(ci, 1 + int(rng.poisson(2.2)))
                for pid in viewed:
                    t += timedelta(seconds=int(rng.integers(15, 200)))
                    emit(t, sess, cust_id, "product_view", pid, device, src)
                if rng.random() < 0.12:
                    t += timedelta(seconds=int(rng.integers(5, 30)))
                    emit(t, sess, cust_id, "add_to_wishlist", viewed[0], device, src)
                if rng.random() < 0.3:
                    t += timedelta(seconds=int(rng.integers(5, 60)))
                    emit(t, sess, cust_id, "add_to_cart", viewed[-1], device, src)
                    if rng.random() < 0.35:
                        t += timedelta(seconds=int(rng.integers(20, 120)))
                        emit(t, sess, cust_id, "checkout", "", device, src)
                    elif rng.random() < 0.25:
                        t += timedelta(seconds=int(rng.integers(5, 60)))
                        emit(t, sess, cust_id, "remove_from_cart", viewed[-1], device, src)
        self.events_by_month = events_by_month
        self._log(f"sessions: {sid}  events: {eid}")
        del pid_index

    # ------------------------------------------------------------ dirty data
    def inject_noise(self):
        rng = self.rng
        # customers: missing / badly formatted cities, duplicates
        for c in self.customers:
            r = rng.random()
            if r < 0.01:
                c["city"] = ""
            elif r < 0.03:
                c["city"] = f"  {c['city'].lower()} "
        dup = [dict(self.customers[i]) for i in rng.choice(len(self.customers), size=len(self.customers) // 300)]
        self.customers.extend(dup)
        # orders: payment method casing, duplicate rows
        for o in self.orders:
            if rng.random() < 0.02:
                o[4] = o[4].lower() if rng.random() < 0.5 else f" {o[4].upper()} "
        dup = [list(self.orders[i]) for i in rng.choice(len(self.orders), size=len(self.orders) // 250)]
        self.orders.extend(dup)
        # order items: invalid quantities
        for it in self.items:
            if rng.random() < 0.0015:
                it[3] = int(rng.choice([0, -1]))
        # events: upper-case event types, missing product ids, duplicates
        for evs in self.events_by_month.values():
            n = len(evs)
            for j in rng.choice(n, size=max(1, n // 400), replace=False):
                evs[j]["event_type"] = evs[j]["event_type"].upper()
            for j in rng.choice(n, size=max(1, n // 600), replace=False):
                if evs[j]["event_type"] == "product_view":
                    evs[j]["product_id"] = None
            evs.extend(dict(evs[j]) for j in rng.choice(n, size=max(1, n // 500), replace=False))
        self._log("injected dirty records (duplicates, casing, nulls, invalid quantities)")

    # ------------------------------------------------------------ writers
    def write(self):
        out = self.out
        (out / "events").mkdir(parents=True, exist_ok=True)
        for f in (out / "events").glob("*.jsonl"):
            f.unlink()

        with open(out / "customers.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(self.customers[0].keys()))
            w.writeheader()
            w.writerows(self.customers)

        with open(out / "products.jsonl", "w", encoding="utf-8") as f:
            for p in self.products:
                f.write(json.dumps({k: v for k, v in p.items() if not k.startswith("_")}) + "\n")

        with open(out / "orders.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["order_id", "customer_id", "order_ts", "status", "payment_method",
                        "device", "coupon_code", "shipping_fee"])
            w.writerows(self.orders)

        with open(out / "order_items.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["order_id", "line_no", "product_id", "quantity", "list_price",
                        "unit_price", "discount_pct", "rating"])
            w.writerows(self.items)

        n_ev = 0
        for month, evs in sorted(self.events_by_month.items()):
            with open(out / "events" / f"events_{month}.jsonl", "w", encoding="utf-8") as f:
                for e in evs:
                    f.write(json.dumps(e, separators=(",", ":")) + "\n")
            n_ev += len(evs)

        meta = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "scale": self.scale, "start": str(START), "end": str(END),
            "customers": self.n_customers, "products": len(self.products),
            "orders": len(self.orders), "order_items": len(self.items), "events": n_ev,
            "planted_anomalies": [
                {"date": str(START + timedelta(days=o)), "reason": r} for o, _, r in PLANTED_ANOMALIES
            ],
        }
        with open(out / "_metadata.json", "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)
        size = sum(p.stat().st_size for p in out.rglob("*") if p.is_file()) / 1e6
        self._log(f"wrote raw data to {out}  ({size:.1f} MB)")
        return meta

    def run(self):
        t0 = time.time()
        self.build_calendar()
        self.build_products()
        self.build_customers()
        self.build_orders()
        self.build_events()
        self.inject_noise()
        meta = self.write()
        self._log(f"done in {time.time() - t0:.1f}s")
        return meta


def generate(scale=None, seed=42, out_dir=None):
    return Generator(scale or config.DATA_SCALE, seed, out_dir).run()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate the ShopSense synthetic dataset")
    ap.add_argument("--scale", default=config.DATA_SCALE, choices=list(SCALES))
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=None, help="output folder (default data/raw)")
    a = ap.parse_args()
    generate(a.scale, a.seed, a.out)
