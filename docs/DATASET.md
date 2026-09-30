# Dataset

ShopSense ships with a **synthetic data generator** (`shopsense/datagen/`) instead of a downloaded dataset:

- No licence or privacy problems: no real customer is involved.
- One command reproduces the exact same data (fixed random seed 42).
- The data has *known* ground truth (personas, bundles, planted anomalies), so the models can be validated.
- Three sizes, so it runs on any laptop.

```bash
python -m shopsense.datagen.generator --scale medium      # or small / large, --seed 7, --out folder
```

| Scale | Customers | Products | Orders | Click-stream events | Raw size | Generation time |
|---|---|---|---|---|---|---|
| small | 2,500 | 350 | ≈19 k | ≈340 k | ≈80 MB | ≈20 s |
| **medium** (default) | 10,000 | 560 | 74 k | 1.35 M | 315 MB | ≈80 s |
| large | 25,000 | 840 | ≈187 k | ≈3.4 M | ≈790 MB | ≈3.5 min |

Period: **1 Jul 2024 → 30 Jun 2026** (730 days).

## Files (`data/raw/`)

### `customers.csv`
| Column | Type | Example |
|---|---|---|
| customer_id | string | `C000123` |
| first_name, last_name, email | string | `Kavya`, `Mehta`, `kavya.mehta512@example.com` |
| gender | Male / Female | |
| age | int | 18–68 |
| city, state, region, city_tier | string / int | `Pune`, `Maharashtra`, `West`, `1` |
| signup_date | date | `2025-03-14` |
| acquisition_channel | string | Organic Search, Paid Search, Social Media, Referral, Email, Direct |

### `products.jsonl`
`product_id`, `product_name`, `category` (8), `sub_category` (35), `brand` (fictional), `list_price`, `cost_price`, `launch_date`.

Categories: Electronics, Fashion, Home & Kitchen, Beauty & Personal Care, Books, Sports & Fitness, Grocery & Gourmet, Toys & Baby.

### `orders.csv`
| Column | Notes |
|---|---|
| order_id | `ORD0001234` |
| customer_id | FK → customers |
| order_ts | `yyyy-MM-dd HH:mm:ss`, IST |
| status | Delivered, Cancelled, Returned, Shipped, Processing |
| payment_method | UPI, Credit Card, Debit Card, Cash on Delivery, Net Banking, Wallet, EMI |
| device | Mobile App, Mobile Web, Desktop |
| coupon_code | empty if none |
| shipping_fee | ₹0 above ₹499, else ₹40 |

### `order_items.csv`
`order_id`, `line_no`, `product_id`, `quantity`, `list_price`, `unit_price` (after discount), `discount_pct`, `rating` (1–5, only for some delivered items).

### `events/events_YYYY-MM.jsonl`: click-stream (24 monthly files)
```json
{"event_id":"E000000635","session_id":"S00000075","customer_id":"C000019","event_time":"2025-01-07 19:13:53",
 "event_type":"product_view","product_id":"P00062","device":"Desktop","traffic_source":"Direct","order_id":""}
```
`event_type` ∈ page_view, product_view, add_to_cart, remove_from_cart, add_to_wishlist, checkout, purchase.

### `_metadata.json`
Generation parameters, row counts and the **planted anomalies** (used to validate the anomaly detector).

## How the behaviour is simulated

| Mechanism | Detail |
|---|---|
| Personas | loyal (10%), regular (25%), bargain hunter (20%), occasional (25%), one-time (12%), browser (8%), each with its own order rate, basket size, coupon probability, premium affinity and lifetime |
| Customer lifetime / churn | exponential lifetime per persona; **engagement drops by 55% in the 60 days before churn**; some churned customers keep window-shopping |
| Growth | 45% existing customers plus a steady stream of sign-ups (about 2.2× revenue growth over two years) |
| Seasonality | weekends +18%, salary days (1st–5th) +10%, 9 Indian sale events (Republic Day, Holi, Summer, End-of-Season, Independence Day, Big Festive Sale, Diwali, Black Friday, Year-End) with deeper discounts |
| Time of day | evening peak (19:00–22:00), quiet after midnight |
| Category preference | Dirichlet preferences per customer with gender effects; cheaper sub-categories bought more often |
| Bundles | 80% of products have a *companion* product bought with them 38% of the time → FP-Growth rules |
| Payments | COD more common in tier-3 cities and for bargain hunters (and cancelled more often); EMI for high-value baskets |
| Returns | category- and quality-dependent (Fashion highest) |
| Anomalies | payment-gateway outage, viral flash sale, website downtime, bulk corporate order |
| Dirty data | duplicate rows, lower/upper-case variants, blank cities, `product_view` events without product, zero/negative quantities |

## Using a real dataset instead

The pipeline only needs the five files above with the same column names. To use a real export (for example the *UCI Online Retail II* dataset or a Kaggle e-commerce event log), convert it to this schema, put it in `data/raw/` and run `python run_pipeline.py` **without** `--generate`.
