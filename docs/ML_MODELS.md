# Machine-Learning Models

All models are trained with **Apache Spark MLlib** inside `run_pipeline.py`. Their metadata and metrics go to the `model_registry` collection, and their predictions are stored on the customer and product documents.
Numbers below come from the default *medium* dataset (seed 42).

| # | Task | Module | Algorithm | Headline result |
|---|---|---|---|---|
| 1 | Customer segmentation | `ml/segmentation.py` | K-Means (+ PCA) | k = 5, silhouette 0.375 |
| 2 | Churn prediction | `ml/churn.py` | Logistic Regression · Random Forest · GBT | AUC 0.955 (Random Forest) |
| 3 | Recommendations | `ml/recommender.py` | ALS (implicit feedback) | HitRate@10 0.180 (1.8× popularity) |
| 4 | Market basket | `ml/basket.py` | FP-Growth | 281 rules, max lift 135 |
| 5 | Sales forecasting | `ml/forecast.py` | LinearRegression + RandomForestRegressor | daily MAPE 20.8%, weekly 6.8% |
| 6 | Anomaly detection | `ml/forecast.py` | Robust z-score (MAD) on model residuals, IQR | 4/4 planted events, 0 false alarms |

---

## 1. Customer segmentation: RFM + K-Means

**Features** (per purchasing customer, as of the last date in the data): recency (days since last order), frequency (successful orders), monetary (total spend), average order value, tenure (days since first order), category diversity, average discount %.
Recency, frequency, monetary and AOV are `log1p`-transformed (they are heavily skewed). All features are standardised: z = (x − μ) / σ.

**K-Means** minimises the within-cluster sum of squares

  J = Σₖ Σ_{x∈Cₖ} ‖x − μₖ‖²

using Spark's scalable k-means|| initialisation. k is chosen by the **silhouette coefficient** s(i) = (b(i) − a(i)) / max(a(i), b(i)), where *a* is the mean intra-cluster distance and *b* the mean distance to the nearest other cluster.

| k | 3 | 4 | **5** | 6 | 7 | 8 |
|---|---|---|---|---|---|---|
| silhouette | 0.372 | 0.354 | **0.375** | 0.355 | 0.362 | 0.353 |

**Naming**: clusters are named automatically from their centroids with business rules (recency ≤ 90 days = active; 90–240 = cooling; > 240 = dormant; ranked by frequency and monetary z-scores):

| Segment | Customers | Avg recency | Avg orders | Avg spend | Revenue share |
|---|---|---|---|---|---|
| Champions | 1,394 | 45 d | 30.1 | ₹2.36 L | **71.6%** |
| Can't Lose Them | 1,541 | 360 d | 7.9 | ₹45,339 | 15.2% |
| Loyal Customers | 1,406 | 38 d | 6.4 | ₹39,102 | 12.0% |
| Lost | 1,223 | 300 d | 1.7 | ₹3,178 | 0.9% |
| Hibernating | 473 | 344 d | 1.9 | ₹3,863 | 0.4% |
| Prospects (never bought) | 3,963 | – | 0 | – | – |

Also computed: classic **RFM quintile scores** (1–5 each, via `ntile` window functions) and a 2-D **PCA** projection (69% of variance: 48.9% + 19.8%) for the cluster map.

---

## 2. Churn prediction

**Definition**: a customer has *churned* if they place **no successful order in the next 90 days**.

**Leak-free, time-based design**

```
|<──────── observation window ────────>| cutoff |<── 90-day label window ──>| today
   features use ONLY data before cutoff            label = no order here?
```

**21 features**: recency, frequency, monetary, AOV, tenure, orders in the last 90 days, orders in the previous 90 days, spend in the last 90 days, sessions in the last 30 / 90 days, days since last visit, cart-abandonment rate, average discount, return rate, cancellation rate, category diversity, COD share, age, city tier, preferred device and acquisition channel (one-hot encoded).

**Models** (80 / 20 random split, seed 42):

| Model | Setup | AUC | PR-AUC | Accuracy | Precision | Recall | F1 |
|---|---|---|---|---|---|---|---|
| Logistic Regression | standardised, 3-fold CV over λ ∈ {0.001, 0.01, 0.1} → 0.01 | 0.953 | 0.971 | 88.0% | 91.0% | 87.7% | 0.893 |
| **Random Forest** | 150 trees, depth 8, √features | **0.955** | **0.972** | **88.5%** | **93.5%** | 85.9% | **0.896** |
| Gradient-Boosted Trees | 50 iterations, depth 5, step 0.1 | 0.951 | 0.969 | 87.8% | 93.3% | 84.8% | 0.888 |

Logistic model: p = σ(β₀ + Σ βᵢ zᵢ) with σ(t) = 1 / (1 + e⁻ᵗ).
Metrics: precision = TP / (TP + FP), recall = TP / (TP + FN), F1 = 2PR / (P + R), AUC = area under the ROC curve (TPR vs FPR).

**Top drivers** (Random Forest importance): days since last visit (20.5%), sessions in the last 30 days (19.6%), sessions in the last 90 days (14.2%), days since last order (9.5%), spend in the last 90 days (8.9%). Click-stream engagement predicts churn better than purchase history alone.

**Scoring**: the best model (Random Forest) re-scores every customer with features as of today. Risk bands: High ≥ 0.7, Medium 0.4–0.7, Low < 0.4 → 2,956 / 607 / 2,474 customers.

**Customer value**: predicted 12-month value = (monetary / tenure in months) × 12 × (1 − p_churn).
**Revenue at risk**: annual spend run-rate of high-risk customers = ₹8.37 Cr.

**Explainability**: the logistic model's coefficients, scaler mean / std and category encodings are exported to MongoDB. The API computes per-feature contributions βᵢ · zᵢ (log-odds) for the *what-if simulator* and the Customer 360 "why this risk level?" panel, without starting Spark.

---

## 3. Recommendations: implicit-feedback ALS

**Interaction strength** per (customer, product), capped at 30:

  r = 5 × purchases + 2 × add-to-cart + 1.5 × wishlist + 0.5 × views

**ALS for implicit feedback** (Hu, Koren & Volinsky, 2008) minimises

  Σ_{u,i} c_{ui} (p_{ui} − xᵤᵀ yᵢ)² + λ (Σ‖xᵤ‖² + Σ‖yᵢ‖²),  with p = 1[r > 0] and confidence c = 1 + α·r

alternating closed-form least-squares updates for user factors x and item factors y (distributed by Spark).

**Evaluation (leave-last-purchase-out)**: for 4,383 customers with at least 3 purchased products, the most recent purchase is hidden and the model is trained on everything else. Items the customer already bought are excluded from the Top-10.

| Model | rank | λ | α | HitRate@10 | NDCG@10 |
|---|---|---|---|---|---|
| Popularity baseline | – | – | – | 0.100 | 0.051 |
| ALS | 16 | 0.05 | 15 | 0.114 | 0.053 |
| ALS | 32 | 1.0 | 5 | 0.169 | 0.087 |
| **ALS (chosen)** | **64** | **1.0** | **5** | **0.180** | **0.093** |
| ALS | 64 | 1.0 | 10 | 0.178 | 0.091 |

Strong regularisation with low confidence scaling works best on this sparse data (415,927 interactions, 7,419 users × 560 items). The final model is re-trained on all data.
**Similar products** come from the cosine similarity of the item-factor vectors. Cold-start customers get the "trending best-sellers" list.

---

## 4. Market-basket analysis: FP-Growth

FP-Growth (Han et al., 2000) builds a compressed FP-tree and mines frequent itemsets without candidate generation. Spark runs the parallel version (PFP).

- support(A→B) = P(A ∪ B)
- confidence(A→B) = P(B | A) = support(A ∪ B) / support(A)
- lift(A→B) = confidence / P(B); lift > 1 means a positive association

| Level | minSupport | minConfidence | Frequent itemsets | Rules (lift > 1) |
|---|---|---|---|---|
| Product | 0.0008 | 0.05 | 581 | 181 |
| Sub-category | 0.002 | 0.05 | 238 | 100 |

Baskets: 70,856 (57.6% contain 2+ products). Top sub-category rules: Cookware ↔ Kitchen Appliances (lift 10.5, confidence 54%), Fiction ↔ Non-Fiction (8.5), Toys ↔ Baby Care (7.9). Product-level rules reach 88% confidence.

---

## 5. Sales forecasting: hybrid trend + seasonality

For each series (total revenue and each of the 8 categories):

  log(1 + yₜ) = trend(t) + season(calendarₜ) + εₜ

- **trend**: Spark `LinearRegression` on the day index t
- **season**: Spark `RandomForestRegressor` (120 trees, depth 7) on the residuals, with day-of-week, month, day-of-month, weekend, salary day (1st–5th), days to month-end and the **planned marketing calendar** (sale flag, day of sale)
- **bias correction**: Duan's smearing factor s = mean(exp(residuals)) removes the downward bias of back-transforming log predictions
- **intervals**: ŷ · exp(± z · σ), with σ the residual standard deviation (z = 1.28 for 80%, 1.96 for 95%)

**Evaluation**: the last 60 days are held out.

| Model | Daily MAPE | Weekly MAPE | MAE / day | R² |
|---|---|---|---|---|
| **Hybrid LR-trend + RF-seasonality** | **20.8%** | 6.8% | **₹1.82 L** | **−0.06** |
| Calendar Linear Regression | 22.7% | 7.6% | ₹1.96 L | −0.13 |
| Seasonal naïve (repeat last week) | 24.7% | 6.3% | ₹2.19 L | −0.45 |
| 28-day moving average | 21.9% | 14.4% | ₹2.08 L | −0.37 |

Daily revenue is noisy because a handful of high-value orders (laptops, phones) can double a day's revenue. Day-level R² is therefore close to 0 for every model, while weekly totals are forecast within about 7%. The hybrid model has the lowest daily error and is the only one that knows about upcoming sale events.

---

## 6. Anomaly detection

1. Fit the same hybrid model on **daily order counts** (less noisy than revenue).
2. Residual rₜ = log(1 + actual) − log(1 + expected). Because the expectation already includes weekends and sale events, a Diwali spike is *not* flagged.
3. Robust z-score: z = (r − median) / (1.4826 · MAD). Flag |z| ≥ 4 (critical ≥ 7).

The generator plants four events. All are recovered and no other day is flagged:

| Date | Planted event | z | Detected |
|---|---|---|---|
| 18 Nov 2024 | Payment gateway outage | −7.2 | ✅ |
| 28 Apr 2025 | Viral influencer flash sale | +7.8 | ✅ |
| 29 Sep 2025 | Website downtime | −6.6 | ✅ |
| 03 Mar 2026 | Bulk corporate gifting orders | +6.6 | ✅ |

Order-level checks (Spark): orders above the **Q3 + 3·IQR** fence, and a **return-abuse watch-list** (≥ 3 returns and a return rate ≥ 40%).

---

## References

1. M. Zaharia et al., "Apache Spark: A unified engine for big data processing," *Communications of the ACM*, 59(11), 2016.
2. X. Meng et al., "MLlib: Machine Learning in Apache Spark," *JMLR*, 17(34), 2016.
3. Y. Hu, Y. Koren, C. Volinsky, "Collaborative Filtering for Implicit Feedback Datasets," *IEEE ICDM*, 2008.
4. J. Han, J. Pei, Y. Yin, "Mining Frequent Patterns without Candidate Generation," *ACM SIGMOD*, 2000.
5. L. Breiman, "Random Forests," *Machine Learning*, 45(1), 2001.
6. J. H. Friedman, "Greedy Function Approximation: A Gradient Boosting Machine," *Annals of Statistics*, 29(5), 2001.
7. P. J. Rousseeuw, "Silhouettes: a graphical aid to the interpretation and validation of cluster analysis," *J. Computational and Applied Mathematics*, 20, 1987.
8. N. Duan, "Smearing Estimate: A Nonparametric Retransformation Method," *JASA*, 78(383), 1983.
9. C. Leys et al., "Detecting outliers: Do not use standard deviation around the mean, use absolute deviation around the median," *J. Experimental Social Psychology*, 49(4), 2013.
