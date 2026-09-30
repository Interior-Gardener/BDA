/* ShopSense dashboard — machine-learning, Customer 360 and pipeline pages */
'use strict';

/* =================================================================== SEGMENTATION */
route('segments', {
  title: 'Customer Segmentation', icon: 'segments', tag: 'ML', sub: 'RFM features + K-Means clustering (Spark MLlib) with silhouette-based model selection',
  async render(view, args, alive) {
    const d = await api('/api/segments');
    if (!alive()) return;
    const segs = d.segments, m = d.model, S = series();
    let current = args[0] || segs[0].segment;
    view.innerHTML = `
    <div class="callout fade-in">${icon('info')}<div>K-Means grouped <b>${fmt.num(m.train_rows)}</b> purchasing customers into <b>k = ${m.k}</b> clusters
      (silhouette <b>${m.silhouette}</b>) using ${m.features.length} standardised features. Clusters are named automatically from their centroids
      (recency, frequency, tenure); browsers who never purchased form the <b>Prospects</b> group.</div></div>
    <div class="grid g4" id="seg-cards">${segs.map((s) => `
      <div class="seg-card fade-in ${s.segment === current ? 'on' : ''}" data-s="${esc(s.segment)}" style="--c:${segColor(s.segment)}">
        <h4>${esc(s.segment)}</h4>
        <div class="big">${fmt.num(s.customers)}</div>
        <div class="meta">${s.share_pct !== null ? `${fmt.pct(s.share_pct)} of buyers · ` : ''}${fmt.pct(s.revenue_share_pct)} of revenue</div>
        ${s.avg_recency_days !== undefined ? `<div class="meta">R ${fmt.num(s.avg_recency_days)}d · F ${fmt.num(s.avg_frequency, 1)} · M ${fmt.inr(s.avg_monetary)}</div>` : ''}
        <div class="act">${esc(s.action)}</div>
      </div>`).join('')}</div>
    <div class="grid g-2-1">
      ${card('Cluster map (PCA projection)', `2-D projection of the ${m.features.length}-D feature space · explained variance ${((m.pca_explained_variance || []).reduce((a, b) => a + b, 0) * 100).toFixed(0)}%`, '<div class="chart xl" id="c-pca"></div>')}
      ${card('Choosing k', 'Silhouette score for k = 3…8 (higher is better)', '<div class="chart" id="c-sil"></div><div class="chart sm" id="c-share"></div>')}
    </div>
    ${card('Segment profile', 'Average feature values per cluster', `<div id="seg-table"></div>`)}
    ${card(`Customers in <span id="seg-name"></span>`, 'Top 20 by lifetime spend — click to open Customer 360', '<div id="seg-cust"></div>', { right: '<a class="btn" id="seg-export" href="#">Export at-risk list</a>' })}`;

    const segNames = [...new Set(d.points.map((p) => p.segment))];
    chart('#c-pca', {
      legend: { show: true, top: 0, textStyle: { color: css('--text-2') } },
      tooltip: { trigger: 'item', ...tooltipStyle(), formatter: (p) => `<b>${p.data.customer_id}</b> · ${esc(p.seriesName)}<br>${fmt.num(p.data.frequency)} orders · ${fmt.inr(p.data.monetary)}<br>last order ${p.data.recency_days} days ago` },
      grid: { left: 8, right: 8, top: 36, bottom: 8, containLabel: true },
      xAxis: { ...yAxis((v) => v.toFixed(1)), name: 'PC1', nameTextStyle: { color: css('--muted') } },
      yAxis: { ...yAxis((v) => v.toFixed(1)), name: 'PC2', nameTextStyle: { color: css('--muted') } },
      series: segNames.map((s) => ({ name: s, type: 'scatter', symbolSize: 6, itemStyle: { color: hexA(segColor(s), .7), borderColor: css('--surface'), borderWidth: .5 },
        data: d.points.filter((p) => p.segment === s).map((p) => ({ value: [p.x, p.y], ...p })) })),
    });
    const curve = m.silhouette_curve;
    chart('#c-sil', {
      tooltip: { trigger: 'axis', ...tooltipStyle() },
      xAxis: xAxis(curve.map((c) => `k=${c.k}`), { boundaryGap: false }), yAxis: yAxis((v) => v.toFixed(2), { min: 'dataMin' }),
      series: [lineSeries('Silhouette', curve.map((c) => ({ value: c.silhouette, symbolSize: c.k === m.k ? 14 : 8, itemStyle: { color: c.k === m.k ? S[1] : S[0] } })), S[0], { showSymbol: true, area: false,
        markPoint: { symbol: 'pin', symbolSize: 40, itemStyle: { color: S[1] }, label: { color: '#fff', fontSize: 10, formatter: 'best' }, data: [{ coord: [`k=${m.k}`, m.silhouette] }] } })],
    });
    const ss = segs.filter((s) => s.share_pct !== null);
    chart('#c-share', {
      legend: { show: true, top: 0, right: 0, textStyle: { color: css('--text-2') } },
      grid: { left: 8, right: 8, top: 30, bottom: 8, containLabel: true },
      tooltip: { trigger: 'axis', ...tooltipStyle(), valueFormatter: (v) => fmt.pct(v) },
      xAxis: xAxis(ss.map((s) => s.segment.split(' ')[0])), yAxis: yAxis((v) => `${v}%`),
      series: [barSeries('Customer share', ss.map((s) => s.share_pct), S[0]), barSeries('Revenue share', ss.map((s) => s.revenue_share_pct), S[1])],
    });
    $('#seg-table').innerHTML = table([
      { label: 'Segment', render: (s) => segBadge(s.segment) },
      { label: 'Customers', num: 1, render: (s) => fmt.num(s.customers) },
      { label: 'Recency (days)', num: 1, render: (s) => fmt.num(s.avg_recency_days) },
      { label: 'Orders', num: 1, render: (s) => fmt.num(s.avg_frequency, 1) },
      { label: 'Spend', num: 1, render: (s) => fmt.inr(s.avg_monetary) },
      { label: 'AOV', num: 1, render: (s) => fmt.inr(s.avg_aov) },
      { label: 'Tenure (days)', num: 1, render: (s) => fmt.num(s.avg_tenure_days) },
      { label: 'Categories', num: 1, render: (s) => fmt.num(s.avg_category_diversity, 1) },
      { label: 'Revenue', num: 1, render: (s) => `<b>${fmt.inr(s.revenue)}</b>` },
    ], ss);
    const loadCust = async (name) => {
      $('#seg-name').textContent = name;
      $('#seg-export').href = `/api/churn/customers.csv?risk=All&segment=${encodeURIComponent(name)}`;
      $$('.seg-card').forEach((c) => c.classList.toggle('on', c.dataset.s === name));
      const rows = await api(`/api/segments/${encodeURIComponent(name)}/customers?limit=20`);
      $('#seg-cust').innerHTML = customerTable(rows);
      bindRows($('#seg-cust'), rows, (c) => { location.hash = `#/customer/${c.customer_id}`; });
    };
    $('#seg-cards').addEventListener('click', (e) => { const c = e.target.closest('.seg-card'); if (c) { current = c.dataset.s; loadCust(current); } });
    loadCust(current);
  },
});

function customerTable(rows) {
  return table([
    { label: 'Customer', render: (c) => `<div class="strong">${esc(c.full_name)}</div><div class="sub">${c.customer_id} · ${esc(c.city)}</div>` },
    { label: 'Segment', render: (c) => segBadge(c.segment) },
    { label: 'Orders', num: 1, render: (c) => fmt.num(c.orders) },
    { label: 'Spend', num: 1, render: (c) => fmt.inr(c.total_spent) },
    { label: 'Last order', num: 1, render: (c) => (c.recency_days !== null && c.recency_days !== undefined ? `${c.recency_days}d ago` : '—') },
    { label: 'Churn risk', render: (c) => (c.churn_probability !== undefined && c.churn_probability !== null ? `${riskBadge(c.churn_risk)} <span class="sub">${(c.churn_probability * 100).toFixed(0)}%</span>` : '—') },
    { label: '12-m value', num: 1, render: (c) => `<b>${fmt.inr(c.clv_12m)}</b>` },
  ], rows, { onRow: 1 });
}

/* =================================================================== CHURN */
const SIM_RANGES = {
  recency_days: [0, 700, 1], frequency: [1, 60, 1], monetary: [100, 600000, 100], aov: [100, 60000, 100],
  tenure_days: [0, 1400, 1], orders_90d: [0, 20, 1], orders_prev_90d: [0, 20, 1], spend_90d: [0, 200000, 100],
  sessions_30d: [0, 30, 1], sessions_90d: [0, 60, 1], days_since_last_session: [0, 700, 1], cart_abandon_rate: [0, 1, 0.01],
  avg_discount_pct: [0, 60, 0.5], return_rate: [0, 1, 0.01], cancel_rate: [0, 1, 0.01], category_diversity: [1, 8, 1],
  cod_share: [0, 1, 0.01], age: [18, 70, 1], city_tier: [1, 3, 1],
};
const SIM_SHOWN = ['recency_days', 'days_since_last_session', 'sessions_30d', 'sessions_90d', 'orders_90d', 'orders_prev_90d',
  'frequency', 'monetary', 'cart_abandon_rate', 'return_rate', 'avg_discount_pct', 'tenure_days'];

route('churn', {
  title: 'Churn Prediction', icon: 'churn', tag: 'ML', sub: 'Who will stop buying in the next 90 days? — LR vs Random Forest vs GBT with a time-based split',
  async render(view, args, alive) {
    const s = await api('/api/churn/summary');
    if (!alive()) return;
    const best = s.models.find((m) => m.model === s.best_model), S = series();
    view.innerHTML = `
    <div class="grid g4">
      ${kpi({ label: 'Historical churn rate', value: fmt.pct(s.training_churn_rate), foot: `${fmt.num(s.training_customers)} customers at cutoff ${fmt.date(s.cutoff)}`, ic: 'churn' })}
      ${kpi({ label: 'Best model · ROC-AUC', value: best.auc.toFixed(3), foot: esc(s.best_model), ic: 'target' })}
      ${kpi({ label: 'High-risk customers', value: fmt.num(s.risk_distribution.High), foot: `${fmt.num(s.risk_distribution.Medium)} medium · ${fmt.num(s.risk_distribution.Low)} low`, ic: 'users' })}
      ${kpi({ label: 'Annual revenue at risk', value: fmt.inr(s.revenue_at_risk_12m), foot: 'run-rate of high-risk customers', ic: 'rupee' })}
    </div>
    <div class="callout">${icon('info')}<div><b>Leak-free design:</b> features use only data before <b>${fmt.date(s.cutoff)}</b>; the label is whether the customer bought again in the following
      <b>${s.label_window_days} days</b>. Models were evaluated on a held-out 20% test set; the winner then scored every customer as of ${fmt.date(s.as_of)}.</div></div>
    <div class="grid g2">
      ${card('Model comparison', 'Test-set metrics (threshold 0.5)', `<div id="m-table"></div>`)}
      ${card('ROC curves', 'True-positive vs false-positive rate', '<div class="chart" id="c-roc"></div>')}
    </div>
    <div class="grid g-2-1">
      ${card('What drives churn?', `Feature importance · ${esc(s.importance_model)}`, '<div class="chart lg" id="c-imp"></div>')}
      ${card('Confusion matrix', esc(s.best_model), '<div id="cm"></div>')}
    </div>
    <div class="grid g-1-2">
      ${card('What-if simulator', 'Move the sliders — the exported logistic model scores instantly (no Spark needed)', `
        <div class="chart sm" id="c-gauge"></div><div id="sim-drivers" style="margin-bottom:6px"></div>
        <div class="toolbar" style="margin:8px 0 12px"><select class="select" id="sim-dev"></select><select class="select" id="sim-chan"></select><div class="grow"></div><button class="btn" id="sim-reset">Reset to median customer</button></div>
        <div class="sim-grid" id="sim"></div>`)}
      <div class="grid" style="gap:20px">
        ${card('Risk distribution', 'Predicted churn probability — all scored customers', '<div class="chart sm" id="c-hist"></div>')}
        ${card('Churn risk by segment', 'Average predicted probability', '<div class="chart sm" id="c-bysef"></div>')}
      </div>
    </div>
    ${card('Win-back target list', 'Sorted by predicted 12-month value — export for the CRM / e-mail campaign',
      '<div id="risk-table"></div>', { right: `${segButtons('risk-f', [['High', 'High'], ['Medium', 'Medium'], ['Low', 'Low']], 'High')} <a class="btn primary" id="risk-csv" href="/api/churn/customers.csv?risk=High">${icon('download')}Export CSV</a>` })}`;

    $('#m-table').innerHTML = table([
      { label: 'Model', render: (m) => `<span class="strong">${esc(m.model)}</span> ${m.model === s.best_model ? '<span class="badge brand">best</span>' : ''}` },
      { label: 'AUC', num: 1, render: (m) => m.auc.toFixed(3) }, { label: 'PR-AUC', num: 1, render: (m) => m.auc_pr.toFixed(3) },
      { label: 'Accuracy', num: 1, render: (m) => fmt.pct(m.accuracy * 100) }, { label: 'Precision', num: 1, render: (m) => fmt.pct(m.precision * 100) },
      { label: 'Recall', num: 1, render: (m) => fmt.pct(m.recall * 100) }, { label: 'F1', num: 1, render: (m) => m.f1.toFixed(3) },
    ], s.models) + (s.lr_best_params ? `<p class="sub" style="margin:10px 4px 0">Logistic regression regParam tuned by 3-fold cross-validation → ${s.lr_best_params.regParam}</p>` : '');
    chart('#c-roc', {
      legend: { show: true, bottom: 0, textStyle: { color: css('--text-2') } },
      grid: { left: 8, right: 16, top: 12, bottom: 70, containLabel: true },
      tooltip: { trigger: 'item', ...tooltipStyle(), formatter: (p) => `${p.seriesName}<br>FPR ${p.value[0]} · TPR ${p.value[1]}` },
      xAxis: { ...yAxis((v) => v), name: 'False-positive rate', nameLocation: 'middle', nameGap: 26, max: 1, nameTextStyle: { color: css('--muted') } },
      yAxis: { ...yAxis((v) => v), name: 'TPR', max: 1, nameTextStyle: { color: css('--muted') } },
      series: [...s.models.map((m, i) => ({ name: `${m.model} (${m.auc.toFixed(3)})`, type: 'line', showSymbol: false, data: m.roc, lineStyle: { width: 2, color: S[i] }, itemStyle: { color: S[i] } })),
        { name: 'Random guess', type: 'line', data: [[0, 0], [1, 1]], showSymbol: false, lineStyle: { type: 'dashed', color: css('--axis'), width: 1 }, itemStyle: { color: css('--axis') } }],
    });
    const imp = s.feature_importance.slice(0, 12);
    chart('#c-imp', {
      tooltip: { trigger: 'axis', ...tooltipStyle(), valueFormatter: (v) => v.toFixed(3) },
      grid: { left: 8, right: 50, top: 4, bottom: 4, containLabel: true },
      xAxis: { type: 'value', show: false }, yAxis: { type: 'category', inverse: true, data: imp.map((f) => f.feature), axisLine: { show: false }, axisTick: { show: false }, axisLabel: { color: css('--text-2') } },
      series: [barSeries('Importance', imp.map((f) => f.importance), S[0], { horizontal: true, label: { show: true, position: 'right', color: css('--muted'), formatter: (p) => `${(p.value * 100).toFixed(1)}%` } })],
    });
    const cm = best.confusion, tot = cm.tp + cm.fp + cm.fn + cm.tn;
    const cmCell = (v, good) => `<div style="padding:22px 10px;border-radius:12px;text-align:center;background:${hexA(good ? css('--good') : css('--critical'), .12)}">
      <div style="font-size:26px;font-weight:800">${fmt.num(v)}</div><div class="sub">${fmt.pct(v / tot * 100)}</div></div>`;
    $('#cm').innerHTML = `<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;font-size:12px">
      <div></div><div class="sub" style="text-align:center">Predicted: stays</div><div class="sub" style="text-align:center">Predicted: churns</div>
      <div class="sub">Actual: stays</div>${cmCell(cm.tn, 1)}${cmCell(cm.fp, 0)}
      <div class="sub">Actual: churns</div>${cmCell(cm.fn, 0)}${cmCell(cm.tp, 1)}</div>
      <p class="sub" style="margin-top:12px">Precision ${fmt.pct(best.precision * 100)} — of customers flagged, this share really churned. Recall ${fmt.pct(best.recall * 100)} — share of churners caught.</p>`;
    const hist = s.probability_histogram;
    chart('#c-hist', {
      tooltip: { trigger: 'axis', ...tooltipStyle(), valueFormatter: (v) => `${fmt.num(v)} customers` },
      xAxis: xAxis(hist.map((_, i) => `${i * 10}–${i * 10 + 10}%`), { axisLabel: { fontSize: 10 } }), yAxis: yAxis(fmt.compact),
      series: [barSeries('Customers', hist.map((v, i) => ({ value: v, itemStyle: { color: i >= 7 ? css('--critical') : i >= 4 ? css('--warning') : css('--good'), borderRadius: [4, 4, 0, 0] } })), S[0], { barCategoryGap: '12%' })],
    });
    const bs = s.by_segment.filter((x) => x.segment);
    chart('#c-bysef', {
      tooltip: { trigger: 'axis', ...tooltipStyle(), valueFormatter: (v) => fmt.pct(v) },
      grid: { left: 8, right: 44, top: 4, bottom: 4, containLabel: true },
      xAxis: { type: 'value', show: false, max: 100 }, yAxis: { type: 'category', inverse: true, data: bs.map((x) => x.segment), axisLine: { show: false }, axisTick: { show: false }, axisLabel: { color: css('--text-2') } },
      series: [barSeries('Avg churn probability', bs.map((x) => ({ value: +(x.avg_probability * 100).toFixed(1), itemStyle: { color: segColor(x.segment), borderRadius: [0, 4, 4, 0] } })), S[0], { horizontal: true, label: { show: true, position: 'right', color: css('--muted'), formatter: (p) => `${p.value}%` } })],
    });

    // ---- simulator
    const sim = s.simulator, vals = { ...sim.defaults };
    vals.preferred_device = sim.categorical.preferred_device?.[0];
    vals.acquisition_channel = sim.categorical.acquisition_channel?.[0];
    $('#sim-dev').innerHTML = (sim.categorical.preferred_device || []).map((v) => `<option>${esc(v)}</option>`).join('');
    $('#sim-chan').innerHTML = (sim.categorical.acquisition_channel || []).map((v) => `<option>${esc(v)}</option>`).join('');
    const gauge = chart('#c-gauge', {
      series: [{
        type: 'gauge', startAngle: 205, endAngle: -25, min: 0, max: 100, radius: '100%', center: ['50%', '60%'],
        progress: { show: true, width: 14, roundCap: true }, axisLine: { roundCap: true, lineStyle: { width: 14, color: [[1, css('--surface-3')]] } },
        pointer: { show: false }, axisTick: { show: false }, splitLine: { show: false }, axisLabel: { show: false },
        detail: { valueAnimation: true, offsetCenter: [0, '0%'], fontSize: 30, fontWeight: 800, color: css('--text'), formatter: '{value}%' },
        title: { offsetCenter: [0, '38%'], color: css('--muted'), fontSize: 12 }, data: [{ value: 0, name: 'churn probability' }],
      }],
    });
    const drawSliders = () => {
      $('#sim').innerHTML = SIM_SHOWN.map((f) => {
        const [mn, mx, st] = SIM_RANGES[f];
        const v = Math.min(mx, Math.max(mn, vals[f] ?? mn));
        return `<div class="slider"><div class="top"><span>${esc(sim.labels[f] || f)}</span><b id="v-${f}">${fmtSim(f, v)}</b></div>
          <input type="range" min="${mn}" max="${mx}" step="${st}" value="${v}" data-f="${f}"></div>`;
      }).join('');
    };
    const fmtSim = (f, v) => (['monetary', 'aov', 'spend_90d'].includes(f) ? fmt.inr(+v) : f.endsWith('rate') || f === 'cod_share' ? fmt.pct(v * 100, 0) : f === 'avg_discount_pct' ? fmt.pct(+v) : fmt.num(+v));
    let tmr;
    const score = async () => {
      const r = await post('/api/churn/simulate', vals);
      const col = riskColor(r.risk);
      gauge.setOption({ series: [{ progress: { itemStyle: { color: col } }, data: [{ value: +(r.probability * 100).toFixed(1), name: `${r.risk} risk` }] }] });
      const maxAbs = Math.max(...r.drivers.map((x) => Math.abs(x.contribution)), 0.01);
      $('#sim-drivers').innerHTML = `<div class="sub" style="margin-bottom:4px">Top drivers (log-odds contribution)</div>` + r.drivers.slice(0, 6).map((x) => {
        const w = Math.abs(x.contribution) / maxAbs * 50;
        const pos = x.contribution > 0;
        return `<div class="driver"><span class="ellipsis" title="${esc(x.feature)}">${esc(x.feature)}</span><div class="track"><i style="${pos ? 'left:50%' : `right:50%`};width:${w}%;background:${pos ? css('--critical') : css('--good')}"></i></div>
          <span class="sub" style="text-align:right">${pos ? '+' : ''}${x.contribution.toFixed(2)}</span></div>`;
      }).join('');
    };
    drawSliders();
    $('#sim').addEventListener('input', (e) => {
      const f = e.target.dataset.f;
      if (!f) return;
      vals[f] = +e.target.value;
      $(`#v-${f}`).textContent = fmtSim(f, vals[f]);
      clearTimeout(tmr); tmr = setTimeout(score, 60);
    });
    $('#sim-dev').addEventListener('change', (e) => { vals.preferred_device = e.target.value; score(); });
    $('#sim-chan').addEventListener('change', (e) => { vals.acquisition_channel = e.target.value; score(); });
    $('#sim-reset').addEventListener('click', () => { Object.assign(vals, sim.defaults); drawSliders(); score(); });
    score();

    const loadRisk = async (risk) => {
      $('#risk-csv').href = `/api/churn/customers.csv?risk=${risk}`;
      const rows = await api(`/api/churn/customers?risk=${risk}&limit=25`);
      $('#risk-table').innerHTML = customerTable(rows);
      bindRows($('#risk-table'), rows, (c) => { location.hash = `#/customer/${c.customer_id}`; });
    };
    onSeg('risk-f', loadRisk);
    loadRisk('High');
  },
});

/* =================================================================== RECOMMENDATIONS */
route('recommendations', {
  title: 'Recommendations', icon: 'recs', tag: 'ML', sub: 'Personalised Top-10 products from implicit-feedback ALS collaborative filtering',
  async render(view, args, alive) {
    let cid = args[0];
    if (!cid) { const top = await api('/api/customers/search?q=&limit=1'); cid = top[0]?.customer_id; }
    const [r, c] = await Promise.all([api(`/api/recommendations/${cid}`), api(`/api/customers/${cid}`)]);
    if (!alive()) return;
    const m = r.model, S = series();
    view.innerHTML = `
    <div class="card fade-in"><div class="toolbar">
      <div class="profile" style="flex:1;min-width:260px"><div class="avatar">${initials(c.full_name)}</div>
        <div><h2>${esc(c.full_name)}</h2><div class="sub">${c.customer_id} · ${esc(c.city)} · ${fmt.num(c.orders)} orders · ${fmt.inr(c.total_spent)} spent</div>
        <div class="chips" style="margin-top:6px">${segBadge(c.segment)} ${c.favourite_category ? catBadge(c.favourite_category) : ''}</div></div></div>
      <input class="input" id="r-q" placeholder="Try another customer (name or ID)…" style="width:280px">
      <a class="btn" href="#/customer/${c.customer_id}">Open Customer 360</a>
    </div><div id="r-res" class="chips" style="margin-top:10px"></div></div>
    ${card('Recently purchased', 'What the model already knows about this customer', `<div class="chips">${(r.purchased || []).map((p) => `<span class="badge"><span class="sw" style="background:${catColor(p.category)}"></span>${esc(p.product_name)}</span>`).join('') || '<span class="sub">No purchases yet</span>'}</div>`)}
    ${card(`Top ${r.items.length} recommendations ${r.fallback ? '<span class="badge">popular fallback</span>' : '<span class="badge brand">ALS</span>'}`, 'Already-purchased products are excluded', `<div class="grid g5" id="r-cards" style="grid-template-columns:repeat(auto-fill,minmax(190px,1fr))"></div>`)}
    <div class="grid g2">
      ${card('Offline evaluation', `Leave-last-purchase-out on ${fmt.num(m.eval_users)} customers`, '<div class="chart" id="c-eval"></div>')}
      ${card('Hyper-parameter search', 'rank × regParam × alpha grid', `<div id="grid-t"></div><p class="sub" style="margin:10px 4px 0">${fmt.num(m.interactions)} customer–product interactions · ${fmt.num(m.users)} users · ${fmt.num(m.items)} items</p>`)}
    </div>`;
    const maxScore = Math.max(...r.items.map((x) => x.score || 0), 1e-6);
    $('#r-cards').innerHTML = r.items.map((p) => `<div class="pcard fade-in" style="cursor:pointer" data-p="${p.product_id}">
      <div class="thumb" style="background:linear-gradient(135deg, ${catColor(p.category)}, ${hexA(catColor(p.category), .55)})">#${p.rank}</div>
      <div class="name">${esc(p.product_name)}</div><div class="sub">${esc(p.sub_category)}</div>
      <div class="toolbar"><span class="price">${fmt.inr(p.list_price)}</span><div class="grow"></div>${p.score ? `<span class="sub">${(p.score / maxScore * 100).toFixed(0)}%</span>` : ''}</div>
      ${p.score ? `<div class="bar-track"><i style="width:${p.score / maxScore * 100}%;background:${catColor(p.category)}"></i></div>` : ''}
      <div class="sub" style="font-size:11.5px">${esc(p.reason)}</div></div>`).join('');
    $('#r-cards').addEventListener('click', (e) => { const el = e.target.closest('[data-p]'); if (el) productModal(el.dataset.p); });
    const mt = m.metrics;
    chart('#c-eval', {
      legend: { show: true, top: 0, right: 0, textStyle: { color: css('--text-2') } },
      grid: { left: 8, right: 8, top: 34, bottom: 8, containLabel: true },
      tooltip: { trigger: 'axis', ...tooltipStyle(), valueFormatter: (v) => fmt.pct(v, 2) },
      xAxis: xAxis(['HitRate@10', 'NDCG@10']), yAxis: yAxis((v) => `${v}%`),
      series: [barSeries('ALS (collaborative filtering)', [mt['hit_rate@10'] * 100, mt['ndcg@10'] * 100], S[0], { label: { show: true, position: 'top', color: css('--text-2'), formatter: (p) => `${p.value.toFixed(1)}%` } }),
        barSeries('Popularity baseline', [mt['popularity_hit_rate@10'] * 100, mt['popularity_ndcg@10'] * 100], S[1], { label: { show: true, position: 'top', color: css('--text-2'), formatter: (p) => `${p.value.toFixed(1)}%` } })],
    });
    $('#grid-t').innerHTML = table([
      { label: 'Rank', key: 'rank' }, { label: 'regParam', key: 'regParam' }, { label: 'alpha', key: 'alpha' },
      { label: 'HitRate@10', num: 1, render: (g) => fmt.pct(g.hit_rate * 100, 2) }, { label: 'NDCG@10', num: 1, render: (g) => fmt.pct(g.ndcg * 100, 2) },
      { label: '', render: (g) => (g.rank === m.params.rank && g.regParam === m.params.regParam && g.alpha === m.params.alpha ? '<span class="badge brand">chosen</span>' : '') },
    ], m.grid || []) + `<div class="callout" style="margin-top:12px">${icon('bolt')}<div>ALS finds the held-out purchase in the Top-10 <b>${mt.lift_vs_popularity}×</b> more often than recommending best-sellers.</div></div>`;
    let t;
    $('#r-q').addEventListener('input', (e) => {
      clearTimeout(t);
      t = setTimeout(async () => {
        const q = e.target.value.trim();
        if (!q) { $('#r-res').innerHTML = ''; return; }
        const rows = await api(`/api/customers/search?q=${encodeURIComponent(q)}&limit=6`, { cache: false });
        $('#r-res').innerHTML = rows.map((x) => `<a class="badge info" href="#/recommendations/${x.customer_id}">${esc(x.full_name)} · ${x.customer_id}</a>`).join('');
      }, 200);
    });
  },
});

/* =================================================================== MARKET BASKET */
const BASKET_STATE = { level: 'product', lift: 1, q: '' };
route('basket', {
  title: 'Market Basket Analysis', icon: 'basket', tag: 'ML', sub: 'FP-Growth association rules — which products and categories are bought together',
  async render(view, args, alive) {
    const [sub, prod] = await Promise.all([api('/api/basket/rules?level=sub_category&limit=150'), api('/api/basket/rules?level=product&limit=5')]);
    if (!alive()) return;
    const mm = prod.model.metrics;
    view.innerHTML = `
    <div class="grid g4">
      ${kpi({ label: 'Baskets analysed', value: fmt.num(mm.baskets), foot: `${fmt.pct(mm.multi_item_share_pct)} contain 2+ products`, ic: 'basket' })}
      ${kpi({ label: 'Product rules', value: fmt.num(mm.product_rules), foot: `${fmt.num(mm.product_frequent_itemsets)} frequent itemsets`, ic: 'products' })}
      ${kpi({ label: 'Category rules', value: fmt.num(mm.subcategory_rules), foot: `${fmt.num(mm.subcategory_frequent_itemsets)} frequent itemsets`, ic: 'segments' })}
      ${kpi({ label: 'Strongest lift', value: `${Math.max(...prod.rules.map((r) => r.lift)).toFixed(1)}×`, foot: 'vs random co-occurrence', ic: 'bolt' })}
    </div>
    <div class="callout">${icon('info')}<div><b>Support</b> = share of baskets with both items · <b>Confidence</b> = P(B | A) · <b>Lift</b> = confidence ÷ P(B); lift &gt; 1 means the items are bought together more than chance.</div></div>
    <div class="grid g-1-2">
      ${card('Sub-category association network', 'Edge width = lift · node size = number of rules', '<div class="chart xl" id="c-net"></div>')}
      ${card('Association rules', '', `<div class="toolbar" style="margin-bottom:10px">${segButtons('b-level', [['product', 'Products'], ['sub_category', 'Sub-categories']], BASKET_STATE.level)}
        <input class="input" id="b-q" placeholder="Filter…" style="width:170px"><span class="sub">min lift</span><input type="range" id="b-lift" min="1" max="20" step="0.5" value="${BASKET_STATE.lift}" style="width:120px"><b id="b-liftv">${BASKET_STATE.lift}</b></div>
        <div id="b-table" style="max-height:470px;overflow:auto"></div>`)}
    </div>`;
    const rules = sub.rules;
    const nodes = {}, S = series();
    rules.forEach((r) => [...r.antecedent, ...r.consequent].forEach((n) => { nodes[n] = (nodes[n] || 0) + 1; }));
    const catOf = {};
    (await api('/api/sales/categories')).subcategories.forEach((s) => { catOf[s.sub_category] = s.category; });
    const edges = rules.filter((r) => r.antecedent.length === 1).map((r) => ({ source: r.antecedent[0], target: r.consequent[0], value: r.lift, conf: r.confidence, lineStyle: { width: Math.min(1 + r.lift / 2, 7) } }));
    chart('#c-net', {
      tooltip: { trigger: 'item', ...tooltipStyle(), formatter: (p) => (p.dataType === 'edge' ? `${esc(p.data.source)} → ${esc(p.data.target)}<br>lift <b>${p.data.value.toFixed(2)}</b> · confidence ${fmt.pct(p.data.conf * 100)}` : `<b>${esc(p.name)}</b><br>${esc(catOf[p.name] || '')} · ${p.value} rules`) },
      series: [{
        type: 'graph', layout: 'force', roam: true, draggable: true, force: { repulsion: 260, edgeLength: [60, 150], gravity: 0.08 },
        label: { show: true, position: 'right', color: css('--text-2'), fontSize: 11 },
        lineStyle: { color: css('--axis'), opacity: .7, curveness: .15 }, edgeSymbol: ['none', 'arrow'], edgeSymbolSize: 6,
        emphasis: { focus: 'adjacency', lineStyle: { color: S[0], opacity: 1 } },
        data: Object.entries(nodes).map(([n, v]) => ({ name: n, value: v, symbolSize: 10 + Math.sqrt(v) * 6, itemStyle: { color: catColor(catOf[n]), borderColor: css('--surface'), borderWidth: 2 } })),
        links: edges,
      }],
    });
    const load = async () => {
      const r = await api(`/api/basket/rules?${qs({ level: BASKET_STATE.level, min_lift: BASKET_STATE.lift, q: BASKET_STATE.q, limit: 120 })}`);
      const isP = BASKET_STATE.level === 'product';
      const maxLift = Math.max(...r.rules.map((x) => x.lift), 1);
      $('#b-table').innerHTML = table([
        { label: 'If customer buys', render: (x) => `<div class="strong ellipsis">${esc((isP ? x.antecedent_names : x.antecedent).join(' + '))}</div>${isP ? `<div class="sub">${esc(x.antecedent_category)}</div>` : ''}` },
        { label: '→ also buys', render: (x) => `<div class="strong ellipsis">${esc((isP ? x.consequent_names : x.consequent).join(' + '))}</div>${isP ? `<div class="sub">${esc(x.consequent_category)}</div>` : ''}` },
        { label: 'Support', num: 1, render: (x) => fmt.pct(x.support * 100, 2) },
        { label: 'Confidence', num: 1, render: (x) => fmt.pct(x.confidence * 100) },
        { label: 'Lift', render: (x) => `<div style="display:flex;align-items:center;gap:8px;min-width:110px"><div class="bar-track" style="flex:1"><i style="width:${x.lift / maxLift * 100}%"></i></div><b>${x.lift.toFixed(1)}</b></div>` },
      ], r.rules, { empty: 'No rules match the filters' });
    };
    onSeg('b-level', (v) => { BASKET_STATE.level = v; load(); });
    $('#b-lift').addEventListener('input', (e) => { BASKET_STATE.lift = +e.target.value; $('#b-liftv').textContent = e.target.value; });
    $('#b-lift').addEventListener('change', load);
    let t;
    $('#b-q').addEventListener('input', (e) => { clearTimeout(t); t = setTimeout(() => { BASKET_STATE.q = e.target.value; load(); }, 250); });
    load();
  },
});

/* =================================================================== FORECAST & ANOMALIES */
route('forecast', {
  title: 'Forecasting & Anomalies', icon: 'forecast', tag: 'ML', sub: 'Hybrid trend + seasonality model with prediction intervals, and model-based anomaly detection',
  async render(view, args, alive) {
    const series_ = args[0] || 'All Categories';
    const [f, an] = await Promise.all([api(`/api/forecast?series=${encodeURIComponent(series_)}`), api('/api/anomalies')]);
    if (!alive()) return;
    const ev = f.evaluation[0], S = series();
    const color = series_ === 'All Categories' ? S[0] : catColor(series_);
    const daily = an.items.filter((a) => a.type === 'daily_revenue');
    const val = an.model.validation;
    view.innerHTML = `
    <div class="card fade-in"><div class="toolbar"><span class="sub">Series</span>
      <select class="select" id="f-series">${f.all_series.map((x) => `<option ${x.series === series_ ? 'selected' : ''}>${esc(x.series)}</option>`).join('')}</select>
      <div class="grow"></div><span class="sub">Model: ${esc(f.model.algorithm)}</span></div></div>
    <div class="grid g4">
      ${kpi({ label: 'Forecast · next 30 days', value: fmt.inr(f.next_30_days), foot: `${delta((f.next_30_days / f.last_30_days - 1) * 100)} vs last 30 days`, ic: 'forecast' })}
      ${kpi({ label: 'Daily MAPE (hold-out)', value: fmt.pct(ev.mape), foot: `last ${f.holdout.length} days held out`, ic: 'target' })}
      ${kpi({ label: 'Weekly MAPE', value: fmt.pct(ev.weekly_mape), foot: 'error on weekly totals', ic: 'percent' })}
      ${kpi({ label: 'Error vs seasonal naïve', value: fmt.pct((1 - ev.mape / (f.evaluation.find((x) => x.model.startsWith('Seasonal'))?.mape || ev.mape)) * 100) + ' lower', foot: `daily MAPE · MAE ${fmt.inr(ev.mae)}/day`, ic: 'bolt' })}
    </div>
    ${card(`Revenue forecast — ${esc(series_)}`, 'History, model fit and the next 60 days with 80% / 95% prediction intervals', '<div class="chart xl" id="c-fc"></div>')}
    <div class="grid g2">
      ${card('Model vs baselines', 'Hold-out window error (lower is better) · daily revenue is noisy because a few high-value baskets dominate some days, so weekly error is also shown', '<div id="f-eval"></div>')}
      ${card('Hold-out check', 'Actual vs predicted on unseen days', '<div class="chart" id="c-hold"></div>')}
    </div>
    ${card('Forecast by category', 'Next 30 days vs last 30 days', '<div id="f-cats"></div>')}
    <div class="grid g-2-1">
      ${card('Anomaly timeline', 'Daily revenue with detected anomalous days', '<div class="chart lg" id="c-anom"></div>')}
      ${card('Detector validation', 'Ground-truth events planted in the synthetic data', `<div id="f-val"></div>`)}
    </div>
    <div class="grid g2">
      ${card('Anomalous days', `Robust z-score ≥ ${an.model.params.z_threshold} on daily orders`, '<div id="f-anom"></div>')}
      ${card('Order-level alerts', 'Unusually large orders (IQR fence) & return-abuse watch-list', '<div id="f-alerts"></div>')}
    </div>`;
    $('#f-series').addEventListener('change', (e) => { location.hash = `#/forecast/${encodeURIComponent(e.target.value)}`; });

    const h = f.history, fc = f.forecast;
    const dates = [...h.map((x) => x.date), ...fc.map((x) => x.date)];
    const pad = (arr, before) => [...Array(before).fill(null), ...arr];
    const lo95 = pad(fc.map((x) => x.lo95), h.length), band95 = pad(fc.map((x) => x.hi95 - x.lo95), h.length);
    const lo80 = pad(fc.map((x) => x.lo80), h.length), band80 = pad(fc.map((x) => x.hi80 - x.lo80), h.length);
    chart('#c-fc', {
      legend: { show: true, top: 0, textStyle: { color: css('--text-2') }, data: ['Actual', 'Model fit', 'Forecast', '95% interval', '80% interval'] },
      grid: { left: 8, right: 18, top: 36, bottom: 50, containLabel: true },
      tooltip: { trigger: 'axis', ...tooltipStyle(), formatter: (ps) => { const i = ps[0].dataIndex; const x = fc[i - h.length]; const hh = h[i];
        return `<b>${fmt.date(dates[i])}</b><br>` + (hh ? `Actual ${fmt.inr(hh.actual)}${hh.fitted ? `<br>Model ${fmt.inr(hh.fitted)}` : ''}` : `Forecast <b>${fmt.inr(x.yhat)}</b><br>80%: ${fmt.inr(x.lo80)} – ${fmt.inr(x.hi80)}<br>95%: ${fmt.inr(x.lo95)} – ${fmt.inr(x.hi95)}`); } },
      xAxis: xAxis(dates, { boundaryGap: false }), yAxis: yAxis(fmt.inrAxis),
      dataZoom: [{ type: 'inside', start: 35 }, { type: 'slider', start: 35, height: 22, bottom: 6, borderColor: css('--border'), textStyle: { color: css('--muted') } }],
      series: [
        { name: '95% interval', type: 'line', data: lo95, stack: 'b95', lineStyle: { opacity: 0 }, showSymbol: false, itemStyle: { color: hexA(color, .12) } },
        { name: '95% interval', type: 'line', data: band95, stack: 'b95', lineStyle: { opacity: 0 }, showSymbol: false, areaStyle: { color: hexA(color, .12) }, itemStyle: { color: hexA(color, .12) } },
        { name: '80% interval', type: 'line', data: lo80, stack: 'b80', lineStyle: { opacity: 0 }, showSymbol: false, itemStyle: { color: hexA(color, .22) } },
        { name: '80% interval', type: 'line', data: band80, stack: 'b80', lineStyle: { opacity: 0 }, showSymbol: false, areaStyle: { color: hexA(color, .2) }, itemStyle: { color: hexA(color, .22) } },
        { name: 'Actual', type: 'line', data: h.map((x) => x.actual), showSymbol: false, lineStyle: { width: 1.5, color: css('--text-2') }, itemStyle: { color: css('--text-2') } },
        ...(h[0].fitted !== undefined ? [{ name: 'Model fit', type: 'line', data: h.map((x) => x.fitted), showSymbol: false, lineStyle: { width: 2, color: S[1] }, itemStyle: { color: S[1] } }] : []),
        { name: 'Forecast', type: 'line', data: pad(fc.map((x) => x.yhat), h.length), showSymbol: false, lineStyle: { width: 2.5, color }, itemStyle: { color } },
      ],
    });
    $('#f-eval').innerHTML = table([
      { label: 'Model', render: (x) => `<span class="strong">${esc(x.model)}</span>` },
      { label: 'MAPE', num: 1, render: (x) => fmt.pct(x.mape) }, { label: 'Weekly MAPE', num: 1, render: (x) => fmt.pct(x.weekly_mape) },
      { label: 'MAE', num: 1, render: (x) => fmt.inr(x.mae) }, { label: 'R²', num: 1, render: (x) => x.r2.toFixed(3) },
    ], f.evaluation);
    chart('#c-hold', {
      legend: { show: true, top: 0, right: 0, textStyle: { color: css('--text-2') } },
      grid: { left: 8, right: 8, top: 34, bottom: 8, containLabel: true },
      tooltip: { trigger: 'axis', ...tooltipStyle(), valueFormatter: (v) => fmt.inr(v) },
      xAxis: xAxis(f.holdout.map((x) => x.date.slice(5)), { boundaryGap: false }), yAxis: yAxis(fmt.inrAxis),
      series: [lineSeries('Actual', f.holdout.map((x) => x.actual), css('--text-2'), { area: false, lineStyle: { width: 1.5, color: css('--text-2') } }),
        lineSeries('Predicted', f.holdout.map((x) => x.predicted), color, { area: false })],
    });
    $('#f-cats').innerHTML = table([
      { label: 'Series', render: (x) => (x.series === 'All Categories' ? '<b>All Categories</b>' : catBadge(x.series)) },
      { label: 'Last 30 days', num: 1, render: (x) => fmt.inr(x.last_30_days) },
      { label: 'Next 30 days (forecast)', num: 1, render: (x) => `<b>${fmt.inr(x.next_30_days)}</b>` },
      { label: 'Change', num: 1, render: (x) => delta((x.next_30_days / x.last_30_days - 1) * 100) },
      { label: 'Hold-out MAPE', num: 1, render: (x) => fmt.pct(x.mape) },
    ], f.all_series, { onRow: 1 });
    bindRows($('#f-cats'), f.all_series, (x) => { location.hash = `#/forecast/${encodeURIComponent(x.series)}`; });

    const all = await api('/api/forecast?series=All%20Categories&history_days=800');
    if (!alive()) return;
    const ah = all.history;
    const anomSet = Object.fromEntries(daily.map((a) => [a.date, a]));
    chart('#c-anom', {
      tooltip: { trigger: 'axis', ...tooltipStyle(), formatter: (ps) => { const d = ah[ps[0].dataIndex]; const a = anomSet[d.date];
        return `<b>${fmt.date(d.date)}</b><br>Revenue ${fmt.inr(d.actual)}<br>Expected ${fmt.inr(d.fitted)}` + (a ? `<br><b style="color:${css('--critical')}">Anomaly · z = ${a.z_score}</b>` : ''); } },
      grid: { left: 8, right: 18, top: 16, bottom: 44, containLabel: true },
      xAxis: xAxis(ah.map((x) => x.date), { boundaryGap: false }), yAxis: yAxis(fmt.inrAxis),
      dataZoom: [{ type: 'inside' }, { type: 'slider', height: 20, bottom: 4, borderColor: css('--border'), textStyle: { color: css('--muted') } }],
      series: [lineSeries('Revenue', ah.map((x) => x.actual), S[0], { lineStyle: { width: 1.2, color: S[0] } }),
        { name: 'Anomaly', type: 'scatter', symbolSize: 13, data: ah.map((x) => (anomSet[x.date] ? x.actual : null)),
          itemStyle: { color: (p) => (anomSet[ah[p.dataIndex].date]?.direction === 'drop' ? css('--critical') : css('--good')), borderColor: css('--surface'), borderWidth: 2 } }],
    });
    $('#f-val').innerHTML = val ? `<div class="gauge-num">${val.detected}/${val.planted}</div><div class="sub" style="margin-bottom:12px">planted events recovered · ${val.flagged_days} days flagged in total</div>` +
      val.details.map((v) => `<div class="alert-row"><div class="alert-ic ${v.detected ? 'spike' : 'drop'}">${icon(v.detected ? 'check' : 'info')}</div>
        <div><div class="strong">${esc(v.reason)}</div><div class="sub">${fmt.date(v.date)} · ${v.detected ? 'detected' : 'missed'}</div></div></div>`).join('') : '<div class="empty">No ground truth available</div>';
    $('#f-anom').innerHTML = table([
      { label: 'Date', render: (a) => `<b>${fmt.date(a.date)}</b>` },
      { label: 'Type', render: (a) => `<span class="badge ${a.direction === 'drop' ? 'high' : 'low'}">${a.direction === 'drop' ? '▼ drop' : '▲ spike'}</span>` },
      { label: 'Orders', num: 1, render: (a) => `${fmt.num(a.orders)} <span class="sub">/ ${fmt.num(a.expected_orders)}</span>` },
      { label: 'Deviation', num: 1, render: (a) => fmt.pct(a.deviation_pct, 0) },
      { label: 'z', num: 1, render: (a) => a.z_score.toFixed(1) },
      { label: 'Severity', render: (a) => `<span class="badge ${a.severity === 'critical' ? 'high' : 'medium'}">${a.severity}</span>` },
    ], daily.sort((a, b) => b.date.localeCompare(a.date)));
    const big = an.items.filter((a) => a.type === 'high_value_order').slice(0, 6);
    const abuse = an.items.filter((a) => a.type === 'return_abuse').slice(0, 6);
    $('#f-alerts').innerHTML = `<div class="sub" style="margin:0 4px 6px">High-value orders (above ${fmt.inr(big[0]?.expected)})</div>` + table([
      { label: 'Order', render: (a) => `<span class="strong">${a.order_id}</span><div class="sub">${fmt.date(a.date)}</div>` },
      { label: 'Customer', render: (a) => `<a class="badge info" href="#/customer/${a.customer_id}">${a.customer_id}</a>` },
      { label: 'Payment', key: 'payment_method' }, { label: 'Value', num: 1, render: (a) => `<b>${fmt.inr(a.actual)}</b>` },
    ], big) + `<div class="sub" style="margin:14px 4px 6px">Return-abuse watch-list</div>` + table([
      { label: 'Customer', render: (a) => `<a class="badge info" href="#/customer/${a.customer_id}">${a.customer_id}</a>` },
      { label: 'Orders', num: 1, key: 'orders' }, { label: 'Returns', num: 1, key: 'returns' },
      { label: 'Return rate', num: 1, render: (a) => `<b style="color:${css('--critical')}">${fmt.pct(a.return_rate)}</b>` },
      { label: 'Returned value', num: 1, render: (a) => fmt.inr(a.actual) },
    ], abuse, { empty: 'No suspicious customers' });
  },
});

/* =================================================================== CUSTOMER 360 */
route('customer', {
  title: 'Customer 360', icon: 'customer', sub: 'Unified customer profile — behaviour, RFM, churn risk, lifetime value and recommendations',
  async render(view, args, alive) {
    if (!args[0]) {
      const [champs, risk] = await Promise.all([api('/api/customers/search?q=&limit=12'), api('/api/churn/customers?risk=High&limit=12')]);
      if (!alive()) return;
      view.innerHTML = `<div class="callout">${icon('info')}<div>Search any customer with the search bar above (press <b>/</b>) or pick one below.</div></div>
        <div class="grid g2">${card('Top champions', 'Highest lifetime spend', `<div id="l1"></div>`)}${card('Valuable customers at risk', 'High churn probability, sorted by 12-month value', `<div id="l2"></div>`)}</div>`;
      $('#l1').innerHTML = customerTable(champs);
      $('#l2').innerHTML = customerTable(risk);
      bindRows($('#l1'), champs, (c) => { location.hash = `#/customer/${c.customer_id}`; });
      bindRows($('#l2'), risk, (c) => { location.hash = `#/customer/${c.customer_id}`; });
      return;
    }
    const c = await api(`/api/customers/${args[0]}`);
    if (!alive()) return;
    const S = series();
    const prob = c.churn_probability;
    view.innerHTML = `
    <div class="card fade-in"><div class="toolbar">
      <div class="profile" style="flex:1;min-width:280px"><div class="avatar">${initials(c.full_name)}</div>
        <div><h2>${esc(c.full_name)}</h2><div class="sub">${c.customer_id} · ${esc(c.email)}</div>
        <div class="chips" style="margin-top:6px">${segBadge(c.segment)} ${prob !== undefined && prob !== null ? riskBadge(c.churn_risk) + ' churn risk' : ''} ${c.favourite_category ? catBadge(c.favourite_category) : ''}</div></div></div>
      <dl class="kv"><dt>Location</dt><dd>${esc(c.city)}, ${esc(c.state)} (Tier ${c.city_tier})</dd><dt>Age · Gender</dt><dd>${c.age} · ${esc(c.gender)}</dd>
        <dt>Customer since</dt><dd>${fmt.date(c.signup_date)} via ${esc(c.acquisition_channel)}</dd><dt>Prefers</dt><dd>${esc(c.preferred_device || '—')} · ${esc(c.preferred_payment || '—')}</dd></dl>
    </div></div>
    <div class="grid g6">
      ${kpi({ label: 'Lifetime spend', value: fmt.inr(c.total_spent), foot: `${fmt.num(c.orders)} successful orders`, ic: 'rupee' })}
      ${kpi({ label: 'Average order', value: fmt.inr(c.aov), foot: `${fmt.inr(c.total_discount)} discounts used`, ic: 'cart' })}
      ${kpi({ label: 'Last order', value: c.recency_days !== null && c.recency_days !== undefined ? `${c.recency_days} days` : '—', foot: fmt.date(c.last_order), ic: 'clock' })}
      ${kpi({ label: 'Predicted 12-m value', value: fmt.inr(c.clv_12m), foot: 'spend run-rate × (1 − churn)', ic: 'forecast' })}
      ${kpi({ label: 'Sessions', value: fmt.num(c.sessions), foot: `last seen ${fmt.date(c.last_seen)}`, ic: 'users' })}
      ${kpi({ label: 'Returns · cancels', value: `${fmt.num(c.returned)} · ${fmt.num(c.cancelled)}`, foot: `${fmt.num(c.orders_placed)} orders placed`, ic: 'repeat' })}
    </div>
    <div class="grid g3">
      ${card('RFM score', c.rfm ? `Quintiles 1–5 · score ${c.rfm.score}` : 'No purchases yet', c.rfm ? `<div class="rfm"><div><b>${c.rfm.R}</b><span>Recency</span></div><div><b>${c.rfm.F}</b><span>Frequency</span></div><div><b>${c.rfm.M}</b><span>Monetary</span></div></div>
        <div class="callout" style="margin-top:14px">${icon('bolt')}<div><b>Recommended action:</b> ${esc(c.segment_action || '')}</div></div>` : `<div class="callout">${icon('bolt')}<div>${esc(c.segment_action || '')}</div></div>`)}
      ${card('Churn risk', prob !== undefined && prob !== null ? 'Probability of no purchase in the next 90 days' : 'Not scored', prob !== undefined && prob !== null ? '<div class="chart sm" id="c-g"></div>' : '<div class="empty">Customer has no purchase history</div>')}
      ${card('Why this risk level?', 'Largest contributions (logistic model)', `<div id="drv">${(c.churn_drivers || []).map((x) => {
        const mx = Math.max(...c.churn_drivers.map((d) => Math.abs(d.contribution)), .01); const w = Math.abs(x.contribution) / mx * 50; const pos = x.contribution > 0;
        return `<div class="driver"><span class="ellipsis">${esc(x.feature)}</span><div class="track"><i style="${pos ? 'left:50%' : 'right:50%'};width:${w}%;background:${pos ? css('--critical') : css('--good')}"></i></div><span class="sub" style="text-align:right">${pos ? '↑ risk' : '↓ risk'}</span></div>`; }).join('') || '<div class="empty">—</div>'}</div>`)}
    </div>
    <div class="grid g-2-1">
      ${card('Monthly spend', 'Successful orders', '<div class="chart" id="c-ms"></div>')}
      ${card('Category mix', 'Lifetime spend by category', '<div class="chart" id="c-cm"></div>')}
    </div>
    <div class="grid g-2-1">
      ${card('Recent orders', 'Click an order to see its items (embedded MongoDB document)', '<div id="c-orders"></div>')}
      ${card('Recommended for this customer', 'ALS collaborative filtering', `<div id="c-recs"></div>`, { right: `<a class="btn" href="#/recommendations/${c.customer_id}">All</a>` })}
    </div>`;
    if (prob !== undefined && prob !== null) {
      chart('#c-g', { series: [{ type: 'gauge', startAngle: 205, endAngle: -25, min: 0, max: 100, radius: '100%', center: ['50%', '62%'],
        progress: { show: true, width: 14, roundCap: true, itemStyle: { color: riskColor(c.churn_risk) } }, axisLine: { roundCap: true, lineStyle: { width: 14, color: [[1, css('--surface-3')]] } },
        pointer: { show: false }, axisTick: { show: false }, splitLine: { show: false }, axisLabel: { show: false },
        detail: { offsetCenter: [0, '0%'], fontSize: 28, fontWeight: 800, color: css('--text'), formatter: '{value}%' }, title: { offsetCenter: [0, '38%'], color: css('--muted'), fontSize: 12 },
        data: [{ value: +(prob * 100).toFixed(1), name: `${c.churn_risk} risk` }] }] });
    }
    chart('#c-ms', {
      tooltip: { trigger: 'axis', ...tooltipStyle(), valueFormatter: (v) => fmt.inr(v) },
      xAxis: xAxis(c.monthly_spend.map((x) => fmt.month(x.month))), yAxis: yAxis(fmt.inrAxis),
      series: [barSeries('Spend', c.monthly_spend.map((x) => x.revenue), S[0])],
    });
    chart('#c-cm', {
      tooltip: { trigger: 'item', ...tooltipStyle(), formatter: (p) => `${p.marker}${p.name}<br><b>${fmt.inr(p.value)}</b> · ${p.percent}%` },
      series: [{ type: 'pie', radius: ['45%', '72%'], itemStyle: { borderColor: css('--surface'), borderWidth: 2, borderRadius: 4 },
        label: { color: css('--text-2'), fontSize: 11, formatter: '{b}' }, data: c.category_spend.map((x) => ({ name: x.category, value: x.revenue, itemStyle: { color: catColor(x.category) } })) }],
    });
    const orders = c.orders_recent;
    $('#c-orders').innerHTML = table([
      { label: 'Order', render: (o) => `<span class="strong">${o.order_id}</span><div class="sub">${fmt.date(o.order_ts)}</div>` },
      { label: 'Items', render: (o) => `<div class="ellipsis" style="max-width:240px">${o.items.map((i) => esc(i.product_name)).join(', ')}</div>` },
      { label: 'Status', render: (o) => `<span class="badge ${o.status === 'Delivered' ? 'low' : o.status === 'Cancelled' || o.status === 'Returned' ? 'high' : 'medium'}">${o.status}</span>` },
      { label: 'Payment', key: 'payment_method' },
      { label: 'Amount', num: 1, render: (o) => `<b>${fmt.inr(o.net_amount)}</b>` },
    ], orders, { onRow: 1, empty: 'No orders yet' });
    bindRows($('#c-orders'), orders, (o) => openModal(`<div class="card-head"><div><h3 style="font-size:18px">${o.order_id}</h3><p>${fmt.date(o.order_ts)} · ${o.status} · ${esc(o.payment_method)} · ${esc(o.device)}</p></div><div class="spacer"></div><button class="icon-btn" data-close>✕</button></div>
      ${table([{ label: '#', key: 'line_no' }, { label: 'Product', render: (i) => `<b>${esc(i.product_name)}</b><div class="sub">${esc(i.category)}</div>` }, { label: 'Qty', num: 1, key: 'quantity' },
        { label: 'List', num: 1, render: (i) => fmt.inr(i.list_price) }, { label: 'Discount', num: 1, render: (i) => fmt.pct(i.discount_pct * 100, 0) }, { label: 'Paid', num: 1, render: (i) => `<b>${fmt.inr(i.revenue)}</b>` },
        { label: 'Rating', num: 1, render: (i) => (i.rating ? `${i.rating}★` : '—') }], o.items)}
      <div class="toolbar" style="margin-top:14px"><div class="grow"></div><span class="sub">Gross ${fmt.inr(o.gross_value)} · Discount ${fmt.inr(o.discount_amount)} · Shipping ${fmt.inr(o.shipping_fee)}</span><b style="font-size:18px">${fmt.inr(o.net_amount)}</b></div>`));
    $('#c-recs').innerHTML = (c.recommendations || []).slice(0, 6).map((p) => `<div class="alert-row" style="cursor:pointer" data-p="${p.product_id}">
      <div class="alert-ic" style="background:${hexA(catColor(p.category), .15)};color:${catColor(p.category)}">${icon('star')}</div>
      <div style="flex:1;min-width:0"><div class="strong ellipsis">${esc(p.product_name)}</div><div class="sub">${esc(p.reason)}</div></div><b>${fmt.inr(p.list_price)}</b></div>`).join('') || '<div class="empty">No recommendations</div>';
    $('#c-recs').addEventListener('click', (e) => { const el = e.target.closest('[data-p]'); if (el) productModal(el.dataset.p); });
  },
});

/* =================================================================== PIPELINE */
route('pipeline', {
  title: 'Data Pipeline & Models', icon: 'pipeline', sub: 'Architecture, Spark job timings, data-quality report, MongoDB collections and model registry',
  async render(view, args, alive) {
    const [p, models] = await Promise.all([api('/api/pipeline/runs', { cache: false }), api('/api/models')]);
    if (!alive()) return;
    const r = p.latest;
    if (!r) { view.innerHTML = '<div class="card empty"><h3>No pipeline runs yet</h3><p>Run <code>python run_pipeline.py</code></p></div>'; return; }
    const ds = r.dataset || {}, S = series();
    const records = r.stages.reduce((s, x) => s + (x.records || 0), 0);
    view.innerHTML = `
    ${card('System architecture', 'Batch Big-Data pipeline (Lambda-style serving layer)', archSVG())}
    <div class="grid g4">
      ${kpi({ label: 'Last run', value: `<span class="badge ${r.status === 'success' ? 'low' : 'high'}" style="font-size:14px">${r.status}</span>`, foot: fmt.date(r.started_at), ic: 'check' })}
      ${kpi({ label: 'Duration', value: fmt.secs(r.duration_sec), foot: `${r.stages.length} stages`, ic: 'clock' })}
      ${kpi({ label: 'Raw events processed', value: fmt.num(ds.events), foot: `${fmt.num(ds.orders)} orders · ${fmt.num(ds.customers)} customers`, ic: 'bolt' })}
      ${kpi({ label: 'Documents written', value: fmt.num(records), foot: `${Object.keys(r.collections || {}).length} MongoDB collections`, ic: 'pipeline' })}
    </div>
    <div class="grid g2">
      ${card('Stage timings', `Spark ${r.spark.version} · ${esc(r.spark.master)} · ${r.spark.default_parallelism} cores`, '<div class="chart xl" id="c-stages"></div>')}
      ${card('Data-quality report', 'Rows in → rows out after cleaning', '<div id="dq"></div>')}
    </div>
    <div class="grid g2">
      ${card('Model registry', 'Stored in the model_registry collection', '<div id="models"></div>')}
      ${card('MongoDB collections', `Database: ${esc(STATE.meta?.version ? 'shopsense' : '')}`, '<div id="colls" style="max-height:420px;overflow:auto"></div>')}
    </div>
    ${card('Run history', '', '<div id="runs"></div>')}`;
    const st = r.stages;
    const gcol = { Generate: S[6], Setup: css('--muted'), ETL: S[0], Analytics: S[2], ML: S[1] };
    chart('#c-stages', {
      tooltip: { trigger: 'axis', ...tooltipStyle(), formatter: (ps) => { const s = st[ps[0].dataIndex]; return `<b>${esc(s.name)}</b><br>${s.group} · ${s.seconds}s${s.records ? `<br>${fmt.num(s.records)} records` : ''}`; } },
      grid: { left: 8, right: 50, top: 4, bottom: 4, containLabel: true },
      xAxis: { type: 'value', show: false }, yAxis: { type: 'category', inverse: true, data: st.map((s) => s.name), axisLine: { show: false }, axisTick: { show: false }, axisLabel: { color: css('--text-2'), fontSize: 11 } },
      series: [barSeries('Seconds', st.map((s) => ({ value: s.seconds, itemStyle: { color: gcol[s.group] || S[0], borderRadius: [0, 4, 4, 0] } })), S[0], { horizontal: true, barMaxWidth: 14, label: { show: true, position: 'right', color: css('--muted'), formatter: (x) => `${x.value}s` } })],
    });
    const dq = Object.entries(r.data_quality || {}).filter(([, v]) => typeof v === 'object');
    $('#dq').innerHTML = table([
      { label: 'Dataset', render: ([k]) => `<b>${k}</b>` },
      { label: 'Raw rows', num: 1, render: ([, v]) => fmt.num(v.raw) },
      { label: 'Clean rows', num: 1, render: ([, v]) => fmt.num(v.clean) },
      { label: 'Fixes applied', render: ([, v]) => Object.entries(v).filter(([k, x]) => !['raw', 'clean'].includes(k) && x).map(([k, x]) => `<span class="badge" style="margin:2px">${k.replace(/_/g, ' ')}${x === true ? '' : `: ${fmt.num(x)}`}</span>`).join('') || '<span class="sub">—</span>' },
    ], dq);
    $('#models').innerHTML = table([
      { label: 'Task', render: (m) => `<b>${esc(m.task)}</b>${m.is_best ? ' <span class="badge brand">best</span>' : ''}<div class="sub">${esc(m.algorithm)}</div>` },
      { label: 'Key metrics', render: (m) => Object.entries(m.metrics || (m.silhouette ? { silhouette: m.silhouette, k: m.k } : {})).slice(0, 4).map(([k, v]) => `<span class="badge" style="margin:2px">${k}: ${typeof v === 'number' ? (Math.abs(v) < 1 && v !== 0 ? v.toFixed(3) : fmt.num(v, v % 1 ? 2 : 0)) : v}</span>`).join('') },
      { label: 'Trained', render: (m) => `<span class="sub">${esc(m.trained_at || '')}</span>` },
    ], models);
    const colls = Object.entries(r.collections || {}).sort((a, b) => b[1] - a[1]);
    $('#colls').innerHTML = table([{ label: 'Collection', render: ([k]) => `<code>${k}</code>` }, { label: 'Documents', num: 1, render: ([, v]) => fmt.num(v) }], colls);
    $('#runs').innerHTML = table([
      { label: 'Run id', render: (x) => `<code>${x._id}</code>` }, { label: 'Started', render: (x) => fmt.date(x.started_at) + ' ' + new Date(x.started_at).toLocaleTimeString('en-IN') },
      { label: 'Status', render: (x) => `<span class="badge ${x.status === 'success' ? 'low' : 'high'}">${x.status}</span>` },
      { label: 'Duration', num: 1, render: (x) => fmt.secs(x.duration_sec) }, { label: 'Scale', render: (x) => esc(x.dataset?.scale || '—') },
      { label: 'Platform', render: (x) => `<span class="sub">${esc(x.platform || '')}</span>` },
    ], p.runs);
  },
});

function archSVG() {
  const box = (x, y, w, h, title, sub, color) => `<g><rect class="box" x="${x}" y="${y}" width="${w}" height="${h}" rx="12"/>
    <rect x="${x}" y="${y}" width="5" height="${h}" rx="2" fill="${color}"/>
    <text x="${x + 18}" y="${y + 26}" font-weight="700" font-size="14">${title}</text>${sub.map((s, i) => `<text class="sub" x="${x + 18}" y="${y + 46 + i * 16}">${s}</text>`).join('')}</g>`;
  const S = series();
  return `<svg class="arch" viewBox="0 0 1180 250" role="img" aria-label="ShopSense architecture">
    <defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 10 5 0 10z" fill="${css('--brand')}" stroke="none"/></marker></defs>
    ${box(10, 30, 190, 190, 'Data sources', ['customers.csv', 'products.jsonl', 'orders.csv · order_items.csv', 'events/*.jsonl (clickstream)', '≈1.3 M raw records'], S[6])}
    ${box(250, 30, 210, 190, 'Spark ETL', ['Bronze: schema-on-read', 'Silver: clean, de-dup,', 'validate, enrich, sessionise', 'Parquet data lake (optional)', 'DQ report per table'], S[1])}
    ${box(510, 30, 210, 88, 'Spark SQL analytics', ['KPIs, funnels, cohorts, geo'], S[2])}
    ${box(510, 132, 210, 88, 'Spark MLlib', ['K-Means · LR/RF/GBT · ALS', 'FP-Growth · RF forecaster'], S[4])}
    ${box(770, 30, 180, 190, 'MongoDB', ['30 gold collections', 'embedded order documents', 'model registry', 'aggregation pipelines', 'indexes'], S[2])}
    ${box(1000, 30, 170, 190, 'Serving', ['FastAPI REST (/docs)', 'Interactive dashboard', 'What-if simulator', 'CSV campaign export'], S[0])}
    <path class="flow" d="M200 125 H246"/><path class="flow" d="M460 90 H506"/><path class="flow" d="M460 170 H506"/>
    <path class="flow" d="M720 74 H766"/><path class="flow" d="M720 176 H766"/><path class="flow" d="M950 125 H996"/>
  </svg>`;
}
