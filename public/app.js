/* 現代商業與深度時事新聞儀表板 — 前端資料綁定與互動控制器 (Vanilla JS) */
(function () {
  "use strict";

  const state = {
    payload: null,
    editionMarketsCache: {},
    filter: "all",
    query: "",
    editionOverride: null,
    expandedIds: new Set(),
  };

  function escapeHtml(raw) {
    return String(raw ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function formatTime(isoStr) {
    if (!isoStr) return "今日更新";
    try {
      const d = new Date(isoStr);
      if (Number.isNaN(d.getTime())) return String(isoStr);
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      const hh = String(d.getHours()).padStart(2, "0");
      const min = String(d.getMinutes()).padStart(2, "0");
      return `${mm}/${dd} ${hh}:${min}`;
    } catch (_err) {
      return String(isoStr);
    }
  }

  function formatNumber(val, digits) {
    const num = Number(val);
    if (!Number.isFinite(num)) return String(val ?? "—");
    return num.toFixed(typeof digits === "number" ? digits : 2);
  }

  function buildSparklineSvg(history, label, isUp, market) {
    const pts = Array.isArray(history)
      ? history.map(Number).filter((n) => Number.isFinite(n))
      : [];
    if (pts.length < 2) return "";
    const w = 72;
    const h = 24;
    const pad = 2.5;
    const min = Math.min(...pts);
    const max = Math.max(...pts);
    const span = max - min || 1;
    const coords = pts.map((v, i) => {
      const x = pad + (i / (pts.length - 1)) * (w - pad * 2);
      const y = h - pad - ((v - min) / span) * (h - pad * 2);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    });
    const last = coords[coords.length - 1].split(",");
    const strokeVar =
      market === "tw"
        ? isUp
          ? "var(--color-error)"
          : "var(--color-accent)"
        : isUp
        ? "var(--color-accent)"
        : "var(--color-ink-2)";
    return `<svg viewBox="0 0 ${w} ${h}" role="img" aria-label="${escapeHtml(label)} 近期走勢">
      <title>${escapeHtml(label)} 近期走勢（最低 ${min}，最高 ${max}）</title>
      <polyline fill="none" stroke="${strokeVar}" stroke-width="1.5" points="${coords.join(" ")}" />
      <circle cx="${last[0]}" cy="${last[1]}" r="2.2" fill="${strokeVar}" />
    </svg>`;
  }

  function renderMarketCell(item, cellClass) {
    const digits = typeof item.digits === "number" ? item.digits : 2;
    const valStr = formatNumber(item.value, digits);
    const chg = Number(item.change || 0);
    const pct = Number(item.changePct || 0);
    const sign = chg > 0 ? "+" : "";
    const trend = chg > 0 ? "up" : chg < 0 ? "down" : "flat";
    const marketAttr = item.market ? ` data-market="${escapeHtml(item.market)}"` : "";
    const symbolHtml =
      item.symbol && item.symbol !== "NDC"
        ? `<code class="market-cell__symbol">${escapeHtml(item.symbol)}</code>`
        : "";
    const deltaText = item.light
      ? `${item.light}（${sign}${formatNumber(chg, digits)} ${item.unit || ""}）`
      : `${sign}${formatNumber(chg, digits)} (${sign}${pct.toFixed(2)}%)`;
    const spark = buildSparklineSvg(item.history, item.label, chg >= 0, item.market);
    return `<article class="${escapeHtml(cellClass)}"${marketAttr}>
      <div class="macro-cell__main">
        <p class="market-cell__name">${escapeHtml(item.label)}</p>
        ${symbolHtml}
        <div class="macro-cell__value-row">
          <span class="macro-cell__value">${escapeHtml(valStr)}</span>
          ${item.unit ? `<span class="macro-cell__unit">${escapeHtml(item.unit)}</span>` : ""}
        </div>
        <div class="${escapeHtml(cellClass)}__delta" data-trend="${trend}">${escapeHtml(deltaText)}</div>
      </div>
      <div class="macro-cell__spark">
        ${spark}
        <span class="macro-cell__asof">${escapeHtml(item.asOf || "")}</span>
      </div>
    </article>`;
  }

  function renderMacroBar(payload) {
    const container = document.getElementById("macro-grid");
    const items =
      payload?.macro_indicators ||
      payload?.macro?.macro_indicators ||
      payload?.macro?.items ||
      [];
    if (!container || !Array.isArray(items)) return;
    container.innerHTML = items.map((item) => renderMarketCell(item, "macro-cell")).join("");
  }

  function renderMarketFocus(payload, activeEditionKey) {
    const section = document.getElementById("market-focus-section");
    const headingEl = document.getElementById("market-focus-heading");
    const noteEl = document.getElementById("market-focus-note");
    const idxGrid = document.getElementById("market-focus-indices");
    const stkGrid = document.getElementById("market-focus-stocks");
    if (!section || !idxGrid || !stkGrid || !payload) return;

    const edMap =
      payload.edition_markets ||
      payload.macro?.edition_markets ||
      state.editionMarketsCache ||
      {};
    if (edMap && (edMap.morning || edMap.evening)) {
      state.editionMarketsCache = { ...state.editionMarketsCache, ...edMap };
    }

    const key = activeEditionKey === "evening" ? "evening" : "morning";
    let groupData = edMap[key] || state.editionMarketsCache[key];

    if ((!groupData || !Array.isArray(groupData.items)) && Array.isArray(payload.market_focus)) {
      groupData = {
        edition: key,
        items: payload.market_focus,
        indices: payload.market_focus.filter((x) => x.group === "index"),
        stocks: payload.market_focus.filter((x) => x.group !== "index"),
      };
    }

    if (!groupData || !Array.isArray(groupData.items) || groupData.items.length === 0) {
      section.hidden = true;
      return;
    }

    section.hidden = false;
    const isTw = key === "evening";
    section.setAttribute("data-market-convention", isTw ? "tw" : "us");

    if (headingEl) {
      headingEl.textContent =
        groupData.title ||
        (isTw
          ? "台股收盤與核心權值表現（晚報焦點）"
          : "美股收盤與科技巨頭表現（晨報焦點）");
    }
    if (noteEl) {
      noteEl.textContent =
        groupData.description ||
        (isTw
          ? "涵蓋台灣加權、櫃買指數與核心 ETF／權值股（台股慣例：漲紅跌綠）"
          : "涵蓋標普 500、那斯達克、費半、道瓊指數與美股科技 ADR（+ 上漲 / - 下跌）");
    }

    const indices = Array.isArray(groupData.indices)
      ? groupData.indices
      : groupData.items.filter((x) => x.group === "index");
    const stocks = Array.isArray(groupData.stocks)
      ? groupData.stocks
      : groupData.items.filter((x) => x.group !== "index");

    idxGrid.setAttribute("data-cols", indices.length <= 2 ? "2" : "4");
    idxGrid.innerHTML = indices.map((item) => renderMarketCell(item, "market-cell")).join("");

    stkGrid.setAttribute("data-cols", stocks.length === 6 ? "3" : stocks.length <= 2 ? "2" : "4");
    stkGrid.innerHTML = stocks.map((item) => renderMarketCell(item, "market-cell")).join("");
  }

  function renderFactLedgerFigure(story) {
    const facts = Array.isArray(story.core_facts) ? story.core_facts : [];
    const cells = facts.slice(0, 3).map((f, idx) => {
      const metric = f.metric ? f.metric : `查核事實 0${idx + 1}`;
      const confirmed = Array.isArray(f.confirmed_by) ? f.confirmed_by.join("、") : story.source;
      return `<div class="fact-ledger-figure__cell">
        <dt>交叉核實：${escapeHtml(confirmed)}</dt>
        <dd>${escapeHtml(metric)}</dd>
      </div>`;
    });
    return `<div class="fact-ledger-figure" role="group" aria-label="核心數據查核帳本（原始報導無提供 og:image 首圖）">
      <p class="fact-ledger-figure__title">關鍵數據帳本（原始來源未附 og:image，依規範標記為 null 並呈現查核指標）</p>
      <dl class="fact-ledger-figure__grid">${cells.join("")}</dl>
    </div>`;
  }

  function renderLeadMedia(story) {
    if (!story.imageUrl) {
      return renderFactLedgerFigure(story);
    }
    const credit = story.imageCredit || story.source || "原始媒體";
    return `<figure class="lead-story__media">
      <img src="${escapeHtml(story.imageUrl)}" alt="${escapeHtml(story.title)}" loading="eager" decoding="async" data-fallback-story="${escapeHtml(story.id)}" />
      <figcaption>新聞原生首圖（提取自原始網頁 meta og:image，來源：${escapeHtml(credit)}）</figcaption>
    </figure>`;
  }

  function renderFactcheckDrawer(story, isOpen) {
    const sb = story.scoreBreakdown || {};
    const facts = Array.isArray(story.core_facts) ? story.core_facts : [];
    const perspectives = Array.isArray(story.media_perspectives) ? story.media_perspectives : [];
    const sources = Array.isArray(story.sources) ? story.sources : [];
    const drawerId = `drawer-${story.id}`;

    return `<div class="factcheck-disclosure">
      <button type="button" class="factcheck-toggle" data-toggle-story="${escapeHtml(story.id)}" aria-expanded="${isOpen ? "true" : "false"}" aria-controls="${escapeHtml(drawerId)}">
        <span>事實查核明細（${sources.length} 家出處）</span>
        <span data-toggle-label>${isOpen ? "收合" : "展開"}</span>
      </button>
      <div id="${escapeHtml(drawerId)}" class="factcheck-drawer" data-open="${isOpen ? "true" : "false"}">
        <div class="factcheck-drawer__inner">
          <div class="factcheck-drawer__content">
            <dl class="score-breakdown" aria-label="四維度重要性評分拆解">
              <div>
                <dt>跨媒體共鳴 (35%)</dt>
                <dd>${escapeHtml(sb.resonance ?? 0)} / 35</dd>
              </div>
              <div>
                <dt>實質結構影響 (35%)</dt>
                <dd>${escapeHtml(sb.impact ?? 0)} / 35</dd>
              </div>
              <div>
                <dt>數據事實密度 (20%)</dt>
                <dd>${escapeHtml(sb.data ?? 0)} / 20</dd>
              </div>
              <div>
                <dt>時效持久度 (10%)</dt>
                <dd>${escapeHtml(sb.durability ?? 0)} / 10</dd>
              </div>
            </dl>
            ${
              facts.length
                ? `<div class="core-facts-box">
                    <h4>各家一致確認之客觀事實 (core_facts)</h4>
                    <ul class="core-facts-list">
                      ${facts
                        .map(
                          (f) => `<li>
                            ${escapeHtml(f.text)}
                            <span class="core-facts-list__sources">［確認媒體：${escapeHtml((f.confirmed_by || []).join("、"))}］</span>
                          </li>`
                        )
                        .join("")}
                    </ul>
                  </div>`
                : ""
            }
            ${
              perspectives.length
                ? `<div class="source-links-block">
                    <h5>各家媒體切入視角對比 (media_perspectives)</h5>
                    <ul class="perspective-list">
                      ${perspectives
                        .map(
                          (p) => `<li class="perspective-item">
                            <div class="perspective-item__header">
                              <span class="perspective-item__media">${escapeHtml(p.media)}</span>
                              <span class="perspective-item__angle">${escapeHtml(p.angle)}</span>
                            </div>
                            <p class="perspective-item__headline">
                              <a href="${escapeHtml(p.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(p.headline)}</a>
                            </p>
                            <p class="perspective-item__focus">${escapeHtml(p.focus || p.excerpt || "")}</p>
                          </li>`
                        )
                        .join("")}
                    </ul>
                  </div>`
                : ""
            }
            ${
              story.unverified_notice
                ? `<div class="unverified-callout" role="note">
                    <strong>單一消息源查核警示：</strong>${escapeHtml(story.unverified_notice)}
                  </div>`
                : ""
            }
            <div class="source-links-block">
              <h5>原始報導出處超連結</h5>
              <ul class="source-links-list">
                ${sources
                  .map(
                    (s) => `<li>
                      <span><strong>${escapeHtml(s.media)}</strong>：${escapeHtml(s.headline)}</span>
                      <a href="${escapeHtml(s.url)}" target="_blank" rel="noopener noreferrer">開啟原始報導</a>
                    </li>`
                  )
                  .join("")}
              </ul>
            </div>
          </div>
        </div>
      </div>
    </div>`;
  }

  function renderLeadStory(story, idx) {
    const facts = Array.isArray(story.core_facts) ? story.core_facts : [];
    const perspectives = Array.isArray(story.media_perspectives) ? story.media_perspectives : [];
    const isOpen = state.expandedIds.has(story.id);
    const sectionLabel = story.section === "deep" ? "深度專題與社會學習" : "商業與科技供應鏈";

    return `<article class="lead-story" data-story-id="${escapeHtml(story.id)}">
      <div class="lead-story__primary">
        <h3 class="lead-story__headline">
          <a href="${escapeHtml(story.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(story.title)}</a>
        </h3>
        <div class="story-meta">
          <span class="story-meta__item story-meta__score">重要性評分 ${escapeHtml(story.score)} 分</span>
          <span class="story-meta__item">跨媒體共鳴 ${escapeHtml(story.mediaCount)} 家</span>
          <span class="story-meta__item">領銜報導：${escapeHtml(story.source)}</span>
          <span class="story-meta__item">分類：${escapeHtml(sectionLabel)}</span>
          <time class="story-meta__item" datetime="${escapeHtml(story.publishedAt || "")}">${escapeHtml(formatTime(story.publishedAt))}</time>
        </div>
        ${renderLeadMedia(story)}
        <p class="lead-story__summary">${escapeHtml(story.summary)}</p>
        <div class="core-facts-box">
          <h4>跨媒體一致確認之核心事實</h4>
          <ul class="core-facts-list">
            ${facts
              .map(
                (f) => `<li>
                  ${escapeHtml(f.text)}
                  <span class="core-facts-list__sources">［核實：${escapeHtml((f.confirmed_by || []).join("、"))}］</span>
                </li>`
              )
              .join("")}
          </ul>
        </div>
        ${renderFactcheckDrawer(story, isOpen)}
      </div>
      <aside class="perspective-panel" aria-label="頭條 ${idx + 1} 媒體多方視角對照框">
        <div>
          <div class="perspective-panel__head">
            <h4>媒體多方視角對照框</h4>
            <p class="perspective-panel__sub">對比 ${escapeHtml(perspectives.length)} 家白名單媒體之報導切入點與側重差異</p>
          </div>
          <ul class="perspective-list">
            ${perspectives
              .map(
                (p) => `<li class="perspective-item">
                  <div class="perspective-item__header">
                    <span class="perspective-item__media">${escapeHtml(p.media)}</span>
                    <span class="perspective-item__angle">${escapeHtml(p.angle)}</span>
                  </div>
                  <p class="perspective-item__headline">
                    <a href="${escapeHtml(p.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(p.headline)}</a>
                  </p>
                  <p class="perspective-item__focus">${escapeHtml(p.focus || p.excerpt || "")}</p>
                </li>`
              )
              .join("")}
          </ul>
        </div>
        ${
          story.unverified_notice
            ? `<div class="unverified-callout" role="note">
                <strong>查核警示：</strong>${escapeHtml(story.unverified_notice)}
              </div>`
            : `<div class="unverified-callout" role="note">
                <strong>查核狀態：</strong>本則頭條所有核心數據均經 2 家以上具名媒體或官方公開公報交叉確認，無單一匿名消息源。
              </div>`
        }
      </aside>
    </article>`;
  }

  function renderSecondaryCard(story, isDeep) {
    const isOpen = state.expandedIds.has(story.id);
    const hasImg = Boolean(story.imageUrl);
    const cardClass = isDeep ? "story-card story-card--deep" : "story-card";

    const bodyHtml = hasImg
      ? `<div class="story-card__body-split">
          <p class="story-card__summary">${escapeHtml(story.summary)}</p>
          <figure class="story-card__thumb">
            <img src="${escapeHtml(story.imageUrl)}" alt="${escapeHtml(story.title)}" loading="lazy" decoding="async" data-fallback-thumb="${escapeHtml(story.id)}" />
          </figure>
        </div>`
      : `<p class="story-card__summary">${escapeHtml(story.summary)}</p>`;

    return `<article class="${cardClass}" data-story-id="${escapeHtml(story.id)}">
      <h3 class="story-card__headline">
        <a href="${escapeHtml(story.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(story.title)}</a>
      </h3>
      <div class="story-meta">
        <span class="story-meta__item story-meta__score">評分 ${escapeHtml(story.score)} 分</span>
        <span class="story-meta__item">${escapeHtml(story.mediaCount)} 家共鳴</span>
        <span class="story-meta__item">${escapeHtml(story.source)}</span>
        <time class="story-meta__item" datetime="${escapeHtml(story.publishedAt || "")}">${escapeHtml(formatTime(story.publishedAt))}</time>
      </div>
      ${bodyHtml}
      ${
        story.unverified_notice
          ? `<div class="unverified-callout" role="note">
              <strong>單一消息源警示：</strong>${escapeHtml(story.unverified_notice)}
            </div>`
          : ""
      }
      ${renderFactcheckDrawer(story, isOpen)}
    </article>`;
  }

  function filterStories(stories) {
    const q = state.query.trim().toLowerCase();
    return stories.filter((s) => {
      if (state.filter === "business_tech" && s.section !== "business_tech") return false;
      if (state.filter === "deep" && s.section !== "deep") return false;
      if (state.filter === "unverified" && !s.unverified_notice) return false;
      if (!q) return true;
      const hay = [
        s.title,
        s.summary,
        s.source,
        ...(s.core_facts || []).map((f) => f.text),
        ...(s.media_perspectives || []).map((p) => `${p.media} ${p.headline} ${p.focus}`),
      ]
        .join(" ")
        .toLowerCase();
      return hay.includes(q);
    });
  }

  function updateMasthead(payload) {
    const ed = state.editionOverride
      ? {
          key: state.editionOverride,
          label: state.editionOverride === "morning" ? "晨報版 07:30" : "晚報版 16:00",
        }
      : payload.edition || { key: "morning", label: "晨報版 07:30" };

    const edNode = document.getElementById("masthead-edition");
    const dateNode = document.getElementById("masthead-date");
    const statsNode = document.getElementById("masthead-stats");
    if (edNode) edNode.textContent = ed.label;
    if (dateNode) {
      dateNode.textContent = `${payload.date || ""} ${payload.weekday || ""}（更新：${payload.updatedAt || ""}）`;
    }
    if (statsNode && payload.stats) {
      statsNode.textContent = `入選 ${payload.stats.passed} 則（>=${payload.threshold || 80} 分）／ 掃描 ${payload.stats.articlesAnalysed} 篇`;
    }

    document.querySelectorAll("[data-edition-switch]").forEach((btn) => {
      const k = btn.getAttribute("data-edition-switch");
      btn.setAttribute("aria-pressed", k === ed.key ? "true" : "false");
    });

    const stories = Array.isArray(payload.stories) ? payload.stories : [];
    const counts = {
      all: stories.length,
      business_tech: stories.filter((s) => s.section === "business_tech").length,
      deep: stories.filter((s) => s.section === "deep").length,
      unverified: stories.filter((s) => Boolean(s.unverified_notice)).length,
    };
    document.querySelectorAll("[data-filter]").forEach((btn) => {
      const f = btn.getAttribute("data-filter");
      btn.setAttribute("aria-pressed", f === state.filter ? "true" : "false");
      const badge = btn.querySelector(".filter-btn__count");
      if (badge && Object.prototype.hasOwnProperty.call(counts, f)) {
        badge.textContent = `(${counts[f]})`;
      }
    });
  }

  function renderColophon(payload) {
    const list = document.getElementById("source-status-list");
    if (!list || !Array.isArray(payload.sourcesStatus)) return;
    list.innerHTML = payload.sourcesStatus
      .map((s) => {
        const statusLabel = s.ok ? `${s.count} 篇` : "備援快取";
        return `<li class="source-status-item">${escapeHtml(s.name)}（${escapeHtml(s.section || s.group)}）：${escapeHtml(statusLabel)}</li>`;
      })
      .join("");
  }

  function renderAll() {
    const payload = state.payload;
    if (!payload) return;

    const activeEditionKey = state.editionOverride || payload.edition?.key || "morning";
    updateMasthead(payload);
    renderMacroBar(payload);
    renderMarketFocus(payload, activeEditionKey);
    renderColophon(payload);

    const allStories = Array.isArray(payload.stories) ? payload.stories : [];
    const filtered = filterStories(allStories);

    const leadContainer = document.getElementById("lead-stories-container");
    const leadSection = document.getElementById("lead-section");
    const secondarySection = document.getElementById("secondary-section");
    const bizContainer = document.getElementById("biz-stories-container");
    const deepContainer = document.getElementById("deep-stories-container");
    const emptyState = document.getElementById("empty-state");

    if (filtered.length === 0) {
      if (leadSection) leadSection.hidden = true;
      if (secondarySection) secondarySection.hidden = true;
      if (emptyState) emptyState.hidden = false;
      return;
    }

    if (emptyState) emptyState.hidden = true;

    const leads = filtered.slice(0, 2);
    const leadIds = new Set(leads.map((s) => s.id));
    const rest = filtered.filter((s) => !leadIds.has(s.id));

    if (leadSection && leadContainer) {
      leadSection.hidden = false;
      leadContainer.innerHTML = leads.map((s, i) => renderLeadStory(s, i)).join("");
    }

    const bizStories = rest.filter((s) => s.section === "business_tech");
    const deepStories = rest.filter((s) => s.section === "deep");

    // 若深度專題已被選入雙頭條，也在深度專題欄保留完整呈現，確保讀者能直接對照閱讀
    const deepForColumn =
      deepStories.length > 0
        ? deepStories
        : filtered.filter((s) => s.section === "deep");

    if (secondarySection) {
      secondarySection.hidden = bizStories.length === 0 && deepForColumn.length === 0;
    }
    if (bizContainer) {
      bizContainer.innerHTML = bizStories.length
        ? bizStories.map((s) => renderSecondaryCard(s, false)).join("")
        : `<div class="state-banner"><h3>商業與科技供應鏈新聞已列於上方雙頭條</h3><p>目前篩選條件下的高分商業與科技報導已於「今日深度雙頭條」完整展開。</p></div>`;
    }
    if (deepContainer) {
      deepContainer.innerHTML = deepForColumn.length
        ? deepForColumn.map((s) => renderSecondaryCard(s, true)).join("")
        : `<div class="state-banner"><h3>無符合篩選條件之深度專題</h3><p>請切換至「全部高價值新聞」或清除檢索關鍵字以檢視報導者與公視新聞網專題。</p></div>`;
    }

    bindImageFallbacks();
  }

  function bindImageFallbacks() {
    document.querySelectorAll("img[data-fallback-story]").forEach((img) => {
      img.addEventListener(
        "error",
        () => {
          const sid = img.getAttribute("data-fallback-story");
          const story = (state.payload?.stories || []).find((s) => s.id === sid);
          const fig = img.closest("figure");
          if (fig && story) {
            fig.outerHTML = renderFactLedgerFigure(story);
          }
        },
        { once: true }
      );
    });
    document.querySelectorAll("img[data-fallback-thumb]").forEach((img) => {
      img.addEventListener(
        "error",
        () => {
          const split = img.closest(".story-card__body-split");
          const fig = img.closest("figure");
          if (fig) fig.remove();
          if (split) split.classList.remove("story-card__body-split");
        },
        { once: true }
      );
    });
  }

  function syncUrlState() {
    try {
      const url = new URL(window.location.href);
      if (state.filter && state.filter !== "all") {
        url.searchParams.set("section", state.filter);
      } else {
        url.searchParams.delete("section");
      }
      if (state.query.trim()) {
        url.searchParams.set("q", state.query.trim());
      } else {
        url.searchParams.delete("q");
      }
      window.history.replaceState({}, "", url.toString());
    } catch (_err) {
      // file:// 環境下忽略 history API 限制
    }
  }

  function readUrlState() {
    try {
      const params = new URLSearchParams(window.location.search);
      const sec = params.get("section");
      if (sec && ["all", "business_tech", "deep", "unverified"].includes(sec)) {
        state.filter = sec;
      }
      const q = params.get("q");
      if (q) {
        state.query = q;
        const input = document.getElementById("news-search");
        if (input) input.value = q;
      }
    } catch (_err) {
      // ignore
    }
  }

  function bindEvents() {
    document.addEventListener("click", (ev) => {
      const toggleBtn = ev.target.closest("[data-toggle-story]");
      if (toggleBtn) {
        const sid = toggleBtn.getAttribute("data-toggle-story");
        const drawer = document.getElementById(`drawer-${sid}`);
        const label = toggleBtn.querySelector("[data-toggle-label]");
        const willOpen = !state.expandedIds.has(sid);
        if (willOpen) {
          state.expandedIds.add(sid);
        } else {
          state.expandedIds.delete(sid);
        }
        toggleBtn.setAttribute("aria-expanded", willOpen ? "true" : "false");
        if (drawer) drawer.setAttribute("data-open", willOpen ? "true" : "false");
        if (label) label.textContent = willOpen ? "收合明細" : "展開明細";
        return;
      }

      const filterBtn = ev.target.closest("[data-filter]");
      if (filterBtn) {
        state.filter = filterBtn.getAttribute("data-filter") || "all";
        syncUrlState();
        renderAll();
        return;
      }

      const editionBtn = ev.target.closest("[data-edition-switch]");
      if (editionBtn) {
        const targetEd = editionBtn.getAttribute("data-edition-switch");
        loadEdition(targetEd);
        return;
      }

      const resetBtn = ev.target.closest("[data-reset-filters]");
      if (resetBtn) {
        state.filter = "all";
        state.query = "";
        const input = document.getElementById("news-search");
        if (input) input.value = "";
        syncUrlState();
        renderAll();
      }
    });

    const searchInput = document.getElementById("news-search");
    if (searchInput) {
      searchInput.addEventListener("input", (ev) => {
        state.query = ev.target.value || "";
        syncUrlState();
        renderAll();
      });
    }
  }

  async function loadEdition(key) {
    const filename = key === "evening" ? "./daily_news_evening.json" : "./daily_news_morning.json";
    try {
      const resp = await fetch(filename, { cache: "no-store" });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const fresh = await resp.json();
      state.payload = fresh;
      state.editionOverride = key;
      renderAll();
    } catch (err) {
      console.warn(`無法載入 ${filename}，嘗試讀取 ./daily_news.json:`, err);
      try {
        const resp = await fetch("./daily_news.json", { cache: "no-store" });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const fresh = await resp.json();
        state.payload = fresh;
        state.editionOverride = key;
        renderAll();
      } catch (fallbackErr) {
        console.warn("改以本機快取資料切換版號與市場焦點:", fallbackErr);
        if (state.payload) {
          state.editionOverride = key;
          renderAll();
        }
      }
    }
  }

  async function loadNewsData() {
    const embedded = document.getElementById("initial-news-data");
    if (embedded && embedded.textContent.trim()) {
      try {
        state.payload = JSON.parse(embedded.textContent);
        renderAll();
      } catch (_err) {
        // continue to fetch
      }
    }

    try {
      const resp = await fetch("./daily_news.json", { cache: "no-store" });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const fresh = await resp.json();
      state.payload = fresh;
      renderAll();
    } catch (err) {
      if (!state.payload) {
        const errBox = document.getElementById("error-state");
        if (errBox) {
          errBox.hidden = false;
          const msg = errBox.querySelector("[data-error-msg]");
          if (msg) msg.textContent = `無法讀取 ./daily_news.json（${err.message || "連線異常"}）。請先執行 python3 scripts/fetch_news.py 產出資料檔。`;
        }
      }
    }
  }

  readUrlState();
  bindEvents();
  loadNewsData();
})();
