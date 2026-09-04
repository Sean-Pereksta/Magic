const ARMORY_POLISH_STYLE_ID = "warrealmsArmoryUiPolishStyles";
const ARMORY_POLISH_CLASS = "wrArmoryUiPolished";

function hasDocument() {
  return typeof document !== "undefined" && !!document.documentElement;
}

function injectArmoryPolishStyles() {
  if (!hasDocument() || document.getElementById(ARMORY_POLISH_STYLE_ID)) return;

  const style = document.createElement("style");
  style.id = ARMORY_POLISH_STYLE_ID;
  style.textContent = `
    body.armoryMode:not(.deckBuilderMode) .catalogGrid.storeCatalogGrid {
      height: auto !important;
      min-height: 0 !important;
      max-height: none !important;
      overflow: visible !important;
      align-content: start;
      padding-bottom: 12px;
      overscroll-behavior: auto;
    }

    body.armoryMode:not(.deckBuilderMode) .catalogGrid.storeCatalogGrid:empty { padding-bottom: 0; }
    body.armoryMode:not(.deckBuilderMode) .catalogGrid.storeCatalogGrid + * { margin-top: 0; }

    body.armoryMode:not(.deckBuilderMode) .armoryToolbar {
      overflow: hidden;
      border-bottom: 1px solid rgba(65, 81, 99, .8);
      background: linear-gradient(180deg, rgba(15, 23, 33, .98), rgba(9, 15, 23, .96));
      box-shadow: inset 0 -1px 0 rgba(255, 255, 255, .025);
    }

    body.armoryMode:not(.deckBuilderMode) .armoryTabsRow {
      gap: 7px;
      padding: 10px 12px 9px;
      border-bottom: 1px solid rgba(49, 63, 78, .78);
      background: rgba(7, 12, 18, .34);
    }

    body.armoryMode:not(.deckBuilderMode) .armoryTabsRow .tabBtn {
      min-height: 32px;
      border-color: #344456;
      border-radius: 999px;
      background: #121b25;
      padding: 6px 12px;
      color: #aeb9c6;
      box-shadow: none;
      transition: background .14s ease, border-color .14s ease, color .14s ease, transform .14s ease;
    }

    body.armoryMode:not(.deckBuilderMode) .armoryTabsRow .tabBtn:hover {
      transform: translateY(-1px);
      border-color: #52677d;
      background: #1a2633;
      color: #eef3f8;
    }

    body.armoryMode:not(.deckBuilderMode) .armoryTabsRow .tabBtn.active {
      border-color: #e3c64f;
      background: linear-gradient(135deg, #e8ce60, #c9a936);
      color: #101318;
      box-shadow: 0 5px 16px rgba(210, 177, 58, .18);
    }

    body.armoryMode:not(.deckBuilderMode) .armorySearchRow {
      grid-template-columns: minmax(240px, 1fr) minmax(180px, auto) auto;
      gap: 8px;
      padding: 10px 12px;
      background: rgba(10, 16, 24, .72);
    }

    body.armoryMode:not(.deckBuilderMode) .armorySearchRow .searchWrap { position: relative; }

    body.armoryMode:not(.deckBuilderMode) .armorySearchRow .searchWrap::before {
      content: "⌕";
      position: absolute;
      z-index: 2;
      left: 11px;
      top: 50%;
      transform: translateY(-52%);
      color: #718195;
      font-size: 15px;
      line-height: 1;
      pointer-events: none;
    }

    body.armoryMode:not(.deckBuilderMode) .armorySearchRow .search {
      min-height: 38px;
      border-color: #344557;
      border-radius: 10px;
      background: #090f16;
      padding-left: 32px;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, .018);
      transition: border-color .14s ease, box-shadow .14s ease, background .14s ease;
    }

    body.armoryMode:not(.deckBuilderMode) .armorySearchRow .search:hover {
      border-color: #465b70;
      background: #0c131c;
    }

    body.armoryMode:not(.deckBuilderMode) .armorySearchRow .search:focus {
      border-color: #728ba5;
      background: #0c141d;
      box-shadow: 0 0 0 3px rgba(114, 139, 165, .12);
      outline: none;
    }

    body.armoryMode:not(.deckBuilderMode) .catalogSortLabel {
      min-width: 182px;
      min-height: 38px;
      border-color: #344557;
      border-radius: 10px;
      background: #0d151e;
      padding-left: 9px;
      color: #7f8fa1;
    }

    body.armoryMode:not(.deckBuilderMode) .catalogSortLabel .catalogSelect {
      height: 29px;
      border-radius: 7px;
      background: #18232f;
    }

    body.armoryMode:not(.deckBuilderMode) .advancedFiltersBtn {
      min-height: 38px;
      border-color: #53687e;
      border-radius: 10px;
      background: linear-gradient(145deg, #1b2a38, #111b25);
      padding: 7px 11px;
      color: #f2f6fa;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, .035), 0 5px 15px rgba(0, 0, 0, .14);
      transition: transform .14s ease, border-color .14s ease, background .14s ease;
    }

    body.armoryMode:not(.deckBuilderMode) .advancedFiltersBtn::before {
      content: "☷";
      margin-right: 6px;
      color: #d9bd54;
      font-size: 11px;
    }

    body.armoryMode:not(.deckBuilderMode) .advancedFiltersBtn:hover {
      transform: translateY(-1px);
      border-color: #728aa2;
      background: linear-gradient(145deg, #223447, #15212d);
    }

    body.armoryMode:not(.deckBuilderMode) .advancedFiltersBtn span {
      min-width: 19px;
      height: 19px;
      border: 1px solid rgba(255, 255, 255, .2);
      background: #e3c64f;
      box-shadow: 0 2px 8px rgba(227, 198, 79, .18);
    }

    body.armoryMode:not(.deckBuilderMode) .advancedFiltersBtn.noFilters span {
      border-color: #3b4b5c;
      background: #253342;
      color: #8e9cad;
      box-shadow: none;
    }

    body.armoryMode:not(.deckBuilderMode) .filterChipRow {
      min-height: 0;
      gap: 6px;
      padding: 0 12px 10px;
      background: rgba(10, 16, 24, .72);
    }

    body.armoryMode:not(.deckBuilderMode) .filterChip {
      min-height: 26px;
      border-color: #3f5164;
      border-radius: 999px;
      background: #101923;
      padding: 4px 9px;
      color: #bcc7d2;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, .025);
    }

    body.armoryMode:not(.deckBuilderMode) .filterChip:hover {
      border-color: #667d94;
      background: #182532;
      color: #fff;
    }

    body.armoryMode:not(.deckBuilderMode) .filterChip.clear {
      border-color: #714757;
      background: #24131a;
      color: #f0a8b4;
    }

    .filterDrawerScrim {
      align-items: center;
      padding: 8px;
      background: rgba(2, 5, 8, .76);
      backdrop-filter: blur(8px);
    }

    .filterDrawer {
      width: min(448px, calc(100vw - 16px));
      height: calc(100dvh - 16px);
      border: 1px solid #405267;
      border-radius: 18px;
      background:
        radial-gradient(440px 220px at 100% 0, rgba(81, 120, 160, .12), transparent 67%),
        linear-gradient(180deg, #111a24, #090f16);
      box-shadow: -24px 0 70px rgba(0, 0, 0, .48), 0 18px 60px rgba(0, 0, 0, .42);
      overflow: hidden;
    }

    .filterDrawerHead {
      min-height: 70px;
      padding: 13px 15px;
      border-bottom-color: #304052;
      background: rgba(12, 19, 28, .82);
    }

    .filterDrawerHead h2 {
      margin-top: 3px;
      font-size: 19px;
      letter-spacing: -.015em;
    }

    .filterDrawerHead .eyebrow {
      color: #d8bc50;
      font-size: 7px;
    }

    .filterDrawerBody {
      gap: 10px;
      padding: 11px;
      scrollbar-width: thin;
      scrollbar-color: #45596d #0a1017;
    }

    .filterDrawerBody::-webkit-scrollbar { width: 9px; }
    .filterDrawerBody::-webkit-scrollbar-track { background: #0a1017; }
    .filterDrawerBody::-webkit-scrollbar-thumb {
      border: 2px solid #0a1017;
      border-radius: 999px;
      background: #45596d;
    }

    .filterGroup {
      border-color: #2e3e4f;
      border-radius: 13px;
      background: linear-gradient(145deg, #101923, #0b121a);
      padding: 10px;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, .02);
    }

    .filterGroup h3 {
      margin-bottom: 9px;
      color: #8f9fb0;
      font-size: 8px;
      letter-spacing: .13em;
    }

    .filterCheckGrid, .filterCheckGrid.abilities { gap: 6px; }

    .filterCheck {
      min-height: 36px;
      border-color: #314255;
      border-radius: 9px;
      background: #111b25;
      padding: 7px 8px;
      color: #c7d0da;
      cursor: pointer;
      transition: border-color .14s ease, background .14s ease, transform .14s ease, box-shadow .14s ease;
    }

    .filterCheck:hover {
      transform: translateY(-1px);
      border-color: #51677e;
      background: #172430;
    }

    .filterCheck input[type="checkbox"], .filterToggle input[type="checkbox"] {
      appearance: none;
      -webkit-appearance: none;
      width: 16px;
      height: 16px;
      flex: 0 0 16px;
      margin: 0;
      border: 1px solid #566a7f;
      border-radius: 5px;
      background: #080e14;
      display: grid;
      place-items: center;
      cursor: pointer;
      transition: border-color .12s ease, background .12s ease, box-shadow .12s ease;
    }

    .filterCheck input[type="checkbox"]::after, .filterToggle input[type="checkbox"]::after {
      content: "✓";
      transform: scale(0);
      color: #11151a;
      font-size: 11px;
      font-weight: 1000;
      transition: transform .1s ease;
    }

    .filterCheck input[type="checkbox"]:checked, .filterToggle input[type="checkbox"]:checked {
      border-color: #e1c44e;
      background: #e1c44e;
      box-shadow: 0 0 0 3px rgba(225, 196, 78, .1);
    }

    .filterCheck input[type="checkbox"]:checked::after, .filterToggle input[type="checkbox"]:checked::after { transform: scale(1); }

    .filterCheck:has(input:checked), .filterToggle:has(input:checked) {
      border-color: #6a7883;
      background: linear-gradient(135deg, rgba(225, 196, 78, .09), #141f2a);
      box-shadow: inset 0 0 0 1px rgba(225, 196, 78, .07);
    }

    .filterCheck.yellow:has(input:checked) { border-color: color-mix(in srgb, var(--yellow) 72%, #465567); }
    .filterCheck.blue:has(input:checked) { border-color: color-mix(in srgb, var(--blue) 72%, #465567); }
    .filterCheck.green:has(input:checked) { border-color: color-mix(in srgb, var(--green) 72%, #465567); }
    .filterCheck.red:has(input:checked) { border-color: color-mix(in srgb, var(--red) 72%, #465567); }

    .filterFieldGrid { gap: 7px; }

    .filterFieldGrid label, .filterFullField, .filterRange label {
      color: #8393a5;
      font-size: 6.5px;
      letter-spacing: .09em;
    }

    .filterFieldGrid select, .filterFullField select, .filterRange input {
      min-height: 37px;
      border-color: #34475a;
      border-radius: 9px;
      background: #0d151e;
      color: #eef3f8;
      transition: border-color .13s ease, background .13s ease, box-shadow .13s ease;
    }

    .filterFieldGrid select:hover, .filterFullField select:hover, .filterRange input:hover {
      border-color: #4e657c;
      background: #111c27;
    }

    .filterFieldGrid select:focus, .filterFullField select:focus, .filterRange input:focus {
      border-color: #708aa4;
      box-shadow: 0 0 0 3px rgba(112, 138, 164, .1);
      outline: none;
    }

    .filterToggle {
      min-height: 37px;
      border-color: #34475a;
      border-radius: 9px;
      background: #0d151e;
    }

    .filterDrawerFoot {
      gap: 8px;
      padding: 10px 11px calc(10px + var(--safeBottom));
      border-top-color: #304052;
      background: rgba(9, 15, 22, .96);
      box-shadow: 0 -12px 28px rgba(0, 0, 0, .18);
    }

    .filterDrawerFoot .btn {
      min-height: 40px;
      border-radius: 10px;
    }

    .filterDrawerFoot .btn.primary, .filterDrawerFoot .btn.good { box-shadow: 0 7px 18px rgba(0, 0, 0, .2); }

    @media (max-width: 760px) {
      body.armoryMode:not(.deckBuilderMode) .armorySearchRow { grid-template-columns: 1fr auto; }
      body.armoryMode:not(.deckBuilderMode) .armorySearchRow .searchWrap { grid-column: 1 / -1; }
      body.armoryMode:not(.deckBuilderMode) .catalogSortLabel { min-width: 0; }
      .filterDrawer {
        width: min(520px, calc(100vw - 12px));
        height: calc(100dvh - 12px);
        border-radius: 16px;
      }
      .filterDrawerScrim { padding: 6px; }
    }

    @media (max-width: 520px) {
      body.armoryMode:not(.deckBuilderMode) .armoryTabsRow { padding-inline: 8px; }
      body.armoryMode:not(.deckBuilderMode) .armorySearchRow {
        grid-template-columns: minmax(0, 1fr) auto;
        padding: 8px;
      }
      body.armoryMode:not(.deckBuilderMode) .filterChipRow { padding: 0 8px 8px; }
      body.armoryMode:not(.deckBuilderMode) .catalogSortLabel { grid-column: 1; }
      body.armoryMode:not(.deckBuilderMode) .advancedFiltersBtn { grid-column: 2; }
      .filterCheckGrid, .filterCheckGrid.abilities, .filterFieldGrid { grid-template-columns: 1fr; }
      .filterDrawerHead {
        min-height: 62px;
        padding: 10px 11px;
      }
      .filterDrawerBody { padding: 8px; }
      .filterGroup { padding: 9px; }
    }

    @media (prefers-reduced-motion: reduce) {
      .filterCheck,
      body.armoryMode:not(.deckBuilderMode) .armoryTabsRow .tabBtn,
      body.armoryMode:not(.deckBuilderMode) .advancedFiltersBtn { transition: none; }
    }
  `;

  document.head.appendChild(style);
}

function polishArmoryDom() {
  if (!hasDocument() || !document.body) return;

  const inArmory = document.body.classList.contains("armoryMode");
  document.body.classList.toggle(ARMORY_POLISH_CLASS, inArmory);
  if (!inArmory) return;

  document.querySelectorAll(".catalogGrid.storeCatalogGrid").forEach(grid => {
    grid.removeAttribute("data-wr-fixed-catalog-height");
    grid.style.removeProperty("height");
    grid.style.removeProperty("max-height");
    grid.style.removeProperty("min-height");
  });

  const filterButton = document.querySelector(".advancedFiltersBtn");
  if (filterButton && !filterButton.getAttribute("aria-label")) {
    filterButton.setAttribute("aria-label", "Open card filters");
  }

  const search = document.querySelector(".armorySearchRow .search");
  if (search && !search.getAttribute("aria-label")) {
    search.setAttribute("aria-label", "Search Armory cards");
  }
}

function installArmoryPolish() {
  if (!hasDocument()) return;
  injectArmoryPolishStyles();

  const start = () => {
    polishArmoryDom();
    if (!document.body) return;

    let queued = false;
    const refresh = () => {
      if (queued) return;
      queued = true;
      const schedule = typeof requestAnimationFrame === "function" ? requestAnimationFrame : callback => setTimeout(callback, 0);
      schedule(() => {
        queued = false;
        polishArmoryDom();
      });
    };

    const observer = new MutationObserver(refresh);
    observer.observe(document.body, {
      childList: true,
      subtree: true,
      attributes: true,
      attributeFilter: ["class", "style"]
    });
  };

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start, { once: true });
  else start();
}

installArmoryPolish();

export { installArmoryPolish, polishArmoryDom };
