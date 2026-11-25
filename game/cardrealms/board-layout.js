/* Fits the five public tabletop zones without changing cards or game rules. */
(function (root) {
  "use strict";
  const ids = ["enemyBases", "tradeRow", "myBases", "inPlay", "hand"];
  const doc = root.document;
  if (!doc) return;
  const zones = ids.map(id => doc.getElementById(id)).filter(Boolean);
  const resize = typeof root.ResizeObserver === "function" ? new root.ResizeObserver(schedule) : null;
  const mutations = typeof root.MutationObserver === "function" ? new root.MutationObserver(schedule) : null;
  let pending = 0;

  function measure(node) {
    const style = root.getComputedStyle(node);
    return {
      width: Math.max(0, node.clientWidth - parseFloat(style.paddingLeft || 0) - parseFloat(style.paddingRight || 0)),
      height: Math.max(0, node.clientHeight - parseFloat(style.paddingTop || 0) - parseFloat(style.paddingBottom || 0)),
      gap: parseFloat(style.columnGap) || 4
    };
  }

  function fitZone(node) {
    const cards = Array.from(node.children).filter(child => child.classList.contains("card"));
    const count = cards.length;
    const badge = doc.querySelector(`[data-zone-count="${node.id}"]`);
    if (badge) badge.textContent = String(count);
    const { width, height, gap } = measure(node);
    if (!count || width < 1 || height < 1) {
      node.dataset.overflow = "false";
      return;
    }
    const portrait = root.innerWidth < 900 && root.innerHeight > 500;
    const short = root.innerHeight <= 500;
    const maxWidth = portrait ? 86 : short ? 84 : 104;
    const minWidth = portrait || short ? 52 : 60;
    const ratio = 1.5;
    let best = { width: 0, columns: 1, rows: count };
    // Try every row/column arrangement and retain the biggest legible cards.
    for (let columns = 1; columns <= count; columns++) {
      const rows = Math.ceil(count / columns);
      const candidate = Math.min(maxWidth, (width - gap * (columns - 1)) / columns, (height - gap * (rows - 1)) / rows / ratio);
      if (candidate > best.width + .1 || (Math.abs(candidate - best.width) <= .1 && rows < best.rows)) best = { width: candidate, columns, rows };
    }
    const overflow = best.width < minWidth;
    if (overflow) {
      // A late-game pile can exceed a screen. Keep the whole board visible and
      // scroll just that pile instead of crushing text or moving the footer.
      const columns = Math.max(1, Math.floor((width + gap) / (minWidth + gap)));
      best = { width: Math.min(maxWidth, (width - gap * (columns - 1)) / columns), columns, rows: Math.ceil(count / columns) };
    }
    const cardWidth = Math.floor(Math.max(1, best.width) * 2) / 2;
    const cardHeight = Math.floor(cardWidth * ratio);
    node.style.setProperty("--cr-card-w", `${cardWidth}px`);
    node.style.setProperty("--cr-card-h", `${cardHeight}px`);
    node.style.setProperty("--cr-cols", String(best.columns));
    node.dataset.cardDensity = cardWidth < 80 ? "compact" : "normal";
    node.dataset.cardDetail = cardHeight < 110 ? "minimal" : "full";
    node.dataset.overflow = String(overflow);
  }

  function schedule() {
    if (pending) return;
    pending = root.requestAnimationFrame(() => {
      pending = 0;
      zones.forEach(fitZone);
    });
  }

  zones.forEach(node => {
    resize?.observe(node);
    mutations?.observe(node, { childList: true });
  });
  root.addEventListener("resize", schedule);
  root.visualViewport?.addEventListener("resize", schedule);

  const menu = doc.getElementById("cr-menu");
  const menuButton = doc.getElementById("crMenuBtn");
  function closeMenu() { if (menu?.open) menu.close(); }
  function openMenu() {
    root.CardRealmsUI?.closeInspector();
    if (menu && !menu.open) menu.showModal();
  }
  menuButton?.addEventListener("click", openMenu);
  menu?.addEventListener("click", event => {
    const control = event.target.closest("button");
    if (control?.matches("[data-close-menu], [data-board-action]")) closeMenu();
    if (event.target === menu) {
      const rect = menu.getBoundingClientRect();
      if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) closeMenu();
    }
  }, true);
  menu?.addEventListener("close", () => menuButton?.focus({ preventScroll: true }));

  const oldStats = new Map();
  const statObserver = typeof root.MutationObserver === "function" ? new root.MutationObserver(records => {
    for (const record of records) {
      const node = record.target.nodeType === 3 ? record.target.parentElement : record.target;
      const tracked = node.closest?.("#tradeVal, #combatVal, #enemyLine, #youLine");
      if (!tracked) continue;
      const next = tracked.textContent;
      if (oldStats.get(tracked) !== next) {
        const visual = tracked.closest(".chip, .cr-resource, .cr-score") || tracked;
        visual.classList.remove("cr-resource-flash");
        // Web Animations avoids a forced layout when a turn resolves many cards.
        if (!root.matchMedia("(prefers-reduced-motion: reduce)").matches) visual.animate?.([{ filter: "brightness(1.65)" }, { filter: "brightness(1)" }], { duration: 350, easing: "ease-out" });
        oldStats.set(tracked, next);
      }
    }
  }) : null;
  ["tradeVal", "combatVal", "enemyLine", "youLine"].forEach(id => {
    const node = doc.getElementById(id);
    if (!node) return;
    oldStats.set(node, node.textContent);
    statObserver?.observe(node, { childList: true, characterData: true, subtree: true });
  });

  root.OrbitalBoard = {
    refresh: schedule,
    setTurn(yourTurn) { doc.body.dataset.turn = yourTurn ? "you" : "opponent"; },
    openMenu,
    closeMenu
  };
  schedule();
})(typeof globalThis === "object" ? globalThis : window);
