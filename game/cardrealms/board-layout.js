/* Keeps each fleet zone readable and independently scrollable. */
(function (root) {
  "use strict";
  const ids = ["enemyBases", "tradeRow", "myBases", "enemyInPlay", "inPlay", "hand"];
  const doc = root.document;
  if (!doc) return;
  const zones = ids.map(id => doc.getElementById(id)).filter(Boolean);
  const resize = typeof root.ResizeObserver === "function" ? new root.ResizeObserver(schedule) : null;
  const mutations = typeof root.MutationObserver === "function" ? new root.MutationObserver(schedule) : null;
  let pending = 0;
  const scrollPositions = new WeakMap();

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
      node.closest(".cr-zone")?.setAttribute("data-scrollable", "false");
      node.tabIndex = -1;
      return;
    }
    const portrait = root.innerWidth < 900 && root.innerHeight > 500;
    const short = root.innerHeight <= 500;
    const deployed = node.id === "inPlay" || node.id === "enemyInPlay";
    const mainZone = node.id === "tradeRow" || node.id === "hand";
    const maxWidth = portrait ? 86 : short ? 84 : 104;
    const minWidth = portrait || short ? 52 : 72;
    const ratio = 1.5;
    // Size from the viewport, never the size of a player's collection. A new
    // base adds another row instead of making every existing card smaller.
    const preferredColumns = mainZone ? 5 : deployed ? 3 : 2;
    const columns = Math.max(1, Math.min(preferredColumns, Math.floor((width + gap) / (minWidth + gap))));
    const availableWidth = Math.max(1, (width - gap * (columns - 1)) / columns);
    const oneRowWidth = Math.max(minWidth, height / ratio);
    const cardWidth = Math.floor(Math.min(maxWidth, availableWidth, oneRowWidth) * 2) / 2;
    // Deployed ships can have several primary gains plus a real Scrap button.
    // Keep those controls inside the face even at the narrowest phone width.
    const cardHeight = Math.max(node.id === "inPlay" ? 92 : 0, Math.floor(cardWidth * ratio));
    const rows = Math.ceil(count / columns);
    const overflow = rows * cardHeight + Math.max(0, rows - 1) * gap > height + 1;
    node.style.setProperty("--cr-card-w", `${cardWidth}px`);
    node.style.setProperty("--cr-card-h", `${cardHeight}px`);
    node.style.setProperty("--cr-cols", String(Math.min(count, columns)));
    node.dataset.cardDensity = cardWidth < 80 ? "compact" : "normal";
    node.dataset.cardDetail = cardHeight < 110 ? "minimal" : "full";
    node.dataset.overflow = String(overflow);
    node.closest(".cr-zone")?.setAttribute("data-scrollable", String(overflow));
    node.tabIndex = overflow ? 0 : -1;
    if (overflow) node.scrollTop = scrollPositions.get(node) || 0;
  }

  function schedule() {
    if (pending) return;
    pending = root.requestAnimationFrame(() => {
      pending = 0;
      zones.forEach(fitZone);
    });
  }

  zones.forEach(node => {
    const title = node.closest(".cr-zone")?.getAttribute("aria-labelledby");
    node.setAttribute("role", "region");
    if (title) node.setAttribute("aria-labelledby", title);
    node.addEventListener("scroll", () => scrollPositions.set(node, node.scrollTop), { passive: true });
    resize?.observe(node);
    mutations?.observe(node, { childList: true });
  });
  root.addEventListener("resize", schedule);
  root.visualViewport?.addEventListener("resize", schedule);

  const menu = doc.getElementById("cr-menu");
  const menuButton = doc.getElementById("crMenuBtn");
  function closeMenu() { if (menu?.open) menu.close(); }
  function openMenu() {
    if (doc.body.classList.contains("cr-private-board") || doc.getElementById("hotseatHandoff")?.open) return;
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
