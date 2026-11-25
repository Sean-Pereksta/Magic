/* Orbital Rivals: painted card artwork and shared action icons. */
(function (root) {
  "use strict";

  const PALETTES = {
    tf:   { main: "#55cbea", light: "#c1f5fa", dark: "#287f9e", sky: "#142d43", planet: "#316480" },
    blob: { main: "#9be95a", light: "#e1ffa7", dark: "#4d9559", sky: "#19372f", planet: "#397750" },
    se:   { main: "#f776a8", light: "#ffcfda", dark: "#a43f72", sky: "#37233e", planet: "#704365" },
    mc:   { main: "#b6a1ed", light: "#e4dafe", dark: "#736398", sky: "#282b46", planet: "#57517e" },
    sd:   { main: "#ffc85e", light: "#fff3b2", dark: "#c68b38", sky: "#3b3040", planet: "#8c6348" },
    vs:   { main: "#a58bff", light: "#e0cfff", dark: "#6754b4", sky: "#241f42", planet: "#574485" },
    n:    { main: "#e8d4ad", light: "#fff4d9", dark: "#ad9778", sky: "#243344", planet: "#4d6679" }
  };

  function palette(key) { return PALETTES[key] || PALETTES.n; }
  function factionColor(key) { return palette(key).main; }

  const ICONS = {
    trade: '<path d="m12 2 9 8-9 12L3 10Z"/><path d="M3 10h18M8 4l4 6 4-6m-4 6v12"/>',
    combat: '<path d="m14 3 7-1-1 7-11 11-5-5Z"/><path d="m4 12 8 8m-8-1-2 3m11-9 4-4"/>',
    authority: '<path fill="currentColor" fill-opacity=".24" d="M12 21 3.5 12.5C-2 7 5 0 10 5l2 2 2-2c5-5 12 2 6.5 7.5Z"/><path d="M5 9h4l2-3 2 8 2-4h4"/>',
    draw: '<rect x="7" y="3" width="13" height="17" rx="2"/><path d="M7 7H4a2 2 0 0 0-2 2v11a2 2 0 0 0 2 2h11m-4-11h6m-3-3v6"/>',
    defense: '<path fill="currentColor" fill-opacity=".18" d="m12 2 9 4v6c0 5-5 9-9 11-4-2-9-6-9-11V6Z"/><path d="M12 6v12M7 9h10"/>',
    ally: '<path d="m9 7 2-2a5 5 0 0 1 7 7l-3 3m-6-6-3 3a5 5 0 0 0 7 7l2-2m-7-1 8-8"/>',
    scrap: '<path d="m8 6 3-4 4 6m-4-6 3 1M5 9l-3 6 7 1m-7-1 2 3m9 3h7l-3-6m3 6 1-3M4 18l3 3m11-15 3 5m-8 0-3 5h6Z"/>',
    discard: '<path d="M3 6h18M8 6V3h8v3M5 6l1 16h12l1-16M10 10v8m4-8v8"/>',
    copy: '<rect x="8" y="7" width="13" height="15" rx="2"/><path d="M16 7V2H3v15h5m4-6h5m-5 4h5"/>',
    market: '<path d="M3 9v12h18V9M2 8l2-6h16l2 6M2 8c0 4 5 4 5 0 0 4 5 4 5 0 0 4 5 4 5 0 0 4 5 4 5 0M9 21v-7h6v7"/>',
    special: '<path fill="currentColor" fill-opacity=".2" d="m12 2 2.5 7.5L22 12l-7.5 2.5L12 22l-2.5-7.5L2 12l7.5-2.5Z"/><path d="m18 3 3 3M3 18l3 3"/>',
    nextDraw: '<rect x="3" y="5" width="12" height="16" rx="2"/><path d="M8 2h11a2 2 0 0 1 2 2v11M7 13h4m7-5 2 3 2-3"/>',
    action: '<path fill="currentColor" fill-opacity=".2" d="m14 2-10 12h7l-1 8L21 9h-8Z"/>',
    info: '<circle cx="12" cy="12" r="10"/><path d="M12 11v6m-1 0h3"/><circle cx="12" cy="7" r="1" fill="currentColor" stroke="none"/>',
    close: '<path d="m6 6 12 12M6 18 18 6"/>',
    ship: '<path fill="currentColor" fill-opacity=".2" d="m13 2 5 12 4 6-9-3-9 3 4-6Z"/><path d="M13 7v6m-3 7-1 3m7-3 1 3"/>',
    base: '<path d="M3 20h18M6 20V9h12v11M4 9l8-7 8 7M10 20v-5h4v5m-4-7h4"/>',
    outpost: '<path d="M3 22V9h5v13m8 0V9h5v13M3 9V4h2v2h1V4h2v5m8 0V4h2v2h1V4h2v5M8 11h8M8 22v-5a4 4 0 0 1 8 0v5"/>',
    stun: '<path d="m12 2 2 6 6-3-3 6 6 2-7 2 1 7-5-5-5 5 1-7-7-2 6-2-3-6 6 3Z"/>',
    tf: '<circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3c-6 6-6 12 0 18 6-6 6-12 0-18Z"/>',
    blob: '<path fill="currentColor" fill-opacity=".2" d="M4 9c-5-5 4-10 7-4 3-7 12-2 8 4 7 3 3 12-3 9-3 7-12 2-9-3-6 2-7-4-3-6Z"/><circle cx="10" cy="11" r="1" fill="currentColor"/><circle cx="16" cy="10" r="1" fill="currentColor"/>',
    se: '<path fill="currentColor" fill-opacity=".2" d="m12 2 3 7 7 1-6 5 2 7-6-4-6 4 2-7-6-5 7-1Z"/>',
    mc: '<path d="M8 3h8l1 4 4 1v8l-4 1-1 4H8l-1-4-4-1V8l4-1Z"/><circle cx="12" cy="12" r="4"/>',
    sd: '<circle cx="12" cy="12" r="5" fill="currentColor" fill-opacity=".2"/><path d="M12 1v3m0 16v3M1 12h3m16 0h3M4 4l2 2m12 12 2 2M4 20l2-2M18 6l2-2"/>',
    vs: '<path fill="currentColor" fill-opacity=".2" d="M18 3C4-2-4 17 9 22c6 2 11-2 13-6-10 3-16-7-4-13Z"/>',
    n: '<path d="m12 2 8 5v10l-8 5-8-5V7Z"/><path d="m8 12 4-5 4 5-4 5Z"/>'
  };
  const ALIASES = { auth: "authority", health: "authority", damage: "combat", bolt: "action", opp_discard: "discard", opp_next_draw: "nextDraw", copy_next_ship: "copy", scrap_trade_row: "market" };

  function icon(name) {
    const key = ALIASES[name] || name;
    return '<svg class="cr-icon" viewBox="0 0 24 24" width="24" height="24" xmlns="http://www.w3.org/2000/svg" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">' + (ICONS[key] || ICONS.special) + '</svg>';
  }
  function factionMark(key) { return icon(PALETTES[key] ? key : "n"); }

  // Capture the module URL synchronously; cards may render long after this
  // script has finished, when document.currentScript is no longer available.
  const scriptURL = root.document && root.document.currentScript && root.document.currentScript.src;
  const ATLAS_URL = scriptURL
    ? new URL("assets/orbital-rivals-card-atlas.webp", scriptURL).href
    : "./cardrealms/assets/orbital-rivals-card-atlas.webp";

  // Generated scene boundaries, with a three-pixel inset to prevent seams.
  // Embedded from assets/orbital-rivals-art-atlas.json so the first render
  // never depends on an asynchronous metadata request.
  const FRAMES = {
    n: { scout: [3,3,307,172], capital: [316,3,308,172], base: [630,3,308,172], outpost: [944,3,307,172] },
    tf: { scout: [3,181,307,173], capital: [316,181,308,173], base: [630,181,308,173], outpost: [944,181,307,173] },
    blob: { scout: [3,360,307,171], capital: [316,360,308,171], base: [630,360,308,171], outpost: [944,360,307,171] },
    se: { scout: [3,537,307,174], capital: [316,537,308,174], base: [630,537,308,174], outpost: [944,537,307,174] },
    mc: { scout: [3,717,307,165], capital: [316,717,308,165], base: [630,717,308,165], outpost: [944,717,307,165] },
    sd: { scout: [3,888,307,164], capital: [316,888,308,164], base: [630,888,308,164], outpost: [944,888,307,164] },
    vs: { scout: [3,1058,307,193], capital: [316,1058,308,193], base: [630,1058,308,193], outpost: [944,1058,307,193] }
  };

  function artFrame(card) {
    card = card || {};
    const faction = Object.prototype.hasOwnProperty.call(FRAMES, card.faction) ? card.faction : "n";
    let role;
    if (card.type === "base") {
      role = card.outpost ? "outpost" : "base";
    } else {
      const combat = (card.play || []).reduce((sum, effect) => sum + (effect.t === "combat" ? Number(effect.n) || 0 : 0), 0);
      const name = `${card.key || ""} ${card.name || ""}`;
      const capital = Number(card.cost) >= 5 || combat >= 3 || /battleship|carrier|dreadnought|titan|cruiser|gunship|frigate|destroyer|flagship|bomber|behemoth|leviathan/i.test(name);
      role = capital ? "capital" : "scout";
    }
    const [x, y, width, height] = FRAMES[faction][role];
    return { faction, role, x, y, width, height, atlasWidth: 1254, atlasHeight: 1254, atlasURL: ATLAS_URL };
  }

  function escapeXML(value) {
    return String(value).replace(/[&<>"']/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&apos;" })[char]);
  }

  function illustration(card) {
    card = card || {};
    const frame = artFrame(card);
    return `<svg class="cr-painted-art cr-illustration" viewBox="${frame.x} ${frame.y} ${frame.width} ${frame.height}" width="160" height="88" preserveAspectRatio="xMidYMid slice" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false"><title>${escapeXML(card.name || "Orbital Rivals spacecraft")}</title><image href="${escapeXML(ATLAS_URL)}" x="0" y="0" width="1254" height="1254" preserveAspectRatio="none"/></svg>`;
  }

  const api = Object.freeze({ illustration, artFrame, icon, factionColor, factionMark });
  root.CardRealmsArt = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof globalThis !== "undefined" ? globalThis : window);
