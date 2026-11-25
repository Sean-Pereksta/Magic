/* Card Realms: tiny, inked SVG illustrations shared by solo and multiplayer. */
(function (root) {
  "use strict";

  const INK = "#111c30";
  const PAPER = "#fff0cf";
  const PALETTES = {
    tf:   { main: "#55cbea", light: "#c1f5fa", dark: "#287f9e", sky: "#142d43", planet: "#316480" },
    blob: { main: "#9be95a", light: "#e1ffa7", dark: "#4d9559", sky: "#19372f", planet: "#397750" },
    se:   { main: "#f776a8", light: "#ffcfda", dark: "#a43f72", sky: "#37233e", planet: "#704365" },
    mc:   { main: "#b6a1ed", light: "#e4dafe", dark: "#736398", sky: "#282b46", planet: "#57517e" },
    sd:   { main: "#ffc85e", light: "#fff3b2", dark: "#c68b38", sky: "#3b3040", planet: "#8c6348" },
    vs:   { main: "#a58bff", light: "#e0cfff", dark: "#6754b4", sky: "#241f42", planet: "#574485" },
    n:    { main: "#e8d4ad", light: "#fff4d9", dark: "#ad9778", sky: "#243344", planet: "#4d6679" }
  };

  function hash(value) {
    let h = 2166136261;
    for (const char of String(value || "scout")) h = Math.imul(h ^ char.charCodeAt(0), 16777619);
    return h >>> 0;
  }

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

  function stars(seed, p) {
    let result = '<rect width="160" height="88" rx="9" fill="' + p.sky + '"/>';
    const planetX = 123 + seed % 18;
    result += '<circle cx="' + planetX + '" cy="16" r="19" fill="' + p.planet + '"/><path d="M112 22q23 8 42-9" fill="none" stroke="' + p.light + '" stroke-opacity=".2" stroke-width="3"/>';
    for (let i = 0; i < 14; i++) {
      const v = hash(seed + ":" + i);
      const x = 7 + v % 146;
      const y = 7 + (v >>> 9) % 74;
      result += '<circle cx="' + x + '" cy="' + y + '" r="' + (i % 4 === 0 ? 1.2 : .7) + '" fill="' + PAPER + '" opacity="' + (i % 3 === 0 ? .7 : .35) + '"/>';
    }
    return result + '<path d="M17 15v6m-3-3h6M142 66v6m-3-3h6" stroke="' + p.light + '" stroke-width="1.2" opacity=".65"/>';
  }

  function flame(p, variant) {
    return '<path d="M-31-7Q-' + (51 + variant * 2) + ' -9-57 0q17 11 26 7" fill="#ffb76c"/><path d="M-32-4-47 0l15 4" fill="' + PAPER + '" stroke="none"/>';
  }

  function ship(card, p, v) {
    const trade = (card.play || []).reduce((n, e) => n + (e.t === "trade" ? Number(e.n) || 0 : 0), 0);
    const combat = (card.play || []).reduce((n, e) => n + (e.t === "combat" ? Number(e.n) || 0 : 0), 0);
    const battle = combat > trade;
    const wing = 19 + v * 3;
    const main = p.main, light = p.light, dark = p.dark;
    let shape = flame(p, v);
    switch (card.faction) {
      case "tf":
        shape += '<path d="M-20-8-33-' + wing + ' -1-' + (wing - 3) + ' 17-7M-20 8-33 ' + wing + ' -1 ' + (wing - 3) + ' 17 7" fill="' + dark + '"/>';
        shape += '<path d="M-33-12H9Q28-12 42 0 28 12 9 12h-42Z" fill="' + main + '"/><path d="M11-10Q23-8 30 0H9Z" fill="' + light + '"/>';
        shape += '<path d="M-24-9v18m10-18v18" stroke="' + light + '" stroke-width="3"/>';
        if (/medical|hospital|lifeboat|peace|outreach/.test(card.key || "")) shape += '<path d="M-3-6v12m-6-6H3" stroke="' + PAPER + '" stroke-width="4"/>';
        else shape += '<circle cx="-2" cy="1" r="4" fill="' + PAPER + '"/>';
        break;
      case "blob":
        shape = '<path d="M-20-13Q-39-' + (wing + 7) + ' -45-16q-3 12 14 16-17 6-13 18 8 7 24-6" fill="' + dark + '"/>';
        shape += '<path d="M-28-8Q-38-' + wing + ' -12-23-4-' + (wing + 7) + ' 8-18 25-23 35-7 44 4 23 17 6 26-15 16-35 25-28-8Z" fill="' + main + '"/>';
        shape += '<path d="M-22-11Q-12-23 2-13M-15 12q14 5 22-1" fill="none" stroke="' + light + '" stroke-width="4"/>';
        shape += '<ellipse cx="20" cy="-5" rx="7" ry="9" fill="' + PAPER + '"/><ellipse cx="22" cy="-4" rx="3" ry="5" fill="' + INK + '" stroke="none"/><path d="M20 10q9 1 12-6" fill="none"/>';
        shape += '<circle cx="-15" cy="-1" r="' + (3 + v) + '" fill="' + dark + '"/><circle cx="3" cy="7" r="3" fill="' + dark + '"/>';
        break;
      case "se":
        shape += '<path d="M-27-5-39-' + (wing + 2) + ' 17-15 42 0 17 15-39 ' + (wing + 2) + ' -27 5Z" fill="' + dark + '"/>';
        shape += '<path d="M-34-8 11-12 46 0 11 12-34 8-21 0Z" fill="' + main + '"/><path d="M-18-5 8-6 26 0 8 6-18 5Z" fill="' + light + '"/>';
        shape += '<path d="M-29-' + (wing - 3) + ' 8-11m-37 ' + (wing * 2 - 6) + 'L8 11" stroke="' + main + '" stroke-width="3"/><path d="m1-5 9 5-9 5" fill="' + INK + '"/>';
        break;
      case "mc":
        shape += '<path d="M-29-9v-' + (10 + v * 2) + 'h31v8M-29 9v' + (10 + v * 2) + 'H2v-8" fill="' + dark + '"/>';
        shape += '<path d="M-30-12H9l13 7v10L9 12h-39Z" fill="' + main + '"/><path d="M20-8 31-18h12v8H33l-5 8m-8 10 11 10h12v-8H33l-5-8" fill="' + light + '"/>';
        shape += '<rect x="-21" y="-7" width="19" height="14" rx="3" fill="' + dark + '"/><circle cx="-11" cy="0" r="5" fill="#ffd881"/><path d="M4-7v14m5-11v8" stroke="' + light + '" stroke-width="3"/>';
        break;
      case "sd":
        shape += '<path d="M-19-4-31-' + wing + ' -2-17 13-5M-19 4-31 ' + wing + ' -2 17 13 5" fill="' + main + '"/><path d="M-23-5-32-' + (wing - 6) + ' -4-10m-19 15-9 ' + (wing - 6) + ' 28-13" fill="' + light + '"/>';
        shape += '<path d="M-30-6-5-12 45 0-5 12-30 6-20 0Z" fill="' + main + '"/><path d="M-8-6 25 0-8 6Z" fill="' + light + '"/><circle cx="-12" cy="0" r="7" fill="#fff4b6"/><circle cx="-12" cy="0" r="3" fill="' + dark + '" stroke="none"/>';
        shape += '<path d="M-6-' + (wing - 4) + ' 20-12M-6 ' + (wing - 4) + ' 20 12" stroke="' + PAPER + '" stroke-width="2"/>';
        break;
      case "vs":
        shape = '<path d="M-10-23q-36 3-36 23 0 22 36 23-16-9-16-23t16-23Z" fill="' + dark + '"/>';
        shape += '<path d="M-28-3-18-' + wing + ' 30-17 4-6 44 0 4 6 30 17-18 ' + wing + ' -28 3-39 0Z" fill="' + main + '"/><path d="M-14-7 12-10-2-2m-12 9 26 3L-2 2" fill="' + light + '"/><path d="m-22 0 13-5 9 5-9 5Z" fill="' + INK + '"/><circle cx="-10" cy="0" r="2.5" fill="#b4ffff" stroke="none"/>';
        shape += '<path d="M-42-12h-9m9 24h-9" stroke="' + light + '" stroke-width="2"/>';
        break;
      default:
        if (v === 0) shape += '<path d="M-23-7-28-24 0-16 10-5M-23 7-28 24 0 16 10 5" fill="' + dark + '"/>';
        if (v === 1) shape += '<rect x="-29" y="-21" width="34" height="42" rx="6" fill="' + dark + '"/><path d="M-22-18v36m14-36v36" stroke="' + main + '" stroke-width="3"/>';
        if (v === 2) shape += '<path d="M-28-8-12-25 16-19 0-6M-28 8-12 25 16 19 0 6" fill="' + dark + '"/>';
        if (v === 3) shape += '<path d="M-30-9-36-23 17-17 25-7M-30 9-36 23 17 17 25 7" fill="' + dark + '"/>';
        shape += '<path d="M-33-10H8Q23-9 38 0 23 9 8 10h-41l9-10Z" fill="' + main + '"/><path d="M3-7h7l16 7H3Z" fill="#91deec"/><path d="M-15-8v16" stroke="' + light + '" stroke-width="5"/>';
    }
    if (battle && card.faction !== "blob") shape += '<path d="M-3-15h25m-25 30h25" stroke="' + INK + '" stroke-width="6"/><path d="M4-15h17M4 15h17" stroke="' + light + '" stroke-width="2"/>';
    if (!battle && trade >= 3 && card.faction !== "blob") shape += '<rect x="-29" y="12" width="15" height="9" rx="2" fill="' + light + '"/><path d="M-22 13v7"/>';
    return '<g transform="translate(82 47) rotate(' + [-5, 3, -2, 5][v] + ')" fill="none" stroke="' + INK + '" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round">' + shape + '</g>';
  }

  function station(card, p, v) {
    let shape = '';
    if (card.outpost) {
      shape += '<path d="M0-36 42-20v23C42 21 19 33 0 39-19 33-42 21-42 3v-23Z" fill="' + p.dark + '" stroke="' + p.light + '" stroke-width="1.8"/><path d="M-30-11v15m60-15v15" stroke="' + p.main + '" stroke-width="4"/>';
    } else {
      shape += '<ellipse cy="6" rx="50" ry="15" transform="rotate(-16)" fill="none" stroke="' + p.dark + '" stroke-width="5"/>';
    }
    if (card.faction === "blob") {
      shape += '<path d="M-37 18q-9-18 8-25-6-25 12-20 13-18 23 0 20-13 22 9 21 2 13 23 10 15-9 22-16 8-27-2-27 13-42-7Z" fill="' + p.main + '"/>';
      shape += '<path d="M-27 14q11-20 29-18 20 1 29 20" fill="' + p.dark + '"/><ellipse cy="5" rx="13" ry="18" fill="' + p.light + '"/><circle cy="5" r="6" fill="' + p.sky + '"/><circle cx="-19" cy="-12" r="' + (3 + v) + '" fill="' + p.light + '"/><circle cx="23" cy="0" r="4" fill="' + p.light + '"/>';
    } else {
      if (v === 0) {
        shape += '<circle r="27" fill="' + p.dark + '"/><circle r="18" fill="' + p.sky + '"/><path d="M-37-7H37v14h-74Z" fill="' + p.main + '"/><rect x="-8" y="-34" width="16" height="68" rx="4" fill="' + p.main + '"/>';
      } else if (v === 1) {
        shape += '<path d="M-36 23v-36h12v-10h8v24h32v-24h8v10h12v36Z" fill="' + p.main + '"/><path d="M-37 18h74M-30-8v17m60-17v17" stroke="' + p.light + '" stroke-width="4"/><path d="m-14 0 14-15L14 0v23h-28Z" fill="' + p.dark + '"/>';
      } else if (v === 2) {
        shape += '<path d="M-38 17h76v10h-76ZM-31 14v-23h12v23m38 0V-9h12v23" fill="' + p.dark + '"/><path d="M-22 14V-4a22 22 0 0 1 44 0v18Z" fill="' + p.main + '"/><path d="M-14-8q13-14 27 0" fill="none" stroke="' + p.light + '" stroke-width="5"/><path d="M0-27v-8m-4 0h8"/>';
      } else {
        shape += '<path d="m0-29 26 14v30L0 29-26 15v-30Z" fill="' + p.main + '"/><path d="M-44-15h17V1h-17Zm71 0h17V1H27ZM-44 9h17v15h-17Zm71 0h17v15H27Z" fill="' + p.dark + '"/><path d="M-39-12v10m6-10v10m0 14v10m-6-10v10m72-34v10m6-10v10m-6 14v10m6-10v10" stroke="' + p.light + '" stroke-width="2"/>';
      }
      if (card.faction === "sd") shape += '<path d="M-16-23-27-31M16-23l11-8M-30 0h-10m70 0h10" stroke="' + p.main + '" stroke-width="4"/>';
      if (card.faction === "vs") shape += '<path d="M-16-25C-38-15-39 10-19 24c-8-18-8-31 3-49Z" fill="' + p.light + '"/>';
      if (card.faction === "se") shape += '<path d="m-25-11-7-20 18 11M25-11l7-20-18 11" fill="' + p.dark + '"/>';
      if (card.faction === "mc") shape += '<path d="M-8-29v-7H8v7m-16 57v7H8v-7" fill="' + p.light + '"/>';
      shape += '<circle cy="1" r="11" fill="' + p.light + '"/><circle cy="1" r="5" fill="' + p.dark + '"/>';
      if (card.faction === "tf") shape += '<path d="M0-5V7m-6-6H6" stroke="' + p.sky + '" stroke-width="3"/>';
      if (card.faction === "sd") shape += '<path d="m0-6 2 5 5 2-5 2-2 5-2-5-5-2 5-2Z" fill="' + p.main + '" stroke-width="1"/>';
    }
    return '<g transform="translate(80 43)" stroke="' + INK + '" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">' + shape + '</g>';
  }

  function illustration(card) {
    card = card || {};
    const p = palette(card.faction);
    const seed = hash(card.key || card.name);
    const variant = seed % 4;
    const scene = card.type === "base" ? station(card, p, variant) : ship(card, p, variant);
    return '<svg class="cr-illustration" viewBox="0 0 160 88" width="160" height="88" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false">' + stars(seed, p) + scene + '</svg>';
  }

  const api = Object.freeze({ illustration, icon, factionColor, factionMark });
  root.CardRealmsArt = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof globalThis !== "undefined" ? globalThis : window);
