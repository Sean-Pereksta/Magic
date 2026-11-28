/* Original card art and the shared scoring presentation for Balatle. */
(function (root) {
  "use strict";

  const SUITS = { S: "♠", H: "♥", D: "♦", C: "♣" };
  const SUIT_PATHS = {
    S: ["M5 0H7V1H8V2H9V3H10V4H11V5H12V8H11V9H7V10H8V11H9V12H3V11H4V10H5V9H1V8H0V5H1V4H2V3H3V2H4V1H5Z", "M4 3H5V4H4V5H3V6H2V5H3V4H4Z"],
    H: ["M1 1H4V2H5V3H7V2H8V1H11V2H12V6H11V7H10V8H9V9H8V10H7V11H5V10H4V9H3V8H2V7H1V6H0V2H1Z", "M1 3H3V4H2V5H1Z"],
    D: ["M5 0H7V1H8V2H9V3H10V4H11V5H12V7H11V8H10V9H9V10H8V11H7V12H5V11H4V10H3V9H2V8H1V7H0V5H1V4H2V3H3V2H4V1H5Z", "M4 3H6V4H5V5H4V6H2V5H3V4H4Z"],
    C: ["M4 0H8V1H9V4H8V5H9V4H11V5H12V8H11V9H7V10H8V11H9V12H3V11H4V10H5V9H1V8H0V5H1V4H3V5H4V4H3V1H4Z", "M5 1H6V2H5V3H4V2H5ZM1 6H3V7H1Z"]
  };
  const PIP_ART = Object.fromEntries(Object.entries(SUIT_PATHS).map(([suit, [silhouette, gleam]]) => [
    suit, `<path d="${silhouette}" fill="currentColor"/><path d="${gleam}" fill="var(--pip-shine, #fff5c5)"/>`
  ]));

  // Pixel rows are grouped by color so portraits only need a handful of SVG paths.
  function pixelPaths(rows, palette) {
    const paths = {};
    rows.forEach((row, y) => {
      for (let x = 0; x < row.length;) {
        const pixel = row[x];
        let end = x + 1;
        while (row[end] === pixel) end++;
        if (palette[pixel]) paths[pixel] = (paths[pixel] || "") + `M${x} ${y}h${end - x}v1H${x}z`;
        x = end;
      }
    });
    return Object.entries(paths).map(([pixel, d]) => `<path d="${d}" fill="${palette[pixel]}"/>`).join("");
  }

  const esc = value => String(value == null ? "" : value).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const hash = value => Array.from(String(value || "Balatle")).reduce((n, c) => ((n * 31) + c.charCodeAt(0)) >>> 0, 7);
  const finite = (n, fallback = 0) => Number.isFinite(Number(n)) ? Number(n) : fallback;
  const number = n => finite(n).toLocaleString(undefined, { maximumFractionDigits: 2 });

  function pip(suit, x, y, size = 1, rotation = 0, color) {
    const tint = color ? ` color="${color}"` : "";
    return `<g${tint} transform="translate(${x} ${y}) rotate(${rotation}) scale(${size * 2}) translate(-6 -6)">${PIP_ART[suit]}</g>`;
  }

  function cornerPip(suit) {
    return `<svg class="suit" viewBox="0 0 12 12" aria-hidden="true" focusable="false">${PIP_ART[suit]}</svg>`;
  }

  function numberArtwork(rank, suit) {
    const n = Number(rank);
    const positions = {
      2: [[40, 26], [40, 102, 180]],
      3: [[40, 26], [40, 64], [40, 102, 180]],
      4: [[19, 26], [61, 26], [19, 102, 180], [61, 102, 180]],
      5: [[19, 26], [61, 26], [40, 64], [19, 102, 180], [61, 102, 180]],
      6: [[19, 25], [61, 25], [19, 64], [61, 64], [19, 103, 180], [61, 103, 180]],
      7: [[19, 25], [61, 25], [40, 44], [19, 64], [61, 64], [19, 103, 180], [61, 103, 180]],
      8: [[19, 25], [61, 25], [40, 44], [19, 64], [61, 64], [40, 84, 180], [19, 103, 180], [61, 103, 180]],
      9: [[19, 18], [61, 18], [19, 48], [61, 48], [40, 64], [19, 80, 180], [61, 80, 180], [19, 110, 180], [61, 110, 180]],
      10: [[19, 18], [61, 18], [40, 34], [19, 48], [61, 48], [19, 80, 180], [61, 80, 180], [40, 94, 180], [19, 110, 180], [61, 110, 180]]
    };
    const pips = positions[n].map(([x, y, rotation]) => pip(suit, x, y, n >= 9 ? .72 : .82, rotation || 0)).join("");
    return `<path d="M8 7H24V10H13V21H8ZM72 7H56V10H67V21H72ZM8 121H24V118H13V107H8ZM72 121H56V118H67V107H72Z" fill="var(--card-trim)"/>
      <path d="M3 62H7V66H3ZM73 62H77V66H73Z" fill="var(--card-trim)"/>${pips}`;
  }

  function aceArtwork(suit) {
    return `<path d="M30 12H50V19H60V27H68V39H73V89H68V101H60V109H50V116H30V109H20V101H12V89H7V39H12V27H20V19H30Z" fill="var(--card-trim)"/>
      <path d="M32 17H48V24H57V32H64V43H68V85H64V96H57V104H48V111H32V104H23V96H16V85H12V43H16V32H23V24H32Z" fill="var(--card-field)"/>
      <path d="M37 26H43V33H50V39H43V46H37V39H30V33H37ZM37 83H43V90H50V96H43V103H37V96H30V90H37Z" fill="var(--card-accent)"/>
      <path d="M17 47H21V51H17ZM59 77H63V81H59ZM20 78H24V82H20ZM56 46H60V50H56Z" fill="var(--card-trim)"/>
      ${pip(suit, 40, 64, 1.83)}`;
  }

  const ROYAL_PIXELS = {
    J: [
      ".............go..", ".......oooo.ggo..", ".....oocccooggo..",
      "....occccccoog...", "...occgggccoo....", "..ooooooooooo....",
      "....ohssshhoo....", "....ososoosho....", "....ossssssho....",
      ".....ossgsho.....", ".....oohshoe.....", "...ooocssocooe...",
      "..ooccoggooccoe..", ".oaccccoocccaco..", ".oaccac.cccacao..",
      "ooccacggggcacooo."
    ],
    Q: [
      "........g........", "....g..gag..g....", "....ogogagogo....",
      ".....gagggag.....", "....oogggggoo....", "...oohsssshoo....",
      "...ohsossohho....", "...ohssssshho....", "...ohgssgshho....",
      "..oohoososhoo....", "..ohhosssohho....", "..oohggsgghoo....",
      "..oacocgocoao....", ".occaccocccaco...", ".occcacacaccco...",
      "oocccogagocccoo.."
    ],
    K: [
      "...g....g....g...", "...og..ogo..go...", "....gggagaggg....",
      "....ogggggggo....", "....ooooooooo..e.", "....ohsssssho..e.",
      "....ohosossho..g.", "....ohsssssho..g.", "....oohohooho..g.",
      ".....ohsshho..ggg", "...ooohhhhho...o.", "..oocccoooccoo.o.",
      ".occoaccgccaoco..", ".occcaccgccacoo..", "occccaccgccacco..",
      "oocccoggagocccoo."
    ]
  };
  const ROYAL_ART = Object.fromEntries(Object.entries(ROYAL_PIXELS).map(([rank, rows]) => [
    rank, pixelPaths(rows, {
      o: "#27203a", g: "#ffd66f", a: "var(--card-accent)", c: "currentColor",
      s: "#f5bd88", h: "#9b5370", e: "#9feadf"
    })
  ]));

  function faceArtwork(rank, suit) {
    const half = `<path d="M10 7H70V12H75V62H5V12H10Z" fill="var(--card-field)"/>
      <path d="M16 15H20V19H16ZM59 22H63V26H59ZM12 35H16V39H12ZM64 41H68V45H64Z" fill="var(--card-trim)"/>
      <g transform="translate(9 8) scale(3.65 3.45)">${ROYAL_ART[rank]}</g>`;
    return `<path d="M9 3H71V8H77V120H71V125H9V120H3V8H9Z" fill="var(--card-trim)"/>
      ${half}<g transform="rotate(180 40 64)">${half}</g>
      <path d="M3 62H77V66H3Z" fill="var(--card-accent)"/>
      ${pip(suit, 9, 64, .43)}${pip(suit, 71, 64, .43, 180)}`;
  }

  const CARD_BACK = `<svg class="card-back-art" viewBox="0 0 56 78" preserveAspectRatio="none" shape-rendering="crispEdges" aria-hidden="true" focusable="false">
    <path d="M4 0H52V4H56V74H52V78H4V74H0V4H4Z" fill="#f8dca1"/>
    <path d="M6 4H50V8H52V70H50V74H6V70H4V8H6Z" fill="#302951"/>
    <path d="M8 9H48V13H50V65H48V69H8V65H6V13H8Z" fill="#7760a0"/>
    <path d="M10 12H46V16H47V62H46V66H10V62H9V16H10Z" fill="#483766"/>
    <path d="M12 14H16V18H12ZM40 14H44V18H40ZM12 60H16V64H12ZM40 60H44V64H40Z" fill="#6ed0cb"/>
    <path d="M24 14H32V18H36V22H40V28H44V50H40V56H36V60H32V64H24V60H20V56H16V50H12V28H16V22H20V18H24Z" fill="#b75d9b"/>
    <path d="M26 20H30V24H34V28H38V34H40V44H38V50H34V54H30V58H26V54H22V50H18V44H16V34H18V28H22V24H26Z" fill="#302951"/>
    <path d="M26 24H30V30H32V34H36V37H40V41H36V44H32V48H30V54H26V48H24V44H20V41H16V37H20V34H24V30H26Z" fill="#ffd476"/>
    <path d="M26 36H30V42H26Z" fill="#fff3c7"/>
    <path d="M18 20H20V22H18ZM36 20H38V22H36ZM18 56H20V58H18ZM36 56H38V58H36Z" fill="#f4a3c4"/>
  </svg>`;

  function cardBackMarkup() {
    return CARD_BACK;
  }

  function cardMarkup(card, options = {}) {
    const rank = /^(?:[2-9]|10|J|Q|K|A)$/.test(String(card.rank)) ? String(card.rank) : "A";
    const suit = Object.prototype.hasOwnProperty.call(SUITS, card.suit) ? card.suit : "S";
    const artwork = rank === "A" ? aceArtwork(suit) : /[JQK]/.test(rank) ? faceArtwork(rank, suit) : numberArtwork(rank, suit);
    const chips = finite(card.baseChip) + finite(card.bonusChip);
    const enhancements = [
      card.wild ? '<span class="enhancement wild" title="Wild suit">W</span>' : "",
      card.crowned ? '<span class="enhancement" title="Crowned">♛</span>' : "",
      card.glass ? '<span class="enhancement glass" title="Glass">◇</span>' : "",
      card.wax ? `<span class="enhancement" title="Wax seal">${finite(card.wax)}✦</span>` : ""
    ].join("");
    const corner = `<b class="rank">${rank}</b>${cornerPip(suit)}`;
    return `<span class="card-inner${card.glass ? " glass-art" : ""}" data-suit="${suit}">
      <span class="card-corner corner-top" aria-hidden="true">${corner}</span>
      <svg class="card-art" viewBox="0 0 80 128" shape-rendering="crispEdges" aria-hidden="true" focusable="false">${artwork}</svg>
      <span class="card-corner corner-bottom" aria-hidden="true">${corner}</span>
      ${options.small ? "" : `<span class="card-value" aria-hidden="true">${number(chips)}</span>`}
      ${enhancements ? `<span class="card-enhancements">${enhancements}</span>` : ""}
      ${card.editionMark ? `<span class="card-edition" title="${esc(card.editionName || "Special edition")}" aria-hidden="true">${esc(card.editionMark)}</span>` : ""}
    </span>`;
  }

  const JESTER_STYLES = {
    bell: `<path d="M33 18H43V24H49V37H54V26H60V20H73V26H83V39H74V45H29V38H20V24H33Z" fill="currentColor"/>
      <path d="M17 19H27V27H17ZM44 14H53V22H44ZM76 18H86V26H76Z" fill="var(--jest-gold)"/>`,
    crown: `<path d="M23 22H34V30H42V17H47V10H54V17H59V30H66V22H77V45H23Z" fill="var(--jest-gold)"/>
      <path d="M26 38H74V45H26ZM46 26H54V34H46Z" fill="currentColor"/>`,
    veil: `<path d="M39 14H59V21H68V29H75V39H82V57H70V43H31V61H19V39H26V27H34V21H39Z" fill="currentColor"/>
      <path d="M30 32H42V29H63V33H70V37H34V43H29Z" fill="var(--jest-gold)"/>`,
    comet: `<path d="M43 16H56V22H64V28H72V34H79V46H22V37H29V28H36V22H43Z" fill="currentColor"/>
      <path d="M53 19H60V24H67V29H73V35H65V32H58V28H53ZM40 27H47V32H52V39H45V35H40V39H34V33H40Z" fill="var(--jest-gold)"/>`,
    plumes: `<path d="M26 19H35V23H43V29H48V34H52V29H58V23H66V19H76V31H71V43H29V33H26Z" fill="currentColor"/>
      <path d="M24 14H30V26H24ZM39 19H44V31H39ZM58 19H63V31H58ZM72 14H78V26H72Z" fill="var(--jest-gold)"/>`,
    moon: `<path d="M25 27H34V18H54V24H65V36H79V45H21V36H25Z" fill="currentColor"/>
      <path d="M43 20H50V24H46V32H52V36H43V33H39V25H43ZM68 25H73V30H68Z" fill="var(--jest-gold)"/>`
  };
  const JESTER_EMBLEMS = {
    chips: "M46 89H54V92H58V97H54V100H46V97H42V92H46Z",
    mult: "M44 88H49V92H52V88H57V92H54V96H57V100H52V96H49V100H44V96H47V92H44Z",
    cash: "M48 86H52V89H56V93H49V94H56V100H52V103H48V100H44V96H51V95H44V89H48Z",
    cards: "M44 87H53V90H57V101H47V98H43V90H44ZM48 93V97H53V93Z",
    suit: "M48 87H52V91H56V95H52V99H48V95H44V91H48Z",
    star: "M48 85H52V90H58V95H53V101H47V95H42V90H48Z"
  };
  const JESTER_DESIGNS = {
    key: ["veil", "cards"], lantern: ["bell", "chips"], thread: ["plumes", "cards"],
    mask: ["moon", "suit"], bells: ["bell", "mult"], music: ["plumes", "mult"],
    crystal: ["veil", "star"], moth: ["moon", "star"], clock: ["comet", "mult"],
    ticket: ["veil", "cash"], coins: ["plumes", "cash"], crown: ["crown", "suit"],
    sundial: ["comet", "chips"], flame: ["crown", "chips"], planet: ["moon", "star"],
    diamond: ["veil", "suit"], cards: ["bell", "cards"]
  };

  function jesterPortrait(name, emblem, variant, palette) {
    const [bg, mid, gold, light] = palette;
    const styles = Object.keys(JESTER_STYLES);
    const [style, detail] = JESTER_DESIGNS[variant] || [styles[hash(name) % styles.length], emblem];
    const hat = JESTER_STYLES[style];
    const charm = JESTER_EMBLEMS[detail] || JESTER_EMBLEMS.star;
    return `<g shape-rendering="crispEdges" color="${mid}" style="--jest-gold:${gold}">
      <path d="M23 14H77V20H83V26H90V84H83V92H77V99H23V92H17V84H10V26H17V20H23Z" fill="${light}" opacity=".16"/>
      <path d="M23 34H77V41H82V68H76V79H65V85H76V92H81V102H19V92H24V85H35V79H24V68H18V41H23Z" fill="#20182d"/>
      ${hat}
      <path d="M29 47H71V70H66V76H60V81H40V76H34V70H29Z" fill="${light}"/>
      <path d="M60 47H71V70H66V76H58V79H54V53H60Z" fill="${gold}" opacity=".66"/>
      <path d="M35 54H46V58H41V63H35ZM54 58H65V54H54ZM60 58H65V63H60ZM41 69H46V73H55V69H61V74H55V77H46V75H41Z" fill="${bg}"/>
      <path d="M25 89H39V83H45V88H55V83H61V89H75V94H79V101H21V94H25Z" fill="${mid}"/>
      <path d="${charm}" fill="${gold}"/>
    </g>`;
  }

  // Motifs follow each item's effect. A name-derived palette keeps collections
  // recognizable at table size.
  function itemArt(item) {
    const name = String(item.name || "Joker"), lower = name.toLowerCase();
    const kind = item.kind || item._kind || item._type || "joker";
    const palettes = [
      ["#293763", "#689ed0", "#ffd079", "#eff9f0"],
      ["#633256", "#c26799", "#ffcb82", "#ffe4d5"],
      ["#205759", "#57b9a1", "#f0cb77", "#eff9d5"],
      ["#79434c", "#e77a73", "#ffcf7f", "#fff0d6"],
      ["#47346c", "#9a82cc", "#efd27a", "#f4eafa"],
      ["#44543a", "#98b866", "#f9ce7b", "#f5f6ce"]
    ];
    const palette = palettes[hash(name) % palettes.length];
    const [bg, mid, gold, light] = palette;
    const ink = "#162a32";
    let motif = "mask";
    if (kind === "voucher") motif = "ticket";
    else if (lower.includes("planet") || /orbit|star|cosmic|galaxy|moon/.test(lower)) motif = "planet";
    else if (/spectral|ghost|soul|haunt|ruin/.test(lower)) motif = "ghost";
    else if (/tarot|paint|polish|wax|glass|sharpen|cleanse/.test(lower)) motif = "potion";
    else if (/chip|calculator|even|odd|fibonacci/.test(lower)) motif = "chips";
    else if (/cash|money|coin|gold|greedy|interest|dealer|change|discount|refund/.test(lower)) motif = "coins";
    else if (/heart/.test(lower)) motif = "heart";
    else if (/club|hoarder|mastery/.test(lower)) motif = "club";
    else if (/spade/.test(lower)) motif = "spade";
    else if (/diamond|prism|rainbow/.test(lower)) motif = "diamond";
    else if (/pair|mirror|twin|tiny|three|four|five|full|grip/.test(lower)) motif = "cards";
    else if (/ace|royal|crown|boss|king|queen/.test(lower)) motif = "crown";
    else if (/straight|cut|blade|fury|break|enforce/.test(lower)) motif = "sword";
    else if (/grow|grind|infinite|spiral|stair|streak/.test(lower)) motif = "growth";
    else if (/lens|arcane|professor|strateg/.test(lower)) motif = "eye";
    else if (/clutch|dynamo|overkill|waste|burn/.test(lower)) motif = "flame";
    const portrait = kind === "jester" ? jesterPortrait(name, item.emblem || motif, item.art, palette) : "";
    const stroke = `stroke="${ink}" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"`;
    const motifs = portrait ? null : {
      mask: `<path d="M14 26H30V32H38V20H45V13H55V20H62V32H70V26H86V39H79V51H75V75H69V83H61V91H39V83H31V75H25V51H21V39H14Z" fill="${ink}"/>
        <path d="M18 30H28V36H36V43H40V27H47V20H53V27H60V43H64V36H72V30H82V39H75V48H25V39H18Z" fill="${gold}"/>
        <path d="M30 53H70V73H64V81H58V86H42V81H36V73H30Z" fill="${light}"/>
        <path d="M50 53H70V73H64V81H58V86H50Z" fill="${mid}"/>
        <path d="M36 59H45V64H42V68H36ZM55 59H64V68H58V64H55ZM39 73H44V77H56V73H61V79H56V82H44V79H39Z" fill="${ink}"/>
        <path d="M14 24H22V32H14ZM46 13H54V21H46ZM78 24H86V32H78Z" fill="${light}"/>`,
      chips: `<g ${stroke}><ellipse cx="43" cy="82" rx="28" ry="9" fill="${gold}"/><path d="M15 72V82Q43 96 71 82V72" fill="${gold}"/><ellipse cx="43" cy="72" rx="28" ry="9" fill="${light}"/><path d="M15 62V72Q43 86 71 72V62" fill="${mid}"/><ellipse cx="43" cy="62" rx="28" ry="9" fill="${gold}"/><path d="M15 52V62Q43 76 71 62V52" fill="${gold}"/><ellipse cx="43" cy="52" rx="28" ry="9" fill="${light}"/><circle cx="65" cy="36" r="21" fill="${mid}"/><circle cx="65" cy="36" r="14" fill="${gold}"/><path d="M65 15V21M65 51V57M44 36H50M80 36H86" stroke="${light}" stroke-width="4"/></g>`,
      coins: `<g ${stroke}><path d="M22 72V84Q48 98 74 84V72" fill="${gold}"/><ellipse cx="48" cy="72" rx="26" ry="9" fill="${light}"/><path d="M22 61V72Q48 86 74 72V61" fill="${gold}"/><ellipse cx="48" cy="61" rx="26" ry="9" fill="${light}"/><circle cx="54" cy="39" r="25" fill="${gold}"/><circle cx="54" cy="39" r="18" fill="none" stroke="${light}"/><path d="M61 29Q46 24 46 34Q46 40 56 40Q66 42 60 49Q54 54 45 48M54 23V55" fill="none" stroke="${ink}" stroke-width="4"/></g>`,
      heart: `<path d="M50 88C33 75 12 56 15 39C18 20 42 20 50 38C59 20 82 20 86 38C90 55 69 76 50 88Z" fill="${mid}" ${stroke}/><path d="M50 80C37 68 23 54 24 42Q26 29 39 33" fill="none" stroke="${light}" stroke-width="5"/><path d="M50 22V12M79 29L87 21M21 29L13 21" stroke="${gold}" stroke-width="3"/>`,
      club: `<path d="M45 67C26 81 13 64 18 52Q23 39 37 43C21 16 47 9 58 20Q69 29 61 43C82 34 92 56 79 68Q68 77 55 66L64 93H36Z" fill="${mid}" ${stroke}/><path d="M50 83V39M50 62L31 54M50 62L70 55" fill="none" stroke="${light}" stroke-width="4"/>`,
      spade: `<path d="M50 13C38 31 15 44 15 62Q18 86 44 71L37 94H64L56 71Q83 85 86 62C86 45 63 30 50 13Z" fill="${mid}" ${stroke}/><path d="M50 82V32M50 65L30 55M50 65L70 55" fill="none" stroke="${light}" stroke-width="4"/>`,
      diamond: `<path d="M29 25H71L88 43L50 92L12 43Z" fill="${mid}" ${stroke}/><path d="M29 25L34 44L50 92L65 44L71 25M12 43H88M34 44L50 25L65 44" fill="none" stroke="${light}" stroke-width="2"/><path d="M50 25L34 44H65Z" fill="${gold}"/><path d="M24 15L22 8M77 16L82 9M91 64L98 68" stroke="${gold}" stroke-width="3"/>`,
      cards: `<g transform="rotate(-15 38 54)" ${stroke}><rect x="15" y="20" width="44" height="66" rx="4" fill="${mid}"/><path d="M23 29H51V76H23Z" fill="none" stroke="${gold}"/></g><g transform="rotate(13 62 56)" ${stroke}><rect x="40" y="23" width="44" height="66" rx="4" fill="${light}"/>${pip("H", 62, 55, 1.2, 0, mid)}${pip("D", 48, 34, .32, 0, ink)}${pip("D", 77, 80, .32, 180, ink)}</g>`,
      crown: `<path d="M22 70L14 33L36 46L50 20L64 46L86 33L78 70Z" fill="${gold}" ${stroke}/><path d="M23 72H77V86H23Z" fill="${mid}" ${stroke}/><path d="M25 77H75" stroke="${light}" stroke-width="3"/><path d="M50 41L56 54L50 65L44 54Z" fill="${mid}" ${stroke}/><circle cx="14" cy="31" r="5" fill="${light}" ${stroke}/><circle cx="50" cy="18" r="5" fill="${light}" ${stroke}/><circle cx="86" cy="31" r="5" fill="${light}" ${stroke}/>`,
      sword: `<path d="M30 80L68 23L86 12L82 33L43 87Z" fill="${light}" ${stroke}/><path d="M38 78L78 24" stroke="${mid}" stroke-width="3"/><path d="M22 65L53 86L47 94L16 73Z" fill="${gold}" ${stroke}/><path d="M28 80L16 96L23 101L36 86Z" fill="${mid}" ${stroke}/><path d="M18 37L26 30M32 20L34 11M69 68L81 74" stroke="${gold}" stroke-width="3"/>`,
      growth: `<path d="M50 94V27M50 69Q23 79 19 54Q37 48 50 69M50 55Q77 63 80 37Q62 33 50 55M50 39Q29 45 30 23Q46 23 50 39" fill="${mid}" ${stroke}/><path d="M31 91Q50 80 69 91L73 99H27Z" fill="${gold}" ${stroke}/><path d="M50 16L54 23L50 29L46 23Z" fill="${light}"/>`,
      eye: `<path d="M12 55Q50 15 88 55Q50 93 12 55Z" fill="${light}" ${stroke}/><circle cx="50" cy="55" r="20" fill="${mid}" ${stroke}/><circle cx="50" cy="55" r="9" fill="${ink}"/><circle cx="44" cy="48" r="4" fill="${light}"/><path d="M50 16V7M25 25L18 18M75 25L82 18M50 93V103" stroke="${gold}" stroke-width="3"/>`,
      flame: `<path d="M54 10Q70 39 58 48Q75 43 77 31Q100 68 74 91Q51 109 28 88Q10 67 31 42Q29 64 42 65Q29 40 54 10Z" fill="${gold}" ${stroke}/><path d="M53 51Q55 70 66 73Q68 90 51 94Q34 92 35 78Q45 82 53 51Z" fill="${mid}" ${stroke}/>`,
      planet: `<circle cx="50" cy="55" r="29" fill="${mid}" ${stroke}/><path d="M30 37Q48 32 73 45M23 54Q48 46 77 63M31 75Q52 65 69 74" stroke="${gold}" stroke-width="4" fill="none"/><ellipse cx="50" cy="55" rx="47" ry="13" transform="rotate(-25 50 55)" fill="none" stroke="${ink}" stroke-width="9"/><ellipse cx="50" cy="55" rx="47" ry="13" transform="rotate(-25 50 55)" fill="none" stroke="${light}" stroke-width="5"/><circle cx="22" cy="18" r="4" fill="${gold}"/><circle cx="79" cy="91" r="3" fill="${gold}"/>`,
      potion: `<path d="M38 18H62V36L79 63Q93 96 50 98Q7 96 21 63L38 36Z" fill="${light}" ${stroke}/><path d="M26 63Q44 72 74 62Q87 91 50 92Q14 91 26 63Z" fill="${mid}"/><rect x="35" y="12" width="30" height="12" rx="3" fill="${gold}" ${stroke}/><circle cx="48" cy="74" r="5" fill="${gold}"/><circle cx="61" cy="84" r="3" fill="${light}"/><path d="M33 48L28 59" stroke="#fff9" stroke-width="4" stroke-linecap="round"/>`,
      ticket: `<path d="M16 25H84V44Q68 51 84 60V85H16V60Q32 51 16 44Z" fill="${gold}" ${stroke}/><path d="M27 34H73V76H27Z" fill="none" stroke="${ink}" stroke-width="1.5"/><path d="M50 40L54 49L64 50L57 57L59 67L50 62L41 67L43 57L36 50L46 49Z" fill="${mid}" ${stroke}/><path d="M34 20V14M66 20V14M34 91V98M66 91V98" stroke="${light}" stroke-width="3"/>`,
      ghost: `<path d="M22 91V48Q22 16 50 16Q78 16 78 48V91L65 82L55 94L44 83L32 94Z" fill="${light}" ${stroke}/><ellipse cx="39" cy="49" rx="6" ry="9" fill="${bg}"/><ellipse cx="62" cy="49" rx="6" ry="9" fill="${bg}"/><ellipse cx="50" cy="69" rx="5" ry="7" fill="${mid}"/><path d="M13 34L10 26M86 34L91 27M16 72L8 77" stroke="${gold}" stroke-width="3"/>`
    };
    return `<svg viewBox="0 0 100 110" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="${esc(name)} illustration" focusable="false">
      <g shape-rendering="crispEdges">
        <rect width="100" height="110" fill="${bg}"/>
        <path d="M20 23H80V33H90V73H80V87H20V77H10V37H20Z" fill="${mid}" opacity=".25"/>
        <path d="M8 8H22V11H11V23H8ZM92 8H78V11H89V23H92ZM8 102H22V99H11V87H8ZM92 102H78V99H89V87H92Z" fill="${gold}"/>
        <path d="M21 18H25V22H21ZM73 20H77V24H73ZM20 79H24V83H20ZM74 84H78V88H74ZM12 53H16V57H12ZM84 53H88V57H84Z" fill="${light}" opacity=".64"/>
      </g>
      ${portrait || motifs[motif] || motifs.mask}
    </svg>`;
  }

  let soundOn = false;
  try { soundOn = root.localStorage?.getItem("balatle_sound_v1") === "true"; } catch (_) { /* Local storage is optional. */ }
  let audioContext = null;
  function audio() {
    if (!soundOn) return null;
    try {
      if (!audioContext) {
        const Constructor = root.AudioContext || root.webkitAudioContext;
        if (!Constructor) return null;
        audioContext = new Constructor();
      }
      if (audioContext.state === "suspended") audioContext.resume().catch(() => {});
      return audioContext;
    } catch (_) { return null; }
  }
  function tone(frequency, duration = .08, volume = .035) {
    const ctx = audio();
    if (!ctx) return;
    const oscillator = ctx.createOscillator(), gain = ctx.createGain();
    const now = ctx.currentTime;
    oscillator.type = "triangle";
    oscillator.frequency.setValueAtTime(frequency, now);
    oscillator.frequency.exponentialRampToValueAtTime(frequency * .8, now + duration);
    gain.gain.setValueAtTime(.0001, now);
    gain.gain.exponentialRampToValueAtTime(volume, now + .009);
    gain.gain.exponentialRampToValueAtTime(.0001, now + duration);
    oscillator.connect(gain);
    gain.connect(ctx.destination);
    oscillator.start(now);
    oscillator.stop(now + duration + .02);
    oscillator.onended = () => { oscillator.disconnect(); gain.disconnect(); };
  }
  function toggleSound(value) {
    soundOn = typeof value === "boolean" ? value : !soundOn;
    try { root.localStorage?.setItem("balatle_sound_v1", String(soundOn)); } catch (_) { /* Optional setting. */ }
    if (soundOn) tone(660, .06, .025);
    return soundOn;
  }

  const PURCHASE_CUES = {
    card: ["#85e6dc", 720],
    jester: ["#f3ace0", 805],
    joker: ["#bbacee", 660],
    cons: ["#9dcdfa", 580],
    voucher: ["#f9d383", 540]
  };
  let shopFlash = null;

  function purchaseFeedback(kind) {
    const [color, frequency] = PURCHASE_CUES[kind] || PURCHASE_CUES.voucher;
    const header = root.document?.querySelector(".shopModalHead");
    tone(frequency, .1, .025);
    if (!header?.animate || root.matchMedia?.("(prefers-reduced-motion: reduce)").matches) return;
    shopFlash?.cancel();
    shopFlash = header.animate([
      { boxShadow: "inset 0 -3px transparent" },
      { boxShadow: `inset 0 -3px ${color}`, offset: .2 },
      { boxShadow: "inset 0 -3px transparent" }
    ], { duration: 540, easing: "ease-out" });
  }

  const pendingDraws = new Set();
  let shufflePending = false;
  let activeDeal = null;

  function stopDealMotion() {
    const deal = activeDeal;
    if (!deal) return;
    activeDeal = null;
    deal.listeners.abort();
    for (const animation of deal.animations) animation.cancel();
    for (const card of deal.cards.values()) card.classList.remove("is-dealing");
    deal.layer.remove();
  }

  function beginShuffle() {
    stopDealMotion();
    pendingDraws.clear();
    shufflePending = true;
  }

  function queueDraw(id) {
    pendingDraws.add(String(id));
  }

  function flyingBack(layer, rect, className = "deal-flyer") {
    const back = layer.ownerDocument.createElement("div");
    back.className = className;
    Object.assign(back.style, {
      left: `${rect.left}px`,
      top: `${rect.top}px`,
      width: `${rect.width}px`,
      height: `${rect.height}px`
    });
    back.innerHTML = cardBackMarkup();
    layer.appendChild(back);
    return back;
  }

  function renderDeal(hand, playing) {
    if (!playing) {
      stopDealMotion();
      pendingDraws.clear();
      shufflePending = false;
      return;
    }

    if (!pendingDraws.size) {
      if (activeDeal) {
        // Selection and network updates may replace the button during its flight.
        for (const card of hand.children) {
          const id = card.dataset.cardId;
          if (!activeDeal.cards.has(id)) continue;
          activeDeal.cards.set(id, card);
          card.classList.add("is-dealing");
        }
      }
      return;
    }

    const drawnIds = [...pendingDraws];
    const shuffled = shufflePending;
    pendingDraws.clear();
    shufflePending = false;
    stopDealMotion();

    const document = hand.ownerDocument;
    const source = document.getElementById("deckDealSource");
    const motion = root.matchMedia?.("(prefers-reduced-motion: reduce)");
    if (!source?.animate || motion?.matches) return;

    const origin = source.getBoundingClientRect();
    if (!origin.width || !origin.height) return;
    const byId = new Map([...hand.children].map(card => [card.dataset.cardId, card]));
    const targets = drawnIds.flatMap(id => {
      const card = byId.get(id);
      return card ? [{id, card, rect: card.getBoundingClientRect()}] : [];
    });
    if (!targets.length) return;

    const layer = document.createElement("div");
    layer.className = "deal-effects";
    layer.setAttribute("aria-hidden", "true");
    document.body.appendChild(layer);

    const listeners = new AbortController();
    const deal = { layer, listeners, cards: new Map(), animations: new Set() };
    activeDeal = deal;
    const interrupt = { capture: true, signal: listeners.signal };
    document.addEventListener("pointerdown", stopDealMotion, interrupt);
    document.addEventListener("keydown", stopDealMotion, interrupt);
    hand.addEventListener("focusin", stopDealMotion, interrupt);
    document.addEventListener("scroll", stopDealMotion, interrupt);
    root.addEventListener("resize", stopDealMotion, interrupt);
    motion?.addEventListener?.("change", stopDealMotion, { signal: listeners.signal });

    const originX = origin.left + origin.width / 2;
    const originY = origin.top + origin.height / 2;
    const shuffleTime = 370;
    const track = (back, frames, timing, onLand) => {
      const animation = back.animate(frames, {fill: "both", ...timing});
      deal.animations.add(animation);
      animation.finished.then(() => {
        deal.animations.delete(animation);
        back.remove();
        onLand?.();
        if (activeDeal === deal && !deal.animations.size) stopDealMotion();
      }, () => {}); // Interruptions reveal the cards and remove the overlay immediately.
    };

    if (shuffled) {
      const rect = {left: originX - 17, top: originY - 23, width: 34, height: 46};
      const edgeOffset = Math.min(0, root.innerWidth - originX - 68) + Math.max(0, 68 - originX);
      for (const [index, spread] of [-43, 44, -29, 27, 0].entries()) {
        const back = flyingBack(layer, rect, "deal-flyer shuffle-flyer");
        track(back, [
          {transform: "scale(.48)", opacity: 0},
          {transform: `translate(${edgeOffset + spread}px, ${-19 - index * 4}px) rotate(${spread * .64}deg) scale(1.08)`, opacity: 1, offset: .43},
          {transform: `translate(${edgeOffset * .46 - spread * .26}px, -7px) rotate(${spread * -.22}deg) scale(.83)`, opacity: 1, offset: .75},
          {transform: "scale(.48)", opacity: 0}
        ], {duration: 310, delay: index * 12, easing: "ease-in-out"});
      }
    }

    const stagger = Math.min(42, 300 / Math.max(1, targets.length - 1));
    targets.forEach(({id, card, rect}, index) => {
      deal.cards.set(id, card);
      card.classList.add("is-dealing");
      const back = flyingBack(layer, rect);
      const x = originX - rect.left - rect.width / 2;
      const y = originY - rect.top - rect.height / 2;
      const scale = origin.width / rect.width;
      const spin = x < 0 ? -18 : 18;
      const start = `translate(${x}px, ${y}px) rotate(${spin}deg) scale(${scale})`;
      track(back, [
        {transform: start, opacity: 0},
        {transform: start, opacity: 1, offset: .1},
        {transform: `translate(${x * .38}px, ${y * .38 - 26}px) rotate(${spin * -.6}deg) scale(.88)`, opacity: 1, offset: .55},
        {transform: "translateY(-3px) rotateY(38deg)", opacity: 1, offset: .88},
        {transform: "rotateY(90deg)", opacity: 0}
      ], {duration: 300, delay: (shuffled ? shuffleTime : 0) + index * stagger, easing: "ease-out"}, () => {
        deal.cards.get(id)?.classList.remove("is-dealing");
        deal.cards.delete(id);
      });
    });
  }

  let activePlayback = null;
  function cancelPlayback() {
    if (activePlayback) activePlayback.finish(true);
  }
  function scorePlayback(result, onDone) {
    cancelPlayback();
    const document = root.document;
    const stage = document?.getElementById("scoreStage");
    const cards = Array.isArray(result.cards) ? result.cards : [];
    const scoringIds = new Set(result.scoringIds || cards.map(c => c.id));
    const triggers = Array.isArray(result.triggers) ? result.triggers : [];
    const modifiers = Array.isArray(result.modifiers) ? result.modifiers : [];
    const motion = root.matchMedia?.("(prefers-reduced-motion: reduce)");
    let resolvePromise;
    const promise = new Promise(resolve => { resolvePromise = resolve; });
    const timers = [];
    const flashes = new Map();
    let completed = false;
    const finish = cancelled => {
      if (completed) return;
      completed = true;
      for (const timer of timers) root.clearTimeout(timer);
      stage?.classList.remove("is-scoring");
      delete stage?.dataset.scoreEffect;
      stage?.querySelectorAll(".score-burst, .chip-pop").forEach(el => el.remove());
      for (const animation of flashes.values()) animation.cancel();
      document?.querySelectorAll(".joker-tile.triggered").forEach(el => el.classList.remove("triggered"));
      if (activePlayback?.finish === finish) activePlayback = null;
      const outcome = { cancelled: !!cancelled };
      resolvePromise(outcome);
      if (typeof onDone === "function") onDone(outcome);
    };
    activePlayback = { finish };
    const setText = (id, text) => { const el = document?.getElementById(id); if (el) el.textContent = text; };
    const setCaption = text => {
      setText("scoreCaption", text);
      setText("scoreBreakdown", text);
    };
    const chipsDisplay = document?.getElementById("scoreChips");
    const multDisplay = document?.getElementById("scoreMult");
    const flash = (value, tint) => {
      const counter = value?.parentElement;
      if (!counter?.animate || motion?.matches) return;
      flashes.get(counter)?.cancel();
      flashes.set(counter, counter.animate([
        { transform: "scale(1)", boxShadow: "0 0 0 0 transparent" },
        { transform: "scale(1.05)", boxShadow: `0 0 0 3px ${tint}`, offset: .26 },
        { transform: "scale(1)", boxShadow: "0 0 0 6px transparent" }
      ], { duration: 280, easing: "ease-out" }));
    };
    const setFinal = () => {
      setText("scoreChips", number(result.chips));
      setText("scoreMult", number(result.mult));
      setText("scorePreview", `+${number(Math.floor(finite(result.gained)))}`);
    };
    const detail = `${result.handName || "Hand"}: ${number(result.chips)} chips × ${number(result.mult)} Mult`;
    const summary = result.clearsBlind ? `Blind cleared · ${detail}` : detail;
    setText("selectionName", result.handName || "Your hand");
    setText("stageLabel", "Scoring your hand");
    stage?.classList.add("is-scoring");
    const played = document?.getElementById("playedCards");
    if (played) {
      played.innerHTML = cards.map(c => `<div class="card${c.red || c.suit === "H" || c.suit === "D" ? " red" : ""}${scoringIds.has(c.id) ? " scoring" : " non-scoring"}" data-scoring-card="${esc(c.id)}">${cardMarkup(c, { small: true })}</div>`).join("");
    }
    if (!stage || motion?.matches) {
      setFinal();
      setCaption(summary);
      timers.push(root.setTimeout(() => finish(false), 30));
      return promise;
    }
    const later = (delay, fn) => { timers.push(root.setTimeout(() => { if (!completed) fn(); }, delay)); };
    const rawCardChips = cards.filter(c => scoringIds.has(c.id)).reduce((sum, c) => sum + finite(c.baseChip) + finite(c.bonusChip), 0);
    const jokerChipTotal = triggers.reduce((sum, trigger) => sum + finite(trigger.chips), 0);
    const initialChips = finite(result.baseChips, Math.max(0, finite(result.chips) - rawCardChips - jokerChipTotal));
    const initialMult = finite(result.baseMult, 1);
    setText("scoreChips", number(initialChips));
    setText("scoreMult", number(initialMult));
    setText("scorePreview", "Scoring…");
    const scoringCards = cards.filter(c => scoringIds.has(c.id));
    const playedById = new Map([...played?.querySelectorAll("[data-scoring-card]") || []].map(node => [node.dataset.scoringCard, node]));
    const jokersById = new Map([...document.querySelectorAll("[data-joker-id]")].map(node => [node.dataset.jokerId, node]));
    let displayedChips = initialChips;
    let displayedMult = initialMult;
    let cursor = 60;
    scoringCards.forEach((card, index) => {
      later(cursor, () => {
        const node = playedById.get(String(card.id));
        const effect = result.cardEffects?.[card.id];
        const value = finite(effect?.chips, finite(card.baseChip) + finite(card.bonusChip));
        const mult = finite(effect?.mult, card.crowned ? 1 : 0);
        const cash = result.payoutMuted ? 0 : finite(effect?.cash);
        const parts = [`+${number(value)} chips`];
        if (mult) parts.push(`+${number(mult)} Mult`);
        if (cash) parts.push(`+$${number(cash)}`);
        if (node) {
          node.classList.add("card-trigger");
          const pop = document.createElement("span");
          pop.className = "chip-pop";
          if (cash) pop.dataset.effect = "coin";
          else if (mult || card.editionMark) pop.dataset.effect = "magic";
          pop.setAttribute("aria-hidden", "true");
          pop.textContent = cash ? `+$${number(cash)}` : mult ? `+${number(mult)} Mult` : `+${number(value)}`;
          node.appendChild(pop);
        }
        displayedChips += value;
        displayedMult += mult;
        setText("scoreChips", number(displayedChips));
        setText("scoreMult", number(displayedMult));
        const source = `${card.rank}${SUITS[card.suit] || ""}`;
        setCaption([source, effect?.label, ...parts].filter(Boolean).join(" · "));
        if (value) flash(chipsDisplay, "#78e3ef");
        if (mult) flash(multDisplay, "#f8a3dd");
        tone((cash ? 540 : 380) + index * 70, .075);
      });
      cursor += 100;
    });
    triggers.forEach((trigger, index) => {
      later(cursor, () => {
        const node = jokersById.get(String(trigger.id));
        if (node) {
          node.classList.remove("triggered");
          void node.offsetWidth;
          node.classList.add("triggered");
        }
        const effects = [];
        displayedChips = Math.max(0, displayedChips + finite(trigger.chips));
        displayedMult = Math.max(0, displayedMult + finite(trigger.mult)) * finite(trigger.xMult, 1);
        setText("scoreChips", number(displayedChips));
        setText("scoreMult", number(displayedMult));
        if (finite(trigger.chips)) effects.push(`${finite(trigger.chips) > 0 ? "+" : ""}${number(trigger.chips)} chips`);
        if (finite(trigger.mult)) effects.push(`${finite(trigger.mult) > 0 ? "+" : ""}${number(trigger.mult)} Mult`);
        if (finite(trigger.xMult, 1) !== 1) effects.push(`×${number(trigger.xMult)} Mult`);
        if (trigger.label) effects.push(String(trigger.label));
        setCaption(`${trigger.name || "Joker"}${effects.length ? "  ·  " + effects.join(" · ") : " activated"}`);
        if (finite(trigger.chips)) flash(chipsDisplay, "#78e3ef");
        if (finite(trigger.mult) || finite(trigger.xMult, 1) !== 1) flash(multDisplay, "#f8a3dd");
        tone((trigger.kind === "jester" ? 730 : 620) + index * 49, .1, .035);
      });
      cursor += Math.max(60, Math.min(105, 450 / Math.max(1, triggers.length)));
    });
    if (modifiers.length) {
      later(cursor, () => {
        stage.dataset.scoreEffect = "boss";
        setCaption(modifiers.map(({name, label}) => `${name} · ${label}`).join(" · "));
        flash(multDisplay, "#f0879c");
        tone(290, .12, .025);
      });
      cursor += 250;
    }
    later(cursor + 25, () => {
      setFinal();
      setCaption(summary);
      stage.classList.remove("is-scoring");
      void stage.offsetWidth;
      stage.classList.add("is-scoring");
      stage.dataset.scoreEffect = result.clearsBlind ? "clear" : "scored";
      tone(880, .16, .045);
      if (result.clearsBlind) later(88, () => tone(1320, .14, .032));
      const burst = document.createElement("div");
      burst.className = "score-burst";
      burst.setAttribute("aria-hidden", "true");
      const particles = result.clearsBlind ? 26 : 16;
      burst.innerHTML = Array.from({ length: particles }, (_, i) => {
        const angle = i / particles * Math.PI * 2;
        const radius = (result.clearsBlind ? 91 : 63) + (i % 4) * 22;
        return `<i class="score-spark" style="--spark-x:${Math.round(Math.cos(angle) * radius)}px;--spark-y:${Math.round(Math.sin(angle) * radius * .58 - 15)}px;--spark-turn:${i % 2 ? 135 : -90}deg"></i>`;
      }).join("");
      stage.appendChild(burst);
    });
    later(cursor + 430, () => {
      finish(false);
    });
    return promise;
  }

  // Each boss has a hand-drawn sprite. The five colors after the backdrop
  // correspond to shade, main, highlight, accent, and eye pixels.
  const BOSS_SPRITES = {
    noDiscards: {
      colors: ["#344769", "#3d4972", "#8193a8", "#d9e5d6", "#eeb96b", "#9effdc"],
      rows: [
        "......oooo", ".....ohhhoo", "....ohhmmsoo", "....ohmssmso",
        "...ohmmssmsoo", "...ohhoeoemso", "...ohhmssseso", "....oshmmmsso",
        "....oohhmsooo", "...ossoooosso", "..ommsgggsmso", "..ohmsgegsmso",
        "..omsogggomso", "...oommmsso", "....osssso", ".....oooo"
      ]
    },
    exactFive: {
      colors: ["#254b64", "#36638a", "#5898a9", "#b3ebe4", "#fed986", "#ff9189"],
      rows: [
        ".......oo", "......oggo", "......oggoog", ".....oggmogo",
        "...oohhggsoo", "..ohhhhommmsso", "..ohhmmmmmssso", "..ohmmmooossso",
        "..ohmoessesosso", "..ohmosssomso", "..ommosssomso", "...omsmmmmsso",
        "....oohmmsoo", "...ommogomsso", "..oshmmgmmssso", "..oooooooooooo"
      ]
    },
    noPairs: {
      colors: ["#572f62", "#814c8f", "#c36db0", "#f7b8d5", "#ffdc84", "#fff5c8"],
      rows: [
        ".....o....o", ".....om..mo", ".ooo..o..o..ooo", "ohhmoo.oo.oomhho",
        "ohhgmsoeeosmgmho", "ohggmsoemoohggho", "ohggmsoeeosmgmho", ".ommmmoohommmmo",
        "..osmsoshossmo", "...oohoshomoo", "..osmmoshommmso", "..ohmmsosommmho",
        "...ommsooosmmo", "....ooo..ooo", ".......ss", ".......oo"
      ]
    },
    noStraights: {
      colors: ["#53374f", "#815342", "#c88b55", "#f7d49a", "#0bd1a9", "#ffe68e"],
      rows: [
        "......oo.g", ".....ohhoog", ".....ohhho", "....oooooo",
        "....ohhhho", "...ohmhmhho", "..ohhmohmhso", "..ohhhohhhso",
        "..ohmhooogso", "..ohhhhhhmso", "...ohghhhso", "....oooooo",
        ".....omso", ".....omsoo", "....ohhmso", "...ooooooo"
      ]
    },
    noFace: {
      colors: ["#51314f", "#63628a", "#c6cad8", "#fff4de", "#ef758e", "#98f7d5"],
      rows: [
        "..ooo.......oo", "..ohho.....ohho", "..ohhhoo.ooohho", "..ohhggmmmmsso",
        "..ohhggmmmmsso", "..ohghhggmmsso", "...ohoohoosso", "...oheehessso",
        "...oohhmmsooo", "....ohgmssoo", "....ohhgsso", ".....ohmso",
        "....osogoso", "...ossogosso", "..osssogossso", "..ooooooo"
      ]
    },
    suitDemand: {
      colors: ["#392f68", "#7753ae", "#b974cf", "#e9bdf8", "#ffc166", "#9ceed4"],
      rows: [
        "........oo", ".......oggo", "......ogmmoo", ".....ogmmsso",
        "....oghmmssso", "....ohhmmssso", "..oohhmmmmsssoo", ".ohhmmoggommssso",
        "..ooooeeemsooo", "....osooooo", "...omsggmsso.oo", "..ohmsgghmso.ogo",
        ".oooosmmssso.ogo", ".ogo.mmosssooogo", ".ogoohmssso..oo", "..ooooooooo"
      ]
    },
    handTight: {
      colors: ["#294d51", "#356b58", "#67a366", "#a7ce74", "#ffd371", "#fff6ba"],
      rows: [
        "........g", ".....oo.g", "....ohhogo", "....ohmhgo",
        "..g.ohmgooo", "...gogmmssoo", "...oghememso.g", "..gohhmmsso.g",
        "...ohmmsssoo", "..ooohgggsooo", ".ommoogggomso", ".ohmssgggmsso",
        "..osmsgggoso", ".....omso", "....ohmmso", "....oooooo"
      ]
    },
    discardTax: {
      colors: ["#593f3c", "#8c513f", "#ca8060", "#ffe2ae", "#ebbf55", "#fff2ba"],
      rows: [
        ".......oooo", "......osssso", "......ommmso", "......oggggo",
        "....oohhhgsooo", "...oggggggggggo", "....ooooooo", ".....ohhhso",
        ".....oheeso", ".....ohhhso", "......ohso", ".....osgoso",
        "....ossgmssoo", "..oogoosgoosso", "..oggoohhmmso", "...oooooooooo"
      ]
    },
    scorePenalty: {
      colors: ["#2d475b", "#396877", "#65a195", "#b9e7b5", "#e69861", "#fff2c8"],
      rows: [
        "................", ".....oo..oo", "....ohhoohho", "....ohmmmmso",
        "...ohhmmmmmso", "..ohhmmmmsmsso", ".ohhheomoeossso", ".ohhhmmmhmmssso",
        ".ohhhmhhhhmssso", "..ohhmogomssso", "...ohmgegmsoo", "....ommmmso",
        "...omsshsssso", "..ossssggsssso", "..ommmgggmmmso", "..oooooooooooo"
      ]
    },
    noFlushes: {
      colors: ["#402f68", "#7452ac", "#a273d8", "#e1c6fc", "#ef86b3", "#c8eeff"],
      rows: [
        ".......ooo", ".....oohhso", "....ohhmmssso", "....ohmmsssooo",
        "...ohhmmssso", "...ohhmeoeso", "...ohhmsssso", "...ohhmmesoo",
        "..ohmmmmsso", "..ohmmsssooo", "..ohhmmmsssoo", "...ohhssmmmsso",
        "....oossoossmso", "...........ooso", "............oso", ".............o"
      ]
    },
    outerHands: {
      colors: ["#3c3b55", "#6f6d91", "#aca9b5", "#f2e6d3", "#e77c90", "#ffcf78"],
      rows: [
        "...o....o....o", "...oh..oho..ho", "...oh.ohoho.ho", "...ohmhgohmho",
        "...ohmogoogho", "...ohhhhhhhho", "...ogmogogmgo", "....ooooooo",
        ".....osssso", "....oheoseoso", "....ohhmmsso", ".....ohmso",
        ".....osso", "...ooshmsoo", "..osogmgososso", "..ooooooooooo"
      ]
    },
    lowRank: {
      colors: ["#5d3d53", "#945643", "#ce8b58", "#ffd0a1", "#e57185", "#fff6ca"],
      rows: [
        "..g...gg..g..g", "..g...g...g..g", "..g..ooo..g..g", "..g.ohhso.g..g",
        "..gohhmmso...g", "..gohmeemso..g", "..g.ohhmssso.g", "..gooohmsso..g",
        "..omooogoo...g", "..os.ohhso..os", "...oohgmso.oso", "...ohmgmsooso",
        "....oommsooo", "....os..os", "...oss..os", "...oo...oo"
      ]
    },
    wideSuits: {
      colors: ["#292b60", "#45427a", "#7768a7", "#bbb6e6", "#fac278", "#ffdca2"],
      rows: [
        ".............g", "...........g", "......oooo", ".....ohhhmo",
        "....ohmemoso", "....ohmmmmsoo", "......osmmgggo", "......ohmmggoo",
        ".go..ohmmssoo", "..g.oohmmmsso", "...gmhmmsssoo", "...ohgmsssoo...g",
        "....oshggooo..g", "....oooggggggg", ".....osoo", ".....oo"
      ]
    },
    slowStart: {
      colors: ["#653554", "#a45545", "#df804a", "#ffc273", "#ff656d", "#fff5b8"],
      rows: [
        ".......oo", "......ohho", "......ohho", ".....ohhhmo",
        "....ohhmmmso", "....ohhmmmso", "...ohhmmmsmso", "...ohmmememso",
        "...ohmmssmmso", "..ohhmmmmmmsso", "..ohhheeehmso", "..oooooooooooo",
        "......ommog", "....gg.oo..gg", "...gmg....gmg", "...gmgg..ggmg"
      ]
    },
    repeatHand: {
      colors: ["#50366f", "#7d4c9c", "#c168aa", "#ffd3bd", "#7edbd8", "#fff1ab"],
      rows: [
        "...g.......g", "..ogo.....ogo", "...ommo..ommo", "...ogmmoogmso",
        "....ohhmmhsso", "...ohhmmmmmsso", "...ohhemoemso", "...ohhmmmmmso",
        "....ohmgmsoo", ".....ohmsoo", "...ogoomoooo", "..oggmgggmggso",
        "..oggmgggmggso", "...ossmmmssoo", ".....oogoo", ".......g"
      ]
    },
    alternating: {
      colors: ["#62424d", "#936057", "#d39662", "#ffdc8d", "#74d4d3", "#e8ffde"],
      rows: [
        "....o..oo..o", "....oh.ogohho", "....ohhggghho", "....ohhmhhmho",
        "....ooooooo", "...ohhmhmmmso", "...ohhegegmso", "...ohhmhmmmso",
        "...ohgogogmso", "....ohmmsoo", "....oogoogo", "..ogmggmggmgo",
        "..ogomgggmogo", "..omoghgggmso", "...osgmgmsoo", "....ooooooo"
      ]
    },
    pureHand: {
      colors: ["#543447", "#734c72", "#9c72a9", "#e7c9e2", "#e381a4", "#ffd5a9"],
      rows: [
        "..oo........oo", "..oho......oho", "...omo....omo", "....og.ooggo",
        "..h.oohhhog.oh", "...hommmmmsoh", "...ohmeemso", "...ohhggmso",
        "..ohommmsoso", ".oho.ossso.oho", "oh..ooogo...oh", "h...ohmso....h",
        "...ohmmmso", "...ommssso", "....ossso", ".....ooo"
      ]
    },
    lessons: {
      colors: ["#2b5351", "#53937f", "#8fbd99", "#d0ecba", "#f2c480", "#f7ffbf"],
      rows: [
        "....oo....oo", "....oho..oho", "....ohmggmho", "....ohggggho",
        "...ohhmmmmsso", "...ohheomeoso", "...ohhmmmmoso", "....ohhggso",
        "....ogggggo", "....ohhmmso", "...ohhmmmmsoo", "..ohhmmmmsmsso",
        "..ohmmgggmmsso", "..ohmgmmmmgso", "...omoommsoo", "...oooooooo"
      ]
    },
    payoutMute: {
      colors: ["#263b5b", "#455d78", "#77a49e", "#aeddc6", "#ffbf66", "#fff0a3"],
      rows: [
        "...........oo", "..........oggo", "..........ogeo", "..........oggo",
        ".......oo.oooo", "......ohhogo", ".....ohmmsgo", "....ohmgmsoo",
        "...ohmmgmso", "...ohmeemso", "...ohmmmso", "....ohmsoo",
        ".....ohmsoo", "..oo..ohmssoo", "..osooohmssso", "...ooooooooo"
      ]
    },
    suitTaboo: {
      colors: ["#583844", "#8b5455", "#b67665", "#e9a279", "#f5c773", "#b7efd5"],
      rows: [
        ".....oo", ".....ohho..g", "....ohmso.ggg", "...ohmsoo..gg",
        "...ohhmmsso..gg", "..ohhmeommsog", "..ohhmmhmmmmsso", "..ohhmmgmgmgmso",
        "..ohhmmmmmsoo", "..ohhmmssso", "...ohmmmssoo", "....ohmmmmssoo",
        "....omhmmmsmso", "...osgmgmmsso", "...osoooosso", "....ooooooo"
      ]
    },
    twoDiscard: {
      colors: ["#2f4e75", "#4a72a3", "#79aad0", "#cbf0f5", "#fde1a4", "#fff7d4"],
      rows: [
        ".....ooo....g", "....ohhho..ggg", "...ohhhmho..g", "...ohmmmmso",
        "...oggggggo", "..ohhhhmmsoo", "..ohmheemo", "..ohhhmmso",
        "...ohhhmso", "....ohmso..oo", "....ohoo....og", "...ohmmso...g",
        "..ohmmmmssoo.g", "..ommmmsmsso.g", "...ohmmsso...g", "...oooooooo"
      ]
    },
    suitTithe: {
      colors: ["#3c5142", "#677a52", "#9cad64", "#cadc91", "#ebb778", "#f5efb3"],
      rows: [
        "..g.......g", "..gg.oo..gg", "...ggogogg", "....ohohoo",
        "....ohmgho", "....ohmgmo", "...ohhmmsso", "...ohmeemso",
        "...ohhmmmso", "....ohmmso", "....oogoo", "...ohmgmso",
        "..ohmmgmsso", "..ohmggggsso", "..ohmmgmmmso", "..oooooooooo"
      ]
    },
    lastReserve: {
      colors: ["#493c69", "#706d93", "#aba4b4", "#dcd5ca", "#b683c4", "#ffca86"],
      rows: [
        "oo............oo", "oho..........oho", "ohho..oo.oo.ohho", "ohmho.ohohoohmho",
        "ohmshoohmhohmsho", "ohmmsooheeehmsho", ".ommsohmmmhmmsso", "..omsohmhmsmso",
        "...ooohhmsoo", "....ohhhmsoo", "...ohmhmmmsso", "..ohmmmmmsmsso",
        "..ohmgmmmgmsso", "..ommmmmmmmsso", "...oooommsoo", ".......ooo"
      ]
    },
    playTax: {
      colors: ["#504857", "#8a7b8e", "#c0b0b5", "#f3dcc3", "#f3ab64", "#ffe8b4"],
      rows: [
        ".....oooooo", "....ohhhmmso", "....ohmmhmso", "....ohmggmso",
        "...ohhmggmsoo", "...oheeheeso", "...ohhmmmmsso", "...ohmhhmmso",
        "....ohmgmso", ".....ooooo", "..ooohhmsoo", ".ohhmhhmhosooo",
        ".ohmmmhmoomhhso", ".ohmgmhmommmso", "..omsommmomgso", "...ooooooooooo"
      ]
    }
  };

  const bossPortraits = new Map();
  function bossPortraitMarkup(key) {
    if (bossPortraits.has(key)) return bossPortraits.get(key);
    if (!Object.prototype.hasOwnProperty.call(BOSS_SPRITES, key)) return "";
    const sprite = BOSS_SPRITES[key];
    const [backdrop, s, m, h, g, e] = sprite.colors;
    const portrait = `<svg viewBox="0 0 20 20" aria-hidden="true" focusable="false" shape-rendering="crispEdges" data-boss-portrait="${key}">
      <path d="M2 0H18V2H20V18H18V20H2V18H0V2H2Z" fill="${g}"/>
      <path d="M2 2H18V18H2Z" fill="#1c142c"/>
      <path d="M3 2H17V17H3Z" fill="${backdrop}"/>
      <path d="M7 2H13V4H15V6H17V12H15V14H13V16H7V14H5V12H3V6H5V4H7Z" fill="${h}" opacity=".12"/>
      <path d="M3 3H5V4H3ZM16 3H17V5H16ZM3 16H4V17H3ZM16 16H17V17H16Z" fill="${g}" opacity=".7"/>
      <g transform="translate(2 2)">${pixelPaths(sprite.rows, { o: "#161425", s, m, h, g, e })}</g>
    </svg>`;
    bossPortraits.set(key, portrait);
    return portrait;
  }

  const avatarFields = ["#775185", "#435b88", "#a96078", "#367c7c", "#753f71", "#8c6744", "#4e7871", "#534981"];

  function paintAvatar(node, name) {
    if (!node) return null;
    const key = hash(name);
    node.classList.add("player-avatar");
    node.setAttribute("aria-hidden", "true");
    node.style.setProperty("--avatar-frame", `${(key % 8) * 100 / 7}%`);
    node.style.setProperty("--avatar-field", avatarFields[(key >>> 4) % avatarFields.length]);
    return node;
  }

  root.BalatleTable = Object.freeze({
    cardMarkup, cardBackMarkup, itemArt, paintAvatar, bossPortraitMarkup,
    beginShuffle, queueDraw, renderDeal, scorePlayback, cancelPlayback, toggleSound, purchaseFeedback,
    isSoundOn: () => soundOn, escapeHTML: esc
  });
})(typeof window !== "undefined" ? window : globalThis);
