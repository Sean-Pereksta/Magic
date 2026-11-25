/* Original vector card art and the shared scoring presentation for Balatle. */
(function (root) {
  "use strict";

  const SUITS = { S: "♠", H: "♥", D: "♦", C: "♣" };
  const SUIT_PATHS = {
    S: "M0-12C-3-7-11-4-11 2A6 6 0 0 0-1 6L-4 12H4L1 6A6 6 0 0 0 11 2C11-4 3-7 0-12Z",
    H: "M0 11C-4 7-12 1-12-5C-12-13-3-14 0-7C3-14 12-13 12-5C12 1 4 7 0 11Z",
    D: "M0-13L10 0L0 13L-10 0Z",
    C: "M-3 4A6 6 0 1 1-5-6A6 6 0 1 1 5-6A6 6 0 1 1 3 4L4 12H-4Z"
  };
  const esc = value => String(value == null ? "" : value).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const hash = value => Array.from(String(value || "Balatle")).reduce((n, c) => ((n * 31) + c.charCodeAt(0)) >>> 0, 7);
  const finite = (n, fallback = 0) => Number.isFinite(Number(n)) ? Number(n) : fallback;
  const number = n => finite(n).toLocaleString(undefined, { maximumFractionDigits: 2 });

  function pip(suit, x, y, size = 1, rotation = 0, color = "currentColor") {
    return `<path d="${SUIT_PATHS[suit] || SUIT_PATHS.S}" fill="${color}" transform="translate(${x} ${y}) rotate(${rotation}) scale(${size})"/>`;
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
    return (positions[n] || [[40, 64]]).map(([x, y, rotation]) => pip(suit, x, y, n >= 9 ? .78 : .88, rotation || 0)).join("");
  }

  function aceArtwork(suit) {
    return `<path d="M40 11L68 36V91L40 117L12 91V36Z" fill="none" stroke="currentColor" stroke-width="1.2" opacity=".4"/>
      <path d="M40 18L62 40V86L40 109L18 86V40Z" fill="none" stroke="#ba9454" stroke-width="1.2"/>
      ${pip(suit, 40, 63, 2.05)}
      <path d="M40 21L43 27L40 33L37 27ZM40 95L43 101L40 107L37 101ZM17 59L21 64L17 69ZM63 59L59 64L63 69Z" fill="#ba9454"/>
      <path d="M24 91Q16 76 25 73M56 91Q64 76 55 73M25 40Q17 50 24 54M55 40Q63 50 56 54" fill="none" stroke="currentColor" stroke-width="1.2" opacity=".6"/>`;
  }

  function faceArtwork(rank, suit) {
    const isQueen = rank === "Q", isKing = rank === "K";
    const crown = isQueen
      ? `<path d="M25 23L21 10L32 16L40 7L48 16L59 10L55 23Z" fill="#d0a553" stroke="currentColor" stroke-width="1.6"/><circle cx="40" cy="17" r="2.3" fill="#ba4f3e"/>`
      : isKing
        ? `<path d="M24 24L21 10L31 15L40 6L49 15L59 10L56 24Z" fill="#d0a553" stroke="currentColor" stroke-width="1.8"/><path d="M27 20H53" stroke="#f6e4ac" stroke-width="2"/>`
        : `<path d="M21 23Q28 5 47 10L59 19L52 25Z" fill="currentColor"/><path d="M48 16Q60 0 65 8Q65 17 50 20" fill="#c9a151"/><path d="M24 23H54" stroke="#d0a553" stroke-width="4"/>`;
    const hair = isQueen
      ? `<path d="M25 22Q17 36 23 50L31 47L30 26M54 22Q63 35 56 50L49 48L50 26" fill="currentColor"/>`
      : `<path d="M25 23L23 40L29 43L29 25M54 23L57 40L51 42L51 25" fill="currentColor"/>`;
    const beard = isKing
      ? `<path d="M29 39L34 43L40 42L47 40L51 37L49 48L40 55L31 49Z" fill="currentColor"/><path d="M37 45H44" stroke="#f6e3ae" stroke-width="1.5"/>`
      : `<path d="M35 42Q40 45 45 41" fill="none" stroke="#a64b3b" stroke-width="1.4"/>`;
    const shoulder = isQueen
      ? `<path d="M12 65L18 52L29 47L40 57L51 47L62 52L69 65Z" fill="currentColor"/><path d="M23 52L40 63L57 52" fill="none" stroke="#d3ac5a" stroke-width="3"/><circle cx="40" cy="59" r="3" fill="#f5dc95"/>`
      : `<path d="M10 66L16 52L28 47L40 55L52 47L65 53L70 66Z" fill="currentColor"/><path d="M20 53L30 64M29 50L41 64M49 51L57 64" stroke="#cfa553" stroke-width="5"/>`;
    const half = `${shoulder}${hair}<path d="M28 23H52L51 39L46 47L39 50L31 45L27 35Z" fill="#e7c88c" stroke="currentColor" stroke-width="1.5"/>
      <path d="M30 30H36M43 30H49" stroke="currentColor" stroke-width="1.5"/>
      <circle cx="34" cy="32" r="1.2" fill="currentColor"/><circle cx="45" cy="32" r="1.2" fill="currentColor"/>
      <path d="M40 31L38 39H42" fill="none" stroke="#aa8151" stroke-width="1.3"/>${beard}${crown}
      ${isQueen ? `<path d="M10 53V32M10 32L6 27L10 22L14 27Z" fill="#cfa553" stroke="currentColor" stroke-width="1.4"/>` : `<path d="M67 52V20L70 13L73 20V52M63 40H76" fill="#ebdfbd" stroke="currentColor" stroke-width="1.6"/>`}`;
    return `<rect x="6" y="4" width="68" height="120" rx="2" fill="#e4d9b7" stroke="currentColor" stroke-width="1.5"/>
      <path d="M6 4L74 124M74 4L6 124" stroke="#bb995b" stroke-width=".9" opacity=".4"/>
      ${half}<g transform="rotate(180 40 64)">${half}</g>
      <path d="M6 63L74 65" stroke="#dec790" stroke-width="3"/>${pip(suit, 11, 66, .35)}${pip(suit, 69, 62, .35, 180)}`;
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
    return `<span class="card-inner${card.glass ? " glass-art" : ""}">
      <span class="card-corner corner-top" aria-hidden="true"><b class="rank">${rank}</b><span class="suit">${SUITS[suit]}</span></span>
      <svg class="card-art" viewBox="0 0 80 128" aria-hidden="true" focusable="false">${artwork}</svg>
      <span class="card-corner corner-bottom" aria-hidden="true"><b class="rank">${rank}</b><span class="suit">${SUITS[suit]}</span></span>
      ${options.small ? "" : `<span class="card-value" aria-hidden="true">${number(chips)}</span>`}
      ${enhancements ? `<span class="card-enhancements">${enhancements}</span>` : ""}
    </span>`;
  }

  // Every illustration is drawn here, not fetched. Motifs follow each item's effect;
  // a name-derived palette keeps large collections recognizable at table size.
  function itemArt(item) {
    const name = String(item.name || "Joker"), lower = name.toLowerCase();
    const kind = item.kind || item._kind || item._type || "joker";
    const palettes = [
      ["#263b59", "#5b85ac", "#e9c071", "#f2e6c0"],
      ["#52354e", "#a45b70", "#e4ad63", "#f1dac8"],
      ["#22504b", "#4c9a81", "#dbbe70", "#e5efd7"],
      ["#683d30", "#cb7151", "#e6c276", "#f6e5c1"],
      ["#3d365c", "#8b7eae", "#c8b677", "#eee1cd"],
      ["#3f4c32", "#8b9a57", "#e8c077", "#f0e6be"]
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
    const stroke = `stroke="${ink}" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"`;
    const motifs = {
      mask: `<path d="M25 47L18 19L37 32L50 13L62 32L82 19L75 47Z" fill="${gold}" ${stroke}/><circle cx="18" cy="19" r="4" fill="${light}" ${stroke}/><circle cx="50" cy="13" r="4" fill="${light}" ${stroke}/><circle cx="82" cy="19" r="4" fill="${light}" ${stroke}/><path d="M25 43H75L72 74L50 93L28 74Z" fill="${light}" ${stroke}/><path d="M50 44H75L72 74L50 93Z" fill="${mid}"/><path d="M33 57L43 53L40 64ZM67 57L57 53L60 64Z" fill="${ink}"/><path d="M36 74Q50 87 64 73Q50 77 36 74Z" fill="${ink}"/><path d="M29 44H71" stroke="${ink}" stroke-width="3"/>`,
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
    const rays = Array.from({ length: 12 }, (_, i) => `<path d="M50 55L50 -12L56 -12Z" fill="${light}" opacity=".055" transform="rotate(${i * 30} 50 55)"/>`).join("");
    return `<svg viewBox="0 0 100 110" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="${esc(name)} illustration" focusable="false">
      <rect width="100" height="110" rx="4" fill="${bg}"/>${rays}<rect x="5" y="5" width="90" height="100" rx="2" fill="none" stroke="${gold}" stroke-width=".8" opacity=".65"/>
      <path d="M11 11H18M11 11V18M89 11H82M89 11V18M11 99H18M11 99V92M89 99H82M89 99V92" fill="none" stroke="${gold}" stroke-width="1.3"/>
      ${motifs[motif] || motifs.mask}
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
    const reduced = !!root.matchMedia?.("(prefers-reduced-motion: reduce)").matches;
    let resolvePromise;
    const promise = new Promise(resolve => { resolvePromise = resolve; });
    const timers = [];
    let completed = false;
    const finish = cancelled => {
      if (completed) return;
      completed = true;
      for (const timer of timers) root.clearTimeout(timer);
      stage?.classList.remove("is-scoring");
      stage?.querySelectorAll(".score-burst").forEach(el => el.remove());
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
    const setFinal = () => {
      setText("scoreChips", number(result.chips));
      setText("scoreMult", number(result.mult));
      setText("scorePreview", `+${number(Math.floor(finite(result.gained)))}`);
    };
    setText("selectionName", result.handName || "Your hand");
    setText("stageLabel", "Scoring your hand");
    stage?.classList.add("is-scoring");
    const played = document?.getElementById("playedCards");
    if (played) {
      played.innerHTML = cards.map(c => `<div class="card${c.red || c.suit === "H" || c.suit === "D" ? " red" : ""}${scoringIds.has(c.id) ? " scoring" : " non-scoring"}" data-scoring-card="${esc(c.id)}">${cardMarkup(c, { small: true })}</div>`).join("");
    }
    if (!stage || reduced) {
      setFinal();
      setCaption(`${result.handName || "Hand"}: ${number(result.chips)} chips × ${number(result.mult)} Mult`);
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
    let displayedChips = initialChips;
    let displayedMult = initialMult;
    let cursor = 60;
    scoringCards.forEach((card, index) => {
      later(cursor, () => {
        const node = [...(played?.querySelectorAll("[data-scoring-card]") || [])].find(el => el.dataset.scoringCard === String(card.id));
        node?.classList.add("card-trigger");
        displayedChips += finite(card.baseChip) + finite(card.bonusChip);
        if (card.crowned) displayedMult += 1;
        setText("scoreChips", number(displayedChips));
        setText("scoreMult", number(displayedMult));
        setCaption(`${card.rank}${SUITS[card.suit] || ""}  +${number(finite(card.baseChip) + finite(card.bonusChip))} chips`);
        tone(380 + index * 70, .075);
      });
      cursor += 100;
    });
    triggers.forEach((trigger, index) => {
      later(cursor, () => {
        const node = [...document.querySelectorAll("[data-joker-id]")].find(el => el.dataset.jokerId === String(trigger.id));
        node?.classList.add("triggered");
        const effects = [];
        displayedChips = Math.max(0, displayedChips + finite(trigger.chips));
        displayedMult = Math.max(0, displayedMult + finite(trigger.mult)) * finite(trigger.xMult, 1);
        setText("scoreChips", number(displayedChips));
        setText("scoreMult", number(displayedMult));
        if (finite(trigger.chips)) effects.push(`${finite(trigger.chips) > 0 ? "+" : ""}${number(trigger.chips)} chips`);
        if (finite(trigger.mult)) effects.push(`${finite(trigger.mult) > 0 ? "+" : ""}${number(trigger.mult)} Mult`);
        if (finite(trigger.xMult, 1) !== 1) effects.push(`×${number(trigger.xMult)} Mult`);
        setCaption(`${trigger.name || "Joker"}${effects.length ? "  ·  " + effects.join(" · ") : " activated"}`);
        tone(620 + index * 65, .1, .035);
      });
      cursor += Math.max(60, Math.min(105, 450 / Math.max(1, triggers.length)));
    });
    later(cursor + 25, () => {
      setFinal();
      setCaption(`${result.handName || "Hand"}: ${number(result.chips)} chips × ${number(result.mult)} Mult`);
      stage.classList.remove("is-scoring");
      void stage.offsetWidth;
      stage.classList.add("is-scoring");
      tone(880, .16, .045);
      const burst = document.createElement("div");
      burst.className = "score-burst";
      burst.setAttribute("aria-hidden", "true");
      burst.innerHTML = Array.from({ length: 10 }, (_, i) => {
        const angle = i / 10 * Math.PI * 2;
        return `<i class="score-spark" style="--spark-x:${Math.round(Math.cos(angle) * (85 + i * 8))}px;--spark-y:${Math.round(Math.sin(angle) * 60 - 35)}px"></i>`;
      }).join("");
      stage.appendChild(burst);
      later(650, () => burst.remove());
    });
    later(cursor + 430, () => {
      stage.querySelectorAll(".score-burst").forEach(el => el.remove());
      finish(false);
    });
    return promise;
  }

  root.BalatleTable = Object.freeze({
    cardMarkup, itemArt, scorePlayback, cancelPlayback, toggleSound,
    isSoundOn: () => soundOn, escapeHTML: esc
  });
})(typeof window !== "undefined" ? window : globalThis);
