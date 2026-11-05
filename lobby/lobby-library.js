(function (root) {
  "use strict";

  const TURNCRAFT_COVER = "https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/effects/ui/turncraft-lobby.webp";
  const turncraftCoverLoads = new WeakMap();
  const FLAG_FACTIONS = `<div class="fc-factions" aria-label="Three factions: Lumen Brood, Crooked Carnival and Aureate Choir">
    <span class="fc-brood" title="Lumen Brood">✧ Brood</span><span class="fc-carnival" title="Crooked Carnival">◈ Carnival</span><span class="fc-choir" title="Aureate Choir">◎ Choir</span>
  </div>`;
  const TURNCRAFT_ART = `<div class="tc-faction-art" aria-hidden="true">
    <span class="tc-portrait tc-frontier"><img src="/game/turncraft/assets/factions/frontier.webp" alt="" loading="lazy" decoding="async"></span>
    <span class="tc-portrait tc-ascendant"><img src="/game/turncraft/assets/factions/ascendant.webp" alt="" loading="lazy" decoding="async"></span>
    <span class="tc-portrait tc-brood"><img src="/game/turncraft/assets/factions/brood.webp" alt="" loading="lazy" decoding="async"></span>
    <img class="tc-cover" alt="" decoding="async" hidden>
  </div><span class="tc-art-badge">Faction warfare</span><span class="tc-art-rule" aria-hidden="true"></span>`;
  const TURNCRAFT_FACTIONS = `<div class="tc-factions" aria-label="Three playable factions">
    <span class="tc-frontier"><svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3 2h10v7l-5 5-5-5V2Z M5 6h6 M8 3v7"/></svg>Frontier</span>
    <span class="tc-ascendant"><svg viewBox="0 0 16 16" aria-hidden="true"><path d="m8 1 5 7-5 7-5-7 5-7Z M3 8h10 M8 1v14"/></svg>Ascendant</span>
    <span class="tc-brood"><svg viewBox="0 0 16 16" aria-hidden="true"><path d="m3 2 3 4 2-2 2 2 3-4 M2 7l4 2 2 5 2-5 4-2 M3 13l3-4 M13 13l-3-4"/></svg>Brood</span>
  </div>`;

  function loadTurncraftCover(card, document) {
    // All catalog, recent and favorite cards share one probe. An unavailable
    // upload leaves the tracked faction artwork in place without retry loops.
    let ready = turncraftCoverLoads.get(document);
    if (!ready) {
      ready = new Promise(resolve => {
        const probe = document.createElement("img");
        probe.addEventListener("load", () => resolve(TURNCRAFT_COVER), { once: true });
        probe.addEventListener("error", () => resolve(null), { once: true });
        probe.src = TURNCRAFT_COVER;
      });
      turncraftCoverLoads.set(document, ready);
    }
    const cover = card.querySelector(".tc-cover");
    ready.then(url => {
      if (!url) return;
      cover.addEventListener("load", () => {
        cover.hidden = false;
        card.classList.add("tc-cover-ready");
      }, { once: true });
      cover.src = url;
    });
    card.querySelectorAll(".tc-portrait img").forEach(image => {
      image.addEventListener("error", () => { image.hidden = true; }, { once: true });
    });
  }

  // The hub's existing registry remains the source of truth. A shared world is
  // not a hosted room: retain its working entry action instead of creating a
  // lobby that the game's networking code cannot use.
  function getActions(game) {
    const modes = Array.isArray(game?.modes) ? game.modes : [];
    const single = modes.includes("single");
    const multi = modes.includes("multi");
    const actions = [];
    if (single) actions.push({ type: "single", label: multi ? "Play Offline" : "Play" });
    if (multi) {
      if (game.multiplayerRoute) {
        actions.push({ type: "online", label: game.joinLabel || "Join Match" });
      } else if (game.launchMode === "realm") {
        actions.push({ type: "online", label: game.joinLabel || "Enter World" });
      } else if (game.launchMode === "matchmaking") {
        actions.push({ type: "online", label: game.joinLabel || "Find Match" });
      } else {
        actions.push({ type: "lobby", label: "Create Lobby" });
      }
    }
    return actions;
  }

  function runAction(game, action, hooks) {
    // Do not allow an unsupported action, including accidental solo launches
    // of online-only games. Keep account gates and lobby selection in the hub.
    if (!getActions(game).some(candidate => candidate.type === action)) {
      throw new Error("This game does not support the selected action.");
    }
    if (action === "single") return hooks.playSingle(game, true);
    if (action === "lobby") return hooks.openCreateLobby(game.key);
    return hooks.joinOnlineGame(game, true);
  }

  function createGameCard(game, hooks) {
    const { document, escapeHtml, gameColor, gameVisual, favorites, gamePlayCounts } = hooks;
    const actions = getActions(game);
    const card = document.createElement("article");
    const isTurncraft = game.key === "turncraft" && !root.GameHubArtwork;
    const isFlagCapture = game.key === "flag";
    const artwork = isTurncraft ? TURNCRAFT_ART : gameVisual(game, "art");
    const cover = isFlagCapture ? artwork.replace(/ sizes="[^"]*"/, ' sizes="auto, (max-width:600px) 92vw, (max-width:1000px) 45vw, 320px"') : artwork;
    card.className = isTurncraft ? "game-card tc-tile" : isFlagCapture ? "game-card fc-tile" : "game-card";
    card.dataset.gameKey = game.key;
    card.style.setProperty("--game", isFlagCapture ? "#8de4d6" : gameColor(game.key));
    const favorite = favorites.has(game.key);
    const modeTags = isFlagCapture ? '<span class="tag fc-mode">AI / local 2P</span><span class="tag">5-mission campaign</span>' : game.modes.map(mode => `<span class="tag">${mode === "single" ? "Solo" : "Multiplayer"}</span>`).join("");
    const download = game.downloadRoute
      ? `<a class="btn secondary small download-game-btn" href="${escapeHtml(game.downloadRoute)}" download>Download</a>` : "";
    const buttons = actions.map((action, index) =>
      `<button type="button" class="btn small play-game-btn${index === 0 && actions.length > 1 ? " secondary" : ""}" data-game-action="${action.type}" aria-label="${escapeHtml(`${action.label}: ${game.title}`)}">${escapeHtml(action.label)}</button>`
    ).join("");
    card.innerHTML = `
      <div class="game-art">${cover}${isFlagCapture ? '<span class="fc-art-badge">COOLDOWN TACTICS</span>' : ""}<button type="button" class="favorite-btn ${favorite ? "active" : ""}" title="Favorite" aria-label="Favorite ${escapeHtml(game.title)}" aria-pressed="${favorite}">${favorite ? "★" : "☆"}</button></div>
      <div class="game-content">${isTurncraft ? '<p class="tc-card-kicker">Build. Command. Conquer.</p>' : ""}<div class="game-name">${escapeHtml(game.title)}</div>${isFlagCapture ? FLAG_FACTIONS : ""}<div class="game-description">${escapeHtml(game.desc)}</div>${isFlagCapture ? '<p class="fc-features">21 troops <span>·</span> formations <span>·</span> living ground</p>' : ""}${isTurncraft ? TURNCRAFT_FACTIONS : ""}<div class="game-meta"><span class="tag">${escapeHtml(game.category)}</span>${modeTags}</div>
      <div class="game-bottom"><span class="play-count">🎮 ${Number(gamePlayCounts[game.key]) || 0} plays</span><span class="game-actions">${download}${buttons}</span></div></div>`;
    if (isTurncraft) loadTurncraftCover(card, document);
    card.querySelector(".favorite-btn").addEventListener("click", event => {
      event.stopPropagation();
      hooks.toggleFavorite(game.key);
    });
    let busy = false;
    const actionButtons = [...card.querySelectorAll("[data-game-action]")];
    actionButtons.forEach(button => button.addEventListener("click", async () => {
      if (busy) return;
      busy = true;
      actionButtons.forEach(item => { item.disabled = true; });
      try {
        await runAction(game, button.dataset.gameAction, hooks);
      } catch (error) {
        console.error("Game Library action failed", game.key, error);
        hooks.toast("Could not open that game. Please try again.", "bad");
      } finally {
        busy = false;
        actionButtons.forEach(item => { item.disabled = false; });
      }
    }));
    return card;
  }

  const MARKER = "<!-- game-hub-library:v1 -->";
  const COPY = "Every game in one place. Play solo, create a multiplayer lobby, or enter a shared online world.";
  const STYLES = `<style id="gameLibraryStyles">
    .arcade-hub .game-card.fc-tile,.game-card.fc-tile{background:linear-gradient(145deg,#21464b,#142f37);border-color:#416c6a;overflow:hidden}
    .game-card.fc-tile .game-art{position:relative;aspect-ratio:16/9;background:#163a42;overflow:hidden}
    .game-card.fc-tile .game-visual-art{position:absolute;inset:0;width:100%;height:100%}
    .game-card.fc-tile .game-visual-art img{width:100%;height:100%;object-fit:cover;object-position:center}
    .game-card.fc-tile .fc-art-badge{position:absolute;left:12px;top:12px;z-index:2;padding:6px 8px;border:1px solid #a1dfcf88;border-radius:6px;background:#10343de8;color:#d3f4dc;font:800 8px 'Segoe UI',sans-serif;letter-spacing:1.1px;pointer-events:none;box-shadow:0 2px 8px #0c25376b}
    .game-card.fc-tile .favorite-btn{z-index:3}
    .game-card.fc-tile .game-content{gap:8px}
    .game-card.fc-tile .game-name{font-family:'Trebuchet MS','Segoe UI',sans-serif;color:#f3f3db}
    .game-card.fc-tile .game-description{color:#b9cfcc}
    .game-card.fc-tile .fc-factions{display:flex;flex-wrap:wrap;gap:5px;margin:0}
    .fc-factions>span{border:1px solid currentColor;border-radius:5px;padding:3px 6px;font:700 9px 'Segoe UI',sans-serif;line-height:1.1;white-space:nowrap;background:#102e345c}
    .fc-factions .fc-brood{color:#9ee8ca}.fc-factions .fc-carnival{color:#ffb4c3}.fc-factions .fc-choir{color:#f2db91}
    .game-card.fc-tile .fc-features{margin:0;color:#d6e8d2;font-size:10px;line-height:1.5;font-weight:650}
    .fc-features>span{color:#679e93;margin:0 3px}
    .game-card.fc-tile .game-meta{gap:5px}.game-card.fc-tile .tag{font-size:9px;border-color:#46716d;background:#1d4148;color:#c4ded5}
    .game-card.fc-tile .fc-mode{color:#d0f3db}.game-card.fc-tile .play-game-btn{background:#a2e8c3;border-color:#a2e8c3;color:#173c40}
    .arcade-hub .game-card.fc-tile:hover{border-color:#8ed7c4;box-shadow:0 10px 26px #061c2f50}
    @media(max-width:600px){.game-card.fc-tile .fc-art-badge{left:10px;top:10px}.game-card.fc-tile .fc-factions>span{font-size:10px}.game-card.fc-tile .fc-features{font-size:11px}.game-card.fc-tile .tag{font-size:10px}}
    .arcade-hub #cnmRecentGrid .game-card.fc-tile .fc-art-badge,.arcade-hub #cnmRecentGrid .game-card.fc-tile .fc-factions,.arcade-hub #cnmRecentGrid .game-card.fc-tile .fc-features{display:none}
    .game-card[data-game-key="crowncouncil"] .game-art{position:relative;overflow:hidden;background:#17232b}
    .game-card[data-game-key="crowncouncil"] .game-visual-art{width:100%;height:100%;position:absolute;inset:0}
    .game-card[data-game-key="crowncouncil"] .game-visual-art img{object-fit:cover;object-position:center 30%}
    .game-card[data-game-key="crowncouncil"] .favorite-btn{z-index:2}
    .game-card .game-bottom{align-items:flex-start;gap:10px;flex-wrap:wrap}
    .game-card .game-actions{display:flex;flex-wrap:wrap;justify-content:flex-end;gap:8px;min-width:0;max-width:100%}
    .game-card .game-actions .btn{min-height:44px;white-space:normal;text-align:center}
    .game-card .game-actions .btn:focus-visible,.game-card .favorite-btn:focus-visible{outline:3px solid var(--brand);outline-offset:3px}
    @media(max-width:760px){.game-card .game-actions{width:100%}.game-card .game-actions .btn{flex:1 1 120px}}
  </style>`;

  function replaceOnce(html, before, after, name) {
    if (html.split(before).length !== 2) {
      throw new Error(`Game Library could not find a unique ${name} in the hub core.`);
    }
    return html.replace(before, () => after);
  }

  function upgradeHtml(input) {
    if (typeof input !== "string") throw new TypeError("Game Library requires HTML text.");
    if (input.includes(MARKER)) return input;
    let html = input;
    // Keep the internal singleplayer view id and saved gh.lastTab values. Only
    // its visible name and contents change, so old navigation remains valid.
    let navCount = 0;
    html = html.replace(/(data-view="singleplayer"[^>]*>)([\s\S]*?)(<\/button>)/g, (match, start, label, end) => {
      if (!label.includes("Singleplayer")) return match;
      navCount++;
      return start + label.replace("Singleplayer", "Library") + end;
    });
    if (navCount !== 2) throw new Error("Game Library could not find both navigation buttons.");
    html = replaceOnce(html, ">Singleplayer Library</div>", ">Game Library</div>", "library heading");
    html = replaceOnce(html, "Continue recent games, save favorites, or browse by category.", COPY, "library introduction");
    html = replaceOnce(html, "let games=SINGLE_GAMES.filter(", "let games=GAMES.filter(", "catalog filter");
    html = replaceOnce(html,
      '.filter(k=>getGame(k).modes.includes("single")).slice(0,8)',
      '.filter(k=>Object.prototype.hasOwnProperty.call(GAME_BY_KEY,k)).slice(0,8)',
      "recent games filter");
    html = replaceOnce(html,
      'const favGames=[...favorites].map(getGame).filter(g=>g.modes.includes("single"));',
      'const favGames=[...favorites].map(getGame).filter(g=>Object.prototype.hasOwnProperty.call(GAME_BY_KEY,g.key));',
      "favorites filter");
    const cardStart = "    function gameCard(game,compact=false){";
    const cardEnd = "    function renderAllGames(){";
    const start = html.indexOf(cardStart);
    const end = html.indexOf(cardEnd, start);
    if (start < 0 || end <= start || html.indexOf(cardStart, start + 1) !== -1) {
      throw new Error("Game Library could not locate the hub game card renderer.");
    }
    html = html.slice(0, start) + `    function gameCard(game,compact=false){
      return globalThis.GameHubLibrary.createGameCard(game,{
        document,escapeHtml,gameColor,gameVisual,favorites,gamePlayCounts,
        toggleFavorite,playSingle,openCreateLobby,joinOnlineGame,toast
      });
    }
` + html.slice(end);
    html = replaceOnce(html, "</head>", `${MARKER}\n${STYLES}\n<link rel="stylesheet" href="./turncraft-tile.css?v=1">\n</head>`, "document head");
    return html;
  }

  const api = Object.freeze({ getActions, runAction, createGameCard, upgradeHtml });
  if (typeof module === "object" && module.exports) module.exports = api;
  else root.GameHubLibrary = api;
})(globalThis);


