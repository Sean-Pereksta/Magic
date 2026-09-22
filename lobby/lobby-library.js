(function (root) {
  "use strict";

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
      if (game.launchMode === "realm") {
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
    card.className = "game-card";
    card.dataset.gameKey = game.key;
    card.style.setProperty("--game", gameColor(game.key));
    const favorite = favorites.has(game.key);
    const modeTags = game.modes.map(mode => `<span class="tag">${mode === "single" ? "Solo" : "Multiplayer"}</span>`).join("");
    const download = game.downloadRoute
      ? `<a class="btn secondary small download-game-btn" href="${escapeHtml(game.downloadRoute)}" download>Download</a>` : "";
    const buttons = actions.map((action, index) =>
      `<button type="button" class="btn small play-game-btn${index === 0 && actions.length > 1 ? " secondary" : ""}" data-game-action="${action.type}" aria-label="${escapeHtml(`${action.label}: ${game.title}`)}">${escapeHtml(action.label)}</button>`
    ).join("");
    card.innerHTML = `
      <div class="game-art">${gameVisual(game, "art")}<button type="button" class="favorite-btn ${favorite ? "active" : ""}" title="Favorite" aria-label="Favorite ${escapeHtml(game.title)}" aria-pressed="${favorite}">${favorite ? "★" : "☆"}</button></div>
      <div class="game-content"><div class="game-name">${escapeHtml(game.title)}</div><div class="game-description">${escapeHtml(game.desc)}</div><div class="game-meta"><span class="tag">${escapeHtml(game.category)}</span>${modeTags}</div>
      <div class="game-bottom"><span class="play-count">🎮 ${Number(gamePlayCounts[game.key]) || 0} plays</span><span class="game-actions">${download}${buttons}</span></div></div>`;
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
    html = replaceOnce(html, "</head>", `${MARKER}\n${STYLES}\n</head>`, "document head");
    return html;
  }

  const api = Object.freeze({ getActions, runAction, createGameCard, upgradeHtml });
  if (typeof module === "object" && module.exports) module.exports = api;
  else root.GameHubLibrary = api;
})(globalThis);
