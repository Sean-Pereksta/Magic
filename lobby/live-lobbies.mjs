import "../shared/managed-servers.js?v=magic-servers-3";

const REFRESH_MS = 30_000;
const VIEW = "livelobbies";
const HASH = "#live-lobbies";
const onLobbyHash = () => location.hash === HASH || location.hash.startsWith(HASH + "/");
const PHASE_LABELS = {waiting: "In lobby", playing: "In game", racing: "Racing", finished: "Match finished"};
const roomKey = room => room.game + ":" + room.code;

function element(tag, value, className) {
  const node = document.createElement(tag);
  node.textContent = value;
  if (className) node.className = className;
  return node;
}

function mount(section, directory) {
  const view = section.closest(".view");
  const list = section.querySelector("[data-live-rooms]");
  const status = section.querySelector("[data-live-status]");
  const feedback = section.querySelector("[data-live-feedback]");
  const search = section.querySelector("[data-live-search]");
  const filters = [...section.querySelectorAll("[data-live-filter]")];
  const refreshButton = section.querySelector("[data-live-refresh]");
  const results = new Map(Object.keys(directory.titles).map(game => [game, {rooms: [], failed: false}]));
  let activeGame = "all";
  let activeRows = new Map();
  let refreshing = false;
  let loaded = false;
  let joining = false;
  let signature;

  function setFeedback(message) {
    feedback.textContent = message;
    feedback.hidden = !message;
  }

  function roomRow(room) {
    const row = document.createElement("li");
    const name = document.createElement("div");
    const title = directory.titles[room.game];
    const key = roomKey(room);
    row.dataset.roomId = key;
    name.append(element("p", title, "live-lobbies-game"), element("h3", room.name));
    const phase = element("span", PHASE_LABELS[room.phase], "live-lobbies-status");
    phase.dataset.phase = room.phase;
    const players = element("span", room.players + " / " + room.capacity + " players", "live-lobbies-players");
    const join = element("button", "Join", "live-lobbies-join");
    join.type = "button";
    join.setAttribute("aria-label", "Join " + room.name + " in " + title);
    join.addEventListener("click", async () => {
      if (joining) return;
      const current = activeRows.get(key);
      if (!current) return;
      joining = true;
      join.disabled = true;
      setFeedback("Checking " + current.name + "…");
      try {
        const available = await directory.probe(current);
        if (available && view.classList.contains("active")) {
          location.assign(directory.joinUrl(available));
        } else {
          setFeedback("This server has no seat available or has gone offline.");
        }
      } catch {
        setFeedback("Could not join this server. Try again.");
      } finally {
        joining = false;
        join.disabled = false;
      }
    });
    row.append(name, phase, players, join);
    return row;
  }

  function render() {
    const query = search.value.trim().toLocaleLowerCase();
    const games = activeGame === "all" ? [...results.values()] : [results.get(activeGame)];
    const rooms = games.flatMap(game => game.rooms)
      .filter(room => room.expires > Date.now() &&
        (room.name + " " + directory.titles[room.game]).toLocaleLowerCase().includes(query))
      .sort((a, b) => Number(b.joinable) - Number(a.joinable) ||
        directory.titles[a.game].localeCompare(directory.titles[b.game]) || a.name.localeCompare(b.name));
    activeRows = new Map(rooms.map(room => [roomKey(room), room]));
    const nextSignature = JSON.stringify(rooms.map(room =>
      [roomKey(room), room.name, room.phase, room.players, room.capacity]));
    if (signature !== nextSignature) {
      const activeKey = document.activeElement?.closest("[data-room-id]")?.dataset.roomId;
      const rows = rooms.map(roomRow);
      list.replaceChildren(...rows);
      rows.find(row => row.dataset.roomId === activeKey)?.querySelector("button")?.focus({preventScroll: true});
      signature = nextSignature;
    }
    const failed = games.filter(game => game.failed).length;
    let message = "";
    if (!loaded) message = "Checking the directory…";
    else if (failed === games.length) message = "Could not refresh the directory. You can still use a host invitation.";
    else if (failed) message = "Some games could not be refreshed.";
    else if (!rooms.length) message = query ? "No servers match your search." : "No public servers are online.";
    if (status.textContent !== message) status.textContent = message;
    status.hidden = !message;
  }

  async function refresh() {
    if (!view.classList.contains("active") || document.hidden || refreshing) return;
    refreshing = true;
    refreshButton.disabled = true;
    const games = [...results.keys()];
    try {
      const batches = await Promise.allSettled(games.map(game => directory.publicServers(game)));
      batches.forEach((batch, index) => {
        const result = results.get(games[index]);
        result.failed = batch.status === "rejected";
        if (!result.failed) result.rooms = batch.value;
      });
      loaded = true;
      render();
    } finally {
      refreshing = false;
      refreshButton.disabled = false;
    }
  }

  function select(key) {
    const game = key === "catandmouse" ? "cat" : key;
    if (game !== "all" && !results.has(game)) return;
    activeGame = game;
    for (const button of filters) button.setAttribute("aria-pressed", String(button.dataset.liveFilter === key));
    setFeedback("");
    render();
  }

  function activateWithoutHub() {
    document.querySelectorAll(".view").forEach(node => node.classList.toggle("active", node === view));
    document.querySelectorAll("[data-view]").forEach(button => {
      const active = button.dataset.view === VIEW;
      button.classList.toggle("active", active);
      if (active) button.setAttribute("aria-current", "page");
      else button.removeAttribute("aria-current");
    });
    try { localStorage.setItem("gh.lastTab", VIEW); } catch {}
  }

  const navButton = document.querySelector('[data-view="livelobbies"]');
  function openHash() {
    if (!onLobbyHash()) return;
    const game = location.hash.slice(HASH.length + 1);
    navButton?.click();
    if (game) select(game);
  }

  document.addEventListener("click", event => {
    const nav = event.target.closest("[data-view]");
    if (nav?.dataset.view === VIEW) {
      // The ordinary hub view handler runs first. The directory also works
      // while the hub's unrelated Firebase SDK is unavailable.
      if (!view.classList.contains("active")) activateWithoutHub();
      if (!onLobbyHash()) history.replaceState(history.state, "", location.pathname + location.search + HASH);
      void refresh();
    } else if (nav && onLobbyHash()) {
      history.replaceState(history.state, "", location.pathname + location.search);
    }
    const gameLink = event.target.closest("[data-live-game]");
    if (gameLink) select(gameLink.dataset.liveGame);
  });
  for (const button of filters) button.addEventListener("click", () => select(button.dataset.liveFilter));
  search.addEventListener("input", render);
  refreshButton.addEventListener("click", refresh);
  new MutationObserver(() => void refresh()).observe(view, {attributes: true, attributeFilter: ["class"]});
  document.addEventListener("visibilitychange", refresh);
  window.addEventListener("pageshow", refresh);
  window.addEventListener("focus", refresh);
  window.addEventListener("hashchange", openHash);
  setInterval(refresh, REFRESH_MS);
  openHash();
  void refresh();
}

const section = globalThis.document?.getElementById("live-lobbies");
if (section && window.CatnmiceServers) mount(section, window.CatnmiceServers);
