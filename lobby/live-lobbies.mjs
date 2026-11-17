const BANNERLANDS = "https://sean-pereksta.github.io/Truth/games/bannerlands/";
const ROOM_ID = /^[a-z0-9_-]{1,100}$/i;
const REFRESH_MS = 30_000;
const GAME_NAMES = {catandmouse: "Cat & Mouse", bannerlands: "Bannerlands"};

function isLoopback(hostname) {
  return ["localhost", "127.0.0.1", "[::1]"].includes(hostname);
}

function hostOrigin(value, local) {
  try {
    const url = new URL(value);
    if (url.username || url.password || url.search || url.hash || url.pathname !== "/") return null;
    if (isLoopback(url.hostname)) return local && url.protocol === "http:" ? url.origin : null;
    const isPublicHost = url.hostname.includes(".") && !/^[\d.]+$/.test(url.hostname) &&
      !/\.(localhost|local|internal|invalid|test)$/.test(url.hostname);
    return url.protocol === "https:" && !url.port && isPublicHost ? url.origin : null;
  } catch {
    return null;
  }
}

function roomText(value, fallback, length = 80) {
  const info = typeof value === "string" ? value.replace(/[\u0000-\u001f\u007f]/g, "").trim() : "";
  return info.slice(0, length) || fallback;
}

export function catRooms(records, {version, now = Date.now(), local = false}) {
  if (!Array.isArray(records)) throw new Error("Invalid lobby directory response.");
  return records.slice(0, 100).flatMap(room => {
    const origin = hostOrigin(room?.serverUrl, local);
    const expiresAt = Date.parse(room?.expiresAt);
    if (room?.game !== "catandmouse" || typeof room.id !== "string" || !ROOM_ID.test(room.id) ||
        !origin || room.public === false || !(expiresAt > now) ||
        !Number.isInteger(room.capacity) || room.capacity < 1 || room.capacity > 4 ||
        !Number.isInteger(room.openSeats) || room.openSeats < 0 || room.openSeats >= room.capacity) return [];
    const compatible = room.version === version;
    return [{
      id: room.id, origin, name: roomText(room.roomName, "Cat & Mouse room"),
      hostName: roomText(room.hostName, "Host", 40), map: roomText(room.map, "Survival"),
      difficulty: roomText(room.difficulty, "Standard", 24), openSeats: room.openSeats,
      expiresAt, compatible, joinable: compatible && room.openSeats > 0
    }];
  });
}

export function joinHref(game, room, arcadeUrl, bannerlands = BANNERLANDS) {
  const arcade = new URL(arcadeUrl);
  const localOverride = isLoopback(arcade.hostname) && arcade.searchParams.get("catDirectory");
  arcade.search = "";
  if (localOverride) arcade.searchParams.set("catDirectory", localOverride);
  arcade.hash = "live-lobbies";

  const params = new URLSearchParams({room: room.id, returnUrl: arcade.href});
  const guest = new URL("/", room.origin);
  if (game === "bannerlands") {
    params.set("host", room.origin);
    return new URL("#" + params, bannerlands).href;
  }
  guest.hash = params;
  return guest.href;
}

async function readCatLobbies(page) {
  const {CAT_DIRECTORY_URL, PROTOCOL_VERSION} = await import("../game/catandmouse/edition/directory-config.mjs");
  const local = isLoopback(page.hostname);
  const base = (local && page.searchParams.get("catDirectory")) || CAT_DIRECTORY_URL;
  if (!base) throw new Error("Cat & Mouse discovery has no directory address.");
  const directory = new URL(base);
  if (directory.username || directory.password || directory.search || directory.hash ||
      !(directory.protocol === "https:" || local && isLoopback(directory.hostname) && directory.protocol === "http:")) {
    throw new Error("Invalid Cat & Mouse directory address.");
  }
  const response = await fetch(new URL("rooms", directory.href.replace(/\/?$/, "/")), {
    cache: "no-store", credentials: "omit", signal: AbortSignal.timeout(8_000)
  });
  if (!response.ok) throw new Error("Could not read the Cat & Mouse directory.");
  const json = await response.json();
  return catRooms(json.rooms, {version: PROTOCOL_VERSION, local});
}

let bannerlandsDirectory;
async function readBannerlandsLobbies() {
  bannerlandsDirectory ??= Promise.all([
    import(new URL("multiplayer/directory.js", BANNERLANDS)),
    import(new URL("multiplayer/protocol.js", BANNERLANDS))
  ]).then(([{createFirebaseDirectory}, protocol]) => ({
    directory: createFirebaseDirectory({requestTimeoutMs: 8_000}),
    compatibility: {protocolVersion: protocol.PROTOCOL_VERSION, gameVersion: protocol.GAME_VERSION, assetVersion: protocol.ASSET_VERSION}
  })).catch(error => {
    bannerlandsDirectory = null;
    throw error;
  });
  const {directory, compatibility} = await bannerlandsDirectory;
  const campaigns = await directory.list({...compatibility, includeUnavailable: true});
  return campaigns.map(room => ({...room, openSeats: room.availableSeats, map: room.map || room.mode || "Conquest"}));
}

function element(tag, value) {
  const node = document.createElement(tag);
  node.textContent = value;
  return node;
}

function roomRow(game, room) {
  const row = document.createElement("li");
  const heading = element("h4", room.name);
  const details = element("p", GAME_NAMES[game] + " · Hosted by " + room.hostName + " · " + room.map + " · " + (room.difficulty || "Standard"));
  const space = room.openSeats + (room.openSeats === 1 ? " open seat" : " open seats");
  const facts = element("p", room.compatible ? space : space + " · Update required");
  const info = document.createElement("div");
  info.append(heading, details, facts);
  const join = element(room.joinable ? "a" : "button", "Join");
  join.className = "live-lobbies-join";
  join.setAttribute("aria-label", "Join " + room.name + " in " + GAME_NAMES[game]);
  if (room.joinable) {
    join.href = joinHref(game, room, location.href);
    join.rel = "noreferrer";
    join.addEventListener("click", event => {
      if (room.expiresAt > Date.now()) return;
      event.preventDefault();
      join.removeAttribute("href");
      join.setAttribute("aria-disabled", "true");
      facts.textContent = "This listing expired. Choose another room.";
    });
  } else {
    join.type = "button";
    join.disabled = true;
  }
  row.append(info, join);
  return row;
}

function mount(section) {
  const results = Object.fromEntries(Object.keys(GAME_NAMES).map(game => [game, {
    section: section.querySelector('[data-live-section="' + game + '"]'),
    rooms: [], status: "loading", pending: false, signature: ""
  }]));
  const page = new URL(location.href);
  let inView = false;
  let filter = "all";

  function render(game) {
    const result = results[game];
    const fresh = result.rooms.filter(room => room.expiresAt > Date.now());
    const status = result.section.querySelector("[data-live-status]");
    const rooms = result.section.querySelector("[data-live-rooms]");
    const message = result.status === "loading" ? "Checking the directory…" :
      result.status === "unavailable" ? "The directory is unavailable. Try an invitation from your host." :
      fresh.length ? "" : "No public rooms are open right now.";
    if (status.textContent !== message) status.textContent = message;
    status.hidden = !message;
    const signature = JSON.stringify(fresh);
    if (signature !== result.signature) {
      const activeRoom = document.activeElement?.closest("[data-room-id]")?.dataset.roomId;
      const rows = fresh.map(room => {
        const row = roomRow(game, room);
        row.dataset.roomId = room.id;
        return row;
      });
      rooms.replaceChildren(...rows);
      rows.find(row => row.dataset.roomId === activeRoom)?.querySelector("a")?.focus({preventScroll: true});
      result.signature = signature;
    }
  }

  async function refresh(game) {
    const result = results[game];
    if (result.pending || !inView || document.hidden || result.section.hidden) return;
    result.pending = true;
    try {
      result.rooms = await (game === "catandmouse" ? readCatLobbies(page) : readBannerlandsLobbies());
      result.status = "ready";
    } catch {
      result.rooms = [];
      result.status = "unavailable";
    } finally {
      result.pending = false;
      render(game);
    }
  }

  function refreshVisible() {
    for (const game of Object.keys(results)) void refresh(game);
  }

  function select(game) {
    if (game !== "all" && !(game in GAME_NAMES)) return;
    filter = game;
    section.querySelectorAll("[data-live-filter]").forEach(button => {
      button.setAttribute("aria-pressed", String(button.dataset.liveFilter === filter));
    });
    for (const [name, result] of Object.entries(results)) {
      result.section.hidden = filter !== "all" && filter !== name;
    }
    refreshVisible();
  }

  section.addEventListener("click", event => {
    const target = event.target.closest("[data-live-filter]");
    if (target) select(target.dataset.liveFilter);
  });
  document.addEventListener("click", event => {
    const target = event.target.closest("[data-live-game]");
    if (target) select(target.dataset.liveGame);
  });

  if (typeof IntersectionObserver === "function") {
    const observer = new IntersectionObserver(([entry]) => {
      inView = entry.isIntersecting;
      if (inView) refreshVisible();
    });
    observer.observe(section);
  } else {
    inView = true;
    refreshVisible();
  }
  document.addEventListener("visibilitychange", refreshVisible);
  setInterval(refreshVisible, REFRESH_MS);
  setInterval(() => Object.keys(results).forEach(render), 1_000);
}

if (typeof document !== "undefined") {
  const section = document.getElementById("live-lobbies");
  if (section) mount(section);
}
