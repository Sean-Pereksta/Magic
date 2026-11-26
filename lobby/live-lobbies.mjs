import "../shared/managed-servers.js?v=magic-servers-1";

const REFRESH_MS = 30_000;

function element(tag, value) {
  const node = document.createElement(tag);
  node.textContent = value;
  return node;
}

function mount(section, directory) {
  const results = new Map([...section.querySelectorAll("[data-live-section]")].map(node => {
    const game = node.dataset.liveSection;
    return [game, {node, rooms: [], message: "Checking the directory…"}];
  }));
  let inView = false;
  let joining = false;

  function roomRow(room, listStatus) {
    const row = document.createElement("li");
    const info = document.createElement("div");
    const heading = element("h4", room.name);
    const facts = element("p", directory.summary(room));
    const join = element("button", room.joinable ? "Join" : "Check for seat");
    info.append(heading, facts);
    join.type = "button";
    join.className = "live-lobbies-join";
    join.setAttribute("aria-label", "Join " + room.name + " in " + directory.titles[room.game]);
    join.addEventListener("click", async () => {
      if (joining) return;
      joining = true;
      join.disabled = true;
      listStatus.hidden = false;
      listStatus.textContent = "Checking " + room.name + "…";
      const available = await directory.probe(room);
      joining = false;
      join.disabled = false;
      if (available) {
        location.assign(directory.joinUrl(available));
      } else {
        listStatus.textContent = "This server has no seat available or has gone offline.";
      }
    });
    row.append(info, join);
    return row;
  }

  function render(result) {
    const status = result.node.querySelector("[data-live-status]");
    const list = result.node.querySelector("[data-live-rooms]");
    const fresh = result.rooms.filter(room => room.expires > Date.now());
    const signature = JSON.stringify(fresh);
    if (result.signature !== signature) {
      const activeCode = document.activeElement?.closest("[data-room-id]")?.dataset.roomId;
      const rows = fresh.map(room => {
        const row = roomRow(room, status);
        row.dataset.roomId = room.code;
        return row;
      });
      list.replaceChildren(...rows);
      rows.find(row => row.dataset.roomId === activeCode)?.querySelector("button")?.focus({preventScroll: true});
      result.signature = signature;
    }
    status.textContent = result.message || (fresh.length ? "" : "No public servers are online.");
    status.hidden = !status.textContent;
  }

  async function refresh(game, result) {
    if (!inView || document.hidden || result.node.hidden || result.pending) return;
    result.pending = true;
    try {
      result.rooms = await directory.publicServers(game);
      result.message = "";
    } catch {
      result.message = "Could not refresh the directory. You can still use a host invitation.";
    } finally {
      result.pending = false;
      render(result);
    }
  }

  function refreshVisible() {
    for (const [game, result] of results) void refresh(game, result);
  }

  function select(game) {
    if (game !== "all" && !results.has(game)) return;
    section.querySelectorAll("[data-live-filter]").forEach(button => {
      button.setAttribute("aria-pressed", String(button.dataset.liveFilter === game));
    });
    for (const [name, result] of results) result.node.hidden = game !== "all" && game !== name;
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
  window.addEventListener("pageshow", refreshVisible);
  window.addEventListener("focus", refreshVisible);
  setInterval(refreshVisible, REFRESH_MS);
}

const section = globalThis.document?.getElementById("live-lobbies");
if (section && window.CatnmiceServers) mount(section, window.CatnmiceServers);
