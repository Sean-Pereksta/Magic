import {
  UNITS,
  UNIT_KEYS,
  ITEMS,
  FACTIONS,
  STEP,
  WORLD,
  armySize,
  armyCap,
  clamp,
  newProfile,
  clone,
} from "./data.mjs";
import { kingdomCards } from "./kingdom-ui.mjs";
import { Realm } from "./realm.mjs";
import { createWorld, battleArmy } from "./world.mjs";
import { SCENARIOS, scenarioOptions } from "./training.mjs";
import { battleMap, hydrateView, formationSlots, createBattle, issueOrder } from "./battle.mjs";
import { CONTRACTS, ownerKey, signatureRecruitment } from "./progression.mjs";
import { loadAssets } from "./assets.mjs";
import { Renderer } from "./renderer.mjs";
import { OnlineClient } from "./network.mjs";
import { RemoteMotion } from "./motion.mjs";
import {
  serializeCampaign,
  parseCampaign,
  readCampaign,
  saveCampaign,
} from "./storage.mjs";
const $ = (id) => document.getElementById(id),
  esc = (v) =>
    String(v ?? "").replace(
      /[&<>"']/g,
      (c) =>
        ({
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;",
        })[c],
    ),
  username =
    (new URLSearchParams(location.search).get("username") || "Wanderer")
      .trim()
      .slice(0, 40) || "Wanderer";
const saveKey = `mountonline:v2:${username}`,
  map = createWorld(),
  remoteMotion = new RemoteMotion();
let assets,
  renderer,
  realm = null,
  network = null,
  state = null,
  battle = null,
  mode = null,
  team = 0,
  pending = null,
  ability = null,
  selectedContext = null,
  panelType = null,
  lastHud = 0,
  lastSave = 0,
  accumulator = 0,
  lastFrame = performance.now(),
  busy = false,
  toastTimer,
  contextSignature = "",
  panMode = false,
  addSelection = false;
const dismissed = new Set(),
  resultShown = new Set(),
  groups = new Map(),
  keys = new Set(),
  pointers = new Map();
let gesture = null,
  pinch = null,
  lastKeyMove = 0,
  lastEvent = 0,
  lastAudio = 0;
let settings = { quality: "auto", sound: true, showFPS: false };
try {
  settings = {
    ...settings,
    ...JSON.parse(localStorage.getItem("mountonline:settings") || "{}"),
  };
} catch {}
let audio;
function sound(kind = "order") {
  if (!settings.sound) return;
  try {
    audio ??= new (window.AudioContext || window.webkitAudioContext)();
    void audio.resume();
    const t = audio.currentTime,
      osc = audio.createOscillator(),
      gain = audio.createGain();
    osc.connect(gain);
    gain.connect(audio.destination);
    osc.type = kind === "impact" ? "triangle" : "sine";
    osc.frequency.setValueAtTime(
      kind === "error"
        ? 140
        : kind === "charge"
          ? 190
          : kind === "impact"
            ? 260
            : 480,
      t,
    );
    osc.frequency.exponentialRampToValueAtTime(
      kind === "error" ? 100 : kind === "charge" ? 280 : 240,
      t + 0.16,
    );
    gain.gain.setValueAtTime(0.05, t);
    gain.gain.exponentialRampToValueAtTime(0.0001, t + 0.22);
    osc.start(t);
    osc.stop(t + 0.23);
  } catch {}
}
function toast(message, error = false) {
  $("toast").textContent = message;
  $("toast").classList.toggle("error", error);
  $("toast").hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(
    () => ($("toast").hidden = true),
    error ? 5500 : 2800,
  );
  if (error) sound("error");
}
function setStatus(text) {
  $("connection").textContent = text;
}
function localState() {
  const p = realm.player("local");
  return {
    time: realm.time,
    profile: p,
    settlements: realm.world.settlements,
    npcs: realm.npcs.filter((n) => n.respawn <= realm.time && !n.capturedBy),
    others: [],
    events: realm.events,
    alliances: realm.alliances,
    kingdoms: state?.kingdoms,
  };
}
function showGame() {
  $("menu").hidden = true;
  $("app").hidden = false;
  renderer.resize();
  renderer.cam.x = state.profile.x;
  renderer.cam.y = state.profile.y;
  renderer.cam.zoom = matchMedia("(pointer:coarse)").matches ? 0.75 : 1;
  renderer.follow = true;
  selectedContext = null;
  contextSignature = "";
  $("context").hidden = true;
  panelType = null;
  updateHUD();
  $("map").focus({ preventScroll: true });
}
function updateLocal() {
  state = localState();
  const p = state.profile,
    next =
      realm.battles.get(p.activeBattle) || realm.battles.get(p.lastBattle?.id);
  if (next && !dismissed.has(next.id)) {
    if (battle?.id !== next.id) beginBattle(next);
    battle = next;
  } else if (!p.activeBattle && battle?.finished && dismissed.has(battle.id))
    battle = null;
}
function beginBattle(next, nextTeam = 0) {
  battle = next;
  team = nextTeam;
  renderer.selected.clear();
  groups.clear();
  pending = null;
  ability = null;
  renderer.preview = null;
  renderer.cam.x = matchMedia("(pointer:coarse)").matches
    ? team === 1
      ? 54
      : 10
    : team === 1
      ? 49
      : next.siege
        ? 17
        : 18;
  renderer.cam.y = 20;
  renderer.cam.zoom = matchMedia("(pointer:coarse)").matches ? 0.45 : 0.7;
  renderer.follow = false;
  selectedContext = null;
  $("context").hidden = true;
  sound("charge");
  selectGroup("all");
}
function remoteState(snapshot) {
  if (network?.stopped) return;
  state = snapshot;
  team = snapshot.team || 0;
  if (snapshot.battle && !dismissed.has(snapshot.battle.id)) {
    const view = hydrateView(
      snapshot.battle,
      battle?.id === snapshot.battle.id ? battle : null,
    );
    if (battle?.id !== view.id) beginBattle(view, snapshot.team || 0);
    battle = view;
    team = snapshot.team || 0;
  } else battle = null;
  remoteMotion.accept(state, battle);
  if (!$("app").hidden) updateHUD();
}
async function startLocal() {
  network?.stop();
  network = null;
  mode = "local";
  realm = readCampaign(saveKey) || new Realm();
  if (!realm.players.has("local")) realm.enter("local", newProfile(username));
  state = localState();
  battle = null;
  team = 0;
  dismissed.clear();
  resultShown.clear();
  setStatus("Local campaign");
  updateLocal();
  showGame();
  if (state.profile.activeBattle && battle) {
    renderer.cam.x = 18;
    renderer.cam.y = 20;
    renderer.cam.zoom = 0.65;
    renderer.follow = false;
  }
  sound();
}
async function startOnline() {
  if (busy) return;
  busy = true;
  $("startOnline").disabled = true;
  $("menuMessage").classList.remove("error");
  $("menuMessage").textContent = "Connecting to your online realm…";
  mode = "online";
  remoteMotion.reset();
  realm = null;
  state = null;
  battle = null;
  dismissed.clear();
  resultShown.clear();
  network = new OnlineClient(
    remoteState,
    (text) => {
      setStatus(text);
      if ($("app").hidden) $("menuMessage").textContent = text;
    },
    { username },
  );
  try {
    await network.start();
    if (!state) throw new Error("The realm did not return a saved company.");
    showGame();
    if (battle) {
      renderer.cam.x = team === 0 ? 18 : 49;
      renderer.cam.y = 20;
      renderer.cam.zoom = 0.65;
      renderer.follow = false;
      selectGroup("all");
    }
    if (!state.linked)
      toast(
        "Online progress saves to this Firebase identity. Your older lobby save needs a verified account link before import.",
      );
  } catch (error) {
    network.stop();
    mode = null;
    $("menuMessage").textContent =
      error.message || "The online realm is unavailable. Retry shortly.";
    $("menuMessage").classList.add("error");
    setStatus("Offline");
  } finally {
    busy = false;
    $("startOnline").disabled = false;
  }
}
function practice(siege = false) {
  network?.stop();
  network = null;
  mode = "practice";
  realm = new Realm();
  const p = newProfile(username);
  p.level = 2;
  p.skillPoints = 1;
  p.army = { ...battleArmy(55, "crown"), cavalry: 8, mages: 3, priests: 3 };
  p.x = 35.5;
  p.y = 44.5;
  realm.enter("local", p);
  if (siege) {
    const castle = realm.world.settlements.find((s) => s.id === "dawnkeep");
    castle.garrison = 44;
    realm.startBattle("local", "siege", castle.id);
  } else {
    const npc = realm.npcs[0];
    npc.x = p.x + 1;
    npc.y = p.y;
    npc.army = { ...battleArmy(55, "ember"), archers: 8, monks: 3 };
    realm.startBattle("local", "npc", npc.id);
  }
  dismissed.clear();
  resultShown.clear();
  battle = null;
  state = localState();
  showGame();
  updateLocal();
  setStatus("Practice · no campaign rewards");
  updateHUD();
}
function training(id) {
  const options=scenarioOptions(id); if(!options) return;
  network?.stop(); network=null; mode='practice'; realm=new Realm();
  const p=newProfile(username); p.level=10; p.army=clone(options.armyA); realm.enter('local',p);
  const b=createBattle({...options,id:'training-'+id});
  b.participants=['local',null]; b.encounter={kind:'practice',targetId:id}; b.settled=false;
  p.activeBattle=b.id; realm.battles.set(b.id,b);
  if(id==='shield') {
    const guards=b.units.filter(u=>u.team===0 && UNITS[u.key].role==='shield'), archers=b.units.filter(u=>u.team===0 && UNITS[u.key].role==='ranged');
    guards.forEach((u,i)=>{u.x=u.px=24;u.y=u.py=13+i*.65;});
    archers.forEach((u,i)=>{u.x=u.px=20;u.y=u.py=i<8 ? 13+i*.7 : 27+(i-8)*.7;});
    issueOrder(b,0,guards.map(u=>u.id),'brace',{x:24,y:17},'shieldwall');
    issueOrder(b,0,archers.map(u=>u.id),'hold');
  }
  dismissed.clear();resultShown.clear();battle=null;state=localState();
  if($('panel').open)$('panel').close(); showGame();updateLocal();setStatus('Training · '+options.name);updateHUD();toast(options.detail);
}
function save(silent = false) {
  if (mode !== "local" || !realm) return;
  try {
    saveCampaign(saveKey, realm, "local");
    lastSave = performance.now();
    if (!silent)
      toast("Campaign saved. Your last good checkpoint is backed up.");
  } catch {
    toast(
      "This device could not save your campaign. Export it from Settings to keep your progress.",
      true,
    );
  }
}
function returnMenu() {
  if (mode === "local") save(true);
  network?.stop();
  network = null;
  mode = null;
  remoteMotion.reset();
  realm = null;
  battle = null;
  state = null;
  pending = null;
  ability = null;
  keys.clear();
  pointers.clear();
  gesture = null;
  renderer.preview = null;
  $("app").hidden = true;
  $("menu").hidden = false;
  if ($("panel").open) $("panel").close();
  if ($("result").open) $("result").close();
  document.body.classList.remove("in-battle");
  $("menuMessage").textContent =
    "Local campaigns save on this device. Online progress belongs to your Firebase identity.";
  $("menuMessage").classList.remove("error");
  updateContinue();
}
async function perform(input) {
  if (!state) return;
  if (busy && !["move", "command", "halt"].includes(input.type)) return;
  const durable = !["move", "command", "halt"].includes(input.type);
  if (durable) busy = true;
  try {
    let answer;
    if (mode === "online") answer = await network.action(input);
    else if (input.type === "move") {
      realm.move("local", input.point);
      answer = { ok: true };
    } else if (input.type === "halt") {
      realm.player("local").route = [];
      answer = { ok: true, message: "Your company holds here." };
    } else if (input.type === "command") {
      realm.command("local", input);
      answer = { ok: true };
    } else if (input.type === "engage") {
      realm.startBattle("local", input.kind, input.target);
      answer = { message: "Your company takes the field." };
    } else answer = { message: realm.action("local", input.type, input) };
    if (mode !== "online") {
      updateLocal();
      if (durable) save(true);
    }
    if (answer.message) toast(answer.message);
    sound(input.order === "charge" ? "charge" : "order");
    updateHUD();
    if (durable) {
      contextSignature = "";
      renderContext();
      if ($("panel").open && panelType) renderPanel(panelType);
    }
    return true;
  } catch (error) {
    toast(error.message || "The order could not be completed.", true);
    return false;
  } finally {
    if (durable) busy = false;
  }
}
function selectedUnits() {
  return (
    battle?.units.filter(
      (u) =>
        u.team === team && u.hp > 0 && !u.routed && renderer.selected.has(u.id),
    ) || []
  );
}
function visibleUnits() {
  return mode === "online" && battle && remoteMotion.battle?.id === battle.id
    ? remoteMotion.battle.units
    : battle?.units || [];
}
function selectionCenter() {
  const units = visibleUnits().filter(
    (u) =>
      u.team === team && u.hp > 0 && !u.routed && renderer.selected.has(u.id),
  );
  return units.length
    ? {
        x: units.reduce((n, u) => n + u.x, 0) / units.length,
        y: units.reduce((n, u) => n + u.y, 0) / units.length,
      }
    : { x: team === 0 ? 12 : 52, y: 20 };
}
function selectGroup(role) {
  if (!battle) return;
  const roles = {
    infantry: ["spear", "shield"],
    ranged: ["ranged", "crossbow", "mage", "horseArcher"],
    cavalry: ["cavalry", "horseArcher"],
    support: ["healer"],
  };
  const eligible = battle.units.filter(
    (u) =>
      u.team === team &&
      u.hp > 0 &&
      !u.routed &&
      (role === "all" || roles[role]?.includes(UNITS[u.key].role)),
  );
  renderer.selected = new Set(eligible.map((u) => u.id));
  updateSelected();
}
function updateSelected() {
  const selected = selectedUnits(), n = selected.length;
  const average = key => n ? Math.round(selected.reduce((sum,u) => sum + (key === 'cohesion' ? (u[key] ?? 0.7) * 100 : u[key] ?? 100), 0) / n) : 0;
  $("selectedCount").textContent = n ? `${n} selected · morale ${average('morale')} · stamina ${average('stamina')} · cohesion ${average('cohesion')}` : '0 selected';
  $("formation").value = battle?.formation?.[team] || "line";
  for (const button of document.querySelectorAll("[data-order]")) {
    button.disabled = !n || battle?.finished;
    button.classList.toggle("active", button.dataset.order === pending);
    button.setAttribute(
      "aria-pressed",
      String(button.dataset.order === pending),
    );
  }
}
async function command(order, point = selectionCenter(), target = null) {
  const units = selectedUnits();
  if (!units.length) {
    toast("Select troops first.", true);
    return;
  }
  const ok = await perform({
    type: "command",
    ids: units.map((u) => u.id),
    order,
    point: { ...point, ...(target == null ? {} : { target }) },
    formation: $("formation").value,
  });
  if (ok) {
    pending = null;
    ability = null;
    renderer.preview = null;
    if (order === 'attackNearest' && battle?.notice) toast(battle.notice);
    $("orderHint").textContent =
      `${order === "auto" ? "Auto-engage" : order[0].toUpperCase() + order.slice(1)} order sent to ${units.length} troops.`;
  }
  updateSelected();
}
function chooseOrder(order) {
  if (!battle) return;
  if (["attackNearest", "charge", "hold", "retreat", "auto", "brace"].includes(order)) {
    void command(order);
    return;
  }
  pending = order;
  ability = null;
  $("orderHint").textContent =
    order === "follow"
      ? "Tap an allied soldier to follow."
      : order === "defend"
        ? "Tap a position, castle gate, or allied soldier to protect."
        : `Choose where your troops should ${order}.`;
  updateSelected();
}
function unitAt(point, ownOnly = false) {
  let best = null,
    d = 0.85;
  for (const u of visibleUnits()) {
    if (u.hp <= 0 || u.routed || (ownOnly && u.team !== team)) continue;
    const dist = Math.hypot(point.x - u.x, point.y - u.y);
    if (dist < d) {
      d = dist;
      best = u;
    }
  }
  return best;
}
function inspect(kind, id) {
  selectedContext = { kind, id };
  contextSignature = "";
  renderContext();
}
function contextEntity() {
  if (!selectedContext) return null;
  const list =
    selectedContext.kind === "settlement"
      ? state.settlements
      : selectedContext.kind === "npc"
        ? state.npcs
        : state.others;
  return list.find((e) => e.id === selectedContext.id);
}
function renderContext() {
  if (!state || battle || !selectedContext) {
    $("context").hidden = true;
    return;
  }
  const e = contextEntity();
  if (!e) {
    $("context").hidden = true;
    return;
  }
  const p = state.profile,
    d = Math.hypot(e.x - p.x, e.y - p.y),
    near = d <= 3,
    nearBattle = d <= 4,
    signature = [
      selectedContext.kind,
      e.id,
      near,
      nearBattle,
      p.gold,
      p.revision,
      e.owner,
      e.level,
      e.garrison,
      e.battleId,
    ].join(":");
  if (signature === contextSignature) return;
  contextSignature = signature;
  $("context").hidden = false;
  let title = e.name || e.username,
    tag =
      selectedContext.kind === "settlement"
        ? e.kind
        : selectedContext.kind === "npc"
          ? e.kind
          : "commander",
    copy = "",
    buttons = `<button data-context="travel">March to ${selectedContext.kind === "settlement" ? "settlement" : "company"} →</button>`;
  if (selectedContext.kind === "settlement") {
    copy =
      ["castle","capital","town"].includes(e.kind)
        ? `${e.ownerName ? `Held by ${esc(e.ownerName)}.` : `${esc(e.faction)} territory.`} ${e.garrison} defenders · fortification ${e.level}. ${e.owner === p.uid ? "Your holdings earn gold every two minutes of active play." : ""}`
        : "Recruit fresh soldiers, refill your wagons, and rest before the next campaign.";
    buttons += `<button data-context="recruit" ${!near || p.gold < 60 ? "disabled" : ""}>Recruit 5 soldiers · 60 gold</button><button data-context="supplies" ${!near || p.gold < 30 || p.supplies >= 200 ? "disabled" : ""}>Restock supplies · 30 gold</button><button data-context="rest" ${!near || p.gold < 15 ? "disabled" : ""}>Rest and rally · 15 gold</button>`;
    if (e.kind === 'village' && e.owner !== p.uid) buttons += `<button data-encounter="raid" ${!nearBattle || e.battleId ? 'disabled' : ''}>Raid supply stores</button>`;
    const faction = FACTIONS.find(f => f.id === (e.currentFaction || e.faction));
    if (faction) buttons += `<button data-faction-info="${faction.id}">${esc(faction.name)} · contact council</button>`;
    if (["castle","capital","town"].includes(e.kind) && e.owner !== p.uid)
      buttons += `<button data-context="engage" ${!nearBattle || e.battleId ? "disabled" : ""}>Lay siege · ${e.garrison} defenders</button>`;
    if (e.owner === p.uid) {
      buttons += `<button data-context="build" ${!near || e.level >= 5 ? "disabled" : ""}>Develop keep · ${e.level * 120} gold + 30 supplies</button><select id="garrisonUnit" aria-label="Troop to assign to garrison">${UNIT_KEYS.filter(
        (k) => p.army[k] > 0,
      )
        .map(
          (k) =>
            `<option value="${k}">${esc(UNITS[k].name)} (${p.army[k]})</option>`,
        )
        .join(
          "",
        )}</select><button data-context="garrison" ${!near ? "disabled" : ""}>Assign troop to garrison</button>`;
    }
  } else if (selectedContext.kind === "npc") {
    const size = armySize(e.army);
    copy =
      e.kind === "merchant"
        ? "A peaceful caravan travels between markets. Nearby settlements offer supplies and standards."
        : `${size} troops. ${e.kind === "patrol" ? "Faction patrols defend threatened holdings and react to your reputation." : "Outlaws pursue vulnerable companies. Use archers behind a shield wall."}`;
    if (e.kind !== "merchant")
      buttons += `<button data-context="engage" ${!nearBattle || e.battleId ? "disabled" : ""}>Engage company · ${size} troops</button>`;
    else
      buttons += `<button data-context="caravan" ${!near || p.gold < 20 || p.supplies >= 200 ? "disabled" : ""}>Trade · 20g for 50 supplies</button>`;
    if(e.kind === 'merchant') buttons += `<button data-encounter="caravanDefense" ${!nearBattle || !state.npcs.some(n=>n.kind==='bandit' && Math.hypot(n.x-e.x,n.y-e.y)<24) ? 'disabled' : ''}>Defend caravan from nearby raiders</button>`;
    else buttons += `<details class="encounter-choices"><summary>Choose battle objective</summary><button data-encounter="defensive" ${!nearBattle ? 'disabled' : ''}>Hold defensive ground</button><button data-encounter="rearguard" ${!nearBattle ? 'disabled' : ''}>Delay & withdraw</button><button data-encounter="reinforcement" ${!nearBattle ? 'disabled' : ''}>Hold for allied aid</button></details>`;
  } else {
    copy = `${e.armySize} troops · ${e.activeBattle ? "currently fighting" : "on the march"}. ${e.allianceId && e.allianceId === p.allianceId ? "An allied commander." : "Close the distance to challenge this commander."}`;
    buttons += `<button data-context="engage" ${!nearBattle || e.activeBattle || (e.allianceId && e.allianceId === p.allianceId) ? "disabled" : ""}>Challenge this commander</button>`;
  }
  $("contextBody").innerHTML =
    `<span class="tag">${esc(tag)}</span><h3>${esc(title)}</h3><p>${copy}</p><small class="eyebrow">${near ? "Within reach" : `${Math.round(d)} leagues away`}</small><div class="context-actions">${buttons}</div>`;
}
function hitWorld(point) {
  const visible = mode === "online" ? remoteMotion.state || state : state;
  const p = visible.profile;
  let best = null,
    dist = 1.8;
  for (const e of visible.settlements) {
    const d = Math.hypot(e.x - point.x, e.y - point.y);
    if (d < dist) {
      dist = d;
      best = { kind: "settlement", id: e.id };
    }
  }
  for (const e of visible.npcs) {
    if (e.battleId) continue;
    const d = Math.hypot(e.x - point.x, e.y - point.y);
    if (d < dist) {
      dist = d;
      best = { kind: "npc", id: e.id };
    }
  }
  for (const e of visible.others) {
    const d = Math.hypot(e.x - point.x, e.y - point.y);
    if (d < dist) {
      dist = d;
      best = { kind: "player", id: e.id };
    }
  }
  if (best) inspect(best.kind, best.id);
  else if (renderer.overlay !== 'none' && renderer.overlay !== 'armies') {
    const nearest=[...state.settlements].sort((a,b)=>Math.hypot(a.x-point.x,a.y-point.y)-Math.hypot(b.x-point.x,b.y-point.y))[0];
    if(nearest) { openPanel('diplomacy'); const detail=document.querySelector(`[data-faction-card="${nearest.faction}"]`); if(detail) { detail.open=true; detail.scrollIntoView({block:'start'}); } }
  }
  else if (Math.hypot(point.x - p.x, point.y - p.y) < 1) openPanel("army");
  else {
    selectedContext = null;
    $("context").hidden = true;
    renderer.follow = true;
    void perform({ type: "move", point });
  }
}
function tap(point, shift = false, right = false) {
  if (!state) return;
  if (!battle) {
    hitWorld(point);
    return;
  }
  if (battle.finished) return;
  if (ability) {
    void perform({ type: "command", ability, point });
    ability = null;
    renderer.preview = null;
    return;
  }
  const hit = unitAt(point);
  if (pending) {
    if (pending === "follow" && (!hit || hit.team !== team)) {
      toast("Choose an allied soldier to follow.", true);
      return;
    }
    void command(
      pending,
      point,
      hit &&
        ((["attack", "focus"].includes(pending) && hit.team !== team) ||
          (["follow", "defend"].includes(pending) && hit.team === team))
        ? hit.id
        : null,
    );
    return;
  }
  if (hit?.team === team && !right) {
    if (shift || addSelection) {
      if (renderer.selected.has(hit.id)) renderer.selected.delete(hit.id);
      else renderer.selected.add(hit.id);
    } else renderer.selected = new Set([hit.id]);
    updateSelected();
    return;
  }
  void command(
    hit && hit.team !== team ? "attack" : "move",
    point,
    hit && hit.team !== team ? hit.id : null,
  );
}
function updateHUD() {
  if (!state) return;
  const p = state.profile;
  if (realm && mode !== "online") state.kingdoms = realm.snapshot("local").kingdoms;
  $("commanderName").textContent = p.username;
  $("gold").textContent = Math.floor(p.gold);
  $("armyCount").textContent = `${armySize(p.army)}/${armyCap(p)}`;
  $("supplies").textContent = Math.ceil(p.supplies);
  $("morale").textContent = Math.round(p.morale);
  $("level").textContent = p.level;
  const inBattle = !!battle;
  const objective = $("objective"),
    title = objective.querySelector("strong"),
    detail = objective.querySelector("small");
  if (p.supplies < 20) {
    title.textContent = "Your wagons are running low.";
    detail.textContent =
      "Visit a settlement or find a caravan before hunger slows your company.";
  } else if (state.settlements.some((s) => s.owner === p.uid)) {
    title.textContent = "Hold what you have won.";
    detail.textContent =
      "Develop your keep, reinforce its garrison, and watch for hostile patrols.";
  } else if (p.stats.wins) {
    title.textContent = "Raise your banner over Dawnkeep.";
    detail.textContent =
      "Claim your contract rewards, promote your veterans, and prepare for your first siege.";
  } else if (armySize(p.army) >= 30) {
    title.textContent = "Become the Roadwarden.";
    detail.textContent =
      "Accept a contract and hunt an outlaw company. Keep your archers behind the line.";
  }
  document.body.classList.toggle("in-battle", inBattle);
  $("battleDock").hidden = !inBattle;
  if(inBattle) document.body.style.setProperty('--battle-dock-height', `${$("battleDock").getBoundingClientRect().height}px`);
  $("worldDock").hidden = inBattle;
  $("battleHeading").hidden = !inBattle;
  $("objective").hidden = inBattle;
  $("saveGame").hidden = mode !== "local";
  $("performance").hidden = !settings.showFPS;
  $("mapTitle").textContent = inBattle ? (battle.siege ? "THE SIEGE FIELD" : `${(battle.terrain || 'field').toUpperCase()} BATTLE`) : (FACTIONS.find(f => f.id === [...state.settlements].sort((a,b) => Math.hypot(a.x-p.x,a.y-p.y)-Math.hypot(b.x-p.x,b.y-p.y))[0]?.faction)?.name || 'LIVING KINGDOMS').toUpperCase();
  $("mapOverlay").hidden = inBattle;
  $("mapOverlay").closest('label').hidden = inBattle;
  if (battle) {
    const own = battle.units.filter(
        (u) => u.team === team && u.hp > 0 && !u.routed,
      ).length,
      enemy = battle.units.filter(
        (u) => u.team !== team && u.hp > 0 && !u.routed,
      ).length;
    $("battleAlly").textContent = `${battle.names[team]} · ${own}`;
    $("battleEnemy").textContent = `${battle.names[1 - team]} · ${enemy}`;
    if(battle.objective) {
      const objective=battle.objective;
      const remaining=Math.max(0,Math.ceil((objective.duration || 0)-(['defensive','reinforcement'].includes(objective.type) ? objective.heldFor || 0 : objective.elapsed || battle.time)));
      $('orderHint').textContent = objective.description || `${objective.type} · ${remaining}s · ${Math.round((objective.progress || 0)*100)}%${objective.contestedFor ? ' · contested' : ''}`;
    }
    $("battleClock").textContent = `${Math.floor(battle.time / 60)
      .toString()
      .padStart(2, "0")}:${Math.floor(battle.time % 60)
      .toString()
      .padStart(2, "0")}`;
    for (const button of document.querySelectorAll("[data-ability]")) {
      const name = button.dataset.ability,
        remaining = Math.ceil(
          (battle.abilities?.[team]?.[name] || 0) - battle.time,
        );
      button.disabled = remaining > 0 || battle.finished;
      button.title =
        remaining > 0
          ? `Ready in ${remaining}s`
          : "Choose a location on the battlefield";
    }
    updateSelected();
    if (battle.finished && !resultShown.has(battle.id)) {
      resultShown.add(battle.id);
      showResult();
    }
  }
  for (const e of state.events || []) {
    if (e.id > lastEvent) {
      lastEvent = e.id;
      if (e.uid && e.message.includes("income")) toast(e.message);
    }
  }
  renderContext();
  $("zoomLabel").textContent = `${Math.round(renderer.cam.zoom * 100)}%`;
}
function showResult() {
  if ($("panel").open) $("panel").close();
  const ours = battle.units.filter((u) => u.team === team),
    alive = ours.filter((u) => u.hp > 0 || u.routed).length,
    kills = battle.units.filter((u) => u.team !== team && u.hp <= 0).length,
    won = battle.winner === team,
    draw = battle.winner == null;
  $("resultTitle").textContent = draw
    ? "Stalemate"
    : won
      ? "Victory"
      : "Defeat";
  $("resultCopy").textContent =
    mode === "practice"
      ? "Your practice battle is complete. Campaign progress is unchanged."
      : battle.seatRoles?.[team] === "garrison"
        ? won
          ? "Your garrison held the keep. Your field company is ready to resume its campaign."
          : "The castle has fallen. Your field company can regroup and return to reclaim it."
        : won
          ? `Your company defeated ${battle.names[1 - team]}. Your survivors, battle experience, and spoils have been recorded.`
          : "The company withdraws. Some fallen soldiers recover as wounded, and a defeated commander can recruit again immediately.";
  $("resultStats").innerHTML =
    `<div><b>${alive}</b><small>Standing or withdrawn</small></div><div><b>${ours.length - alive}</b><small>Fallen</small></div><div><b>${kills}</b><small>Enemies defeated</small></div>`;
  $("returnRealm").textContent =
    mode === "practice" ? "Back to the main menu →" : "Return to the realm →";
  $("result").showModal();
  if (mode === "local") save(true);
  sound(won ? "charge" : "order");
}
function returnRealm() {
  if (mode === "practice") {
    returnMenu();
    return;
  }
  dismissed.add(battle.id);
  battle = null;
  pending = null;
  ability = null;
  renderer.preview = null;
  renderer.selected.clear();
  $("result").close();
  renderer.follow = true;
  renderer.cam.zoom = 0.85;
  renderer.cam.x = state.profile.x;
  renderer.cam.y = state.profile.y;
  $("orderHint").textContent =
    "Tap a destination to march. Tap a settlement or army to inspect it.";
  updateHUD();
}
const metric = (label, value) =>
  `<div class="metric"><span>${esc(label)}</span><b>${esc(value)}</b></div>`;
function nearestSettlement() {
  return state?.settlements
    .filter(
      (s) => Math.hypot(s.x - state.profile.x, s.y - state.profile.y) <= 3,
    )
    .sort(
      (a, b) =>
        Math.hypot(a.x - state.profile.x, a.y - state.profile.y) -
        Math.hypot(b.x - state.profile.x, b.y - state.profile.y),
    )[0];
}
function openPanel(type) {
  panelType = type;
  renderPanel(type);
  if (!$("panel").open) $("panel").showModal();
  sound();
}
function renderPanel(type) {
  const p = state?.profile || newProfile(username),
    s = nearestSettlement(),
    body = $("panelBody");
  const titles = {
    army: "The company",
    kingdom: "Your kingdom",
    journal: "Contracts & chronicles",
    diplomacy: "The council table",
    standards: "Standards & heirlooms",
    settings: "Campaign settings",
    help: "The art of command",
    training: "Tactical proving grounds",
  };
  $("panelTitle").textContent = titles[type] || "The company";
  $("panelEyebrow").textContent =
    type === "help"
      ? "A FIELD GUIDE"
      : mode === "online"
        ? "THE ONLINE REALM"
        : mode === "practice"
          ? "THE TRAINING FIELD"
          : "YOUR CAMPAIGN";
  if (type === "training") body.innerHTML = `<p class="panel-intro">Playable scenarios use the shared campaign combat system. Your saved company is untouched. Choose a tactical problem and try different orders.</p><div class="two-column">${SCENARIOS.map(s=>`<article class="story-card"><span class="eyebrow">${esc(s.terrain)} · ${esc(s.encounterType || 'field')}</span><h3>${esc(s.name)}</h3><p>${esc(s.detail)}</p><button data-scenario="${s.id}">Take command →</button></article>`).join('')}</div>`;
  else if (type === "help")
    body.innerHTML = `<div class="help-grid"><div><h3>Travel and build</h3><p>Tap open ground to march along a safe route. Tap a village, castle, caravan, or company to inspect it. Visit Hearthford for recruits and supplies. Promotions are available from the Army screen. Win contracts to earn gold and standards; capture castles for income and stronger defenses.</p><h3>Command on desktop</h3><p>Click a soldier or drag a selection box. Shift adds troops. Right-click issues a move or attack order. Select a command, then click its target. A immediately attacks the nearest accessible enemies. Shift+A targets an enemy or destination; C charges; H holds; B braces; K sets a skirmish fallback marker. Arrow keys pan during battles; WASD marches on the world map. Hold Space and drag to pan. Scroll to zoom.</p></div><div><h3>Command on touch</h3><p>Tap a friendly soldier to select it. Use All, Infantry, Ranged, Cavalry, or Support for quick groups. Enable Add to selection to select several soldiers, or hold briefly and drag a selection box. Tap an enemy to attack, or open ground to move. Tap Pan to drag the map. Pinch to zoom; the mini-map also moves the camera.</p><h3>Choose your tactics</h3><p>Directional shield walls intercept arrows for allies behind them. Flank their rear to break protection. Brace spears to stop a frontal cavalry charge. Horse archers in Skirmish fire, retreat toward your marker, and return while ammunition and stamina last. Cavalry must build momentum and regroup after impact; cavalry charges punish missile troops. Keep healers behind the line. Crossbows pierce armor and mages deal splash damage. Defend returns to a protected area. Hold never chases enemies. Retreat withdraws troops toward their deployment edge. Patrol loops from your soldiers' positions to the destination. Rally restores health and morale; special orders recharge.</p><h3>Control groups</h3><p>1–5 select troop roles. Ctrl+1–5 stores the current selection; press that number again to recall it. Esc cancels a pending order. Siege attackers must breach the gate before entering the keep.</p></div></div>`;
  else if (type === "army")
    body.innerHTML = `<div class="metric-row">${metric("FIELD COMPANY", `${armySize(p.army)} / ${armyCap(p)}`)}${metric("RESERVES", armySize(p.reserve))}${metric("COMMANDER", `Level ${p.level}`)}${metric("NEXT LEVEL", `${Math.floor(p.xp)} / ${100 * p.level} XP`)}</div><p class="panel-intro">Promote recruits into distinct roles, then specialize your veterans. Reserve troops retain their class. All 14 original troop types remain available. ${p.activeBattle ? "Finish your battle before changing the army." : ""}</p><div class="army-grid">${UNIT_KEYS.map(
      (key) => {
        const t = UNITS[key], signature = !!t.faction, eligibility = signature && s ? signatureRecruitment(p, {time:state?.time || 0, kingdoms:realm?.kingdoms}, s, key) : {allowed:false,reason:"Visit a recruitment settlement.",price:t.cost},
          can =
            t.from &&
            p.army[t.from] > 0 &&
            p.gold >= t.cost &&
            p.level >= t.tier - 1 &&
            !p.activeBattle;
        return `<article class="unit-card"><b class="count">${p.army[key] || 0}</b><div class="unit-art"><canvas data-portrait="${key}" width="128" height="128"></canvas></div><h3>${esc(t.name)}</h3><span class="tier">Tier ${t.tier} · ${t.role}</span><p>${t.hp} health · ${t.damage} ${t.role === "healer" ? "healing" : "damage"} · ${t.range} range${signature ? `<br>${esc(FACTIONS.find(f => f.id === t.faction)?.name || t.faction)} · regional recruitment` : t.from ? `<br>Promotes from ${esc(UNITS[t.from].name)}` : ""}</p><div class="unit-actions">${signature ? `<button data-signature="${key}" title="${esc(eligibility.reason || 'Recruit this regional specialty')}" ${s && eligibility.allowed && !p.activeBattle && p.gold >= eligibility.price ? "" : "disabled"}>Recruit · ${eligibility.price}g</button>` : t.from ? `<button data-action="train" data-unit="${key}" ${can ? "" : "disabled"}>Train · ${t.cost}g</button>` : `<button data-recruit ${s && !p.activeBattle && p.gold >= 60 ? "" : "disabled"}>Recruit 5 · 60g</button>`}<button data-action="reserve" data-unit="${key}" ${p.army[key] && !p.activeBattle ? "" : "disabled"}>Reserve</button><button data-action="reserve" data-unit="${key}" data-withdraw="true" ${p.reserve?.[key] && !p.activeBattle ? "" : "disabled"}>Field (${p.reserve?.[key] || 0})</button></div></article>`;
      },
    ).join("")}</div>`;
  else if (type === "kingdom") {
    const owned = state?.settlements.filter((s) => s.owner === p.uid) || [];
    body.innerHTML = `<div class="metric-row">${metric("CASTLES", owned.length)}${metric("INCOME / 2 MIN", `${owned.reduce((n, s) => n + s.level * 24, 0)}g`)}${metric("RENOWN", p.renown)}${metric("SKILL POINTS", p.skillPoints)}</div><div class="two-column"><section><h3>Commander development</h3>${[
      ["leadership", "Larger army capacity: +15 per rank."],
      ["logistics", "Faster travel and lower supply consumption."],
      ["tactics", "Stronger troops and commander abilities."],
    ]
      .map(
        ([skill, detail]) =>
          `<div class="skill-row"><div><b>${esc(skill[0].toUpperCase() + skill.slice(1))} · ${p.skills[skill]}</b><small>${detail}</small></div><button data-action="skill" data-skill="${skill}" ${p.skillPoints && !p.activeBattle ? "" : "disabled"}>Improve</button></div>`,
      )
      .join(
        "",
      )}</section><section><h3>Royal holdings</h3><div class="card-list">${owned.length ? owned.map((c) => `<article class="story-card"><h3>${esc(c.name)}</h3><p>Fortification ${c.level} · ${c.garrison} defenders<br>${c.level * 24} gold per income cycle</p><button data-travel="${esc(c.id)}">Visit & develop →</button></article>`).join("") : '<p class="panel-intro">Conquer a castle to establish your kingdom. Dawnkeep has the smallest garrison. Develop captured keeps to improve walls, defenders, and income. Assign troops while visiting to reinforce the garrison.</p>'}</div></section></div><p class="panel-intro" style="margin-top:20px">Supply and income cycles occur during active campaign play. Troops need food; hungry companies march slower and lose morale. Upkeep is one gold per 30 soldiers every two minutes.</p>`;
  } else if (type === "journal")
    body.innerHTML = `<p class="panel-intro">Choose contracts that match your next goal. Completed rewards can only be claimed once. Exploration, battles, and conquest all develop your commander.</p><div class="two-column">${CONTRACTS.map(
      (c) => {
        const progress = c.progress(p, map),
          claimed = p.claimedContracts.includes(c.id),
          accepted = p.contracts.includes(c.id),
          ready = progress >= c.goal;
        return `<article class="story-card"><span class="eyebrow">${claimed ? "COMPLETED" : accepted ? "IN YOUR JOURNAL" : "AVAILABLE CONTRACT"}</span><h3>${esc(c.name)}</h3><p>${c.detail}<br>${c.gold} gold · 50 XP${c.item ? ` · ${esc(ITEMS[c.item].name)}` : ""}</p><div class="progress"><i style="width:${Math.min(100, (progress / c.goal) * 100)}%"></i></div><div class="inline-actions"><small>${Math.min(progress, c.goal)} / ${c.goal}</small><button data-action="contract" data-id="${c.id}" ${claimed || (accepted && !ready) || p.activeBattle ? "disabled" : ""}>${claimed ? "Reward claimed" : !accepted ? "Accept contract" : ready ? "Claim reward" : "In progress"}</button></div></article>`;
      },
    ).join(
      "",
    )}</div><h3 style="margin-top:25px">Campaign chronicle</h3><div class="card-list">${
      (state?.events || [])
        .slice(-10)
        .reverse()
        .map((e) => `<small>${esc(e.message)}</small>`)
        .join("") || '<p class="panel-intro">Your story begins on the road.</p>'
    }</div>`;
  else if (type === "standards")
    body.innerHTML = `<p class="panel-intro">Equip up to three standards. Each encourages a different company build. Merchants at settlements sell new standards; the Roadwarden contract rewards a travel standard.</p><div class="two-column">${Object.entries(
      ITEMS,
    )
      .map(([id, item]) => {
        const owned = p.inventory.banners.includes(id),
          equipped = p.inventory.equipped.includes(id);
        return `<article class="story-card"><span class="eyebrow">${equipped ? "FLYING ABOVE YOUR COMPANY" : owned ? "IN YOUR INVENTORY" : "MERCHANT STOCK"}</span><h3>${esc(item.name)}</h3><p>${item.description}</p><button data-action="${owned ? "equip" : "buyBanner"}" data-item="${id}" data-settlement="${esc(s?.id || "")}" ${p.activeBattle || (!owned && (!s || p.gold < item.cost)) ? "disabled" : ""}>${owned ? (equipped ? "Lower standard" : "Equip standard") : `Purchase · ${item.cost} gold`}</button></article>`;
      })
      .join(
        "",
      )}</div>${p.legacyInventory ? '<p class="panel-intro" style="margin-top:20px">Your original inventory is retained in the migrated save, including older custom banners.</p>' : ""}`;
  else if (type === "diplomacy")
    body.innerHTML = `${kingdomCards(state, esc)}${mode === "online" ? `<h3 style="margin-top:24px">Player alliances</h3><p class="panel-intro">Found an alliance or request membership. Founders approve applications; allied commanders and their castles are protected from friendly attacks.</p><div class="inline-actions"><input id="allianceName" maxlength="30" placeholder="Alliance name" aria-label="New alliance name"><button id="createAlliance" ${p.allianceId ? "disabled" : ""}>Found · 100g</button></div><div class="card-list" style="margin-top:14px">${(state.alliances || []).map((a) => `<article class="story-card"><h3>${esc(a.name)}</h3><p>${a.members.length} / 6 commanders</p><button data-action="${p.allianceId === a.id ? "allianceLeave" : "allianceRequest"}" data-alliance="${esc(a.id)}" ${p.allianceId && p.allianceId !== a.id ? "disabled" : ""}>${p.allianceId === a.id ? "Leave alliance" : "Request membership"}</button>${a.owner === p.uid ? a.requests.map((r) => `<p>${esc(r.username)} <button data-action="allianceAccept" data-alliance="${esc(a.id)}" data-member="${esc(r.uid)}">Approve</button></p>`).join("") : ""}</article>`).join("") || '<p class="panel-intro">No alliances have been founded yet.</p>'}</div>` : ""}`;
  else if (type === "settings")
    body.innerHTML = `<div class="settings-row"><label for="quality">Graphics quality</label><select id="quality"><option value="auto" ${settings.quality === "auto" ? "selected" : ""}>Adaptive</option><option value="high" ${settings.quality === "high" ? "selected" : ""}>High</option><option value="low" ${settings.quality === "low" ? "selected" : ""}>Performance</option></select></div><div class="settings-row"><label for="sound">Sound effects</label><input id="sound" type="checkbox" ${settings.sound ? "checked" : ""}></div><div class="settings-row"><label for="fps">Show performance</label><input id="fps" type="checkbox" ${settings.showFPS ? "checked" : ""}></div><p class="panel-intro" style="margin-top:18px">Adaptive quality limits high-density displays and reduces effects when frames slow down. Camera zoom is available on the mini-map, mouse wheel, or by pinching.</p><div class="inline-actions"><button data-panel="help">Read the field guide</button>${mode === "local" ? '<button id="exportCampaign">Export campaign</button><label><button id="importCampaign">Import campaign</button><input type="file" id="importFile" accept="application/json,.json" hidden></label>' : ""}</div><p class="panel-intro" style="margin-top:16px">${mode === "online" ? `Firebase UID: <code>${esc(network?.uid || "")}</code><br>Online saves synchronize directly through Firestore. Reconnect with the same Firebase identity to resume your company. Local imports remain separate from online progress.` : "Local campaigns keep a previous save as a recovery checkpoint. Export a campaign to move it to another device."}</p>`;
  if (assets)
    for (const canvas of body.querySelectorAll("[data-portrait]")) {
      const c = canvas.getContext("2d"),
        u = UNITS[canvas.dataset.portrait];
      c.drawImage(
        assets.images[u.sheet],
        2,
        u.row * assets.manifest.atlases[u.sheet].frameHeight + 2,
        assets.manifest.atlases[u.sheet].frameWidth - 4,
        assets.manifest.atlases[u.sheet].frameHeight - 4,
        0,
        0,
        128,
        128,
      );
    }
}
function updateContinue() {
  try {
    $("startLocal").textContent = localStorage.getItem(saveKey)
      ? "Continue local campaign →"
      : "Begin a local campaign →";
  } catch {}
}
document.addEventListener("click", (event) => {
  const button = event.target.closest("button");
  if (!button || button.disabled) return;
  const d = button.dataset;
  if (d.scenario) { training(d.scenario); return; }
  if (d.encounter) { const e=contextEntity(); if(e) void perform({type:'engage',kind:d.encounter,target:e.id}); return; }
  if (d.panel) {
    openPanel(d.panel);
    return;
  }
  if (d.group) {
    selectGroup(d.group);
    sound();
    return;
  }
  if (d.order) {
    chooseOrder(d.order);
    return;
  }
  if (d.ability) {
    ability = d.ability;
    pending = null;
    $("orderHint").textContent = `Choose where to use ${button.textContent}.`;
    sound();
    return;
  }
  if (d.diplomaticAccept) {
    void perform({type:'diplomacy', faction:d.faction, action:'accept', offerId:d.diplomaticAccept}); return;
  }
  if (d.factionInfo) { openPanel('diplomacy'); const detail = document.querySelector(`[data-faction-card="${d.factionInfo}"]`); if(detail) { detail.open=true; detail.scrollIntoView({block:'start'}); } return; }
  if (d.signature) {
    const s=nearestSettlement(); if(s) void perform({type:'recruit', settlement:s.id, unit:d.signature, count:1}); return;
  }
  if (d.action) {
    const input = { type: d.action };
    for (const [k, v] of Object.entries(d))
      if (k !== "action") input[k] = v === "true" ? true : v;
    void perform(input);
    return;
  }
  if (d.recruit !== undefined) {
    const s = nearestSettlement();
    if (s) void perform({ type: "recruit", settlement: s.id, count: 5 });
    return;
  }
  if (d.travel) {
    const s = state.settlements.find((s) => s.id === d.travel);
    if (s) {
      $("panel").close();
      renderer.follow = true;
      void perform({ type: "move", point: { x: s.x, y: s.y } });
      inspect("settlement", s.id);
    }
    return;
  }
  if (d.context) {
    const e = contextEntity();
    if (!e) return;
    if (d.context === "travel") {
      renderer.follow = true;
      void perform({ type: "move", point: { x: e.x, y: e.y } });
      return;
    }
    const input =
      d.context === "caravan"
        ? { type: "caravan", target: e.id }
        : d.context === "engage"
          ? {
              type: "engage",
              kind:
                selectedContext.kind === "settlement"
                  ? "siege"
                  : selectedContext.kind === "npc"
                    ? "npc"
                    : "pvp",
              target: e.id,
            }
          : {
              type: d.context,
              settlement: e.id,
              ...(d.context === "recruit" ? { count: 5 } : {}),
              ...(d.context === "garrison"
                ? { unit: $("garrisonUnit").value }
                : {}),
            };
    void perform(input);
    return;
  }
  if (button.id === "createAlliance") {
    void perform({
      type: "allianceCreate",
      name: $("allianceName").value.trim(),
    });
    return;
  }
  if (button.id === "exportCampaign") {
    const blob = new Blob([serializeCampaign(realm, "local")], {
        type: "application/json",
      }),
      url = URL.createObjectURL(blob),
      a = document.createElement("a");
    a.href = url;
    a.download = "Mountain-Blade-Campaign.json";
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 500);
    return;
  }
  if (button.id === "importCampaign") $("importFile").click();
});
document.addEventListener("change", (event) => {
  const id = event.target.id;
  if (id === "formation" && battle && selectedUnits().length) {
    void command("move", selectionCenter());
    return;
  }
  if (["quality", "sound", "fps"].includes(id)) {
    settings.quality = $("quality").value;
    settings.sound = $("sound").checked;
    settings.showFPS = $("fps").checked;
    try {
      localStorage.setItem("mountonline:settings", JSON.stringify(settings));
    } catch {}
    renderer.quality = settings.quality;
    renderer.resize();
    updateHUD();
    return;
  }
  if (id === "importFile" && event.target.files[0]) {
    void event.target.files[0]
      .text()
      .then((text) => {
        const loaded = parseCampaign(text);
        save(true);
        realm = loaded;
        mode = "local";
        battle = null;
        dismissed.clear();
        resultShown.clear();
        state = localState();
        updateLocal();
        $("panel").close();
        save(true);
        showGame();
        toast("Campaign imported.");
      })
      .catch((error) => toast(error.message, true));
  }
});
$("startLocal").onclick = () => void startLocal();
$("startOnline").onclick = () => void startOnline();
$("practice").onclick = () => practice(false);
$("practiceSiege").onclick = () => practice(true);
document.addEventListener('submit', event => {
  const form=event.target.closest('[data-negotiation]'); if(!form) return; event.preventDefault();
  const fields=new FormData(form), input={type:'diplomacy', faction:form.dataset.negotiation, action:fields.get('proposal'), gold:Number(fields.get('gold'))};
  if(input.action==='demandTribute') {input.action='tribute'; input.demand=true; input.gold=0;}
  if(fields.get('target')) input.target=fields.get('target');
  if(fields.get('settlement')) input.settlement=fields.get('settlement');
  void perform(input);
});
$("mapOverlay").onchange = event => { renderer.overlay=event.target.value; renderer.lastMini=0; };
$("help").onclick = () => openPanel("help");
$("closePanel").onclick = () => $("panel").close();
$("menuButton").onclick = returnMenu;
$("returnRealm").onclick = returnRealm;
$("closeContext").onclick = () => {
  selectedContext = null;
  $("context").hidden = true;
};
$("saveGame").onclick = () => save();
$("haltMarch").onclick = () => void perform({ type: "halt" });
$("multiSelect").onclick = () => {
  addSelection = !addSelection;
  $("multiSelect").setAttribute("aria-pressed", String(addSelection));
};
$("panToggle").onclick = () => {
  panMode = !panMode;
  $("panToggle").setAttribute("aria-pressed", String(panMode));
};
function zoom(amount) {
  if (!renderer) return;
  renderer.cam.zoom = clamp(renderer.cam.zoom * amount, 0.35, 1.8);
  $("zoomLabel").textContent = `${Math.round(renderer.cam.zoom * 100)}%`;
}
$("zoomIn").onclick = () => zoom(1.15);
$("zoomOut").onclick = () => zoom(1 / 1.15);
$("centerCamera").onclick = () => {
  if (!state) return;
  if (battle) {
    const p = selectionCenter();
    renderer.cam.x = p.x;
    renderer.cam.y = p.y;
  } else renderer.follow = true;
};
const canvas = $("map");
canvas.addEventListener("contextmenu", (event) => event.preventDefault());
function canvasPoint(event) {
  const r = canvas.getBoundingClientRect();
  return { x: event.clientX - r.left, y: event.clientY - r.top };
}
canvas.addEventListener("pointerdown", (event) => {
  if (!state || $("panel").open || $("result").open) return;
  event.preventDefault();
  canvas.setPointerCapture(event.pointerId);
  const pos = canvasPoint(event);
  pointers.set(event.pointerId, pos);
  if (pointers.size === 2) {
    const [a, b] = [...pointers.values()];
    pinch = {
      distance: Math.hypot(a.x - b.x, a.y - b.y),
      zoom: renderer.cam.zoom,
    };
    gesture = null;
    renderer.marquee = null;
    renderer.preview = null;
    renderer.follow = false;
    return;
  }
  gesture = {
    id: event.pointerId,
    start: pos,
    last: pos,
    at: performance.now(),
    moved: false,
    pan: panMode || event.button === 1 || keys.has(" "),
    right: event.button === 2,
    shift: event.shiftKey,
    touch: event.pointerType === "touch",
  };
});
canvas.addEventListener("pointermove", (event) => {
  const pos = canvasPoint(event);
  if (pointers.has(event.pointerId)) pointers.set(event.pointerId, pos);
  if (pinch && pointers.size === 2) {
    const [a, b] = [...pointers.values()];
    renderer.cam.zoom = clamp(
      (pinch.zoom * Math.hypot(a.x - b.x, a.y - b.y)) /
        Math.max(1, pinch.distance),
      0.35,
      1.8,
    );
    return;
  }
  if (gesture && gesture.id === event.pointerId) {
    const dx = pos.x - gesture.last.x,
      dy = pos.y - gesture.last.y,
      distance = Math.hypot(pos.x - gesture.start.x, pos.y - gesture.start.y);
    if (distance > 7) {
      gesture.moved = true;
      const box =
        battle &&
        !gesture.pan &&
        !gesture.right &&
        (!gesture.touch || performance.now() - gesture.at > 300);
      if (box) renderer.marquee = { a: gesture.start, d: pos };
      else {
        renderer.cam.x -= dx / (48 * renderer.cam.zoom);
        renderer.cam.y -= dy / (48 * renderer.cam.zoom);
        renderer.follow = false;
        gesture.pan = true;
      }
    }
    gesture.last = pos;
  }
  if (battle && pending && selectedUnits().length && !gesture) {
    renderer.preview = {
      point: renderer.toWorld(pos.x, pos.y),
      formation: $("formation").value,
      count: selectedUnits().length,
      center: selectionCenter(),
    };
  }
});
function finishPointer(event, cancel = false) {
  const pos = canvasPoint(event);
  pointers.delete(event.pointerId);
  if (pinch) {
    if (pointers.size < 2) pinch = null;
    gesture = null;
    return;
  }
  if (!gesture || gesture.id !== event.pointerId) return;
  const g = gesture;
  gesture = null;
  if (cancel) {
    renderer.marquee = null;
    return;
  }
  if (renderer.marquee && battle) {
    const a = renderer.toWorld(renderer.marquee.a.x, renderer.marquee.a.y),
      b = renderer.toWorld(pos.x, pos.y);
    if (!g.shift && !addSelection) renderer.selected.clear();
    for (const u of visibleUnits())
      if (
        u.team === team &&
        u.hp > 0 &&
        !u.routed &&
        u.x >= Math.min(a.x, b.x) &&
        u.x <= Math.max(a.x, b.x) &&
        u.y >= Math.min(a.y, b.y) &&
        u.y <= Math.max(a.y, b.y)
      )
        renderer.selected.add(u.id);
    renderer.marquee = null;
    updateSelected();
  } else if (!g.moved && !g.pan)
    tap(renderer.toWorld(pos.x, pos.y), g.shift, g.right);
}
canvas.addEventListener("pointerup", (event) => finishPointer(event));
canvas.addEventListener("pointercancel", (event) => finishPointer(event, true));
canvas.addEventListener(
  "wheel",
  (event) => {
    event.preventDefault();
    const p = canvasPoint(event),
      before = renderer.toWorld(p.x, p.y);
    zoom(Math.exp(-event.deltaY * 0.001));
    const after = renderer.toWorld(p.x, p.y);
    renderer.cam.x += before.x - after.x;
    renderer.cam.y += before.y - after.y;
    renderer.follow = false;
  },
  { passive: false },
);
$("minimap").addEventListener("pointerdown", (event) => {
  if (!state) return;
  const rect = $("minimap").getBoundingClientRect(),
    m = battle ? { width: 64, height: 40 } : WORLD;
  renderer.cam.x = ((event.clientX - rect.left) / rect.width) * m.width;
  renderer.cam.y = ((event.clientY - rect.top) / rect.height) * m.height;
  renderer.follow = false;
});
addEventListener("keydown", (event) => {
  if (/INPUT|SELECT|TEXTAREA/.test(event.target.tagName)) return;
  if (event.key === "Escape") {
    pending = null;
    ability = null;
    renderer && (renderer.preview = null);
    if ($("result").open) event.preventDefault();
    return;
  }
  if (!state || $("panel").open || $("result").open) return;
  const key = event.key.toLowerCase();
  keys.add(key);
  if (key === " " || key.startsWith("arrow")) event.preventDefault();
  if (event.repeat) return;
  if (battle) {
    const shortcuts = {
      a: event.shiftKey ? "attack" : "attackNearest",
      k: "skirmish",
      b: "brace",
      m: "move",
      c: "charge",
      h: "hold",
      d: "defend",
      f: "follow",
      r: "retreat",
      p: "patrol",
      g: "auto",
    };
    if (shortcuts[key]) { event.preventDefault(); chooseOrder(shortcuts[key]); }
    if (/^[1-5]$/.test(key)) {
      event.preventDefault();
      if (event.ctrlKey || event.metaKey) {
        groups.set(key, [...renderer.selected]);
        toast(`Control group ${key} saved.`);
      } else if (groups.has(key)) {
        renderer.selected = new Set(groups.get(key));
        updateSelected();
      } else
        selectGroup(
          ["all", "infantry", "ranged", "cavalry", "support"][Number(key) - 1],
        );
    }
  }
});
addEventListener("keyup", (event) => keys.delete(event.key.toLowerCase()));
addEventListener("blur", () => {
  keys.clear();
  pointers.clear();
  gesture = null;
});
addEventListener("resize", () => renderer?.resize());
addEventListener("pagehide", () => {
  if (mode === "local") save(true);
});
document.addEventListener("visibilitychange", () => {
  lastFrame = performance.now();
  accumulator = 0;
  if (!document.hidden && mode === "online" && state) {
    remoteMotion.reset();
    remoteMotion.accept(state, battle);
  }
  if (document.hidden && mode === "local") save(true);
});
$("result").addEventListener("cancel", (event) => event.preventDefault());
function frame(now) {
  const dt = Math.min(0.1, (now - lastFrame) / 1000);
  lastFrame = now;
  requestAnimationFrame(frame);
  if (!renderer || !state || $("app").hidden || document.hidden) return;
  const began = performance.now();
  if (mode !== "online" && !$("panel").open && !$("result").open) {
    accumulator += dt;
    while (accumulator >= STEP) {
      realm.tick(STEP);
      accumulator -= STEP;
    }
    updateLocal();
  }
  if (battle) {
    const speed = (16 * dt) / renderer.cam.zoom;
    if (keys.has("arrowup")) renderer.cam.y -= speed;
    if (keys.has("arrowdown")) renderer.cam.y += speed;
    if (keys.has("arrowleft")) renderer.cam.x -= speed;
    if (keys.has("arrowright")) renderer.cam.x += speed;
  } else if (
    now - lastKeyMove > (mode === "online" ? 1000 : 250) &&
    (keys.has("w") || keys.has("a") || keys.has("s") || keys.has("d"))
  ) {
    lastKeyMove = now;
    const p = state.profile,
      dx = (keys.has("d") ? 1 : 0) - (keys.has("a") ? 1 : 0),
      dy = (keys.has("s") ? 1 : 0) - (keys.has("w") ? 1 : 0);
    renderer.follow = true;
    void perform({
      type: "move",
      point: {
        x: clamp(p.x + dx * 6, 0, WORLD.width - 0.1),
        y: clamp(p.y + dy * 6, 0, WORLD.height - 0.1),
      },
    });
  }
  let drawState = state,
    drawBattle = battle,
    alpha = accumulator / STEP;
  if (mode === "online") {
    const presentation = remoteMotion.sample(now);
    drawState = presentation?.state || state;
    drawBattle =
      battle && presentation?.battle?.id === battle.id
        ? presentation.battle
        : battle;
    alpha = 1;
  } else if (!battle) {
    const p = state.profile;
    drawState = {
      ...state,
      profile: {
        ...p,
        x: (p.px ?? p.x) + (p.x - (p.px ?? p.x)) * alpha,
        y: (p.py ?? p.y) + (p.y - (p.py ?? p.y)) * alpha,
      },
      npcs: state.npcs.map((n) => {
        const f = clamp(
          (realm.time + (accumulator || 0) - (n.updatedAt || 0)) / 0.2,
          0,
          1,
        );
        return {
          ...n,
          x: (n.px ?? n.x) + (n.x - (n.px ?? n.x)) * f,
          y: (n.py ?? n.y) + (n.y - (n.py ?? n.y)) * f,
        };
      }),
    };
  }
  if (!battle && renderer.follow) {
    renderer.cam.x +=
      (drawState.profile.x - renderer.cam.x) * Math.min(1, dt * 9);
    renderer.cam.y +=
      (drawState.profile.y - renderer.cam.y) * Math.min(1, dt * 9);
  }
  renderer.draw(drawState, map, drawBattle, team, alpha, now);
  if (now - lastHud > 250) {
    lastHud = now;
    updateHUD();
    if (settings.showFPS)
      $("performance").textContent =
        `${Math.round(1 / Math.max(0.001, dt))} FPS · draw ${renderer.frameMs.toFixed(1)}ms · ${battle?.units.length || state.npcs.length} actors`;
  }
  if (mode === "local" && now - lastSave > 5000) save(true);
  if (battle && battle.effects.length && now - lastAudio > 240) {
    const e = battle.effects.at(-1);
    if (
      ["impact", "fire", "siege"].includes(e.kind) &&
      battle.time - e.time < 0.12
    ) {
      sound("impact");
      lastAudio = now;
    }
  }
  renderer.frameMs =
    renderer.frameMs * 0.94 + (performance.now() - began) * 0.06;
}
async function boot() {
  try {
    assets = await loadAssets((value) => {
      $("loadFill").style.width = `${Math.round(value * 100)}%`;
      $("loadPercent").textContent = `${Math.round(value * 100)}%`;
    });
    renderer = new Renderer(canvas, $("minimap"), assets);
    renderer.quality = settings.quality;
    renderer.resize();
    $("loadLabel").textContent = "The realm is ready";
    for (const id of ["startLocal", "startOnline", "practice", "practiceSiege", "training"])
      $(id).disabled = false;
    updateContinue();
    $("welcome").textContent = `Welcome, ${username}. Raise your banner.`;
    if (new URLSearchParams(location.search).has("legacy"))
      $("menuMessage").textContent =
        "Your original encounter link now opens the rebuilt realm. Verified online accounts import their old company and castles when joining.";
    requestAnimationFrame(frame);
  } catch (error) {
    $("loadLabel").textContent = "Artwork could not load. Reload to retry.";
    $("menuMessage").textContent =
      "Check your connection, then reload this page to finish preparing the realm.";
    $("menuMessage").classList.add("error");
  }
}
void boot();
