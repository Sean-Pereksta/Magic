/* Encrypted Firebase cloud saves for War Realms campaigns.
   Local storage remains the authoritative offline cache. A named cloud slot can be
   attached to the current browser session; while its password is present in memory,
   local campaign updates are mirrored to Firebase. The password is never persisted
   or written to Firestore. */

const WR_CLOUD_FIREBASE_CONFIG = Object.freeze({
  apiKey: "AIzaSyB7twY7z31ucB6pGA8JC_HrVMZhA8lNaJA",
  authDomain: "bible-game-246c0.firebaseapp.com",
  projectId: "bible-game-246c0",
  storageBucket: "bible-game-246c0.appspot.com",
  messagingSenderId: "959619818996",
  appId: "1:959619818996:web:5a9fbf492e23c765e445a1"
});

const WR_CLOUD_APP_NAME = "warrealms-campaign-cloud";
const WR_CLOUD_COLLECTION = "lobbies";
const WR_CLOUD_GAME_TYPE = "warrealms-campaign-cloud-save";
const WR_CLOUD_BINDING_KEY = "warRealmsCampaign.cloudSave.name.v1";
const WR_CLOUD_FORMAT = 1;
const WR_CLOUD_KDF_ITERATIONS = 210000;
const WR_CLOUD_SYNC_INTERVAL_MS = 5000;
const WR_CLOUD_MIN_WRITE_GAP_MS = 8000;
const WR_CLOUD_MODAL_ID = "wrCampaignCloudModal";
const WR_CLOUD_STYLE_ID = "wrCampaignCloudSaveStyles";
const encoder = new TextEncoder();
const decoder = new TextDecoder();

let campaignApi = null;
let firebasePromise = null;
let installed = false;
let busy = false;
let autoSyncBusy = false;
let syncTimer = null;
let observer = null;
let boundName = readBoundName();
let sessionSecret = "";
let lastCloudSaveAt = 0;
let lastSyncedLocalStamp = 0;
let lastSyncError = "";

export function cleanCampaignCloudName(value) {
  return String(value || "").trim().replace(/\s+/g, " ").slice(0, 48);
}

export function normalizeCampaignCloudName(value) {
  return cleanCampaignCloudName(value).toLowerCase();
}

export function campaignCloudDocumentNamespace(value) {
  return `warrealms-campaign:${normalizeCampaignCloudName(value)}`;
}

export function validateCampaignCloudProfileShape(raw) {
  return Boolean(
    raw
    && typeof raw === "object"
    && !Array.isArray(raw)
    && typeof raw.runId === "string"
    && raw.runId.length > 0
    && Array.isArray(raw.deck)
    && raw.deck.length > 0
    && Number.isFinite(Number(raw.region))
    && Number.isFinite(Number(raw.level))
  );
}

function readBoundName() {
  try {
    return cleanCampaignCloudName(globalThis.localStorage?.getItem(WR_CLOUD_BINDING_KEY) || "");
  } catch {
    return "";
  }
}

function persistBoundName(name) {
  boundName = cleanCampaignCloudName(name);
  try {
    if (boundName) globalThis.localStorage?.setItem(WR_CLOUD_BINDING_KEY, boundName);
    else globalThis.localStorage?.removeItem(WR_CLOUD_BINDING_KEY);
  } catch {}
}

function bytesToHex(bytes) {
  return Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("");
}

async function campaignCloudDocumentId(name) {
  const digest = await crypto.subtle.digest("SHA-256", encoder.encode(campaignCloudDocumentNamespace(name)));
  return `warrealms-campaign-save--${bytesToHex(new Uint8Array(digest))}`;
}

function randomBytes(length) {
  const bytes = new Uint8Array(length);
  crypto.getRandomValues(bytes);
  return bytes;
}

async function deriveKey(password, salt, iterations = WR_CLOUD_KDF_ITERATIONS) {
  const base = await crypto.subtle.importKey("raw", encoder.encode(password), "PBKDF2", false, ["deriveKey"]);
  return crypto.subtle.deriveKey(
    { name: "PBKDF2", salt, iterations, hash: "SHA-256" },
    base,
    { name: "AES-GCM", length: 256 },
    false,
    ["encrypt", "decrypt"]
  );
}

async function pack(raw) {
  const plain = encoder.encode(raw);
  if (typeof CompressionStream !== "function") return { bytes: plain, compression: "none" };
  try {
    const stream = new Blob([plain]).stream().pipeThrough(new CompressionStream("gzip"));
    const zipped = new Uint8Array(await new Response(stream).arrayBuffer());
    return zipped.length + 32 < plain.length
      ? { bytes: zipped, compression: "gzip" }
      : { bytes: plain, compression: "none" };
  } catch {
    return { bytes: plain, compression: "none" };
  }
}

async function unpack(bytes, compression) {
  if (compression !== "gzip") return decoder.decode(bytes);
  if (typeof DecompressionStream !== "function") throw new Error("This browser cannot decompress this cloud campaign.");
  const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
  return decoder.decode(await new Response(stream).arrayBuffer());
}

async function encryptRaw(raw, password) {
  const packed = await pack(raw);
  const salt = randomBytes(16);
  const iv = randomBytes(12);
  const key = await deriveKey(password, salt);
  const payload = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, packed.bytes));
  return {
    salt,
    iv,
    payload,
    compression: packed.compression,
    iterations: WR_CLOUD_KDF_ITERATIONS
  };
}

function firestoreBytes(value) {
  if (value instanceof Uint8Array) return value;
  if (value?.toUint8Array) return value.toUint8Array();
  if (Array.isArray(value)) return new Uint8Array(value);
  throw new Error("Cloud campaign data is malformed.");
}

async function decryptRecord(record, password) {
  const salt = firestoreBytes(record.salt);
  const iv = firestoreBytes(record.iv);
  const payload = firestoreBytes(record.payload);
  const iterations = Number(record.iterations) || WR_CLOUD_KDF_ITERATIONS;
  const key = await deriveKey(password, salt, iterations);
  const plain = new Uint8Array(await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, payload));
  return unpack(plain, record.compression || "none");
}

async function firebase() {
  if (firebasePromise) return firebasePromise;
  firebasePromise = (async () => {
    const [appMod, fireMod, authMod] = await Promise.all([
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js"),
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js"),
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js")
    ]);
    let app = appMod.getApps().find(candidate => candidate.name === WR_CLOUD_APP_NAME);
    if (!app) app = appMod.initializeApp(WR_CLOUD_FIREBASE_CONFIG, WR_CLOUD_APP_NAME);
    const auth = authMod.getAuth(app);
    if (!auth.currentUser) await authMod.signInAnonymously(auth);
    return {
      db: fireMod.getFirestore(app),
      doc: fireMod.doc,
      getDoc: fireMod.getDoc,
      setDoc: fireMod.setDoc,
      serverTimestamp: fireMod.serverTimestamp,
      Bytes: fireMod.Bytes
    };
  })().catch(error => {
    firebasePromise = null;
    throw error;
  });
  return firebasePromise;
}

function requireApi() {
  if (!campaignApi?.loadCampaignProfile || !campaignApi?.saveCampaignProfile || !campaignApi?.normalizeCampaignProfile) {
    throw new Error("War Realms campaign storage is not ready yet.");
  }
  return campaignApi;
}

function validateCredentials(name, password) {
  const clean = cleanCampaignCloudName(name);
  if (clean.length < 2) throw new Error("Use at least 2 characters for the cloud campaign name.");
  if (String(password || "").length < 4) throw new Error("Use at least 4 characters for the cloud campaign password.");
  return clean;
}

function currentProfile() {
  const profile = requireApi().loadCampaignProfile();
  if (!profile) throw new Error("There is no local War Realms campaign to save.");
  return profile;
}

function bindSlot(name, password) {
  persistBoundName(name);
  sessionSecret = String(password || "");
  lastSyncError = "";
  renderStatus();
}

function forgetSlot() {
  persistBoundName("");
  sessionSecret = "";
  lastCloudSaveAt = 0;
  lastSyncedLocalStamp = 0;
  lastSyncError = "";
  renderStatus();
}

async function saveNamedCampaign(name, password, options = {}) {
  const clean = validateCredentials(name, password);
  const profile = options.profile || currentProfile();
  const raw = JSON.stringify(profile);
  const store = await firebase();
  const id = await campaignCloudDocumentId(clean);
  const ref = store.doc(store.db, WR_CLOUD_COLLECTION, id);
  const existing = await store.getDoc(ref);
  let prior = null;

  if (existing.exists()) {
    prior = existing.data();
    if (prior.gameType !== WR_CLOUD_GAME_TYPE) throw new Error("That cloud save name is already reserved by another game.");
    try {
      await decryptRecord(prior, password);
    } catch {
      throw new Error("That campaign name already exists, but the password does not match.");
    }
  }

  const encrypted = await encryptRaw(raw, password);
  const now = Date.now();
  await store.setDoc(ref, {
    kind: "warRealmsCampaignCloudSave",
    gameType: WR_CLOUD_GAME_TYPE,
    status: "active",
    name: `War Realms Campaign: ${clean}`,
    saveName: clean,
    host: "warrealms",
    players: [],
    format: WR_CLOUD_FORMAT,
    cipher: "AES-GCM-256",
    kdf: "PBKDF2-SHA256",
    iterations: encrypted.iterations,
    salt: store.Bytes.fromUint8Array(encrypted.salt),
    iv: store.Bytes.fromUint8Array(encrypted.iv),
    payload: store.Bytes.fromUint8Array(encrypted.payload),
    compression: encrypted.compression,
    campaignSchema: Number(profile.schema) || Number(campaignApi?.campaignSchema) || null,
    runId: String(profile.runId || ""),
    commanderId: String(profile.commanderId || ""),
    region: Number(profile.region) || 1,
    level: Number(profile.level) || 1,
    battlesWon: Number(profile.battlesWon) || 0,
    createdAtMs: Number(prior?.createdAtMs) || Number(profile.createdAtMs) || now,
    updatedAtMs: now,
    updatedAt: store.serverTimestamp()
  });

  bindSlot(clean, password);
  lastCloudSaveAt = now;
  lastSyncedLocalStamp = Number(profile.updatedAtMs) || now;
  renderStatus();
  return { name: clean, profile, updatedAtMs: now };
}

async function fetchNamedCampaign(name, password) {
  const clean = validateCredentials(name, password);
  const store = await firebase();
  const id = await campaignCloudDocumentId(clean);
  const ref = store.doc(store.db, WR_CLOUD_COLLECTION, id);
  const snap = await store.getDoc(ref);
  if (!snap.exists()) throw new Error("No War Realms cloud campaign exists with that name.");
  const record = snap.data();
  if (record.gameType !== WR_CLOUD_GAME_TYPE) throw new Error("That name does not belong to a War Realms campaign save.");

  let raw;
  try {
    raw = await decryptRecord(record, password);
  } catch {
    throw new Error("The password is incorrect, or the cloud campaign is damaged.");
  }

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("The cloud campaign could not be decoded.");
  }
  if (!validateCampaignCloudProfileShape(parsed)) throw new Error("The cloud campaign is missing required campaign data.");

  const profile = requireApi().normalizeCampaignProfile(parsed);
  if (!profile) throw new Error("The cloud campaign could not be normalized for this version of War Realms.");
  return { name: clean, profile, updatedAtMs: Number(record.updatedAtMs) || 0 };
}

function installFetchedCampaign(result, password) {
  const api = requireApi();
  if (!api.saveCampaignProfile(result.profile)) throw new Error("The downloaded campaign could not be written to local storage.");
  bindSlot(result.name, password);
  lastCloudSaveAt = result.updatedAtMs || Date.now();
  lastSyncedLocalStamp = Number(result.profile.updatedAtMs) || Date.now();
  renderStatus();
  window.dispatchEvent(new CustomEvent("warrealms:campaign-cloud-loaded", { detail: { profile: result.profile, saveName: result.name } }));
  window.dispatchEvent(new CustomEvent("warrealms:open-war-table", { detail: { source: "cloud-load" } }));
}

function statusText() {
  if (lastSyncError) return `Cloud sync paused: ${lastSyncError}`;
  if (!boundName) return "No Firebase campaign slot is assigned yet. Your existing local campaign remains safe.";
  if (!sessionSecret) return `Cloud slot “${boundName}” is remembered. Enter its password to resume encrypted syncing.`;
  if (lastCloudSaveAt) return `Cloud slot “${boundName}” is active. Last Firebase save: ${new Date(lastCloudSaveAt).toLocaleString()}.`;
  return `Cloud slot “${boundName}” is active. Local campaign changes will sync automatically.`;
}

function renderStatus(message = "", bad = false) {
  const status = document.getElementById("wrCampaignCloudStatus");
  if (status) {
    status.textContent = message || statusText();
    status.classList.toggle("bad", bad || Boolean(lastSyncError));
  }
  const button = document.querySelector("[data-wr-cloud-save]");
  if (button) {
    button.textContent = sessionSecret && boundName ? "☁ Cloud Synced" : boundName ? "☁ Cloud Slot" : "☁ Cloud Save";
    button.title = statusText();
  }
  const forget = document.querySelector("[data-wr-cloud-forget]");
  if (forget) forget.hidden = !boundName;
}

function injectStyles() {
  if (document.getElementById(WR_CLOUD_STYLE_ID)) return;
  const style = document.createElement("style");
  style.id = WR_CLOUD_STYLE_ID;
  style.textContent = `
    #${WR_CLOUD_MODAL_ID}{position:fixed;inset:0;z-index:260;display:grid;place-items:center;padding:18px;background:rgba(2,5,9,.86);backdrop-filter:blur(10px)}
    #${WR_CLOUD_MODAL_ID}[hidden]{display:none!important}
    #${WR_CLOUD_MODAL_ID} *{box-sizing:border-box}
    .wrCloudCard{width:min(610px,100%);border:1px solid #61718a;border-radius:18px;background:linear-gradient(155deg,#172334,#090f18 72%);box-shadow:0 30px 100px rgba(0,0,0,.72);color:#f4f7fb;padding:20px}
    .wrCloudHead{display:flex;align-items:flex-start;gap:12px}.wrCloudHeadCopy{min-width:0;flex:1}.wrCloudEyebrow{font-size:8px;font-weight:950;letter-spacing:.16em;text-transform:uppercase;color:#8bd4ff}.wrCloudTitle{margin:5px 0 4px;font:850 24px/1.08 var(--displayFont,"Cinzel",serif)}.wrCloudCopy{margin:0;color:#9eafc0;font-size:9px;line-height:1.55}
    .wrCloudClose{width:34px;height:34px;border:1px solid #46566b;border-radius:10px;background:#162231;color:#e8eef5;font-size:18px;cursor:pointer}
    .wrCloudFields{display:grid;grid-template-columns:1fr 1fr;gap:9px;margin-top:16px}.wrCloudField{display:grid;gap:5px;color:#a8b7c8;font-size:8px;font-weight:900;letter-spacing:.05em;text-transform:uppercase}.wrCloudField input{width:100%;border:1px solid #40516a;border-radius:9px;background:#0b121c;color:#f6f8fb;padding:10px 11px;font:inherit;font-size:11px;text-transform:none;letter-spacing:0;outline:none}.wrCloudField input:focus{border-color:#8bd4ff;box-shadow:0 0 0 2px rgba(139,212,255,.12)}
    .wrCloudStatus{margin-top:11px;border:1px solid rgba(139,212,255,.2);border-radius:10px;background:rgba(139,212,255,.07);padding:9px 10px;color:#c8d8e8;font-size:8px;line-height:1.5}.wrCloudStatus.bad{border-color:rgba(226,99,115,.35);background:rgba(226,99,115,.09);color:#ffd3da}
    .wrCloudActions{display:flex;flex-wrap:wrap;gap:8px;margin-top:14px}.wrCloudAction{min-height:38px;border:1px solid #52647c;border-radius:9px;background:#1a293a;color:#f3f7fb;padding:8px 12px;font-size:9px;font-weight:950;cursor:pointer}.wrCloudAction.primary{border-color:#72bfe9;background:#17628c}.wrCloudAction:hover{filter:brightness(1.12)}.wrCloudAction:disabled{opacity:.48;cursor:wait}.wrCloudAction.danger{margin-left:auto;border-color:#6c4e56;background:#291820;color:#e8bdc6}
    .wrCloudNote{margin:12px 0 0;color:#7f91a5;font-size:7px;line-height:1.5}.wrCloudNote strong{color:#c7d3df}
    @media(max-width:620px){.wrCloudFields{grid-template-columns:1fr}.wrCloudAction.danger{margin-left:0}}
  `;
  document.head.appendChild(style);
}

function ensureModal() {
  let modal = document.getElementById(WR_CLOUD_MODAL_ID);
  if (modal) return modal;
  modal = document.createElement("div");
  modal.id = WR_CLOUD_MODAL_ID;
  modal.hidden = true;
  modal.innerHTML = `
    <section class="wrCloudCard" role="dialog" aria-modal="true" aria-labelledby="wrCampaignCloudTitle">
      <div class="wrCloudHead"><div class="wrCloudHeadCopy"><div class="wrCloudEyebrow">War Realms · Firebase Campaigns</div><h2 class="wrCloudTitle" id="wrCampaignCloudTitle">Cloud Campaign Save</h2><p class="wrCloudCopy">Upload the current campaign or reopen it on another browser. Campaign data is encrypted before it leaves your device.</p></div><button type="button" class="wrCloudClose" data-wr-cloud-close aria-label="Close cloud save">×</button></div>
      <div class="wrCloudFields"><label class="wrCloudField">Campaign save name<input id="wrCampaignCloudName" maxlength="48" autocomplete="username" placeholder="Example: Varek Campaign"></label><label class="wrCloudField">Save password<input id="wrCampaignCloudPassword" type="password" autocomplete="current-password" placeholder="Required to decrypt"></label></div>
      <div class="wrCloudStatus" id="wrCampaignCloudStatus"></div>
      <div class="wrCloudActions"><button type="button" class="wrCloudAction primary" data-wr-cloud-upload>Save Current Campaign</button><button type="button" class="wrCloudAction" data-wr-cloud-download>Load From Firebase</button><button type="button" class="wrCloudAction danger" data-wr-cloud-forget>Forget Slot</button></div>
      <p class="wrCloudNote"><strong>Local fallback stays enabled.</strong> Saving to Firebase does not remove the browser copy. The slot name is remembered locally, but the password is kept only for the current page session.</p>
    </section>`;
  document.body.appendChild(modal);
  return modal;
}

function setBusy(nextBusy, action = "") {
  busy = nextBusy;
  document.querySelectorAll("#wrCampaignCloudModal .wrCloudAction").forEach(button => {
    button.disabled = nextBusy;
  });
  const upload = document.querySelector("[data-wr-cloud-upload]");
  const download = document.querySelector("[data-wr-cloud-download]");
  if (upload) upload.textContent = nextBusy && action === "upload" ? "Saving…" : "Save Current Campaign";
  if (download) download.textContent = nextBusy && action === "download" ? "Loading…" : "Load From Firebase";
}

export function openCampaignCloudSave() {
  if (typeof document === "undefined") return;
  injectStyles();
  const modal = ensureModal();
  const name = document.getElementById("wrCampaignCloudName");
  const password = document.getElementById("wrCampaignCloudPassword");
  if (name) name.value = boundName || "";
  if (password) password.value = sessionSecret || "";
  modal.hidden = false;
  renderStatus();
  setTimeout(() => (name?.value ? password : name)?.focus?.(), 0);
}

function closeModal() {
  if (busy) return;
  const modal = document.getElementById(WR_CLOUD_MODAL_ID);
  if (modal) modal.hidden = true;
}

async function uploadFromModal() {
  if (busy) return;
  const name = document.getElementById("wrCampaignCloudName")?.value || "";
  const password = document.getElementById("wrCampaignCloudPassword")?.value || "";
  setBusy(true, "upload");
  renderStatus("Encrypting and saving the current campaign to Firebase…");
  try {
    const result = await saveNamedCampaign(name, password);
    renderStatus(`Saved “${result.name}” to Firebase. Automatic cloud sync is active for this session.`);
  } catch (error) {
    console.warn("[War Realms campaign cloud save]", error);
    renderStatus(error?.message || "Firebase campaign save failed. The local campaign is still safe.", true);
  } finally {
    setBusy(false);
  }
}

async function downloadFromModal() {
  if (busy) return;
  const name = document.getElementById("wrCampaignCloudName")?.value || "";
  const password = document.getElementById("wrCampaignCloudPassword")?.value || "";
  setBusy(true, "download");
  renderStatus("Downloading and decrypting the Firebase campaign…");
  try {
    const result = await fetchNamedCampaign(name, password);
    const local = campaignApi?.loadCampaignProfile?.();
    const replacingDifferentRun = local?.runId && local.runId !== result.profile.runId;
    if (replacingDifferentRun && !window.confirm(`Load “${result.name}” and replace the campaign currently stored in this browser?`)) {
      renderStatus("Cloud load cancelled. The local campaign was not changed.");
      return;
    }
    installFetchedCampaign(result, password);
    renderStatus(`Loaded “${result.name}” from Firebase and restored it as the local campaign.`);
    closeModal();
  } catch (error) {
    console.warn("[War Realms campaign cloud load]", error);
    renderStatus(error?.message || "Firebase campaign load failed. The local campaign was not changed.", true);
  } finally {
    setBusy(false);
  }
}

function attachWarTableButton() {
  const secondary = document.querySelector("#warrealmsPlayLauncher .wrWarSecondary");
  if (!secondary || secondary.querySelector("[data-wr-cloud-save]")) return;
  const button = document.createElement("button");
  button.type = "button";
  button.dataset.wrCloudSave = "1";
  button.textContent = "☁ Cloud Save";
  button.addEventListener("click", event => {
    event.preventDefault();
    event.stopPropagation();
    openCampaignCloudSave();
  });
  secondary.appendChild(button);
  renderStatus();
}

async function autoSync() {
  if (!installed || !boundName || !sessionSecret || autoSyncBusy || busy) return;
  const profile = campaignApi?.loadCampaignProfile?.();
  if (!profile) return;
  const stamp = Number(profile.updatedAtMs) || 0;
  if (!stamp || stamp <= lastSyncedLocalStamp) return;
  if (Date.now() - lastCloudSaveAt < WR_CLOUD_MIN_WRITE_GAP_MS) return;

  autoSyncBusy = true;
  try {
    await saveNamedCampaign(boundName, sessionSecret, { profile });
    lastSyncError = "";
  } catch (error) {
    console.warn("[War Realms campaign cloud auto-sync]", error);
    lastSyncError = error?.message || "Firebase save failed.";
    sessionSecret = "";
    renderStatus();
  } finally {
    autoSyncBusy = false;
  }
}

function installEvents() {
  document.addEventListener("click", event => {
    const target = event.target instanceof Element ? event.target : null;
    if (!target) return;
    if (target.closest("[data-wr-cloud-close]")) return closeModal();
    if (target.closest("[data-wr-cloud-upload]")) return void uploadFromModal();
    if (target.closest("[data-wr-cloud-download]")) return void downloadFromModal();
    if (target.closest("[data-wr-cloud-forget]")) {
      forgetSlot();
      const name = document.getElementById("wrCampaignCloudName");
      const password = document.getElementById("wrCampaignCloudPassword");
      if (name) name.value = "";
      if (password) password.value = "";
      renderStatus("Cloud slot binding cleared. Firebase data was not deleted.");
    }
  });

  document.addEventListener("keydown", event => {
    if (event.key === "Escape" && !document.getElementById(WR_CLOUD_MODAL_ID)?.hidden) closeModal();
  });

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") void autoSync();
  });

  observer = new MutationObserver(() => attachWarTableButton());
  observer.observe(document.body, { childList: true, subtree: true });
}

export function campaignCloudSaveStatus() {
  return Object.freeze({
    boundName,
    sessionUnlocked: Boolean(boundName && sessionSecret),
    lastCloudSaveAt,
    lastSyncError
  });
}

export function installCampaignCloudSave(api = {}) {
  if (typeof window === "undefined" || typeof document === "undefined") return false;
  campaignApi = { ...campaignApi, ...api };
  if (installed) {
    attachWarTableButton();
    return true;
  }
  installed = true;
  injectStyles();
  ensureModal();
  installEvents();
  attachWarTableButton();
  if (syncTimer) clearInterval(syncTimer);
  syncTimer = setInterval(() => void autoSync(), WR_CLOUD_SYNC_INTERVAL_MS);
  renderStatus();
  return true;
}
