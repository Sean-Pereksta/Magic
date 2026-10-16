import { API_BASE } from "./config.mjs";
export class OnlineClient {
  constructor(onState, onStatus) {
    this.onState = onState;
    this.onStatus = onStatus;
    this.socket = null;
    this.poll = null;
    this.auth = null;
    this.connected = false;
    this.stopped = false;
    this.busy = false;
    this.pending = new Map();
  }
  async start() {
    this.stopped = false;
    this.onStatus("Signing in…");
    const [
      { initializeApp, getApps },
      { getAuth, signInAnonymously, onAuthStateChanged },
      { firebaseConfig },
    ] = await Promise.all([
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js"),
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js"),
      import("../../lobby/firebase-config.mjs"),
    ]);
    this.auth = getAuth(getApps()[0] || initializeApp(firebaseConfig));
    await new Promise((resolve, reject) => {
      const stop = onAuthStateChanged(
        this.auth,
        () => {
          stop();
          resolve();
        },
        reject,
      );
    });
    if (!this.auth.currentUser) await signInAnonymously(this.auth);
    this.outboxKey = "mountonline:outbox:" + this.auth.currentUser.uid;
    try {
      this.pending = new Map(
        JSON.parse(sessionStorage.getItem(this.outboxKey) || "[]"),
      );
    } catch {}
    const state = await this.call("/join", {});
    if (this.stopped) return;
    this.connected = true;
    this.onState(state);
    this.onStatus("Online realm");
    // WebSocket snapshots are preferred. HTTP fallback polls cached server state,
    // never Firestore, and stops in hidden tabs.
    const base = new URL(API_BASE, location.origin),
      origin = base.origin;
    try {
      const { io } = await import(origin + "/socket.io/socket.io.esm.min.js");
      if (this.stopped) return;
      this.socket = io(origin + "/mountonline", {
        auth: async (cb) =>
          cb({ token: await this.auth.currentUser.getIdToken() }),
        transports: ["websocket", "polling"],
        reconnection: true,
        reconnectionDelay: 1000,
        reconnectionDelayMax: 8000,
      });
      this.socket.on("realm", (s) => {
        if (this.stopped) return;
        this.connected = true;
        this.onState(s);
        this.onStatus("Online realm");
      });
      this.socket.on("disconnect", () => {
        this.connected = false;
        this.onStatus("Reconnecting…");
        this.startPolling();
      });
      this.socket.on("connect_error", () => this.startPolling());
      this.socket.on("connect", () => {
        this.connected = true;
        this.stopPolling();
      });
      this.socket.on("realmError", (e) => {
        this.connected = false;
        this.onStatus(e.message || "Reconnecting…");
        this.startPolling();
      });
    } catch {
      this.startPolling();
    }
    this.heartbeat = setInterval(() => this.socket?.emit("heartbeat"), 10000);
    this.refresh = setInterval(() => {
      if (this.socket && !this.stopped) this.socket.disconnect().connect();
    }, 40 * 60000);
  }
  async call(path, body) {
    const controller = new AbortController(),
      timeout = setTimeout(() => controller.abort(), 10000);
    try {
      const token = await this.auth.currentUser.getIdToken();
      const response = await fetch(API_BASE + path, {
        method: body === undefined ? "GET" : "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          ...(body === undefined ? {} : { "Content-Type": "application/json" }),
        },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: controller.signal,
      });
      const result = await response.json();
      if (!response.ok || result.error)
        throw new Error(
          result.error?.message || "The online realm could not be reached.",
        );
      return result;
    } finally {
      clearTimeout(timeout);
    }
  }
  startPolling() {
    if (this.poll || this.stopped) return;
    this.poll = setInterval(async () => {
      if (document.hidden || this.busy || this.stopped) return;
      this.busy = true;
      try {
        const s = await this.call("/state");
        if (!this.stopped) {
          this.connected = true;
          this.onState(s);
          this.onStatus("Online realm");
        }
      } catch {
        this.connected = false;
        this.onStatus("Reconnecting…");
      } finally {
        this.busy = false;
      }
    }, 1500);
  }
  stopPolling() {
    clearInterval(this.poll);
    this.poll = null;
  }
  persistOutbox() {
    try {
      sessionStorage.setItem(this.outboxKey, JSON.stringify([...this.pending]));
    } catch {}
  }
  async action(input) {
    if (this.stopped) throw new Error("Reconnect to the online realm.");
    const durable = !["move", "command", "halt"].includes(input.type),
      key = JSON.stringify(
        Object.fromEntries(
          Object.entries(input)
            .filter(([k]) => k !== "requestId")
            .sort(([a], [b]) => a.localeCompare(b)),
        ),
      );
    if (durable) {
      input.requestId =
        this.pending.get(key) || input.requestId || crypto.randomUUID();
      this.pending.set(key, input.requestId);
      this.persistOutbox();
    }
    let result;
    if (this.socket?.connected) {
      result = await new Promise((resolve, reject) => {
        this.socket
          .timeout(10000)
          .emit("order", input, (error, value) =>
            error
              ? reject(
                  new Error(
                    "The server has not acknowledged this order. Retry the same action safely.",
                  ),
                )
              : resolve(value),
          );
      });
      if (result.error) {
        if (durable && result.error.code !== "SERVICE_UNAVAILABLE") {
          this.pending.delete(key);
          this.persistOutbox();
        }
        throw new Error(result.error.message);
      }
    } else result = await this.call("/action", input);
    if (durable) {
      this.pending.delete(key);
      this.persistOutbox();
    }
    if (result.snapshot) this.onState(result.snapshot);
    return result;
  }
  stop() {
    this.stopped = true;
    this.connected = false;
    this.socket?.disconnect();
    this.socket = null;
    this.stopPolling();
    clearInterval(this.heartbeat);
    clearInterval(this.refresh);
  }
}
