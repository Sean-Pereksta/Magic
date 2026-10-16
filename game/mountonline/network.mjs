import { connectFirebase } from "./firebase.mjs";
import {
  FirestoreRealm,
  REALM_PATH,
  LEASE_MS,
  EPHEMERAL,
  ACTIONS,
  millis,
} from "./firestore-realm.mjs";
import { unpack } from "./codec.mjs";
const canonical = (value) =>
  JSON.stringify(value, (_key, v) =>
    v && typeof v === "object" && !Array.isArray(v)
      ? Object.fromEntries(
          Object.entries(v).sort(([a], [b]) => a.localeCompare(b)),
        )
      : v,
  );

export class OnlineClient {
  constructor(
    onState,
    onStatus,
    { username = "Wanderer", connect = connectFirebase } = {},
  ) {
    Object.assign(this, { onState, onStatus, username, connect });
    this.sessionId = crypto.randomUUID();
    this.stopped = true;
    this.connected = false;
    this.unsubscribers = [];
    this.waiters = new Set();
    this.pending = new Map();
  }
  ref(path) {
    return this.sdk.doc(this.db, path);
  }
  async start() {
    this.stopped = false;
    this.onStatus("Connecting to Firestore…");
    Object.assign(this, await this.connect());
    if (this.stopped) return;
    this.uid = this.user.uid;
    this.outboxKey = `mountonline:firestore-outbox:${this.uid}`;
    try {
      this.pending = new Map(
        JSON.parse(sessionStorage.getItem(this.outboxKey) || "[]"),
      );
    } catch {}
    try {
      await this.heartbeat();
    } catch (error) {
      if (error.code === "permission-denied")
        throw new Error(
          "Publish the updated Mountain Blade Firestore rules, then reconnect.",
        );
      throw error;
    }
    const firstView = new Promise((resolve, reject) => {
      this.initialResolve = resolve;
      this.initialReject = reject;
    });
    this.unsubscribers.push(
      this.sdk.onSnapshot(
        this.ref(REALM_PATH),
        { includeMetadataChanges: true },
        (snapshot) => {
          if (
            this.stopped ||
            snapshot.metadata.fromCache ||
            snapshot.metadata.hasPendingWrites
          )
            return;
          this.lease = snapshot.data();
          if (
            this.authority &&
            (this.lease?.ownerUid !== this.uid ||
              this.lease?.ownerSession !== this.sessionId ||
              this.lease?.epoch !== this.authority.epoch ||
              this.lease?.released)
          ) {
            const host = this.authority;
            this.authority = null;
            void host.close({ release: false });
          }
          if (this.lease?.released) this.onStatus("Changing realm host…");
          void this.receiveView();
          void this.elect();
        },
        (error) => this.connectionError(error),
      ),
    );
    this.unsubscribers.push(
      this.sdk.onSnapshot(
        this.ref(`${REALM_PATH}/views/${this.uid}`),
        { includeMetadataChanges: true },
        (snapshot) => {
          if (
            snapshot.metadata.fromCache ||
            snapshot.metadata.hasPendingWrites ||
            this.stopped
          )
            return;
          this.view = snapshot.data();
          void this.receiveView();
        },
        (error) => this.connectionError(error),
      ),
    );
    this.heartbeatTimer = setInterval(() => {
      if (!this.stopped)
        void this.heartbeat().catch((error) => this.connectionError(error));
    }, 10000);
    this.electionTimer = setInterval(() => {
      if (this.stopped) return;
      if (this.lease && Date.now() - millis(this.lease.renewedAt) > LEASE_MS) {
        this.connected = false;
        this.onStatus("Resuming the realm…");
      }
      void this.elect();
    }, 1500);
    this.visibility = () => {
      if (!document.hidden) {
        void this.heartbeat().catch(() => {});
        void this.elect();
      }
    };
    document.addEventListener("visibilitychange", this.visibility);
    this.startTimeout = setTimeout(
      () =>
        this.initialReject?.(
          new Error("The Firestore realm is reconnecting. Retry shortly."),
        ),
      30000,
    );
    void this.elect();
    await firstView;
    clearTimeout(this.startTimeout);
    this.initialReject = null;
  }
  async heartbeat() {
    const ref = this.ref(`${REALM_PATH}/sessions/${this.uid || this.user.uid}`);
    await this.sdk.runTransaction(this.db, async (tx) => {
      const previous = await tx.get(ref);
      // A join deliberately takes over this UID's old tab. Later heartbeats
      // never fight another tab for the same company.
      if (this.registered && previous.data()?.sessionId !== this.sessionId) {
        const error = new Error(
          "This company is open in another tab. Reconnect here to take over.",
        );
        error.code = "SESSION_REPLACED";
        throw error;
      }
      tx.set(ref, {
        uid: this.uid || this.user.uid,
        sessionId: this.sessionId,
        username: this.username.slice(0, 40) || "Wanderer",
        seenAt: this.sdk.serverTimestamp(),
      });
    });
    this.registered = true;
  }
  async elect() {
    if (this.stopped || this.electing || this.authority || document.hidden)
      return;
    const lease = this.lease;
    const ours =
      lease?.ownerUid === this.uid && lease.ownerSession === this.sessionId;
    if (
      lease &&
      !lease.released &&
      !ours &&
      lease.ownerUid !== this.uid &&
      Date.now() - millis(lease.renewedAt) < LEASE_MS + 250
    )
      return;
    this.electing = true;
    try {
      const epoch = await this.sdk.runTransaction(this.db, async (tx) => {
        const ref = this.ref(REALM_PATH),
          old = (await tx.get(ref)).data();
        if (
          old &&
          !old.released &&
          old.ownerUid !== this.uid &&
          Date.now() - millis(old.renewedAt) < LEASE_MS + 250
        )
          return null;
        const ownSession = await tx.get(
          this.ref(`${REALM_PATH}/sessions/${this.uid}`),
        );
        if (ownSession.data()?.sessionId !== this.sessionId) return null;
        const epoch = (old?.epoch || 0) + 1;
        tx.set(ref, {
          schemaVersion: 1,
          ownerUid: this.uid,
          ownerSession: this.sessionId,
          epoch,
          released: false,
          renewedAt: this.sdk.serverTimestamp(),
        });
        return epoch;
      });
      if (!epoch || this.stopped) return;
      const authority = new FirestoreRealm({
        db: this.db,
        sdk: this.sdk,
        uid: this.uid,
        sessionId: this.sessionId,
        epoch,
        onLost: (error) => {
          if (this.authority === authority) this.authority = null;
          this.connected = false;
          this.onStatus("Resuming the realm…");
          this.lastHostError = error;
        },
      });
      this.authority = authority;
      await authority.start();
    } catch (error) {
      const host = this.authority;
      this.authority = null;
      await host?.close({ release: false });
      if (!this.stopped) this.connectionError(error);
    } finally {
      this.electing = false;
    }
  }
  async receiveView() {
    const view = this.view;
    if (
      this.stopped ||
      !view ||
      view.epoch !== this.lease?.epoch ||
      view.hostSession !== this.lease?.ownerSession ||
      this.lease.released
    )
      return;
    const version = `${view.epoch}:${view.updatedAt?.seconds}:${view.updatedAt?.nanoseconds}`;
    if (version === this.viewVersion) return;
    this.viewVersion = version;
    try {
      const state = await unpack(view.packed);
      if (this.stopped || this.viewVersion !== version) return;
      if (state.error) {
        this.initialReject?.(new Error(state.error));
        this.onStatus(state.error);
        return;
      }
      this.connected = true;
      this.onState(state);
      this.onStatus("Online · Firestore");
      this.initialResolve?.();
      this.initialResolve = null;
    } catch (error) {
      this.connectionError(error);
    }
  }
  connectionError(error) {
    if (this.stopped) return;
    if (error.code === "SESSION_REPLACED") {
      void this.stop();
      this.onStatus(error.message);
      return;
    }
    this.connected = false;
    const message =
      error.code === "permission-denied"
        ? "Publish the updated Mountain Blade Firestore rules, then reconnect."
        : error.message || "Reconnecting to Firestore…";
    this.onStatus(message);
    if (error.code === "permission-denied")
      this.initialReject?.(new Error(message));
  }
  persistOutbox() {
    try {
      sessionStorage.setItem(this.outboxKey, JSON.stringify([...this.pending]));
    } catch {}
  }
  async action(input) {
    if (this.stopped || !this.connected)
      throw new Error("Reconnect to Firestore before issuing an order.");
    if (!ACTIONS.includes(input.type)) throw new Error("Unknown command.");
    const { requestId, ...body } = input;
    const payload = JSON.parse(canonical(body));
    const key = canonical(payload),
      durable = !EPHEMERAL.has(input.type);
    const id = this.pending.get(key) || input.requestId || crypto.randomUUID();
    if (durable) {
      this.pending.set(key, id);
      this.persistOutbox();
    }
    const ref = this.ref(`${REALM_PATH}/commands/${id}`);
    await this.sdk.runTransaction(this.db, async (tx) => {
      const existing = await tx.get(ref);
      if (existing.exists()) {
        if (
          existing.data().uid !== this.uid ||
          canonical(existing.data().payload) !== canonical(payload)
        )
          throw new Error(
            "This order ID already belongs to a different command.",
          );
        return;
      }
      tx.set(ref, {
        uid: this.uid,
        sessionId: this.sessionId,
        type: input.type,
        payload,
        state: "pending",
        createdAt: this.sdk.serverTimestamp(),
      });
    });
    const result = await new Promise((resolve, reject) => {
      let unsubscribe;
      const cancel = (error) => {
        clearTimeout(timeout);
        unsubscribe?.();
        this.waiters.delete(cancel);
        reject(error);
      };
      const timeout = setTimeout(
        () =>
          cancel(
            new Error(
              "The realm has not acknowledged this order yet. Retry the same action safely.",
            ),
          ),
        30000,
      );
      this.waiters.add(cancel);
      unsubscribe = this.sdk.onSnapshot(
        ref,
        { includeMetadataChanges: true },
        (snapshot) => {
          if (snapshot.metadata.fromCache || snapshot.metadata.hasPendingWrites)
            return;
          const data = snapshot.data();
          if (!data || data.state === "pending") return;
          clearTimeout(timeout);
          unsubscribe?.();
          this.waiters.delete(cancel);
          resolve(data.result);
        },
        cancel,
      );
    });
    if (durable) {
      this.pending.delete(key);
      this.persistOutbox();
    } else void this.sdk.deleteDoc(ref).catch(() => {});
    if (!result?.ok)
      throw new Error(result?.message || "The order could not be completed.");
    return result;
  }
  stop() {
    if (this.stopped) return Promise.resolve();
    this.stopped = true;
    this.connected = false;
    this.initialReject?.(new Error("You left the online realm."));
    clearTimeout(this.startTimeout);
    clearInterval(this.heartbeatTimer);
    clearInterval(this.electionTimer);
    document.removeEventListener("visibilitychange", this.visibility);
    for (const stop of this.unsubscribers) stop();
    for (const cancel of [...this.waiters])
      cancel(new Error("You left the online realm."));
    const host = this.authority;
    this.authority = null;
    return host?.close() || Promise.resolve();
  }
}
