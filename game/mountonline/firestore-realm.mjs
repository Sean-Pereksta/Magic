import { Realm } from "./realm.mjs";
import { STEP, WORLD, clone, newProfile, migrateLegacy } from "./data.mjs";
import { requireThat } from "./progression.mjs";
import { pack, unpack } from "./codec.mjs";

export const REALM_ID = "living-realm";
export const REALM_PATH = `mountOnlineRealms/${REALM_ID}`;
export const LEASE_MS = 20000;
export const EPHEMERAL = new Set(["move", "command", "halt"]);
export const ACTIONS = [
  "move",
  "command",
  "halt",
  "engage",
  "recruit",
  "train",
  "reserve",
  "supplies",
  "rest",
  "buyBanner",
  "equip",
  "contract",
  "skill",
  "build",
  "garrison",
  "diplomacy",
  "caravan",
  "allianceCreate",
  "allianceRequest",
  "allianceAccept",
  "allianceLeave",
];
export const millis = (stamp) => stamp?.toMillis?.() || 0;

/** An elected browser runs the existing shared simulation. Firestore rules fence
 * every write to its current lease. This is a peer-hosted game, not a trusted
 * anti-cheat server: rules cannot recompute an entire medieval battle. */
export class FirestoreRealm {
  constructor({ db, sdk, uid, sessionId, epoch, onLost = () => {} }) {
    Object.assign(this, { db, sdk, uid, sessionId, epoch, onLost });
    this.realm = new Realm();
    this.sessions = new Map();
    this.joinedSessions = new Map();
    this.queue = Promise.resolve();
    this.unsubscribers = [];
    this.ready = false;
    this.closed = false;
    this.lastCheckpoint = 0;
    this.lastPublish = 0;
    this.checkpointRevision = 0;
  }
  ref(path) {
    return this.sdk.doc(this.db, path);
  }
  serial(fn) {
    const result = this.queue.then(() => {
      if (!this.closed) return fn();
    });
    this.queue = result.catch(() => {});
    return result;
  }
  checkLease(data) {
    requireThat(
      data?.ownerUid === this.uid &&
        data.ownerSession === this.sessionId &&
        data.epoch === this.epoch &&
        !data.released,
      "The realm is changing hosts. Your saved progress will resume.",
      "LEASE_EXPIRED",
    );
  }
  backup() {
    return {
      world: this.realm.checkpoint(),
      players: clone([...this.realm.players]),
      battles: clone([...this.realm.battles]),
      events: clone(this.realm.events),
    };
  }
  restore(saved) {
    this.realm = new Realm(saved.world);
    this.realm.sequence = saved.world?.sequence || 0;
    this.realm.players = new Map(saved.players || []);
    this.realm.battles = new Map(saved.battles || []);
    this.realm.events = saved.events || [];
  }
  async start() {
    const saved = await this.sdk.getDoc(
      this.ref(`${REALM_PATH}/state/checkpoint`),
    );
    if (saved.exists()) {
      this.restore(await unpack(saved.data().packed));
      this.checkpointRevision = saved.data().revision;
    }
    if (this.closed) return;
    this.ready = true;
    const failed = (error) => this.fail(error);
    this.unsubscribers.push(
      this.sdk.onSnapshot(
        this.sdk.collection(this.db, `${REALM_PATH}/sessions`),
        { includeMetadataChanges: true },
        (snapshot) => {
          if (
            snapshot.metadata.hasPendingWrites ||
            snapshot.metadata.fromCache ||
            !this.ready
          )
            return;
          for (const doc of snapshot.docs)
            this.sessions.set(doc.id, doc.data());
          void this.serial(async () => {
            for (const [uid, session] of this.sessions)
              if (
                Date.now() - millis(session.seenAt) < 90000 &&
                this.joinedSessions.get(uid) !== session.sessionId
              )
                await this.join(uid, session);
          }).catch(failed);
        },
        failed,
      ),
    );
    const pending = this.sdk.query(
      this.sdk.collection(this.db, `${REALM_PATH}/commands`),
      this.sdk.where("state", "==", "pending"),
      this.sdk.limit(64),
    );
    this.unsubscribers.push(
      this.sdk.onSnapshot(
        pending,
        { includeMetadataChanges: true },
        (snapshot) => {
          if (
            snapshot.metadata.hasPendingWrites ||
            snapshot.metadata.fromCache ||
            !this.ready
          )
            return;
          for (const change of snapshot.docChanges())
            if (change.type !== "removed")
              void this.serial(() => this.process(change.doc)).catch(failed);
        },
        failed,
      ),
    );
    this.clock = performance.now();
    this.timer = setInterval(() => {
      if (this.ticking || !this.ready || this.closed) return;
      this.ticking = true;
      void this.serial(() => this.advance())
        .catch(failed)
        .finally(() => {
          this.ticking = false;
        });
    }, 50);
  }
  installHoldings(p) {
    for (const h of p.legacyHoldings || []) {
      if (this.realm.world.settlements.some((s) => s.id === `legacy-${h.id}`))
        continue;
      const pos = this.realm.world.nav.nearest(
        (h.x * WORLD.width) / 180,
        (h.y * WORLD.height) / 120,
      ) || { x: 15, y: 44 };
      this.realm.world.settlements.push({
        id: `legacy-${h.id}`,
        name: `${p.username}'s ancestral keep`,
        kind: "castle",
        x: pos.x + 0.5,
        y: pos.y + 0.5,
        owner: p.uid,
        ownerName: p.username,
        faction: "crown",
        level: 1,
        prosperity: 1,
        garrison: h.garrison,
      });
    }
  }
  async join(uid, session) {
    this.sessions.set(uid, session);
    if (!this.realm.players.has(uid) && this.realm.players.size >= 16) {
      const stale = [...this.realm.players].find(
        ([id, p]) =>
          !p.activeBattle &&
          Date.now() - millis(this.sessions.get(id)?.seenAt) > 90000,
      );
      if (stale) {
        await this.persist({ checkpoint: true });
        this.realm.players.delete(stale[0]);
      } else {
        const packed = await pack({
          error: "The realm has 16 active commanders. Try again shortly.",
        });
        await this.sdk.runTransaction(this.db, async (tx) => {
          this.checkLease((await tx.get(this.ref(REALM_PATH))).data());
          tx.set(this.ref(`${REALM_PATH}/views/${uid}`), {
            uid,
            epoch: this.epoch,
            hostSession: this.sessionId,
            packed,
            updatedAt: this.sdk.serverTimestamp(),
          });
        });
        this.joinedSessions.set(uid, session.sessionId);
        return false;
      }
    }
    const profileDoc = await this.sdk.getDoc(
      this.ref(`mountOnlineProfiles/${uid}`),
    );
    const accountDoc = await this.sdk.getDoc(
      this.ref(`mountOnlineAccounts/${uid}`),
    );
    const account = accountDoc.data();
    let name = null;
    if (
      account?.verified &&
      /^[A-Za-z0-9 _.-]{1,40}$/.test(account.username || "")
    ) {
      const reverse = await this.sdk.getDoc(
        this.ref(`mountOnlineNames/${account.username}`),
      );
      if (reverse.data()?.uid === uid) name = account.username;
    }
    let p = this.realm.players.get(uid) ||
      profileDoc.data() || { ...newProfile(session.username), uid };
    // A retired server save can reference a battle outside the new realm.
    // Resume its saved company without inventing casualties or a reward.
    if (p.activeBattle && !this.realm.battles.has(p.activeBattle))
      p.activeBattle = null;
    if (name && !p.migrated && !p.activeBattle) {
      const old = await this.sdk.getDoc(
        this.ref(`games_rogueduel/mountandblade_online/players/${name}`),
      );
      if (old.exists()) {
        const imported = migrateLegacy(old.data(), name);
        // Late bindings retain new skills, contracts, achievements, inventory,
        // and whichever progression/counts are greater, without double imports.
        if (profileDoc.exists()) {
          imported.gold = Math.max(imported.gold, p.gold);
          if (p.level > imported.level) {
            imported.level = p.level;
            imported.xp = p.xp;
          } else imported.xp = Math.max(imported.xp, p.xp);
          for (const key of Object.keys(imported.army))
            imported.reserve[key] += Math.max(
              0,
              (p.army[key] || 0) +
                (p.reserve[key] || 0) -
                imported.army[key] -
                imported.reserve[key],
            );
          imported.inventory.banners.push(
            ...p.inventory.banners.filter(
              (key) =>
                typeof key === "string" &&
                !imported.inventory.banners.includes(key),
            ),
          );
          if (p.inventory.equipped.length)
            imported.inventory.equipped = clone(p.inventory.equipped);
        }
        p = {
          ...p,
          ...imported,
          uid,
          stats: p.stats,
          achievements: p.achievements,
          contracts: p.contracts,
          claimedContracts: p.claimedContracts,
          renown: p.renown,
          skills: p.skills,
          skillPoints: p.skillPoints,
          relations: p.relations,
          supplies: p.supplies,
          morale: p.morale,
          explored: p.explored,
        };
        const holdings = [];
        for (const field of ["owner", "ownerName"]) {
          const found = await this.sdk.getDocs(
            this.sdk.query(
              this.sdk.collection(
                this.db,
                "games_rogueduel/mountandblade_online/castles",
              ),
              this.sdk.where(field, "==", name),
            ),
          );
          for (const doc of found.docs)
            holdings.push({
              id: doc.id,
              x: Number(doc.data().x) || 0,
              y: Number(doc.data().y) || 0,
              garrison: Math.min(
                400,
                Math.max(0, Number(doc.data().garrison) || 0),
              ),
            });
        }
        p.legacyHoldings = [
          ...new Map(
            [...(p.legacyHoldings || []), ...holdings].map((h) => [h.id, h]),
          ).values(),
        ];
      }
    }
    if (name) p.username = name;
    p.linked = !!name;
    this.installHoldings(p);
    this.realm.enter(uid, p);
    this.joinedSessions.set(uid, session.sessionId);
    // Return players to the correct side of a persisted, unfinished battle.
    await this.persist({ checkpoint: true });
    return true;
  }
  snapshot(uid) {
    const snapshot = this.realm.snapshot(uid);
    snapshot.linked = !!snapshot.profile.linked;
    snapshot.alliances = this.realm.alliances.map((a) => ({
      ...a,
      requests: a.owner === uid ? a.requests : [],
    }));
    snapshot.syncInterval =
      snapshot.battle && !snapshot.battle.finished ? 500 : 1200;
    return snapshot;
  }
  async persist({ checkpoint = false, command = null, answer = null } = {}) {
    const saved = checkpoint ? await pack(this.backup()) : null;
    requireThat(
      !saved || saved.length < 850000,
      "This realm has reached its checkpoint size limit.",
    );
    const views = await Promise.all(
      [...this.realm.players.keys()].map(async (uid) => [
        uid,
        await pack(this.snapshot(uid)),
      ]),
    );
    requireThat(
      views.every(([, value]) => value.length < 850000),
      "This battle is too large to synchronize.",
    );
    const profiles = checkpoint ? clone([...this.realm.players]) : [];
    const { runTransaction, serverTimestamp } = this.sdk;
    await runTransaction(this.db, async (tx) => {
      const lease = await tx.get(this.ref(REALM_PATH));
      this.checkLease(lease.data());
      let receipt;
      if (command) receipt = await tx.get(command.ref);
      if (receipt && receipt.data()?.state !== "pending") {
        const error = new Error(
          "The command was already committed. Reloading the realm.",
        );
        error.code = "ALREADY_COMMITTED";
        throw error;
      }
      if (checkpoint) {
        const current = await tx.get(
          this.ref(`${REALM_PATH}/state/checkpoint`),
        );
        requireThat(
          (current.data()?.revision || 0) === this.checkpointRevision,
          "A newer realm checkpoint is available.",
          "LEASE_EXPIRED",
        );
      }
      const stamp = () => serverTimestamp();
      // Renew independently of snapshot frequency: changing the shared lease
      // on every battle frame makes every connected player read it again.
      if (Date.now() - millis(lease.data().renewedAt) >= 5000)
        tx.update(this.ref(REALM_PATH), { renewedAt: stamp() });
      if (checkpoint) {
        tx.set(this.ref(`${REALM_PATH}/state/checkpoint`), {
          schemaVersion: 1,
          epoch: this.epoch,
          hostSession: this.sessionId,
          revision: this.checkpointRevision + 1,
          packed: saved,
          updatedAt: stamp(),
        });
        for (const [uid, profile] of profiles)
          tx.set(this.ref(`mountOnlineProfiles/${uid}`), profile);
      }
      for (const [uid, packed] of views)
        tx.set(this.ref(`${REALM_PATH}/views/${uid}`), {
          uid,
          epoch: this.epoch,
          hostSession: this.sessionId,
          packed,
          updatedAt: stamp(),
        });
      if (command)
        tx.update(command.ref, {
          state: answer.ok ? "done" : "error",
          result: answer,
          epoch: this.epoch,
          handledAt: stamp(),
        });
    });
    this.lastPublish = Date.now();
    if (checkpoint) {
      this.lastCheckpoint = Date.now();
      this.checkpointRevision++;
    }
  }
  async process(command) {
    if (!this.ready) return;
    const latest = await this.sdk.getDoc(command.ref);
    if (!latest.exists() || latest.data().state !== "pending") return;
    const data = latest.data(),
      input = data.payload;
    if (!this.realm.players.has(data.uid)) {
      const session = await this.sdk.getDoc(
        this.ref(`${REALM_PATH}/sessions/${data.uid}`),
      );
      if (!session.exists()) return;
      const joined = await this.join(data.uid, session.data());
      if (!joined) {
        await this.persist({
          command,
          answer: {
            ok: false,
            message: "The realm is full. Reconnect shortly.",
          },
        });
        return;
      }
    }
    const backup = this.backup();
    let answer;
    try {
      requireThat(
        ACTIONS.includes(data.type) && input.type === data.type,
        "Unknown command.",
      );
      // Creation rules authenticated this UID/session when the order was sent.
      // A reconnect may change the session before an already accepted order is
      // processed; its immutable receipt must still settle exactly once.
      if (input.type === "move") this.realm.move(data.uid, input.point);
      else if (input.type === "halt") this.realm.player(data.uid).route = [];
      else if (input.type === "command") this.realm.command(data.uid, input);
      else if (input.type === "engage")
        this.realm.startBattle(data.uid, input.kind, input.target);
      else if (input.type.startsWith("alliance"))
        answer = { ok: true, message: await this.alliance(data.uid, input) };
      else
        answer = {
          ok: true,
          message: this.realm.action(data.uid, input.type, input),
        };
      answer ||= {
        ok: true,
        ...(input.type === "engage"
          ? { message: "Your battle has begun." }
          : {}),
      };
    } catch (error) {
      this.restore(backup);
      answer = {
        ok: false,
        message: String(
          error.message || "The order could not be completed.",
        ).slice(0, 240),
      };
    }
    try {
      // Durable results, both player profiles, territory, and this permanent
      // command receipt commit together. A retry cannot award the result twice.
      await this.persist({
        checkpoint: !EPHEMERAL.has(data.type),
        command,
        answer,
      });
    } catch (error) {
      this.restore(backup);
      throw error;
    }
  }
  async alliance(uid, input) {
    const p = this.realm.player(uid),
      alliances = this.realm.alliances,
      a = alliances.find((a) => a.id === input.alliance);
    requireThat(!p.activeBattle, "Finish the battle first.");
    if (input.type === "allianceCreate") {
      requireThat(!p.allianceId, "Leave your current alliance first.");
      requireThat(
        /^[A-Za-z0-9 .'-]{3,30}$/.test(input.name || ""),
        "Use 3–30 letters or numbers for the alliance name.",
      );
      requireThat(
        p.gold >= 100 && alliances.length < 64,
        "An alliance costs 100 gold; the realm supports 64 alliances.",
      );
      p.gold -= 100;
      p.allianceId = crypto.randomUUID();
      alliances.push({
        id: p.allianceId,
        name: input.name,
        owner: uid,
        members: [uid],
        requests: [],
      });
      return "Alliance founded.";
    }
    requireThat(a, "This alliance is unavailable.");
    if (input.type === "allianceRequest") {
      requireThat(
        !p.allianceId && a.requests.length < 20,
        "Leave your alliance first, or wait for a pending request to clear.",
      );
      if (!a.requests.some((r) => r.uid === uid))
        a.requests.push({ uid, username: p.username });
      return "Membership requested.";
    }
    if (input.type === "allianceAccept") {
      requireThat(a.owner === uid, "Only the founder can approve members.");
      requireThat(a.members.length < 6, "This alliance is full.");
      const pending = a.requests.find((r) => r.uid === input.member);
      requireThat(pending, "There is no pending request from this commander.");
      let member = this.realm.players.get(pending.uid);
      if (!member) {
        const saved = await this.sdk.getDoc(
          this.ref(`mountOnlineProfiles/${pending.uid}`),
        );
        requireThat(
          saved.exists() && this.realm.players.size < 16,
          "This commander is unavailable. Try while they are online.",
        );
        member = this.realm.enter(pending.uid, saved.data());
      }
      requireThat(
        !member.allianceId,
        "This commander has already joined another alliance.",
      );
      member.allianceId = a.id;
      a.members.push(member.uid);
      a.requests = a.requests.filter((r) => r.uid !== member.uid);
      return "Commander admitted to your alliance.";
    }
    requireThat(
      input.type === "allianceLeave" && a.members.includes(uid),
      "You do not belong to this alliance.",
    );
    a.members = a.members.filter((id) => id !== uid);
    p.allianceId = null;
    if (a.owner === uid) a.owner = a.members[0] || null;
    if (!a.members.length) alliances.splice(alliances.indexOf(a), 1);
    return "You left the alliance.";
  }
  async advance() {
    if (!this.ready) return;
    const now = performance.now();
    const elapsed = Math.min(0.25, (now - this.clock) / 1000);
    this.clock = now;
    this.accumulator = (this.accumulator || 0) + elapsed;
    let durable = false;
    while (this.accumulator >= STEP) {
      const before = [...this.realm.battles.values()]
        .map((b) => `${b.id}:${b.settled}`)
        .join();
      this.realm.tick(STEP);
      this.accumulator -= STEP;
      if (
        before !==
        [...this.realm.battles.values()]
          .map((b) => `${b.id}:${b.settled}`)
          .join()
      )
        durable = true;
    }
    for (const [uid, p] of this.realm.players) {
      const seen = millis(this.sessions.get(uid)?.seenAt);
      if (seen && Date.now() - seen > 30000) p.route = [];
    }
    const inBattle = [...this.realm.battles.values()].some((b) => !b.settled);
    if (
      durable ||
      Date.now() - this.lastCheckpoint > (inBattle ? 2000 : 5000)
    ) {
      await this.persist({ checkpoint: true });
      // Evict only after saving. Disconnected battle seats keep their company.
      for (const [uid, p] of this.realm.players)
        if (
          !p.activeBattle &&
          Date.now() - millis(this.sessions.get(uid)?.seenAt) > 90000
        )
          this.realm.players.delete(uid);
    } else if (Date.now() - this.lastPublish >= (inBattle ? 500 : 1200))
      await this.persist();
  }
  fail(error) {
    if (this.closed) return;
    this.closed = true;
    this.ready = false;
    clearInterval(this.timer);
    for (const stop of this.unsubscribers) stop();
    this.onLost(error);
  }
  async close({ release = true } = {}) {
    if (this.closed) return;
    clearInterval(this.timer);
    for (const stop of this.unsubscribers) stop();
    await this.serial(async () => {
      if (release && this.ready) {
        await this.persist({ checkpoint: true });
        await this.sdk.runTransaction(this.db, async (tx) => {
          const ref = this.ref(REALM_PATH),
            lease = await tx.get(ref);
          this.checkLease(lease.data());
          tx.update(ref, {
            released: true,
            renewedAt: this.sdk.serverTimestamp(),
          });
        });
      }
    }).catch(() => {});
    this.closed = true;
    this.ready = false;
  }
}
