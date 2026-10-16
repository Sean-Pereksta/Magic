import { randomUUID } from "node:crypto";
import { gzipSync, gunzipSync } from "node:zlib";
import { Realm } from "../../game/mountonline/realm.mjs";
import {
  STEP,
  WORLD,
  clone,
  newProfile,
  migrateLegacy,
  armySize,
} from "../../game/mountonline/data.mjs";
import { requireThat, GameError } from "../../game/mountonline/progression.mjs";
const pack = (b) => gzipSync(Buffer.from(JSON.stringify(b))).toString("base64");
const unpack = (text) =>
  JSON.parse(gunzipSync(Buffer.from(text, "base64")).toString());
/** One leased authority owns this realm. Firestore persists checkpoints and receipts;
 * it is never a 20Hz message bus. All mutations pass through a serial queue. */
export class MountService {
  constructor({
    db,
    auth,
    now = Date.now,
    serverId = randomUUID(),
    maxPlayers = 64,
  }) {
    this.db = db;
    this.auth = auth;
    this.now = now;
    this.serverId = serverId;
    this.maxPlayers = maxPlayers;
    this.realm = new Realm();
    this.ready = false;
    this.queue = Promise.resolve();
    this.online = new Map();
    this.leaderRef = db.doc("mountOnlineRealm/authority");
    this.stateRef = db.doc("mountOnlineRealm/state");
    this.lastSave = 0;
    this.lastBattleSave = 0;
    this.epoch = 0;
  }
  serial(fn) {
    const task = this.queue.then(fn);
    this.queue = task.catch(() => {});
    return task;
  }
  async acquire() {
    return this.serial(async () => {
      this.epoch = await this.db.runTransaction(async (tx) => {
        const snap = await tx.get(this.leaderRef),
          data = snap.data() || {};
        requireThat(
          !data.owner ||
            data.owner === this.serverId ||
            data.until <= this.now(),
          "Another server is hosting this realm. Reconnect to its address.",
          "REALM_BUSY",
          503,
        );
        const epoch = (data.epoch || 0) + 1;
        tx.set(this.leaderRef, {
          owner: this.serverId,
          epoch,
          until: this.now() + 30000,
        });
        return epoch;
      });
      const state = await this.stateRef.get();
      this.realm = new Realm(state.data() || {});
      this.realm.sequence = state.data()?.sequence || 0;
      this.realm.alliances = state.data()?.alliances || [];
      const battles = await this.db
        .collection("mountOnlineBattles")
        .where("settled", "==", false)
        .limit(16)
        .get();
      for (const doc of battles.docs) {
        const b = unpack(doc.data().packed);
        for (const uid of b.participants.filter(Boolean)) {
          const s = await this.db.doc(`mountOnlineProfiles/${uid}`).get();
          if (s.exists && !this.realm.players.has(uid)) {
            this.installHoldings(s.data());
            this.realm.enter(uid, s.data());
            this.online.set(uid, this.now());
          }
        }
        this.realm.battles.set(b.id, b);
      }
      // Recover connected travelers as well as battle seats after transient I/O
      // failures; their sockets must not be stranded outside the restored realm.
      for (const [uid, seen] of this.online)
        if (!this.realm.players.has(uid) && this.now() - seen < 90000) {
          const snap = await this.db.doc(`mountOnlineProfiles/${uid}`).get();
          if (snap.exists) {
            this.installHoldings(snap.data());
            this.realm.enter(uid, snap.data());
          }
        }
      this.ready = true;
      this.lastSave = this.now();
      return this;
    });
  }
  assertReady() {
    requireThat(
      this.ready,
      "The realm server is reconnecting. Your saved progress is safe.",
      "UNAVAILABLE",
      503,
    );
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
  async identity(token) {
    requireThat(
      typeof token === "string" && token.length > 0,
      "Sign in with Firebase to enter the realm.",
      "UNAUTHENTICATED",
      401,
    );
    const decoded = await this.auth.verifyIdToken(token, true);
    requireThat(
      typeof decoded.uid === "string" &&
        /^[A-Za-z0-9_-]{1,128}$/.test(decoded.uid),
      "Invalid Firebase identity.",
      "UNAUTHENTICATED",
      401,
    );
    const own = await this.db.doc(`mountOnlineAccounts/${decoded.uid}`).get();
    let account = own.data();
    if (!account?.verified) {
      const forge = await this.db
        .doc(`factionForgeAccounts/${decoded.uid}`)
        .get();
      const f = forge.data();
      if (f?.verified && /^[A-Za-z0-9 _.-]{1,40}$/.test(f.username || "")) {
        const name = await this.db.doc(`factionForgeNames/${f.username}`).get();
        if (name.data()?.uid === decoded.uid) account = f;
      }
    }
    const username =
      account?.verified &&
      /^[A-Za-z0-9 _.-]{1,40}$/.test(account.username || "")
        ? account.username
        : null;
    return { uid: decoded.uid, username, linked: !!username };
  }
  async join(who) {
    return this.serial(async () => {
      this.assertReady();
      if (this.realm.players.has(who.uid)) {
        const existing = this.realm.player(who.uid);
        if (who.linked && !existing.linked) {
          requireThat(
            !existing.activeBattle,
            "Finish your battle before linking the older company.",
          );
          await this.persist();
          this.realm.players.delete(who.uid);
        } else {
          this.online.set(who.uid, this.now());
          return this.snapshot(who.uid, who.linked);
        }
      }
      requireThat(
        this.realm.players.size < this.maxPlayers,
        "The realm is full. Try again shortly.",
        "REALM_FULL",
        409,
      );
      const loaded = await this.db.runTransaction(async (tx) => {
        const ref = this.db.doc(`mountOnlineProfiles/${who.uid}`),
          snap = await tx.get(ref),
          lease = await tx.get(this.leaderRef);
        this.checkLease(lease.data());
        let p = snap.data();
        let legacy = null,
          holdings = [];
        if (who.username && !p?.migrated) {
          const [name, old, byOwner, byName] = await Promise.all([
            tx.get(this.db.doc(`mountOnlineNames/${who.username}`)),
            tx.get(
              this.db.doc(
                `games_rogueduel/mountandblade_online/players/${who.username}`,
              ),
            ),
            tx.get(
              this.db
                .collection("games_rogueduel/mountandblade_online/castles")
                .where("owner", "==", who.username),
            ),
            tx.get(
              this.db
                .collection("games_rogueduel/mountandblade_online/castles")
                .where("ownerName", "==", who.username),
            ),
          ]);
          requireThat(
            !name.exists || name.data().uid === who.uid,
            "This name is bound to another verified identity.",
            "ACCOUNT_CONFLICT",
            403,
          );
          legacy = old.data();
          holdings = [
            ...new Map(
              [...byOwner.docs, ...byName.docs].map((d) => [
                d.id,
                { id: d.id, ...d.data() },
              ]),
            ).values(),
          ];
        }
        if (!p)
          p = legacy
            ? migrateLegacy(legacy, who.username)
            : newProfile(who.username || `Wanderer ${who.uid.slice(0, 6)}`);
        else if (legacy) {
          const imported = migrateLegacy(legacy, who.username);
          imported.gold = Math.max(imported.gold, p.gold);
          if (p.level > imported.level) {
            imported.level = p.level;
            imported.xp = p.xp;
          } else imported.xp = Math.max(imported.xp, p.xp);
          imported.reserve = Object.fromEntries(
            Object.keys(imported.reserve).map((k) => [
              k,
              imported.reserve[k] +
                Math.max(
                  0,
                  (p.army[k] || 0) +
                    (p.reserve?.[k] || 0) -
                    imported.army[k] -
                    imported.reserve[k],
                ),
            ]),
          );
          imported.inventory.banners.push(
            ...p.inventory.banners.filter(
              (k) =>
                typeof k === "string" &&
                !imported.inventory.banners.includes(k),
            ),
          );
          if (p.inventory.equipped.length)
            imported.inventory.equipped = clone(p.inventory.equipped);
          p = {
            ...p,
            ...imported,
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
        }
        p.uid = who.uid;
        if (who.username) p.username = who.username;
        p.linked = who.linked;
        p.legacyHoldings = [
          ...new Map(
            [
              ...(p.legacyHoldings || []),
              ...holdings.map((h) => ({
                id: h.id,
                x: Number(h.x) || 0,
                y: Number(h.y) || 0,
                garrison: Math.min(400, Math.max(0, Number(h.garrison) || 0)),
              })),
            ].map((h) => [h.id, h]),
          ).values(),
        ];
        if (who.username) {
          tx.set(this.db.doc(`mountOnlineAccounts/${who.uid}`), {
            username: who.username,
            verified: true,
          });
          tx.set(this.db.doc(`mountOnlineNames/${who.username}`), {
            uid: who.uid,
          });
        }
        tx.set(ref, p);
        return p;
      });
      this.installHoldings(loaded);
      this.realm.enter(who.uid, loaded);
      this.online.set(who.uid, this.now());
      await this.persist();
      return this.snapshot(who.uid, who.linked);
    });
  }
  checkLease(data) {
    requireThat(
      data?.owner === this.serverId &&
        data.epoch === this.epoch &&
        data.until > this.now(),
      "This server no longer owns the realm. Reconnect.",
      "LEASE_EXPIRED",
      503,
    );
  }
  backup() {
    return {
      state: this.realm.checkpoint(),
      alliances: clone(this.realm.alliances || []),
      players: clone([...this.realm.players]),
      battles: clone([...this.realm.battles]),
    };
  }
  restore(b) {
    this.realm = new Realm(b.state);
    this.realm.sequence = b.state.sequence;
    this.realm.alliances = b.alliances;
    this.realm.players = new Map(b.players);
    this.realm.battles = new Map(b.battles);
  }
  async persist(receipt = null, { onlyBattles = false } = {}) {
    const state = this.realm.checkpoint();
    state.alliances = clone(this.realm.alliances || []);
    // Capture before awaiting I/O; the queue prevents a half-settled world being acknowledged.
    const profiles = [...this.realm.players].map(([id, p]) => [id, clone(p)]),
      battles = [...this.realm.battles.values()].map((b) => ({
        id: b.id,
        participants: b.participants,
        settled: b.settled,
        packed: pack(b),
        updatedAt: this.now(),
      }));
    this.saving = true;
    try {
      await this.db.runTransaction(async (tx) => {
        const lease = await tx.get(this.leaderRef);
        this.checkLease(lease.data());
        let existing;
        if (receipt) existing = await tx.get(receipt.ref);
        requireThat(
          !existing?.exists,
          "This request has already committed. Retry to read its receipt.",
          "REQUEST_COMMITTED",
          409,
        );
        tx.set(this.leaderRef, {
          owner: this.serverId,
          epoch: this.epoch,
          until: this.now() + 30000,
        });
        if (!onlyBattles) {
          tx.set(this.stateRef, state);
          for (const [uid, p] of profiles)
            tx.set(this.db.doc(`mountOnlineProfiles/${uid}`), p);
        }
        for (const b of battles)
          tx.set(this.db.doc(`mountOnlineBattles/${b.id}`), b);
        if (receipt)
          tx.create(receipt.ref, {
            answer: receipt.answer,
            type: receipt.type,
            at: this.now(),
          });
      });
      this.lastBattleSave = this.now();
      if (!onlyBattles) this.lastSave = this.now();
    } catch (error) {
      this.ready = false;
      throw error;
    } finally {
      this.saving = false;
    }
  }
  async request(who, input) {
    return this.serial(async () => {
      this.assertReady();
      requireThat(
        input &&
          typeof input === "object" &&
          !Array.isArray(input) &&
          typeof input.type === "string" &&
          input.type.length < 40,
        "Invalid command.",
      );
      this.realm.player(who.uid);
      this.online.set(who.uid, this.now());
      if (input.type === "move") {
        this.realm.move(who.uid, input.point);
        return { ok: true };
      }
      if (input.type === "halt") {
        this.realm.player(who.uid).route = [];
        return { ok: true, message: "Your company holds here." };
      }
      if (input.type === "command") {
        this.realm.command(who.uid, input);
        return { ok: true };
      }
      requireThat(
        typeof input.requestId === "string" &&
          /^[A-Za-z0-9_-]{8,80}$/.test(input.requestId),
        "A unique request ID is required.",
      );
      const ref = this.db.doc(
          `mountOnlineProfiles/${who.uid}/receipts/${input.requestId}`,
        ),
        existing = await ref.get();
      if (existing.exists)
        return {
          ...existing.data().answer,
          snapshot: this.snapshot(who.uid, who.linked),
        };
      const backup = this.backup();
      let answer;
      try {
        if (input.type === "engage") {
          requireThat(
            this.realm.battles.size < 16,
            "Too many battles are active. Try again shortly.",
            "BATTLE_LIMIT",
            409,
          );
          this.realm.startBattle(who.uid, input.kind, input.target);
          answer = { ok: true, message: "Your battle has begun." };
        } else if (input.type.startsWith("alliance"))
          answer = { ok: true, message: await this.alliance(who, input) };
        else
          answer = {
            ok: true,
            message: this.realm.action(who.uid, input.type, input),
          };
        await this.persist({ ref, type: input.type, answer });
        return { ...answer, snapshot: this.snapshot(who.uid, who.linked) };
      } catch (error) {
        this.restore(backup);
        throw error;
      }
    });
  }
  async alliance(who, input) {
    const p = this.realm.player(who.uid);
    requireThat(!p.activeBattle, "Finish the battle first.");
    const alliances = this.realm.alliances ?? (this.realm.alliances = []),
      a = alliances.find((a) => a.id === input.alliance);
    if (input.type === "allianceCreate") {
      requireThat(!p.allianceId, "Leave your current alliance first.");
      requireThat(
        typeof input.name === "string" &&
          /^[A-Za-z0-9 .'-]{3,30}$/.test(input.name),
        "Use 3–30 letters or numbers for the alliance name.",
      );
      requireThat(p.gold >= 100, "Founding an alliance costs 100 gold.");
      requireThat(
        alliances.length < 64,
        "The realm has reached its alliance limit.",
      );
      p.gold -= 100;
      p.allianceId = randomUUID();
      alliances.push({
        id: p.allianceId,
        name: input.name,
        owner: who.uid,
        members: [who.uid],
        requests: [],
      });
      return "Alliance founded.";
    }
    requireThat(a, "This alliance is unavailable.");
    if (input.type === "allianceRequest") {
      requireThat(!p.allianceId, "Leave your current alliance first.");
      requireThat(
        a.requests.length < 20,
        "This alliance has too many pending requests.",
      );
      if (!a.requests.some((r) => r.uid === who.uid))
        a.requests.push({ uid: who.uid, username: p.username });
      return "Membership requested.";
    }
    if (input.type === "allianceAccept") {
      requireThat(
        a.owner === who.uid,
        "Only the founder can approve members.",
        "FORBIDDEN",
        403,
      );
      requireThat(a.members.length < 6, "This alliance is full.");
      const pending = a.requests.find((r) => r.uid === input.member);
      requireThat(pending, "There is no pending request from this commander.");
      let member = this.realm.players.get(pending.uid);
      if (!member) {
        const doc = await this.db
          .doc(`mountOnlineProfiles/${pending.uid}`)
          .get();
        requireThat(doc.exists, "This commander is unavailable.");
        requireThat(
          this.realm.players.size < this.maxPlayers,
          "The realm is full. Approve this commander after a seat opens.",
          "REALM_FULL",
          409,
        );
        member = this.realm.enter(pending.uid, doc.data());
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
    if (input.type === "allianceLeave") {
      requireThat(
        a.members.includes(who.uid),
        "You do not belong to this alliance.",
      );
      a.members = a.members.filter((id) => id !== who.uid);
      p.allianceId = null;
      if (a.owner === who.uid) a.owner = a.members[0] || null;
      if (!a.members.length) alliances.splice(alliances.indexOf(a), 1);
      return "You left the alliance.";
    }
    throw new GameError("Unknown alliance action.");
  }
  snapshot(uid, linked) {
    const s = this.realm.snapshot(uid);
    s.linked = linked ?? s.profile.linked;
    s.alliances = (this.realm.alliances || []).map((a) => ({
      ...a,
      requests: a.owner === uid ? a.requests : [],
    }));
    return s;
  }
  heartbeat(uid) {
    if (this.realm.players.has(uid)) this.online.set(uid, this.now());
  }
  async advance(dt = STEP) {
    return this.serial(async () => {
      if (!this.ready) return;
      const before = new Set(
        [...this.realm.battles.values()]
          .filter((b) => b.settled)
          .map((b) => b.id),
      );
      const stale = [];
      for (const [uid, p] of this.realm.players)
        if (this.now() - (this.online.get(uid) || 0) > 30000) {
          p.route = [];
          if (
            !p.activeBattle &&
            this.now() - (this.online.get(uid) || 0) > 90000
          )
            stale.push(uid);
        }
      if (stale.length) {
        await this.persist();
        for (const uid of stale) {
          this.realm.players.delete(uid);
          this.online.delete(uid);
        }
      }
      this.realm.tick(dt);
      const settled = [...this.realm.battles.values()].some(
        (b) => b.settled && !before.has(b.id),
      );
      const started = [...this.realm.battles.values()].some(
        (b) => !b.persisted,
      );
      for (const b of this.realm.battles.values()) b.persisted = true;
      if (settled || started || this.now() - this.lastSave >= 10000)
        await this.persist();
      else if (
        [...this.realm.battles.values()].some((b) => !b.settled) &&
        this.now() - this.lastBattleSave >= 2000
      )
        await this.persist(null, { onlyBattles: true });
    });
  }
  async close() {
    return this.serial(async () => {
      if (this.ready) {
        await this.persist();
        await this.db.runTransaction(async (tx) => {
          const s = await tx.get(this.leaderRef);
          if (s.data()?.owner === this.serverId)
            tx.update(this.leaderRef, { until: 0 });
        });
      }
      this.ready = false;
    });
  }
}
