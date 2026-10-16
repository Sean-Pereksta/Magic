const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const {
  initializeTestEnvironment,
  assertSucceeds,
  assertFails,
} = require("@firebase/rules-unit-testing");
const sdk = require("firebase/firestore");
const {
  doc,
  setDoc,
  getDoc,
  getDocs,
  updateDoc,
  deleteDoc,
  collection,
  serverTimestamp,
  runTransaction,
} = sdk;

async function suite(rulesFile) {
  const [host, port] = process.env.FIRESTORE_EMULATOR_HOST.split(":");
  const env = await initializeTestEnvironment({
    projectId: "demo-mountonline",
    firestore: {
      host,
      port: Number(port),
      rules: fs.readFileSync(
        path.resolve(__dirname, "../../..", rulesFile),
        "utf8",
      ),
    },
  });
  const alice = env.authenticatedContext("alice").firestore(),
    bob = env.authenticatedContext("bob").firestore(),
    guest = env.unauthenticatedContext().firestore();
  const root = "mountOnlineRealms/living-realm";
  let checks = 0;
  const yes = async (p) => {
    await assertSucceeds(p);
    checks++;
  };
  const no = async (p) => {
    await assertFails(p);
    checks++;
  };
  const session = (uid) => ({
    uid,
    sessionId: "session-" + uid,
    username: uid,
    seenAt: serverTimestamp(),
  });
  const lease = (uid, epoch = 1) => ({
    schemaVersion: 1,
    ownerUid: uid,
    ownerSession: "session-" + uid,
    epoch,
    released: false,
    renewedAt: serverTimestamp(),
  });
  const command = (uid, type = "recruit") => ({
    uid,
    sessionId: "session-" + uid,
    type,
    payload: { type, count: 1, settlement: "village" },
    state: "pending",
    createdAt: serverTimestamp(),
  });
  const blob = (uid) => ({
    uid,
    epoch: 1,
    hostSession: "session-alice",
    packed: "json:{}",
    updatedAt: serverTimestamp(),
  });
  try {
    await env.clearFirestore();
    const { newProfile } = await import("../data.mjs");
    await yes(setDoc(doc(alice, `${root}/sessions/alice`), session("alice")));
    await yes(setDoc(doc(bob, `${root}/sessions/bob`), session("bob")));
    await no(setDoc(doc(guest, `${root}/sessions/guest`), session("guest")));
    await no(setDoc(doc(bob, `${root}/sessions/alice`), session("alice")));
    await no(
      setDoc(doc(alice, root), { ...lease("alice"), renewedAt: new Date(0) }),
    );
    await yes(setDoc(doc(alice, root), lease("alice")));
    await no(setDoc(doc(bob, root), lease("bob", 2)));
    await no(
      updateDoc(doc(alice, root), { epoch: 20, renewedAt: serverTimestamp() }),
    );
    await yes(
      setDoc(doc(alice, "mountOnlineProfiles/alice"), {
        ...newProfile("Alice"),
        uid: "alice",
      }),
    );
    await yes(
      setDoc(doc(alice, "mountOnlineProfiles/bob"), {
        ...newProfile("Bob"),
        uid: "bob",
      }),
    );
    await yes(getDoc(doc(bob, "mountOnlineProfiles/bob")));
    await no(getDoc(doc(bob, "mountOnlineProfiles/alice")));
    await no(getDoc(doc(guest, "mountOnlineProfiles/bob")));
    await no(getDocs(collection(alice, "mountOnlineProfiles")));
    await no(updateDoc(doc(bob, "mountOnlineProfiles/bob"), { gold: 99999 }));
    await no(
      setDoc(doc(bob, "mountOnlineProfiles/random"), {
        ...newProfile(),
        uid: "random",
      }),
    );
    await no(
      setDoc(doc(alice, "mountOnlineProfiles/random"), {
        ...newProfile(),
        uid: "random",
      }),
    );
    await no(deleteDoc(doc(alice, "mountOnlineProfiles/bob")));
    await no(
      updateDoc(doc(alice, "mountOnlineProfiles/bob"), { uid: "alice" }),
    );
    await no(
      updateDoc(doc(alice, "mountOnlineProfiles/bob"), {
        password: "injected",
      }),
    );
    await no(
      setDoc(doc(alice, "mountOnlineAccounts/alice"), {
        verified: true,
        username: "Captain",
      }),
    );
    await no(setDoc(doc(bob, "mountOnlineNames/Captain"), { uid: "bob" }));
    await yes(setDoc(doc(alice, `${root}/views/bob`), blob("bob")));
    await yes(getDoc(doc(bob, `${root}/views/bob`)));
    await no(getDoc(doc(bob, `${root}/views/alice`)));
    await no(setDoc(doc(bob, `${root}/views/bob`), blob("bob")));
    await no(
      setDoc(doc(alice, `${root}/views/bob`), { ...blob("bob"), epoch: 999 }),
    );
    const state = {
      schemaVersion: 1,
      epoch: 1,
      hostSession: "session-alice",
      revision: 1,
      packed: "json:{}",
      updatedAt: serverTimestamp(),
    };
    await yes(setDoc(doc(alice, `${root}/state/checkpoint`), state));
    await no(getDoc(doc(bob, `${root}/state/checkpoint`)));
    await no(setDoc(doc(alice, `${root}/state/checkpoint`), state));
    await yes(setDoc(doc(bob, `${root}/commands/request-one`), command("bob")));
    await no(
      setDoc(doc(bob, `${root}/commands/forged-owner`), command("alice")),
    );
    await no(
      setDoc(doc(bob, `${root}/commands/forged-result`), {
        ...command("bob"),
        type: "reward",
        payload: { type: "reward", winner: "bob" },
      }),
    );
    await no(
      setDoc(doc(bob, `${root}/commands/forged-input`), {
        ...command("bob"),
        payload: { type: "recruit", gold: 10000 },
      }),
    );
    await no(
      setDoc(doc(bob, `${root}/commands/forged-state`), {
        ...command("bob"),
        state: "done",
      }),
    );
    await no(
      updateDoc(doc(bob, `${root}/commands/request-one`), {
        state: "done",
        result: { ok: true },
      }),
    );
    await no(
      updateDoc(doc(alice, `${root}/commands/request-one`), {
        uid: "alice",
        state: "done",
        result: { ok: true },
        epoch: 1,
        handledAt: serverTimestamp(),
      }),
    );
    await yes(
      updateDoc(doc(alice, `${root}/commands/request-one`), {
        state: "done",
        result: { ok: true },
        epoch: 1,
        handledAt: serverTimestamp(),
      }),
    );
    await no(
      updateDoc(doc(alice, `${root}/commands/request-one`), {
        state: "pending",
      }),
    );
    await no(deleteDoc(doc(bob, `${root}/commands/request-one`)));
    await no(getDocs(collection(bob, `${root}/commands`)));
    await yes(getDocs(collection(alice, `${root}/commands`)));
    await yes(
      setDoc(
        doc(bob, `${root}/commands/ephemeral-one`),
        command("bob", "halt"),
      ),
    );
    await yes(
      updateDoc(doc(alice, `${root}/commands/ephemeral-one`), {
        state: "done",
        result: { ok: true },
        epoch: 1,
        handledAt: serverTimestamp(),
      }),
    );
    await yes(deleteDoc(doc(bob, `${root}/commands/ephemeral-one`)));
    // Keep all 16 profile/view writes plus checkpoint, receipt and lease within
    // Firestore's per-transaction security-rule document access budget.
    await env.withSecurityRulesDisabled(async (context) => {
      const db = context.firestore();
      for (let i = 0; i < 14; i++)
        await setDoc(
          doc(db, `${root}/sessions/member-${i}`),
          session(`member-${i}`),
        );
    });
    await yes(
      runTransaction(alice, async (tx) => {
        for (const uid of [
          "alice",
          "bob",
          ...Array.from({ length: 14 }, (_, i) => `member-${i}`),
        ]) {
          tx.set(doc(alice, `mountOnlineProfiles/${uid}`), {
            ...newProfile(uid),
            uid,
          });
          tx.set(doc(alice, `${root}/views/${uid}`), blob(uid));
        }
        tx.set(doc(alice, `${root}/state/checkpoint`), {
          ...state,
          revision: 2,
        });
        tx.update(doc(alice, root), { renewedAt: serverTimestamp() });
      }),
    );
    const { FirestoreRealm } = await import("../firestore-realm.mjs");
    const { unpack } = await import("../codec.mjs");
    const overflow = env.authenticatedContext("overflow").firestore();
    await yes(
      setDoc(doc(overflow, `${root}/sessions/overflow`), session("overflow")),
    );
    const authority = new FirestoreRealm({
      db: alice,
      sdk,
      uid: "alice",
      sessionId: "session-alice",
      epoch: 1,
    });
    for (const uid of [
      "alice",
      "bob",
      ...Array.from({ length: 14 }, (_, i) => `member-${i}`),
    ]) {
      authority.realm.enter(uid, { ...newProfile(uid), uid });
      authority.sessions.set(uid, { seenAt: sdk.Timestamp.now() });
    }
    assert.equal(
      await authority.join("overflow", {
        ...session("overflow"),
        seenAt: sdk.Timestamp.now(),
      }),
      false,
    );
    assert.equal(authority.realm.players.size, 16);
    assert.match(
      (
        await unpack(
          (await getDoc(doc(overflow, `${root}/views/overflow`))).data().packed,
        )
      ).error,
      /16 active/,
    );
    checks++;
    await env.withSecurityRulesDisabled(async (context) => {
      await setDoc(doc(context.firestore(), `${root}/sessions/orphan`), {
        ...session("orphan"),
        seenAt: sdk.Timestamp.now(),
      });
      await setDoc(doc(context.firestore(), "mountOnlineProfiles/orphan"), {
        ...newProfile("Returning commander"),
        uid: "orphan",
        gold: 771,
        activeBattle: "retired-server-battle",
      });
    });
    const returning = new FirestoreRealm({
      db: alice,
      sdk,
      uid: "alice",
      sessionId: "session-alice",
      epoch: 1,
    });
    returning.checkpointRevision = 2;
    await returning.join("orphan", {
      ...session("orphan"),
      seenAt: sdk.Timestamp.now(),
    });
    assert.equal(returning.realm.player("orphan").activeBattle, null);
    assert.equal(returning.realm.player("orphan").gold, 771);
    checks++;
    await yes(
      updateDoc(doc(alice, root), {
        released: true,
        renewedAt: serverTimestamp(),
      }),
    );
    await yes(setDoc(doc(bob, root), lease("bob", 2)));
    await no(updateDoc(doc(alice, "mountOnlineProfiles/bob"), { gold: 99999 }));
    await no(setDoc(doc(alice, `${root}/views/alice`), blob("alice")));
    await yes(updateDoc(doc(bob, "mountOnlineProfiles/bob"), { gold: 200 }));
    await env.withSecurityRulesDisabled(async (context) => {
      await updateDoc(doc(context.firestore(), root), {
        renewedAt: sdk.Timestamp.fromMillis(Date.now() - 21000),
      });
    });
    await yes(setDoc(doc(alice, root), lease("alice", 3)));
    await no(setDoc(doc(bob, root), lease("bob", 4)));
    await no(
      setDoc(
        doc(alice, "games_rogueduel/mountandblade_online/players/Captain"),
        { gold: 99999 },
      ),
    );
    await yes(
      setDoc(doc(bob, "games_rogueduel/other-game/players/bob"), { gold: 1 }),
    );
    for (const prefix of ["users/Bob", "gameStats/other-game"])
      await yes(setDoc(doc(bob, prefix), { example: true }));
    await env.withSecurityRulesDisabled(async (context) => {
      await setDoc(doc(context.firestore(), "lobbies/other-game"), {
        gameType: "regression",
      });
    });
    await yes(
      setDoc(doc(bob, "lobbies/other-game/players/bob"), { example: true }),
    );
    await no(setDoc(doc(guest, "lobbies/guest"), { example: true }));
    assert.equal(
      (await getDoc(doc(bob, "mountOnlineProfiles/bob"))).data().gold,
      200,
    );
    console.log(
      JSON.stringify({
        suite: "Firestore-only rules",
        rulesFile,
        checks,
        passed: true,
      }),
    );
  } finally {
    await env.cleanup();
  }
}
(async () => {
  if (!process.env.FIRESTORE_EMULATOR_HOST)
    throw new Error("Use the demo Firestore emulator.");
  sdk.setLogLevel("silent");
  await suite("firestore.rules");
  await suite("docs/mountonline/firestore-complete.rules");
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
