const assert = require("node:assert/strict"),
  fs = require("node:fs"),
  path = require("node:path");
const {
  initializeTestEnvironment,
  assertSucceeds,
  assertFails,
} = require("@firebase/rules-unit-testing");
const {
  doc,
  setDoc,
  getDoc,
  updateDoc,
  deleteDoc,
  collection,
  getDocs,
  setLogLevel,
} = require("firebase/firestore");
async function main() {
  setLogLevel("silent");
  if (!process.env.FIRESTORE_EMULATOR_HOST)
    throw new Error("Run this suite through the Firestore emulator.");
  const [host, port] = process.env.FIRESTORE_EMULATOR_HOST.split(":"),
    env = await initializeTestEnvironment({
      projectId: "demo-mountonline",
      firestore: {
        host,
        port: Number(port),
        rules: fs.readFileSync(
          path.resolve(__dirname, "../../../firestore.rules"),
          "utf8",
        ),
      },
    });
  try {
    await env.clearFirestore();
    await env.withSecurityRulesDisabled(async (context) => {
      const db = context.firestore();
      await setDoc(doc(db, "mountOnlineProfiles/alice"), {
        uid: "alice",
        gold: 140,
      });
      await setDoc(doc(db, "mountOnlineRealm/state"), { settlements: [] });
      await setDoc(
        doc(db, "games_rogueduel/mountandblade_online/players/Captain"),
        { gold: 700 },
      );
      await setDoc(doc(db, "users/Captain"), {
        username: "Captain",
        theme: "forest",
      });
    });
    const alice = env.authenticatedContext("alice").firestore(),
      bob = env.authenticatedContext("bob").firestore(),
      guest = env.unauthenticatedContext().firestore();
    let checks = 0;
    await assertSucceeds(getDoc(doc(alice, "mountOnlineProfiles/alice")));
    checks++;
    await assertFails(getDoc(doc(bob, "mountOnlineProfiles/alice")));
    checks++;
    await assertFails(getDoc(doc(guest, "mountOnlineProfiles/alice")));
    checks++;
    await assertFails(getDocs(collection(alice, "mountOnlineProfiles")));
    checks++;
    for (const db of [alice, bob])
      for (const action of [
        () => setDoc(doc(db, "mountOnlineProfiles/alice"), { gold: 99999 }),
        () => updateDoc(doc(db, "mountOnlineProfiles/alice"), { gold: 99999 }),
        () => deleteDoc(doc(db, "mountOnlineProfiles/alice")),
        () =>
          setDoc(doc(db, "mountOnlineProfiles/alice/receipts/fake"), {
            won: true,
          }),
        () =>
          setDoc(doc(db, "mountOnlineAccounts/alice"), {
            verified: true,
            username: "Captain",
          }),
        () => setDoc(doc(db, "mountOnlineNames/Captain"), { uid: "alice" }),
        () => setDoc(doc(db, "mountOnlineBattles/fake"), { winner: 0 }),
        () => setDoc(doc(db, "mountOnlineRealm/state"), { settlements: [] }),
        () =>
          setDoc(doc(db, "games_rogueduel/mountandblade_online"), {
            gold: 1000,
          }),
        () =>
          setDoc(doc(db, "games_rogueduel/mountandblade_online/castles/fake"), {
            owner: "Captain",
          }),
        () =>
          updateDoc(
            doc(db, "games_rogueduel/mountandblade_online/players/Captain"),
            { gold: 99999 },
          ),
      ]) {
        await assertFails(action());
        checks++;
      }
    await assertFails(getDoc(doc(alice, "mountOnlineRealm/state")));
    checks++;
    await assertSucceeds(
      getDoc(
        doc(alice, "games_rogueduel/mountandblade_online/players/Captain"),
      ),
    );
    checks++;
    await assertSucceeds(
      setDoc(doc(alice, "games_rogueduel/unrelated_regression/players/alice"), {
        gold: 1,
      }),
    );
    checks++;
    await assertSucceeds(
      updateDoc(doc(alice, "users/Captain"), { theme: "medieval" }),
    );
    checks++;
    assert.equal(
      (await getDoc(doc(alice, "users/Captain"))).data().theme,
      "medieval",
    );
    console.log(
      JSON.stringify({
        suite: "Mountain Blade Firestore security",
        checks,
        passed: true,
      }),
    );
  } finally {
    await env.cleanup();
  }
}
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
