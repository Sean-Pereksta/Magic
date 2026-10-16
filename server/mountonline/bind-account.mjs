import { pathToFileURL } from "node:url";
/** Operator-only migration binding. Never expose this as a browser endpoint.
 * Existing publicly readable lobby passwords are not proof of account ownership. */
export async function bindAccount(db, auth, { uid, username, commit = false }) {
  if (
    !/^[A-Za-z0-9_-]{1,128}$/.test(uid || "") ||
    !/^[A-Za-z0-9 _.-]{1,40}$/.test(username || "")
  )
    throw new Error("Supply a valid Firebase UID and exact legacy username.");
  await auth.getUser(uid);
  return db.runTransaction(async (tx) => {
    const nameRef = db.doc(`mountOnlineNames/${username}`),
      accountRef = db.doc(`mountOnlineAccounts/${uid}`),
      [name, account, user] = await Promise.all([
        tx.get(nameRef),
        tx.get(accountRef),
        tx.get(db.doc(`users/${username}`)),
      ]);
    if (!user.exists)
      throw new Error("The legacy CatnMice profile does not exist.");
    if (name.exists && name.data().uid !== uid)
      throw new Error("This username is already bound to another identity.");
    if (account.exists && account.data().username !== username)
      throw new Error(
        "This identity is already bound to a different username.",
      );
    if (commit) {
      tx.set(nameRef, { uid });
      tx.set(accountRef, {
        username,
        verified: true,
        source: "operator-verified",
        boundAt: Date.now(),
      });
    }
    return { username, uid, committed: commit };
  });
}
if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const args = process.argv.slice(2),
    value = (flag) =>
      args.indexOf(flag) >= 0 ? args[args.indexOf(flag) + 1] : undefined;
  try {
    const { default: admin } = await import("firebase-admin");
    const serviceAccount = process.env.FIREBASE_SERVICE_ACCOUNT
      ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
      : null;
    if (!serviceAccount || serviceAccount.project_id !== "bible-game-246c0")
      throw new Error(
        "Use the existing CatnMice project service account on the server.",
      );
    const app = admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
    try {
      console.log(
        JSON.stringify(
          await bindAccount(admin.firestore(), admin.auth(), {
            uid: value("--uid"),
            username: value("--username"),
            commit: args.includes("--commit"),
          }),
        ),
      );
    } finally {
      await app.delete();
    }
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
