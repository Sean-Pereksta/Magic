export async function connectFirebase() {
  const [{ initializeApp, getApps }, authSDK, sdk, { firebaseConfig }] =
    await Promise.all([
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js"),
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js"),
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js"),
      import("../../lobby/firebase-config.mjs"),
    ]);
  const app = getApps()[0] || initializeApp(firebaseConfig),
    auth = authSDK.getAuth(app);
  await new Promise((resolve, reject) => {
    const stop = authSDK.onAuthStateChanged(
      auth,
      () => {
        stop();
        resolve();
      },
      reject,
    );
  });
  if (!auth.currentUser) await authSDK.signInAnonymously(auth);
  return { sdk, db: sdk.getFirestore(app), user: auth.currentUser };
}
