import { connectRoom } from './session.mjs';

/** Loaded only after entering an online room. Local play stays fully offline. */
export async function openOnlineRoom(options) {
  const [appSDK, authSDK, sdk] = await Promise.all([
    import('https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js'),
    import('https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js'),
    import('https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js')
  ]);
  // Public web configuration; permissions continue to come from Firebase Auth/rules.
  const config = {
    apiKey: 'AIzaSyB7twY7z31ucB6pGA8JC_HrVMZhA8lNaJA',
    authDomain: 'bible-game-246c0.firebaseapp.com', projectId: 'bible-game-246c0',
    storageBucket: 'bible-game-246c0.appspot.com', messagingSenderId: '959619818996',
    appId: '1:959619818996:web:5a9fbf492e23c765e445a1'
  };
  const app = appSDK.getApps().find(app => app.name === '[DEFAULT]') || appSDK.initializeApp(config);
  const auth = authSDK.getAuth(app);
  await auth.authStateReady();
  const user = auth.currentUser || (await authSDK.signInAnonymously(auth)).user;
  const db = sdk.getFirestore(app);
  const profile = await sdk.getDocFromServer(sdk.doc(db, 'users', options.username));
  if (!profile.exists() || profile.data().chatAuthUid !== user.uid) throw new Error('Open Spell Tower from your signed-in arcade lobby so your player name can be verified.');
  return connectRoom({ ...options, sdk, db });
}
