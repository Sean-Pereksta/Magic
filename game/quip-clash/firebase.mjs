import { initializeApp, getApps } from 'https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js';
import { getAuth, signInAnonymously } from 'https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js';
import * as firestore from 'https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js';
import { firebaseConfig } from '../../lobby/firebase-config.mjs';

export const sdk = firestore;
const app = getApps().find(app => app.name === '[DEFAULT]') || initializeApp(firebaseConfig);
export const db = firestore.getFirestore(app);
export const auth = getAuth(app);
export async function loadIdentity() {
  await auth.authStateReady();
  const user = auth.currentUser || (await signInAnonymously(auth)).user;
  let username = '';
  try { username = localStorage.getItem('gh.username')?.trim() || ''; } catch { /* The hub can still pass a username when browser storage is disabled. */ }
  const requested = new URLSearchParams(location.search).get('username')?.trim();
  if (!username && requested) username = requested;
  if (!username || username.includes('/') || username.length > 32) return null;
  const snapshot = await firestore.getDocFromServer(firestore.doc(db, 'users', username));
  if (!snapshot.exists()) return null;
  const profile = snapshot.data();
  // The hub binds this Firebase session after its normal account login.
  if (profile.chatAuthUid !== user.uid) return null;
  return { uid: user.uid, username, displayName: String(profile.displayName || username).slice(0, 80) };
}
