export function firebaseError(error) {
  const code = String(error?.code || '').replace(/^firestore\//, '');
  let message = error?.message || 'Could not connect to Firestore. Check your connection and retry account sync.';
  if (code === 'permission-denied') message = 'Firestore denied this TurnCraft operation. Reopen the same Game Hub profile and lobby seat, verify the deployed rules match the current firestore.rules, then retry account sync.';
  else if (code === 'auth/operation-not-allowed' || code === 'auth/admin-restricted-operation') message = 'Firebase Anonymous sign-in is disabled. Enable it in Firebase Authentication, then retry account sync.';
  else if (code === 'unavailable' || code === 'auth/network-request-failed') message = 'Firestore is unavailable. Check your connection, then retry account sync. Pending progress is kept on this device.';
  else if (code === 'failed-precondition' && /index/i.test(message)) message = 'Firestore needs an index for this list. Open the index link in the error details, create it, then retry. ' + message;
  return Object.assign(new Error(message), {code: error?.code || '', cause: error});
}

export async function withFirebaseTimeout(operation, timeoutMs = 15000) {
  let timer;
  try {
    return await Promise.race([
      operation,
      new Promise((_, reject) => { timer = setTimeout(() => reject(Object.assign(new Error('Firebase did not respond. Check your connection, then retry account sync. Your saved data has not been replaced.'), {code: 'deadline-exceeded'})), timeoutMs); })
    ]);
  } finally { clearTimeout(timer); }
}

const restoringAuth = new WeakMap();
// The game and profile menu share one restore/sign-in even when they start together.
export function restoreFirebaseUser(auth, authApi, timeoutMs = 15000) {
  if (restoringAuth.has(auth)) return restoringAuth.get(auth);
  const pending = restoreUser(auth, authApi, timeoutMs).finally(() => {if (restoringAuth.get(auth) === pending) restoringAuth.delete(auth);});
  restoringAuth.set(auth, pending);
  return pending;
}
// An empty currentUser during startup does not mean persisted credentials are absent.
async function restoreUser(auth, {onAuthStateChanged, signInAnonymously}, timeoutMs) {
  let stopAuth;
  try {
    if (typeof auth.authStateReady === 'function') await withFirebaseTimeout(auth.authStateReady(), timeoutMs);
    else await withFirebaseTimeout(new Promise((resolve, reject) => {
      let stop, settled = false;
      const complete = error => { if (settled) return; settled = true; stop?.(); error ? reject(error) : resolve(); };
      stop = onAuthStateChanged(auth, () => complete(), complete); stopAuth = stop;
      if (settled) stop?.();
    }), timeoutMs);
    if (!auth.currentUser) await withFirebaseTimeout(signInAnonymously(auth), timeoutMs);
    if (!auth.currentUser) throw new Error('Firebase sign-in did not return a player. Retry account sync.');
    return auth.currentUser;
  } catch (error) { throw firebaseError(error); }
  finally { stopAuth?.(); }
}
