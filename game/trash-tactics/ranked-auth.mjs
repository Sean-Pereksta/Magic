export const RANKED_ORIGIN = 'https://trash-tactics.catnmice.com';

export async function getRankedToken(serverURL) {
  if (new URL(serverURL).origin !== RANKED_ORIGIN) throw new Error('Ranked sign-in is available on the official Catnmice server.');

  const [{getApps, initializeApp}, {getAuth}, {firebaseConfig}] = await Promise.all([
    import('https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js'),
    import('https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js'),
    import('../../lobby/firebase-config.mjs')
  ]);
  const app = getApps().find(app => app.name === '[DEFAULT]') || initializeApp(firebaseConfig);
  if (app.options.projectId !== firebaseConfig.projectId) throw new Error('Open Trash Tactics from the Catnmice Game Hub to play ranked.');

  const auth = getAuth(app);
  await auth.authStateReady();
  if (!auth.currentUser) throw new Error('Open the Catnmice Game Hub and sign in before joining ranked play.');
  return auth.currentUser.getIdToken();
}
