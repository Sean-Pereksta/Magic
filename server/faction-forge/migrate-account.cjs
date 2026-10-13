'use strict';
// Run only by the project operator AFTER independently verifying account ownership.
// A legacy plaintext password or chatAuthUid is not proof: the old rules exposed both.
const D=require('./domain.cjs');
async function migrate({db,auth,username,uid,now=Date.now()}) {
  D.safeUsername(username);D.safeId(uid,'UID');
  const user=await auth.getUser(uid);D.fail(!user.disabled,'INVALID_ACCOUNT','The Firebase account is disabled.');
  return db.runTransaction(async tx=>{
    const nameRef=db.doc(`factionForgeNames/${username}`),accountRef=db.doc(`factionForgeAccounts/${uid}`),profileRef=db.doc(`factionForgeProfiles/${uid}`),legacyRef=db.doc(`users/${username}`);
    const [name,account,profile,legacy]=await Promise.all([tx.get(nameRef),tx.get(accountRef),tx.get(profileRef),tx.get(legacyRef)]);
    D.fail(legacy.exists,'PROFILE_NOT_FOUND','Legacy profile does not exist.');
    D.fail(!name.exists||name.data().uid===uid,'OWNERSHIP_CONFLICT','This username is already bound to another Firebase account.');
    D.fail(!account.exists||account.data().username===username,'OWNERSHIP_CONFLICT','This Firebase identity already owns another username.');
    const data=profile.exists?profile.data():D.newProfile(uid,username,legacy.data().crowncouncilElo,now);
    if(!profile.exists)tx.create(profileRef,data);
    tx.set(nameRef,{uid,username,verified:true,migratedAt:now});tx.set(accountRef,{uid,username,verified:true,migratedAt:now});
    // Do not overwrite the existing rating, password, chat identity, or other games.
    return {uid,username,crowncouncilElo:data.crowncouncilElo,created:!profile.exists};
  });
}
if(require.main===module) {
  const admin=require('firebase-admin');
  const [username,uid,flag]=process.argv.slice(2);
  if(flag!=='--verified-owner')throw new Error('Usage: node migrate-account.cjs USERNAME FIREBASE_UID --verified-owner (only after independently verifying ownership).');
  admin.initializeApp({credential:admin.credential.applicationDefault()});
  migrate({db:admin.firestore(),auth:admin.auth(),username,uid}).then(result=>console.log(JSON.stringify(result))).catch(error=>{console.error(error.message);process.exitCode=1;});
}
module.exports={migrate};
