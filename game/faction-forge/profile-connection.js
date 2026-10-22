/** Give saved actions a bounded, shared wait while Auth and Firestore restore. */
export function createProfileConnectionWaiter({connect,timeoutMs=15000,onTimeout=()=>{}}){
  let pending=null;
  return function ready(){
    if(pending)return pending;
    let timer;
    const attempt=Promise.race([
      Promise.resolve().then(connect),
      new Promise((_,reject)=>{timer=setTimeout(()=>{
        const error=Object.assign(new Error('Firestore did not respond. Check your connection, then choose Retry profile connection. Your saved data has not been replaced.'),{code:'deadline-exceeded'});
        onTimeout(error);reject(error);
      },timeoutMs);})
    ]).finally(()=>{clearTimeout(timer);if(pending===attempt)pending=null;});
    pending=attempt;
    return attempt;
  };
}

export function profileConnectionError(error){
  const code=String(error?.code||'');
  let message=error?.message||'Could not connect your Firestore profile. Check your connection, then choose Retry profile connection.';
  if(code==='permission-denied'||code==='firestore/permission-denied')message='Firestore denied access to your Faction Forge save. Publish the updated Firestore rules, then choose Retry profile connection.';
  else if(code==='auth/operation-not-allowed'||code==='auth/admin-restricted-operation')message='Firebase sign-in is disabled. Enable Anonymous sign-in in Firebase Authentication, then choose Retry profile connection.';
  else if(['unavailable','auth/network-request-failed'].includes(code))message='Could not reach Firebase. Check your connection, then choose Retry profile connection. Your saved data has not been replaced.';
  return Object.assign(new Error(message),{code,cause:error});
}
