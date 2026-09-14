(function(){
"use strict";
/* Firebase-backed official cloud saves for Receiver Window QB.
   Mirrors Space Tyrants' proven Firebase project/auth/rules surface. The base
   journey and Receiver Window QB expansion save are bundled and encrypted client-side,
   so the password is never written to Firebase. */

const AW_CLOUD_FIREBASE_CONFIG={
  apiKey:"AIzaSyB7twY7z31ucB6pGA8JC_HrVMZhA8lNaJA",
  authDomain:"bible-game-246c0.firebaseapp.com",
  projectId:"bible-game-246c0",
  storageBucket:"bible-game-246c0.appspot.com",
  messagingSenderId:"959619818996",
  appId:"1:959619818996:web:5a9fbf492e23c765e445a1"
};
const AW_CLOUD_APP_NAME="receiver-window-qb-cloud";
const AW_CLOUD_COLLECTION="lobbies";
const AW_CLOUD_GAME_TYPE="receiver-window-qb-cloud-save";
const AW_CLOUD_BINDING_KEY="receiverWindowQB.cloudSave.name.v1";
const AW_CLOUD_FORMAT=1;
const AW_CLOUD_KDF_ITERATIONS=210000;
const AW_CLOUD_ENCODER=new TextEncoder();
const AW_CLOUD_DECODER=new TextDecoder();
let awCloudFirebaseCache=null;
let awCloudSecret="";

let awCloudBusy=false;
let awCloudMode="save";
let awCloudLastSavedAt=0;

function awCloudCleanName(value){return String(value||"").trim().replace(/\s+/g," ").slice(0,48)}
function awCloudNormalizeName(value){return awCloudCleanName(value).toLowerCase()}
function awCloudHex(bytes){return Array.from(bytes,b=>b.toString(16).padStart(2,"0")).join("")}
async function awCloudDocumentId(name){
  const digest=await crypto.subtle.digest("SHA-256",AW_CLOUD_ENCODER.encode(`receiver-window-qb:${awCloudNormalizeName(name)}`));
  return `receiver-window-qb-save--${awCloudHex(new Uint8Array(digest))}`;
}
function awCloudRandomBytes(length){const bytes=new Uint8Array(length);crypto.getRandomValues(bytes);return bytes}
async function awCloudKey(password,salt,iterations=AW_CLOUD_KDF_ITERATIONS){
  const base=await crypto.subtle.importKey("raw",AW_CLOUD_ENCODER.encode(password),"PBKDF2",false,["deriveKey"]);
  return crypto.subtle.deriveKey({name:"PBKDF2",salt,iterations,hash:"SHA-256"},base,{name:"AES-GCM",length:256},false,["encrypt","decrypt"]);
}
async function awCloudPack(raw){
  const plain=AW_CLOUD_ENCODER.encode(raw);
  if(typeof CompressionStream!=="function")return{bytes:plain,compression:"none"};
  try{
    const stream=new Blob([plain]).stream().pipeThrough(new CompressionStream("gzip"));
    const zipped=new Uint8Array(await new Response(stream).arrayBuffer());
    return zipped.length+32<plain.length?{bytes:zipped,compression:"gzip"}:{bytes:plain,compression:"none"};
  }catch{return{bytes:plain,compression:"none"}}
}
async function awCloudUnpack(bytes,compression){
  if(compression!=="gzip")return AW_CLOUD_DECODER.decode(bytes);
  if(typeof DecompressionStream!=="function")throw new Error("This browser cannot decompress this cloud save.");
  const stream=new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
  return AW_CLOUD_DECODER.decode(await new Response(stream).arrayBuffer());
}
async function awCloudEncrypt(raw,password){
  const packed=await awCloudPack(raw),salt=awCloudRandomBytes(16),iv=awCloudRandomBytes(12),key=await awCloudKey(password,salt);
  const encrypted=new Uint8Array(await crypto.subtle.encrypt({name:"AES-GCM",iv},key,packed.bytes));
  return{salt,iv,payload:encrypted,compression:packed.compression,iterations:AW_CLOUD_KDF_ITERATIONS};
}
function awCloudBytes(value){
  if(value instanceof Uint8Array)return value;
  if(value?.toUint8Array)return value.toUint8Array();
  if(Array.isArray(value))return new Uint8Array(value);
  throw new Error("Cloud save data is malformed.");
}
async function awCloudDecrypt(record,password){
  const salt=awCloudBytes(record.salt),iv=awCloudBytes(record.iv),payload=awCloudBytes(record.payload),iterations=Number(record.iterations)||AW_CLOUD_KDF_ITERATIONS;
  if(iterations!==AW_CLOUD_KDF_ITERATIONS||salt.length!==16||iv.length!==12||payload.length>800000)throw new Error("Cloud save data is malformed.");
  const key=await awCloudKey(password,salt,iterations);
  const plain=new Uint8Array(await crypto.subtle.decrypt({name:"AES-GCM",iv},key,payload));
  return awCloudUnpack(plain,record.compression||"none");
}
async function awCloudFirebase(){
  if(awCloudFirebaseCache)return awCloudFirebaseCache;
  awCloudFirebaseCache=(async()=>{
    const [appMod,fireMod,authMod]=await Promise.all([
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js"),
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js"),
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js")
    ]);
    let app=appMod.getApps().find(candidate=>candidate.name===AW_CLOUD_APP_NAME);
    if(!app)app=appMod.initializeApp(AW_CLOUD_FIREBASE_CONFIG,AW_CLOUD_APP_NAME);
    const auth=authMod.getAuth(app);
    if(!auth.currentUser)await authMod.signInAnonymously(auth);
    return{db:fireMod.getFirestore(app),doc:fireMod.doc,getDoc:fireMod.getDoc,setDoc:fireMod.setDoc,serverTimestamp:fireMod.serverTimestamp,Bytes:fireMod.Bytes};
  })().catch(err=>{awCloudFirebaseCache=null;throw err});
  return awCloudFirebaseCache;
}
function awCloudValidateCredentials(name,password){
  const clean=awCloudCleanName(name);
  if(clean.length<2)throw new Error("Use at least 2 characters for the save name.");
  if(String(password||"").length<4)throw new Error("Use at least 4 characters for the save password.");
  return clean;
}

async function save(name,password,franchise){
  const clean=awCloudValidateCredentials(name,password),checked=QBProgression.validateSave(franchise);
  const raw=JSON.stringify({format:1,franchise:checked}),fb=await awCloudFirebase(),id=await awCloudDocumentId(clean),ref=fb.doc(fb.db,AW_CLOUD_COLLECTION,id),existing=await fb.getDoc(ref);
  if(existing.exists()){
    if(existing.data().gameType!==AW_CLOUD_GAME_TYPE)throw new Error('That name is reserved.');
    try{await awCloudDecrypt(existing.data(),password)}catch{throw new Error('That save already exists, but the password does not match.');}
  }
  const encrypted=await awCloudEncrypt(raw,password),now=Date.now();
  await fb.setDoc(ref,{kind:'receiverWindowQBCloudSave',gameType:AW_CLOUD_GAME_TYPE,status:'active',name:`Receiver Window QB Save: ${clean}`,saveName:clean,host:'receiver-window-qb',players:[],format:1,cipher:'AES-GCM-256',kdf:'PBKDF2-SHA256',iterations:encrypted.iterations,salt:fb.Bytes.fromUint8Array(encrypted.salt),iv:fb.Bytes.fromUint8Array(encrypted.iv),payload:fb.Bytes.fromUint8Array(encrypted.payload),compression:encrypted.compression,round:checked.round,createdAtMs:existing.exists()?(existing.data().createdAtMs||now):now,updatedAtMs:now,updatedAt:fb.serverTimestamp()});
  return clean;
}
async function load(name,password){
  const clean=awCloudValidateCredentials(name,password),fb=await awCloudFirebase(),id=await awCloudDocumentId(clean),snap=await fb.getDoc(fb.doc(fb.db,AW_CLOUD_COLLECTION,id));
  if(!snap.exists())throw new Error('No football cloud save exists with that name.');
  const record=snap.data();if(record.gameType!==AW_CLOUD_GAME_TYPE||record.format!==1)throw new Error('This is not a supported football save.');
  let bundle;try{bundle=JSON.parse(await awCloudDecrypt(record,password));}catch{throw new Error('Incorrect password or damaged cloud save.');}
  if(bundle.format!==1)throw new Error('Unsupported save version.');
  return QBProgression.validateSave(bundle.franchise);
}
globalThis.QBCloud={save,load};
})();
