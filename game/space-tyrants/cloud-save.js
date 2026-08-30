/* Firebase-backed official cloud saves for Space Tyrants.
   Reuses the same Firebase project, anonymous auth flow, and Firestore `lobbies`
   rules surface already used by the repository lobby. Save payloads are encrypted
   client-side, so the password is never written to Firebase. */

const STX_CLOUD_FIREBASE_CONFIG={
  apiKey:"AIzaSyB7twY7z31ucB6pGA8JC_HrVMZhA8lNaJA",
  authDomain:"bible-game-246c0.firebaseapp.com",
  projectId:"bible-game-246c0",
  storageBucket:"bible-game-246c0.appspot.com",
  messagingSenderId:"959619818996",
  appId:"1:959619818996:web:5a9fbf492e23c765e445a1"
};
const STX_CLOUD_APP_NAME="space-tyrants-cloud";
const STX_CLOUD_COLLECTION="lobbies";
const STX_CLOUD_GAME_TYPE="space-tyrants-cloud-save";
const STX_CLOUD_BINDING_KEY="spaceTyrants.cloudSave.name.v1";
const STX_CLOUD_FORMAT=1;
const STX_CLOUD_KDF_ITERATIONS=210000;
const STX_CLOUD_ENCODER=new TextEncoder();
const STX_CLOUD_DECODER=new TextDecoder();
let stxCloudFirebaseCache=null;
let stxCloudSecret="";
let stxCloudBoundName=localStorage.getItem(STX_CLOUD_BINDING_KEY)||"";
let stxCloudBusy=false;
let stxCloudMode="save";
let stxCloudLastSavedAt=0;

function stxCloudCleanName(value){return String(value||"").trim().replace(/\s+/g," ").slice(0,48)}
function stxCloudNormalizeName(value){return stxCloudCleanName(value).toLowerCase()}
function stxCloudHex(bytes){return Array.from(bytes,b=>b.toString(16).padStart(2,"0")).join("")}
async function stxCloudDocumentId(name){
  const digest=await crypto.subtle.digest("SHA-256",STX_CLOUD_ENCODER.encode(`space-tyrants:${stxCloudNormalizeName(name)}`));
  return `space-tyrants-save--${stxCloudHex(new Uint8Array(digest))}`;
}
function stxCloudRandomBytes(length){const bytes=new Uint8Array(length);crypto.getRandomValues(bytes);return bytes}
async function stxCloudKey(password,salt,iterations=STX_CLOUD_KDF_ITERATIONS){
  const base=await crypto.subtle.importKey("raw",STX_CLOUD_ENCODER.encode(password),"PBKDF2",false,["deriveKey"]);
  return crypto.subtle.deriveKey({name:"PBKDF2",salt,iterations,hash:"SHA-256"},base,{name:"AES-GCM",length:256},false,["encrypt","decrypt"]);
}
async function stxCloudPack(raw){
  const plain=STX_CLOUD_ENCODER.encode(raw);
  if(typeof CompressionStream!=="function")return{bytes:plain,compression:"none"};
  try{
    const stream=new Blob([plain]).stream().pipeThrough(new CompressionStream("gzip"));
    const zipped=new Uint8Array(await new Response(stream).arrayBuffer());
    return zipped.length+32<plain.length?{bytes:zipped,compression:"gzip"}:{bytes:plain,compression:"none"};
  }catch{return{bytes:plain,compression:"none"}}
}
async function stxCloudUnpack(bytes,compression){
  if(compression!=="gzip")return STX_CLOUD_DECODER.decode(bytes);
  if(typeof DecompressionStream!=="function")throw new Error("This browser cannot decompress this cloud save.");
  const stream=new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
  return STX_CLOUD_DECODER.decode(await new Response(stream).arrayBuffer());
}
async function stxCloudEncrypt(raw,password){
  const packed=await stxCloudPack(raw),salt=stxCloudRandomBytes(16),iv=stxCloudRandomBytes(12),key=await stxCloudKey(password,salt);
  const encrypted=new Uint8Array(await crypto.subtle.encrypt({name:"AES-GCM",iv},key,packed.bytes));
  return{salt,iv,payload:encrypted,compression:packed.compression,iterations:STX_CLOUD_KDF_ITERATIONS};
}
function stxCloudBytes(value){
  if(value instanceof Uint8Array)return value;
  if(value?.toUint8Array)return value.toUint8Array();
  if(Array.isArray(value))return new Uint8Array(value);
  throw new Error("Cloud save data is malformed.");
}
async function stxCloudDecrypt(record,password){
  const salt=stxCloudBytes(record.salt),iv=stxCloudBytes(record.iv),payload=stxCloudBytes(record.payload),iterations=Number(record.iterations)||STX_CLOUD_KDF_ITERATIONS;
  const key=await stxCloudKey(password,salt,iterations);
  const plain=new Uint8Array(await crypto.subtle.decrypt({name:"AES-GCM",iv},key,payload));
  return stxCloudUnpack(plain,record.compression||"none");
}
async function stxCloudFirebase(){
  if(stxCloudFirebaseCache)return stxCloudFirebaseCache;
  stxCloudFirebaseCache=(async()=>{
    const [appMod,fireMod,authMod]=await Promise.all([
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js"),
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js"),
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js")
    ]);
    let app=appMod.getApps().find(candidate=>candidate.name===STX_CLOUD_APP_NAME);
    if(!app)app=appMod.initializeApp(STX_CLOUD_FIREBASE_CONFIG,STX_CLOUD_APP_NAME);
    const auth=authMod.getAuth(app);
    if(!auth.currentUser)await authMod.signInAnonymously(auth);
    return{db:fireMod.getFirestore(app),doc:fireMod.doc,getDoc:fireMod.getDoc,setDoc:fireMod.setDoc,serverTimestamp:fireMod.serverTimestamp,Bytes:fireMod.Bytes,uid:auth.currentUser?.uid||"anonymous"};
  })().catch(err=>{stxCloudFirebaseCache=null;throw err});
  return stxCloudFirebaseCache;
}
function stxCloudValidateCredentials(name,password){
  const clean=stxCloudCleanName(name);
  if(clean.length<2)throw new Error("Use at least 2 characters for the save name.");
  if(String(password||"").length<4)throw new Error("Use at least 4 characters for the save password.");
  return clean;
}
function stxCloudBind(name,password){
  stxCloudBoundName=stxCloudCleanName(name);stxCloudSecret=String(password||"");
  localStorage.setItem(STX_CLOUD_BINDING_KEY,stxCloudBoundName);stxCloudRenderPanel();
}
function stxCloudForgetBinding(){
  stxCloudBoundName="";stxCloudSecret="";localStorage.removeItem(STX_CLOUD_BINDING_KEY);stxCloudRenderPanel();showToast("Cloud save binding cleared")
}
function stxCloudCurrentRaw(){
  saveGame(false);
  const raw=localStorage.getItem(SAVE_KEY);
  if(!raw)throw new Error("There is no running galaxy to save.");
  return raw;
}
async function stxCloudSaveNamed(name,password){
  const clean=stxCloudValidateCredentials(name,password),raw=stxCloudCurrentRaw(),parsed=JSON.parse(raw),firebase=await stxCloudFirebase(),id=await stxCloudDocumentId(clean),ref=firebase.doc(firebase.db,STX_CLOUD_COLLECTION,id),existing=await firebase.getDoc(ref);
  if(existing.exists()){
    const prior=existing.data();
    if(prior.gameType!==STX_CLOUD_GAME_TYPE)throw new Error("That save name is already reserved.");
    try{await stxCloudDecrypt(prior,password)}catch{throw new Error("That save name already exists, but the password does not match.")}
  }
  const encrypted=await stxCloudEncrypt(raw,password),now=Date.now(),prior=existing.exists()?existing.data():null;
  await firebase.setDoc(ref,{
    kind:"spaceTyrantsCloudSave",
    gameType:STX_CLOUD_GAME_TYPE,
    status:"active",
    name:`Space Tyrants Save: ${clean}`,
    saveName:clean,
    host:"space-tyrants",
    players:[],
    format:STX_CLOUD_FORMAT,
    cipher:"AES-GCM-256",
    kdf:"PBKDF2-SHA256",
    iterations:encrypted.iterations,
    salt:firebase.Bytes.fromUint8Array(encrypted.salt),
    iv:firebase.Bytes.fromUint8Array(encrypted.iv),
    payload:firebase.Bytes.fromUint8Array(encrypted.payload),
    compression:encrypted.compression,
    saveVersion:Number(parsed?.version)||null,
    simTime:Number(parsed?.simTime)||0,
    worlds:Array.isArray(parsed?.planets)?parsed.planets.filter(p=>p?.owner===0).length:0,
    createdAtMs:Number(prior?.createdAtMs)||now,
    updatedAtMs:now,
    updatedAt:firebase.serverTimestamp()
  });
  stxCloudBind(clean,password);stxCloudLastSavedAt=now;stxCloudRenderPanel();return clean;
}
async function stxCloudLoadNamed(name,password){
  const clean=stxCloudValidateCredentials(name,password),firebase=await stxCloudFirebase(),id=await stxCloudDocumentId(clean),ref=firebase.doc(firebase.db,STX_CLOUD_COLLECTION,id),snap=await firebase.getDoc(ref);
  if(!snap.exists())throw new Error("No Space Tyrants cloud save exists with that name.");
  const record=snap.data();
  if(record.gameType!==STX_CLOUD_GAME_TYPE)throw new Error("That name does not belong to a Space Tyrants cloud save.");
  let raw;
  try{raw=await stxCloudDecrypt(record,password)}catch{throw new Error("The password is incorrect, or the cloud save is damaged.")}
  let parsed;
  try{parsed=JSON.parse(raw)}catch{throw new Error("The cloud save could not be decoded.")}
  if(!parsed||!Array.isArray(parsed.planets)||!Array.isArray(parsed.empires))throw new Error("The cloud save is missing required galaxy data.");
  localStorage.setItem(SAVE_KEY,raw);stxCloudBind(clean,password);
  document.querySelectorAll(".modal-wrap").forEach(modal=>{if(modal.id!=="startModal"&&modal.id!=="stxCloudModal")modal.hidden=true});
  $("stxCloudModal").hidden=true;startGame(true);stxCloudLastSavedAt=Number(record.updatedAtMs)||Date.now();stxCloudRenderPanel();return clean;
}
function stxCloudStatusText(){
  if(!stxCloudBoundName)return"No cloud slot assigned yet.";
  if(stxCloudSecret)return`Bound to “${stxCloudBoundName}”. Official saves overwrite this Firebase slot.`;
  return`Bound name: “${stxCloudBoundName}”. Enter its password once this session to resume cloud saving.`;
}
function stxCloudRenderPanel(){
  const status=$("stxCloudStatus"),save=$("stxCloudSaveNow"),forget=$("stxCloudForget");
  if(status)status.textContent=stxCloudStatusText();
  if(save)save.textContent=stxCloudBoundName&&stxCloudSecret?"Save to Firebase Now":"Assign Name & Password";
  if(forget)forget.hidden=!stxCloudBoundName;
  const stamp=$("stxCloudStamp");if(stamp)stamp.textContent=stxCloudLastSavedAt?`Last Firebase save: ${new Date(stxCloudLastSavedAt).toLocaleString()}`:"Firebase saves are encrypted before upload.";
}
function stxCloudSetModalMessage(message,bad=false){const el=$("stxCloudMessage");if(!el)return;el.textContent=message||"";el.classList.toggle("bad",bad)}
function stxCloudOpenModal(mode){
  stxCloudMode=mode;const modal=$("stxCloudModal"),name=$("stxCloudName"),password=$("stxCloudPassword"),title=$("stxCloudModalTitle"),copy=$("stxCloudModalCopy"),action=$("stxCloudModalAction");
  if(!modal)return;name.value=stxCloudBoundName||"";password.value="";title.textContent=mode==="load"?"Reopen a Firebase Chronicle":"Assign the Official Save";copy.textContent=mode==="load"?"Enter the save name and password from any device to restore that exact galaxy.":"Choose a save name and password. Future official saves in this session will overwrite the same encrypted Firebase slot.";action.textContent=mode==="load"?"Reopen Chronicle":"Assign & Save";stxCloudSetModalMessage("");modal.hidden=false;setTimeout(()=>{(name.value?password:name).focus()},0)
}
function stxCloudCloseModal(){if(!stxCloudBusy)$("stxCloudModal").hidden=true}
async function stxCloudSubmitModal(){
  if(stxCloudBusy)return;const name=$("stxCloudName").value,password=$("stxCloudPassword").value,action=$("stxCloudModalAction"),old=action.textContent;stxCloudBusy=true;action.disabled=true;action.textContent=stxCloudMode==="load"?"Reopening…":"Saving…";stxCloudSetModalMessage(stxCloudMode==="load"?"Contacting Firebase…":"Encrypting and uploading…");
  try{
    if(stxCloudMode==="load"){
      if(state.running&&!confirm("Reopen this cloud save and replace the galaxy currently in memory?"))return;
      const clean=await stxCloudLoadNamed(name,password);showToast(`Reopened ${clean} from Firebase`);
    }else{
      if(!state.running)throw new Error("Start or reopen a galaxy before saving it.");
      const clean=await stxCloudSaveNamed(name,password);$("stxCloudModal").hidden=true;showToast(`Official save uploaded: ${clean}`);
    }
  }catch(err){console.warn("[Space Tyrants cloud save]",err);stxCloudSetModalMessage(err?.message||"Firebase save failed.",true)}
  finally{stxCloudBusy=false;action.disabled=false;action.textContent=old;stxCloudRenderPanel()}
}
async function stxOfficialSave(){
  if(!state.running)return showToast("Start a galaxy before saving");
  if(!stxCloudBoundName||!stxCloudSecret)return stxCloudOpenModal("save");
  if(stxCloudBusy)return;stxCloudBusy=true;
  try{await stxCloudSaveNamed(stxCloudBoundName,stxCloudSecret);showToast(`Official save uploaded: ${stxCloudBoundName}`)}catch(err){console.warn("[Space Tyrants cloud save]",err);showToast("Firebase save failed — local autosave remains safe");stxCloudOpenModal("save");stxCloudSetModalMessage(err?.message||"Firebase save failed.",true)}finally{stxCloudBusy=false;stxCloudRenderPanel()}
}
function stxInstallCloudSaveUI(){
  if($("stxCloudModal"))return;
  const style=document.createElement("style");style.id="stx-cloud-save-style";style.textContent=`
    .stx-cloud-grid{display:grid;grid-template-columns:minmax(0,1.35fr) minmax(250px,.65fr);gap:12px;padding:8px 2px}.stx-cloud-card{border:1px solid rgba(120,161,255,.18);border-radius:15px;padding:15px;background:linear-gradient(145deg,rgba(58,76,137,.14),rgba(15,24,53,.22))}.stx-cloud-card h3{margin:3px 0 7px;font-size:1rem}.stx-cloud-actions{display:flex;flex-wrap:wrap;gap:8px;margin-top:13px}.stx-cloud-status{color:#c9d8f3;font-size:.72rem;line-height:1.5;padding:9px 10px;border-radius:10px;background:rgba(78,231,255,.07);border:1px solid rgba(78,231,255,.16)}.stx-cloud-stamp{color:#788db3;font-size:.64rem;margin-top:7px}.stx-cloud-card ul{padding-left:18px;margin:8px 0;color:#91a7cd;font-size:.69rem;line-height:1.55}.stx-cloud-modal-card{width:min(560px,94vw);border-radius:22px;padding:22px;background:linear-gradient(155deg,rgba(13,22,48,.99),rgba(5,8,20,.99));border:1px solid rgba(111,156,255,.3);box-shadow:0 32px 100px rgba(0,0,0,.7)}.stx-cloud-fields{display:grid;gap:10px;margin:16px 0}.stx-cloud-field label{display:block;color:#90a8cf;font-size:.62rem;font-weight:900;letter-spacing:.1em;text-transform:uppercase;margin-bottom:5px}.stx-cloud-field input{width:100%;border:1px solid rgba(120,161,255,.25);border-radius:10px;background:rgba(7,12,28,.85);color:#eef5ff;padding:11px 12px;outline:none}.stx-cloud-field input:focus{border-color:rgba(78,231,255,.65);box-shadow:0 0 0 3px rgba(78,231,255,.08)}.stx-cloud-message{min-height:20px;color:#7ddfb0;font-size:.68rem;margin-top:5px}.stx-cloud-message.bad{color:#ff8192}.stx-cloud-modal-actions{display:flex;justify-content:flex-end;gap:8px;margin-top:12px}@media(max-width:760px){.stx-cloud-grid{grid-template-columns:1fr}}
  `;document.head.appendChild(style);
  const tabs=document.querySelector(".hub-tabs"),fleets=$("fleetsSection");
  if(tabs&&fleets){const tab=document.createElement("button");tab.className="hub-tab";tab.dataset.hubTab="save";tab.textContent="Save / Reopen";tab.onclick=()=>{stxCloudRenderPanel();switchHubTab("save")};tabs.appendChild(tab);const section=document.createElement("div");section.className="hub-section";section.id="saveSection";section.hidden=true;section.innerHTML=`<div class="stx-cloud-grid"><article class="stx-cloud-card"><div class="kicker">Official Chronicle Save</div><h3>Firebase Cloud Save</h3><div class="stx-cloud-status" id="stxCloudStatus"></div><div class="stx-cloud-stamp" id="stxCloudStamp"></div><div class="stx-cloud-actions"><button class="primary" id="stxCloudSaveNow">Assign Name & Password</button><button class="secondary" id="stxCloudReopen">Reopen Cloud Save</button><button class="secondary" id="stxCloudLocalOnly">Local Backup</button><button class="secondary" id="stxCloudForget" hidden>Forget Binding</button></div></article><article class="stx-cloud-card"><div class="kicker">Portable Chronicle</div><h3>How reopening works</h3><ul><li>Your save name finds the Firebase record.</li><li>Your password encrypts and decrypts the galaxy locally with AES-GCM.</li><li>The password itself is never uploaded to Firebase.</li><li>After reopening, official saves continue using the same name and password for this session.</li><li>18-second simulation autosaves remain local to avoid excessive cloud writes.</li></ul></article></div>`;fleets.parentNode.insertBefore(section,fleets.nextSibling)}
  const modal=document.createElement("div");modal.className="modal-wrap";modal.id="stxCloudModal";modal.hidden=true;modal.style.zIndex="180";modal.innerHTML=`<section class="stx-cloud-modal-card"><div class="kicker">Space Tyrants // Cloud Chronicle</div><h2 id="stxCloudModalTitle" style="margin:6px 0">Official Save</h2><div class="subtle" id="stxCloudModalCopy"></div><div class="stx-cloud-fields"><div class="stx-cloud-field"><label for="stxCloudName">Save name</label><input id="stxCloudName" maxlength="48" autocomplete="username" placeholder="Example: Aurelian Run"></div><div class="stx-cloud-field"><label for="stxCloudPassword">Save password</label><input id="stxCloudPassword" type="password" autocomplete="current-password" placeholder="Password for this chronicle"></div></div><div class="stx-cloud-message" id="stxCloudMessage"></div><div class="stx-cloud-modal-actions"><button class="secondary" id="stxCloudModalCancel">Cancel</button><button class="primary" id="stxCloudModalAction">Assign & Save</button></div></section>`;document.body.appendChild(modal);
  $("stxCloudModalCancel").onclick=stxCloudCloseModal;$("stxCloudModalAction").onclick=stxCloudSubmitModal;modal.addEventListener("click",e=>{if(e.target===modal)stxCloudCloseModal()});$("stxCloudPassword").addEventListener("keydown",e=>{if(e.key==="Enter")stxCloudSubmitModal()});
  $("stxCloudSaveNow")?.addEventListener("click",()=>{if(stxCloudBoundName&&stxCloudSecret)stxOfficialSave();else stxCloudOpenModal("save")});$("stxCloudReopen")?.addEventListener("click",()=>stxCloudOpenModal("load"));$("stxCloudLocalOnly")?.addEventListener("click",()=>saveGame(true));$("stxCloudForget")?.addEventListener("click",stxCloudForgetBinding);
  const startActions=document.querySelector(".start-actions");if(startActions&&!$("stxCloudStartReopen")){const reopen=document.createElement("button");reopen.className="secondary";reopen.id="stxCloudStartReopen";reopen.textContent="Reopen Cloud Save";reopen.onclick=()=>stxCloudOpenModal("load");startActions.appendChild(reopen)}
  const official=$("saveBtn");if(official){official.title="Official save — local + Firebase";official.textContent="☁";official.onclick=stxOfficialSave}
  stxCloudRenderPanel();
}

globalThis.SpaceTyrantsCloudSaves={
  collection:STX_CLOUD_COLLECTION,
  gameType:STX_CLOUD_GAME_TYPE,
  cleanName:stxCloudCleanName,
  normalizeName:stxCloudNormalizeName,
  documentId:stxCloudDocumentId,
  save:stxCloudSaveNamed,
  reopen:stxCloudLoadNamed,
  officialSave:stxOfficialSave
};
stxInstallCloudSaveUI();