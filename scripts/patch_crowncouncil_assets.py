from pathlib import Path
import re

p=Path('game/crowncouncil.html')
s=p.read_text(encoding='utf-8')
old=s

def one(a,b):
    global s
    if s.count(a)!=1: raise RuntimeError('anchor mismatch: '+a[:80])
    s=s.replace(a,b,1)

one('  .unit .troop-art svg{ position:relative; z-index:2; }\n','''  .unit .troop-art svg{ position:relative; z-index:2; }
  .unit .troop-art .troop-image{position:relative;z-index:2;display:block;width:100%;height:100%;object-fit:contain;object-position:center;pointer-events:none;user-select:none;-webkit-user-drag:none}
  .unit.big .troop-art .troop-image,.unit.big.troop-golem .troop-art .troop-image{position:absolute;inset:0;width:100%;height:100%;object-fit:fill}
''')
one('''  .card .icon{
    height:48px;
    border-radius:10px;
    display:grid;
    place-items:center;
    font-size:26px;
    background:linear-gradient(180deg, rgba(148,163,184,.16), rgba(2,6,23,.5));
    border:1px solid rgba(148,163,184,.35);
  }
''','''  .card .icon{
    height:48px;
    border-radius:10px;
    display:grid;
    place-items:center;
    font-size:26px;
    background:linear-gradient(180deg, rgba(148,163,184,.16), rgba(2,6,23,.5));
    border:1px solid rgba(148,163,184,.35);
  }
  .card .icon.troopIcon{position:relative;overflow:hidden;padding:2px}
  .card .icon .troop-card-image{display:block;width:100%;height:100%;object-fit:contain;pointer-events:none;user-select:none;-webkit-user-drag:none}
  .card .icon.factionIcon{grid-template-columns:repeat(3,minmax(0,1fr));grid-auto-rows:minmax(0,1fr);gap:1px;padding:2px;overflow:hidden}
  .card .icon .faction-troop-image{display:block;width:100%;height:100%;min-width:0;min-height:0;object-fit:contain;pointer-events:none;user-select:none;-webkit-user-drag:none}
  .card.hasTroopStars{padding-bottom:24px}
  .card .troopStars{position:absolute;left:50%;bottom:5px;transform:translateX(-50%);z-index:3;color:#fde047;font-size:.58rem;line-height:1;letter-spacing:1px;white-space:nowrap;text-shadow:0 1px 2px #000,0 0 5px rgba(245,158,11,.8);pointer-events:none}
''')
one('  .endSummary{\n','''  .assetGate{position:fixed;inset:0;z-index:1000;display:grid;place-items:center;padding:20px;background:radial-gradient(circle at top,rgba(15,23,42,.97),rgba(2,6,23,.995));color:#e5e7eb}
  .assetGatePanel{width:min(460px,94vw);display:grid;gap:10px;padding:18px;border:1px solid rgba(96,165,250,.7);border-radius:16px;background:linear-gradient(180deg,rgba(15,23,42,.98),rgba(2,6,23,.98));box-shadow:0 24px 70px rgba(0,0,0,.65);text-align:center}
  .assetGate.error .assetGatePanel{border-color:rgba(248,113,113,.9)}
  .assetGateTitle{font-size:1.05rem;font-weight:900;color:#dbeafe;letter-spacing:.04em;text-transform:uppercase}.assetGateStatus{font-size:.88rem;color:#cbd5e1;line-height:1.35}.assetGatePlayers{font-size:.78rem;color:#93c5fd;line-height:1.45}

  .endSummary{
''')
one('<body>\n  <div class="topbar">\n','''<body>
  <div id="assetGate" class="assetGate"><div class="assetGatePanel" role="status" aria-live="polite"><div class="assetGateTitle">Loading troop graphics</div><div id="assetGateStatus" class="assetGateStatus">Preparing the Faction Forge image set…</div><div id="assetGatePlayers" class="assetGatePlayers">Waiting for both players to finish loading.</div></div></div>
  <div class="topbar">
''')
one('''};
const LEGENDARY_ABILITIES = {
''','''};
const TROOP_IMAGE_VERSION="factionforge-2026-07-17-v1";
const TROOP_IMAGE_PATHS=Object.freeze({
 rifleman:"../graphics/factionforge/rifleman.png",vrocket:"../graphics/factionforge/rocket.png",vguard:"../graphics/factionforge/vanir guard.png",miner:"../graphics/factionforge/miner.png",medic:"../graphics/factionforge/field medic.png",suppressor:"../graphics/factionforge/suppressor.png",shieldcap:"../graphics/factionforge/shield captain.png",marksman:"../graphics/factionforge/marksman.png",emp:"../graphics/factionforge/emp grenadier.png",
 phaser:"../graphics/factionforge/phasers.png",blade:"../graphics/factionforge/blademaster.png",storm:"../graphics/factionforge/storm seer.png",guardian:"../graphics/factionforge/guardian.png",wardmaker:"../graphics/factionforge/wardmaker.png",architect:"../graphics/factionforge/aegis architect.png",golem:"../graphics/factionforge/sentinel golem.png",runebinder:"../graphics/factionforge/rune binder.png",
 hellspawn:"../graphics/factionforge/hell spawn.png",roach:"../graphics/factionforge/roach.png",defiler:"../graphics/factionforge/defiler.png",blood:"../graphics/factionforge/bloodburst.png",spitter:"../graphics/factionforge/spitter brood.png",matron:"../graphics/factionforge/brood matron.png",viper:"../graphics/factionforge/viper.png",carrion:"../graphics/factionforge/carrion priest.png",burrower:"../graphics/factionforge/burrower.png"
});
const LEGENDARY_ABILITIES = {
''')
one('''function unitEmoji(type){ return TROOPS[type]?.emoji || "❓"; }
function troopVisual(type){
''','''function unitEmoji(type){ return TROOPS[type]?.emoji || "❓"; }
function troopImagePath(type){return TROOP_IMAGE_PATHS[type]||""}
function troopImageTag(type,cls){const src=troopImagePath(type),name=TROOPS[type]?.name||type||"Troop";return src?'<img class="'+escapeHtml(cls||"troop-card-image")+'" src="'+escapeHtml(src)+'" alt="'+escapeHtml(name)+'" draggable="false" decoding="async">':escapeHtml(unitEmoji(type))}
function troopVisual(type){
''')
pat=re.compile(r'function troopArt\(type, u\)\{.*?\n\}\nfunction unitActionClass',re.S)
s,n=pat.subn('''function troopArt(type,u){const hpPct=Math.max(0,Math.min(1,(u.hp||1)/Math.max(1,u.maxhp||1))),hpGlow=hpPct<.35?"drop-shadow(0 0 6px rgba(248,113,113,.85))":"drop-shadow(0 0 0 transparent)",src=troopImagePath(type),name=TROOPS[type]?.name||type||"Troop",image=src?'<img class="troop-image" src="'+escapeHtml(src)+'" alt="'+escapeHtml(name)+'" draggable="false" decoding="async">':'<span>'+escapeHtml(unitEmoji(type))+'</span>';return '<div class="troop-art" style="--hp-filter:'+hpGlow+'">'+image+'</div>'}
function unitActionClass''',s,1)
if n!=1: raise RuntimeError('troopArt mismatch')
one('''    phase: players.length < 2 ? "waiting" : "faction-select",
    phaseEndsAt: players.length < 2 ? 0 : now + 20000,
''','''    phase: players.length < 2 ? "waiting" : "asset-loading",
    phaseEndsAt:0,
    assetReady:{p0:null,p1:null},
''')
one('''      const stalePlayers = !!(existing && !samePlayers(existing.players, players) && !["waiting","faction-select"].includes(existing.phase));
''','''      const stalePlayers = !!(existing && !samePlayers(existing.players, players) && !["waiting","asset-loading","faction-select"].includes(existing.phase));
''')
one('''let S=null, lobbyPlayers=[], meIndex=0, isHost=false;
let lastRenderedBattleTickId = -1;
''','''let S=null,lobbyPlayers=[],meIndex=0,isHost=false;
let localTroopImagesReady=false,troopImageLoadError=null,troopImageLoadPass=0,troopImageLoadPromise=null,assetReadyPublishedForSession=null;
function assetReadyEntryValid(e,n,k){return !!e&&e.username===n&&e.sessionKey===k&&e.version===TROOP_IMAGE_VERSION}
function bothAssetsReady(st){if(!st||!Array.isArray(st.players)||st.players.length<2||!st.sessionKey)return false;const r=st.assetReady||{};return assetReadyEntryValid(r.p0,st.players[0],st.sessionKey)&&assetReadyEntryValid(r.p1,st.players[1],st.sessionKey)}
function updateAssetGate(){const g=$("assetGate"),st=$("assetGateStatus"),pl=$("assetGatePlayers");if(!g||!st||!pl)return;const ph=S&&S.phase,show=!!troopImageLoadError||!localTroopImagesReady||!S||ph==="waiting"||ph==="asset-loading";g.style.display=show?"grid":"none";g.classList.toggle("error",!!troopImageLoadError);st.textContent=troopImageLoadError?"Troop graphics failed: "+troopImageLoadError.message:!localTroopImagesReady?(troopImageLoadPass?"Verifying all troop graphics — pass "+troopImageLoadPass+" of 2…":"Preparing the Faction Forge image set…"):ph==="waiting"?"Your graphics are ready. Waiting for the second player…":ph==="asset-loading"?"Your graphics are ready. Waiting for both players to confirm…":"All troop graphics are ready.";const names=(S&&Array.isArray(S.players)?S.players:lobbyPlayers).slice(0,2),r=S&&S.assetReady||{},k=S&&S.sessionKey;pl.textContent=[0,1].map(i=>{const n=names[i]||(i?"Player 2":"Player 1");return n+": "+(S&&assetReadyEntryValid(r[pKey(i)],n,k)?"ready":"loading")}).join("  •  ")}
function loadTroopImage(src){return new Promise((ok,bad)=>{const im=new Image();let done=false;const end=e=>{if(done)return;done=true;e?bad(e):ok(src)};im.onload=async()=>{try{if(im.decode)await im.decode()}catch(_){ }im.naturalWidth&&im.naturalHeight?end():end(new Error("Invalid image: "+src))};im.onerror=()=>end(new Error("Missing image: "+src));im.decoding="async";im.src=src;if(im.complete&&im.naturalWidth)queueMicrotask(()=>im.onload())})}
async function preloadTroopImagePass(pass){troopImageLoadPass=pass;updateAssetGate();const es=Object.entries(TROOP_IMAGE_PATHS),rs=await Promise.allSettled(es.map(([,src])=>loadTroopImage(src))),bad=[];rs.forEach((r,i)=>{if(r.status!=="fulfilled")bad.push(es[i][1])});if(bad.length)throw new Error(bad.join(", "))}
async function preloadTroopImagesTwice(){await preloadTroopImagePass(1);await preloadTroopImagePass(2);localTroopImagesReady=true;updateAssetGate();await publishLocalAssetReady()}
async function publishLocalAssetReady(){if(!localTroopImagesReady||!S||!S.sessionKey||disposed)return;const ps=Array.isArray(S.players)?S.players.slice(0,2):[],i=ps.indexOf(username);if(i<0)return;const side=pKey(i),cur=S.assetReady&&S.assetReady[side];if(assetReadyEntryValid(cur,username,S.sessionKey)){assetReadyPublishedForSession=S.sessionKey;return}if(assetReadyPublishedForSession===S.sessionKey)return;try{await runTransaction(db,async tx=>{const snap=await tx.get(stateRef);if(!snap.exists())return;const d=snap.data(),players=Array.isArray(d.players)?d.players.slice(0,2):[],ix=players.indexOf(username);if(ix<0||!d.sessionKey)return;const key=pKey(ix),ready=Object.assign({p0:null,p1:null},d.assetReady||{});if(assetReadyEntryValid(ready[key],username,d.sessionKey))return;ready[key]={username,version:TROOP_IMAGE_VERSION,sessionKey:d.sessionKey,readyAt:Date.now()};tx.update(stateRef,{assetReady:ready})});assetReadyPublishedForSession=S.sessionKey}catch(e){console.error("asset readiness publish failed",e);assetReadyPublishedForSession=null}}
function startTroopImagePreload(){if(troopImageLoadPromise)return troopImageLoadPromise;troopImageLoadPromise=preloadTroopImagesTwice().catch(e=>{troopImageLoadError=e instanceof Error?e:new Error(String(e));updateAssetGate();throw troopImageLoadError});return troopImageLoadPromise}
let lastRenderedBattleTickId = -1;
''')
one('''    if ((st.phase==="waiting" || st.phase==="faction-select") && !st.eloCommitted){
      next.mmr=await fetchPlayerMMR(want);
      next.mmrChange=[0,0];
    }
''','''    if (["waiting","asset-loading","faction-select"].includes(st.phase) && !st.eloCommitted){next.mmr=await fetchPlayerMMR(want);next.mmrChange=[0,0];next.assetReady={p0:null,p1:null};next.phase=want.length>=2?"asset-loading":"waiting";next.phaseEndsAt=0;assetReadyPublishedForSession=null}
''')
one('''    if (d.phase==="waiting" && lobbyPlayers.length>=2){
      await writeStatePatch({ phase:"faction-select", phaseEndsAt: now+20000 });
      return;
    }

    if (d.phase==="faction-select"){
''','''    if(d.phase==="waiting"&&lobbyPlayers.length>=2){await writeStatePatch({phase:"asset-loading",phaseEndsAt:0,assetReady:{p0:null,p1:null}});return}
    if(d.phase==="asset-loading"){if(bothAssetsReady(d))await writeStatePatch({phase:"faction-select",phaseEndsAt:now+20000});return}

    if (d.phase==="faction-select"){
''')
one('''      const units = (FACTIONS[f]||[]).map(k=>unitEmoji(k)+" "+TROOPS[k].name).join(" · ");
      el.innerHTML=''+
        '<div class="meta">FACTION</div><div class="icon">🏰</div>'+ 
        '<div class="title">'+f+'</div>'+ 
        '<div class="desc">'+units+'</div>';
''','''      const units=(FACTIONS[f]||[]).map(k=>TROOPS[k].name).join(" · "),factionImages=(FACTIONS[f]||[]).map(k=>troopImageTag(k,"faction-troop-image")).join("");
      el.innerHTML='<div class="meta">FACTION</div><div class="icon factionIcon">'+factionImages+'</div><div class="title">'+f+'</div><div class="desc">'+units+'</div>';
''')
one('''      const draftFit = enemyRoster.length ? evaluateDraftFit(meta, enemyRoster) : "";
      const legendary = (((S&&S.roster&&S.roster[sk])||[]).find(r=>r.type===c.key)?.lvl===2 && LEGENDARY_ABILITIES[c.key]) ? LEGENDARY_ABILITIES[c.key] : null;
      const effectLine = meta.roleLabel || (tro.role==="back" ? "Backline damage" : "Frontline fighter");
''','''      const draftFit=enemyRoster.length?evaluateDraftFit(meta,enemyRoster):"",ownedTroop=((S&&S.roster&&S.roster[sk])||[]).find(r=>r.type===c.key),cardLevel=Math.max(1,Math.min(3,ownedTroop?(ownedTroop.lvl||1):1)),legendary=cardLevel===2&&LEGENDARY_ABILITIES[c.key]?LEGENDARY_ABILITIES[c.key]:null,troopStars=cardLevel>=2?'<div class="troopStars" aria-label="'+cardLevel+' star troop">'+"★".repeat(cardLevel)+'</div>':"";
      if(cardLevel>=2)el.classList.add("hasTroopStars");
      const effectLine = meta.roleLabel || (tro.role==="back" ? "Backline damage" : "Frontline fighter");
''')
one('''        '<div class="typeTag"><span class="typeIcon">🛡️</span>TROOP</div>'+ 
        '<div class="icon">'+escapeHtml(c.emoji)+'</div>'+ 
        '<div class="title">'+escapeHtml(c.name)+'</div>'+ 
        '<div class="desc short">'+escapeHtml(effectLine)+'</div>'+ 
        '<div class="counterRow"><span class="chip chip-role">'+escapeHtml(meta.roleLabel)+'</span></div>';
''','''        '<div class="typeTag"><span class="typeIcon">🛡️</span>TROOP</div><div class="icon troopIcon">'+troopImageTag(c.key,"troop-card-image")+'</div><div class="title">'+escapeHtml(c.name)+'</div><div class="desc short">'+escapeHtml(effectLine)+'</div><div class="counterRow"><span class="chip chip-role">'+escapeHtml(meta.roleLabel)+'</span></div>'+troopStars;
''')
one('''    fitBoard();
    render();
''','''    fitBoard();publishLocalAssetReady();updateAssetGate();render();
''')
one('''  meIndex = Math.max(0, Math.min(1, idx));
  if (hasHostLease()) processHostPhase();
  render();
''','''  meIndex=Math.max(0,Math.min(1,idx));publishLocalAssetReady();updateAssetGate();if(hasHostLease())processHostPhase();render();
''')
one('''  console.log("Signed in as anon uid:", user.uid);
  ensureVisualLoop();
  startListeners();
''','''  console.log("Signed in as anon uid:",user.uid);startTroopImagePreload().catch(()=>{});ensureVisualLoop();startListeners();
''')

if s==old: raise RuntimeError('no changes')
keys=set(re.findall(r'^\s*([a-z][a-z0-9]*):"\.\./graphics/factionforge/',re.search(r'const TROOP_IMAGE_PATHS=Object\.freeze\(\{(.*?)\}\);',s,re.S).group(1),re.M))
if len(keys)!=26: raise RuntimeError('expected 26 image mappings, got '+str(len(keys)))
if 'const ART = {' in s: raise RuntimeError('legacy SVG art remains')
p.write_text(s,encoding='utf-8')
print('patched',p)
