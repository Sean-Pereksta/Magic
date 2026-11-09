import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {ORDER_CAP,ORDER_INTERVAL,braceActive,canBrace,createGame,unitDef,at,legalActions,applyAction,formationStatus,groundEffectsAt,GROUND_EFFECTS,FACTIONS} from './engine.mjs';
import {createInputController,previewActions} from './input.mjs';
import {actionColor,displayTargets,groundPreviewCells} from './targeting.mjs';
import {abilityIcon} from './icons.mjs';
import {groundLayerHTML,groundDescription,formationOverlayHTML,groundArt} from './board-effects.mjs';

// Exercise the real app drawing, preview, timer and gesture code. Native HTML
// parsing/animation is covered separately by renderer tests and browser checks.
const source=readFileSync(new URL('./app.mjs',import.meta.url),'utf8').replace(/\r\n/g,'\n');
function functionSource(name){
  const start=source.indexOf(`function ${name}(`),end=source.indexOf('\nfunction ',start+1);
  assert.ok(start>=0&&end>start,`app function ${name}`);return source.slice(start,end);
}
function fixture(){
  const game=createGame({opponentMode:'local',playerFaction:'bloom',aiFaction:'chaos',playerArmy:['leader','guardian','ranger','shaper'],aiArmy:['leader','guardian','ranger','shaper'],seed:42});
  game.tiles.forEach(tile=>tile.terrain='grass');game.nextPulseAt=99;
  const caster=game.units.find(u=>u.side==='player'&&u.role==='shaper'),guardian=game.units.find(u=>u.side==='player'&&u.role==='guardian'),ranger=game.units.find(u=>u.side==='player'&&u.role==='ranger'),other=game.units.find(u=>u.side==='ai'&&u.role==='ranger');
  Object.assign(caster,{x:4,y:4});Object.assign(guardian,{x:3,y:4});Object.assign(ranger,{x:3,y:5});Object.assign(other,{x:4,y:1});
  const handlers=new Map(),cells=new Map(),effects=[],clocks=[],calls={patches:0,captures:[]};
  const classes=()=>{const names=new Set();return {add(...values){values.forEach(v=>names.add(v));},remove(...values){values.forEach(v=>names.delete(v));},contains:value=>names.has(value),toggle(name,value){if(value)names.add(name);else names.delete(name);},names};};
  const makeNode=()=>({dataset:{},style:{setProperty(name,value){this[name]=value;}},classList:classes(),attributes:{},isConnected:true,innerHTML:'',textContent:'',remove(){this.isConnected=false;},getAttribute(name){return this.attributes[name]??null;},setAttribute(name,value){this.attributes[name]=String(value);},querySelector(){return null;},querySelectorAll(){return [];},getBoundingClientRect(){return {x:0,y:0,width:50,height:50};}});
  const app=makeNode(),board=makeNode(),clock=makeNode(),ready=makeNode();
  app.setPointerCapture=id=>calls.captures.push(id);app.contains=()=>true;
  for(const tile of game.tiles){const cell=makeNode();cell.dataset.cell=`${tile.x},${tile.y}`;cell.classList.add('tile');const unit=at(game,tile.x,tile.y);if(unit)cell.dataset.unit=unit.id;cell.closest=selector=>selector==='.board'?board:selector.split(',').some(s=>s==='[data-cell]'||s==='.tile[data-cell]'||s==='.tile[data-unit]'&&cell.dataset.unit)?cell:null;cells.set(cell.dataset.cell,cell);}
  const context={game,ORDER_CAP,ORDER_INTERVAL,braceActive,canBrace,FACTIONS,GROUND_EFFECTS,groundEffectsAt,formationStatus,unitDef,at,previewActions,actionColor,displayTargets,groundPreviewCells,groundLayerHTML,groundDescription,formationOverlayHTML,groundArt,abilityIcon,
    roles:['leader','scout','brute','mystic'],tacticalRoles:['guardian','ranger','shaper'],factionKeys:['bloom','chaos','choir'],flagSVG:'<svg></svg>',
    esc:value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),
    scene:'battle',paused:false,countdownRemainingMs:0,inspectResult:false,activeSeat:'player',selectedId:caster.id,mode:'ability',selectedSkillIndex:0,
    seatSelections:{player:caster.id,ai:other.id},seatModes:{player:'ability',ai:'ability'},seatSkills:{player:0,ai:1},
    input:createInputController(),pointers:new Map(),heldPointers:new Set(),consumedPointers:new Map(),previewCellKey:'',lastFieldAriaSecond:-1,
    motionReduced:true,scrollX:0,scrollY:0,renderStats:{calls:0,boardMounts:0,samples:[]},lastReadySignature:'',
    performance:{now:()=>1000},setTimeout:()=>1,hoverCell:null,hit:null,
    patchBattleMarkup(_container,html){calls.patches++;app.innerHTML=html;},persist(){},announce(){},toast(){},finishDeploymentPointer(){},
    document:{activeElement:null,body:{classList:classes(),append(node){effects.push(node);}},hidden:false,
      addEventListener(type,handler){if(!handlers.has(type))handlers.set(type,[]);handlers.get(type).push(handler);},
      createElement:makeNode,elementFromPoint:()=>context.hit,
      querySelector:selector=>context.$(selector),
      querySelectorAll(selector){
        if(selector==='[data-ground-id]')return clocks;
        if(selector==='.tile[data-ground-description]')return [...cells.values()].filter(c=>c.dataset.groundDescription!==undefined);
        if(selector==='.ground-preview')return [...cells.values()].filter(c=>c.classList.contains('ground-preview'));
        if(selector.startsWith('.drag-over')){const pointer=/data-drag-pointer="(\d+)"/.exec(selector)?.[1];return [...cells.values()].filter(c=>c.classList.contains('drag-over')&&(!pointer||String(c.dataset.dragPointer)===pointer));}
        return [];
      }}
  };
  context.$=selector=>selector==='#app'?app:selector==='.board'?board:selector==='#clock'?clock:selector==='#ready-count'?ready:selector==='.tile:hover'?context.hoverCell:selector==='#victory'?null:selector.startsWith('[data-cell=')?cells.get(/"([^"\]]+)"/.exec(selector)?.[1]):selector.startsWith('[data-unit-token=')?makeNode():selector.startsWith('[data-unit=')?cells.get(`${game.units.find(u=>u.id===/"([^"\]]+)"/.exec(selector)?.[1])?.x},${game.units.find(u=>u.id===/"([^"\]]+)"/.exec(selector)?.[1])?.y}`):null;
  vm.createContext(context);
  for(const name of ['sprite','coordinate','alive','localGame','current','commandMode','abilities','skillIndex','setMode','selectUnit','remaining','cooldownLabel','terrainHTML','colorFor','targetActions','sideName','orderMeterHTML','foldhornForecast','battleForecastHTML','commandStatus','tileHTML','combatOutcome','abilityDockHTML','countdownHTML','compactBattleHTML','render','formatTime','readySignature','updateTimers','drawSpecial','clearGroundPreview','previewGroundAt'])vm.runInContext(functionSource(name),context);
  const start=source.indexOf("document.addEventListener('pointerdown',event=>{\n  const tile="),end=source.indexOf("document.addEventListener('visibilitychange'",start);
  vm.runInContext(source.slice(start,end),context);
  const dispatch=(type,event)=>{for(const handler of handlers.get(type)||[])handler(event);};
  const event=(pointerId,target,x=0,y=0)=>({pointerId,target,clientX:x,clientY:y,button:0,preventDefault(){}});
  const previewKeys=()=>[...cells].filter(([,cell])=>cell.classList.contains('ground-preview')).map(([key])=>key).sort();
  return {context,game,caster,guardian,ranger,other,cells,app,calls,effects,clocks,makeNode,dispatch,event,previewKeys};
}

test('focused and hovered ground footprints survive ordinary app renders without turning into painted ground',()=>{
  for(const interaction of ['focus','hover']){
    const {context,game,cells,dispatch,previewKeys}=fixture(),target=cells.get('4,3');
    if(interaction==='focus'){context.document.activeElement=target;dispatch('focusin',{target});}
    else{context.hoverCell=target;dispatch('pointerover',{target});}
    assert.equal(previewKeys().length,5);const before=previewKeys();context.render();
    assert.deepEqual(previewKeys(),before,`${interaction} footprint survives a redraw`);assert.equal(game.groundEffects.length,0);
    context.seatModes.player='move';context.render();assert.equal(previewKeys().length,0,'changing command removes a stale footprint');
  }
});

test('captured ground-cast dragging follows its own footprint after the selected skill changes',()=>{
  const {context,caster,cells,dispatch,event,previewKeys}=fixture(),sourceCell=cells.get('4,4');
  dispatch('pointerdown',event(21,sourceCell,20,100));context.seatSkills.player=1;
  context.hit=cells.get('4,3');dispatch('pointermove',event(21,sourceCell,40,60));
  assert.equal(context.pointers.get(21).previewGesture.unitId,caster.id);
  assert.equal(context.pointers.get(21).previewGesture.skillIndex,0);
  assert.equal(previewKeys().length,5);assert.ok(cells.get('4,3').classList.contains('preview-center'));
  context.render();assert.equal(previewKeys().length,5,'a render while holding the pointer retains its captured footprint');
});

test('a second player drag previews the captured secondary hop rather than a later command selection',()=>{
  const {context,other,cells,dispatch,event,previewKeys}=fixture(),sourceCell=cells.get('4,1');
  dispatch('pointerdown',event(22,sourceCell,100,20));context.seatSkills.ai=0;context.activeSeat='player';
  context.hit=cells.get('4,2');dispatch('pointermove',event(22,sourceCell,100,70));
  assert.equal(context.pointers.get(22).previewGesture.unitId,other.id);
  assert.deepEqual(previewKeys(),['4,1','4,2'],'Riftstep marks its departure and destination');
  context.render();assert.deepEqual(previewKeys(),['4,1','4,2']);
});

test('field clock text and tile accessibility descriptions track game seconds together',()=>{
  const {context,game,caster,cells,clocks,makeNode}=fixture();
  const action=legalActions(game,caster.id).find(a=>a.type==='ability'&&a.skillIndex===0&&a.x===4&&a.y===3);
  assert.equal(applyAction(game,action).ok,true);
  const field=game.groundEffects[0],tile=cells.get(`${field.x},${field.y}`),description=groundDescription([field],game.time,GROUND_EFFECTS,'local');
  tile.dataset.groundDescription=description;tile.setAttribute('aria-label',`Ground: ${description}`);tile.setAttribute('title',`Ground: ${description}`);
  const badge=makeNode(),label={textContent:'12s'};badge.dataset={groundId:field.id,expiresAt:String(field.expiresAt)};badge.setAttribute('aria-label',`${GROUND_EFFECTS[field.kind].name}, 12 seconds remaining`);badge.querySelector=()=>label;clocks.push(badge);
  game.time=8.1;context.updateTimers();
  assert.equal(label.textContent,'4s');assert.match(badge.getAttribute('aria-label'),/4 seconds remaining$/);
  assert.match(tile.getAttribute('aria-label'),/4 seconds:/);assert.equal(tile.getAttribute('title'),tile.getAttribute('aria-label'));
  const before=context.readySignature().split('#')[1];assert.ok(before.includes(field.id));game.time=field.expiresAt;
  assert.equal(context.readySignature().split('#')[1],'','expiry triggers a battlefield redraw even without combat events');
});

test('damage from a lasting field always draws attack effects even when its creator has a support primary',()=>{
  const {context,guardian,effects}=fixture();
  assert.equal(actionColor({type:'ability',skillIndex:0},guardian,unitDef),'support');
  context.drawSpecial({type:'damage',sourceId:guardian.id,groundKind:'thornfield',x:4,y:3,value:1});
  assert.match(effects.at(-1).className,/fx-attack/);assert.match(effects.at(-1).className,/fx-thornfield/);
  context.drawSpecial({type:'heal',sourceId:guardian.id,groundKind:'mycelium',x:4,y:3,value:1});assert.match(effects.at(-1).className,/fx-heal/);
});

test('selected-creature badges, member highlights and links use the same live formation rules',()=>{
  const {context,game,caster,guardian}=fixture();
  assert.equal(formationStatus(game,caster).active,true);
  const before=context.compactBattleHTML();assert.match(before,/Living Constellation.*ACTIVE/);
  assert.ok(before.includes(`data-formation-links="${caster.id}"`));assert.equal((before.match(/formation-member/g)||[]).length,3);
  guardian.hp=0;const after=context.compactBattleHTML();assert.doesNotMatch(after,/data-formation-links=/);assert.doesNotMatch(after,/formation-member/);
  assert.match(after,/Living Constellation formation not active/);
});

test('all nine new creature sprites use their matching atlas row without replacing original or minion art',()=>{
  const {context}=fixture();
  for(const [row,faction]of ['bloom','chaos','choir'].entries())for(const [column,role]of ['guardian','ranger','shaper'].entries()){
    const html=context.sprite(faction,role);assert.match(html,/tactical-sprite/);assert.ok(html.includes(`background-position:${column*50}% ${[0,48,94][row]}%`));
  }
  assert.doesNotMatch(context.sprite('bloom','leader'),/tactical-sprite/);assert.doesNotMatch(context.sprite('chaos','minion'),/tactical-sprite/);
});

test('battle HUD distinguishes order reserves, contested light and actual ward damage',()=>{
  const {context,game,caster}=fixture();
  game.orders={player:0,ai:2};game.mode='control';
  game.flags.forEach((flag,index)=>Object.assign(flag,{owner:index===1?'ai':null,contested:index===0}));
  caster.shield=3;
  let html=context.compactBattleHTML();
  assert.match(html,/P1 · 0 of 2 orders/);assert.match(html,/P2 · 2 of 2 orders/);
  assert.match(html,/P1 \+0 · P2 \+1/);assert.match(html,/TIE /);assert.match(html,/OPEN /);assert.match(html,/3 ward/);
  assert.match(html,/data-command-timer="ability"[^>]*>Order /);
  assert.equal(context.combatOutcome({type:'capture',side:null,contested:true}).main,'Contested');
  assert.equal(context.combatOutcome({type:'capture',side:null,contested:false}).main,'Uncontrolled');
  const result=context.combatOutcome({type:'damage',value:2,wardBroken:true,wardAbsorbed:1,absorbed:1});
  assert.equal(result.main,'−2 HP');assert.equal(result.detail,'Ward broken');
  game.mode='elimination';html=context.battleForecastHTML();assert.doesNotMatch(html,/Light pulse/);
});

test('Foldhorn shows contact and push tiles separately, with updated outcomes when the defender braces or dodges',()=>{
  const {context,game,caster}=fixture();
  const mark={kind:'foldhorn',unitId:'ram',side:'ai',fromX:1,fromY:4,x:4,y:4,resolvesAt:3};
  game.telegraphs=[mark];
  let html=context.compactBattleHTML();assert.match(html,/threat-impact/);assert.match(html,/threat-row/);
  assert.match(html,/threat-push/);assert.match(html,/push to F3/);
  game.tiles.find(t=>t.x===5&&t.y===4).terrain='rock';assert.match(context.battleForecastHTML(),/Blocked landing/);
  caster.braceUntil=2;assert.match(context.battleForecastHTML(),/brace expires/);
  caster.braceUntil=3;html=context.compactBattleHTML();assert.doesNotMatch(html,/threat-push/);assert.match(html,/Braced · holds position/);
  caster.x=5;caster.y=3;html=context.compactBattleHTML();assert.match(html,/Clear square · will miss/);assert.match(html,/threat-impact/);
});
