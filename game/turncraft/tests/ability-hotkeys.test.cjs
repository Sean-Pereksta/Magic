const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

test('ability shortcuts stay unique and stable when selection order or readiness changes',()=>{
  const run=simulation('ascendant');
  const result=run.plain(`(()=>{const ids=['phaseStep','forcefield','phaseLance','shieldWall'],first=Object.fromEntries(allocateAbilityHotkeys(ids));
    const reverse=Object.fromEntries(allocateAbilityHotkeys(ids.slice().reverse()));
    const duplicate=Object.fromEntries(allocateAbilityHotkeys([...ids,'phaseStep']));
    return {first,reverse,duplicate,reserved:[...RESERVED_COMMAND_KEYS],single:Object.fromEntries(allocateAbilityHotkeys(['psionicStorm'])),sentry:Object.fromEntries(allocateAbilityHotkeys(['shieldWall','forcefield']))}})()`);
  assert.deepEqual(result.first,result.reverse);
  assert.deepEqual(result.first,result.duplicate);
  assert.equal(new Set(Object.values(result.first)).size,4);
  for(const key of Object.values(result.first))assert.ok(!result.reserved.includes(key.replace(/^Shift\+/,'').toLowerCase()));
  assert.equal(result.single.psionicStorm,'Q');
  assert.deepEqual(result.sentry,{shieldWall:'Q',forcefield:'E'});
});

test('current item shortcuts reserve exact chords and overflow never aliases another ability',()=>{
  const run=simulation();
  const result=run.plain(`(()=>{const ids=Array.from({length:90},(_,i)=>'ability'+String(i).padStart(3,'0'));
    return {keys:[...allocateAbilityHotkeys(ids,['Q','Shift+Q','E',';']).values()],normal:[normalizeHotkey('SHIFT+q'),normalizeHotkey('Q')],last:abilityKeyLabel(100)}})()`);
  const assigned=result.keys.filter(Boolean);
  assert.equal(assigned.length,new Set(assigned).size);
  assert.ok(!assigned.includes('Q')&&!assigned.includes('Shift+Q')&&!assigned.includes('E')&&!assigned.includes(';'));
  assert.ok(assigned.length>35);
  assert.equal(result.last,'');
  assert.deepEqual(result.normal,['shift+q','q']);
});

test('all abilities in every faction fit the automatic shortcut pool without command collisions',()=>{
  const run=simulation();
  const rosters=run.plain(`Object.fromEntries(Object.keys(FACTIONS).map(f=>[f,[...allocateAbilityHotkeys([...new Set(Object.keys(UNITS).filter(type=>UNITS[type].f===f).flatMap(type=>abilitiesFor({kind:'unit',type,faction:0})))])].map(([id,key])=>({id,key}))]))`);
  for(const [faction,entries]of Object.entries(rosters)){
    assert.ok(entries.length>10,faction);
    assert.ok(entries.every(entry=>entry.key),faction);
    assert.equal(new Set(entries.map(entry=>entry.key)).size,entries.length,faction);
  }
});

test('ability dispatch respects modifiers, typing, composition, repeats, dialogs and cooldown',()=>{
  const run=simulation();
  run(`globalThis.hotkeyClicks=0;globalThis.hotkeyPrevented=0;globalThis.fixtureButton={dataset:{ability:'autoRepair',hotkey:'Q'},disabled:false,click(){hotkeyClicks++}};
    commandAbilityButtons=()=>[fixtureButton];syncAbilityHotkeys=()=>{};
    globalThis.pressAbility=(options={})=>handleAbilityHotkey({key:'q',target:{tagName:'CANVAS'},preventDefault(){hotkeyPrevented++},...options});`);
  for(const option of ['ctrlKey','metaKey','altKey','repeat','isComposing','defaultPrevented'])run(`pressAbility({${option}:true})`);
  run(`for(const tagName of ['INPUT','SELECT','TEXTAREA'])pressAbility({target:{tagName}});pressAbility({target:{isContentEditable:true}});pressAbility({target:{closest:()=>({})}})`);
  assert.equal(run('hotkeyClicks'),0);
  run(`pressAbility();pressAbility({shiftKey:true});fixtureButton.disabled=true;pressAbility()`);
  assert.equal(run('hotkeyClicks'),1);
  assert.equal(run('hotkeyPrevented'),2);
  run(`fixtureButton.disabled=false;paused=true;pressAbility();paused=false;gameOver=true;pressAbility();gameOver=false;
    endOverlay.style.display='flex';pressAbility();endOverlay.style.display='none';
    document.querySelector=()=>({});pressAbility()`);
  assert.equal(run('hotkeyClicks'),1);
});

test('desktop and touch activation share group commands and cancel clears targeting only',()=>{
  const run=simulation('ascendant');
  const result=run.plain(`(()=>{const a=addEntity('unit','dragoon',0,8,8),b=addEntity('unit','dragoon',0,9,8);selectedIds=[a.id,b.id];selectedId=a.id;
    activateSelectionAbility('phaseStep');const armed={mode:interactionMode,id:interactionPayload};
    const orders=[a.order,b.order],cooldowns=[a.abilityReadyAt,b.abilityReadyAt];cancelArmedAbility();
    return {armed,cancel:{mode:interactionMode,payload:interactionPayload,legal:legalTiles.length},ordersUnchanged:JSON.stringify(orders)===JSON.stringify([a.order,b.order]),cooldownsUnchanged:JSON.stringify(cooldowns)===JSON.stringify([a.abilityReadyAt,b.abilityReadyAt])}})()`);
  assert.deepEqual(result.armed,{mode:'ability',id:'phaseStep'});
  assert.deepEqual(result.cancel,{mode:null,payload:null,legal:0});
  assert.equal(result.ordersUnchanged,true);
  assert.equal(result.cooldownsUnchanged,true);
});
