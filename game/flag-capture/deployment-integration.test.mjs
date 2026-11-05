import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {createGame,FACTIONS} from './engine.mjs';
import {createDraft,resizeDraft,applyDraftAction} from './draft-ui.mjs';
import {CAMPAIGN,createCampaignBattle} from './campaign.mjs';

// Exercise the app's real setup/input code with real army purchases and battle
// creation. The small DOM boundary observes captures, ghosts and announcements.
const source=readFileSync(new URL('./app.mjs',import.meta.url),'utf8').replace(/\r\n/g,'\n');
function region(start,end){
  const first=source.indexOf(start),last=source.indexOf(end,first+start.length);
  assert.ok(first>=0&&last>first,`Missing app region: ${start}`);
  return source.slice(first,last);
}
const plain=value=>JSON.parse(JSON.stringify(value));
function fixture(){
  const handlers=new Map(),nodes=new Set(),captures=new Set(),timers=[];
  const calls={render:0,ghosts:[],announcements:[],capture:[],release:[],persist:0};
  function classList(initial=[]){const values=new Set(initial);return {add:name=>values.add(name),remove:name=>values.delete(name),contains:name=>values.has(name)};}
  function element(dataset={},occupied=false){
    const node={dataset:{...dataset},classList:classList(),style:{},removed:false,
      append(child){this.child=child;},remove(){this.removed=true;nodes.delete(this);},cloneNode(){return element();},
      querySelector:()=>element(),getBoundingClientRect:()=>({width:60,height:60}),
      closest(selector){
        return selector.split(',').some(part=>{
          if(part==='.deployment-cell.is-occupied[data-index]')return occupied&&this.dataset.index!==undefined;
          if(part==='.deployment-cell[data-draft-action="place"]')return this.dataset.draftAction==='place';
          if(part==='[data-draft-action]')return Boolean(this.dataset.draftAction);
          if(part==='[data-cell]')return Boolean(this.dataset.cell);
          if(part==='[data-command]')return Boolean(this.dataset.command);
          const command=part.match(/^\[data-command="(.+)"\]$/);if(command)return this.dataset.command===command[1];
          const attr=part.match(/^\[data-([a-z-]+)\]$/);if(attr)return this.dataset[attr[1].replace(/-([a-z])/g,(_,letter)=>letter.toUpperCase())]!==undefined;
          return false;
        })?this:null;
      }};
    nodes.add(node);return node;
  }
  const app={setPointerCapture(id){captures.add(id);calls.capture.push(id);},hasPointerCapture:id=>captures.has(id),releasePointerCapture(id){captures.delete(id);calls.release.push(id);}};
  const dialog={close(){}};
  const context={createGame,FACTIONS,createDraft,resizeDraft,applyDraftAction,CAMPAIGN,createCampaignBattle,
    setupMode:'custom',setupStep:'deploy',setupSeed:5301,deploymentSide:'player',chosen:'bloom',opponent:'chaos',difficulty:'easy',map:'glade',battleMode:'control',boardSize:'9x7',terrain:'wild',campaignLevel:1,startingMoney:30,opponentMode:'local',
    draftCache:new Map(),deploymentPointers:new Map(),consumedPointers:new Map(),heldPointers:new Set(),pointers:new Map(),
    scene:'setup',game:null,activeSeat:'player',seatSelections:{},seatModes:{},seatSkills:{},selectedId:null,mode:'auto',deferredEvents:[],selectedSkillIndex:0,paused:false,countdownRemainingMs:0,countdownNumber:0,inspectResult:false,accumulated:0,
    now:1000,hit:null,performance:{now:()=>context.now},setTimeout(callback){timers.push(callback);return timers.length;},
    document:{hidden:false,querySelectorAll(selector){return [...nodes].filter(node=>{
      if(!node.classList.contains('deployment-drag-over'))return false;
      const pointer=selector.match(/data-deploy-pointer="(\d+)"/);return !pointer||String(node.dataset.deployPointer)===pointer[1];
    });},addEventListener(type,callback){if(!handlers.has(type))handlers.set(type,[]);handlers.get(type).push(callback);},
      elementFromPoint:()=>context.hit,createElement(){const node=element();calls.ghosts.push(node);return node;},body:{append(){}}},
    $:selector=>selector==='#app'?app:dialog,window:{scrollTo(){}},
    announce:message=>calls.announcements.push(message),toast:message=>calls.announcements.push(message),
    render(){calls.render++;},persist(){calls.persist++;},resetInput(){}
  };
  vm.createContext(context);
  vm.runInContext(region('function draftFor(','function setupStepsHTML('),context);
  vm.runInContext(region('function start(){','document.addEventListener(\'click\''),context);
  vm.runInContext(region("document.addEventListener('click'","document.addEventListener('change'"),context);
  vm.runInContext(region('function resetDeploymentPointers(){',"document.addEventListener('pointerdown',event=>{\n  const tile="),context);
  const event=(pointerId,target,x=0,y=0)=>({pointerId,target,clientX:x,clientY:y,button:0,preventDefault(){this.prevented=true;}});
  const dispatch=(type,event={})=>{for(const handler of handlers.get(type)||[])handler(event);return event;};
  function cell(side,x,y){
    const draft=context.draftFor(side),index=draft.positions.findIndex(point=>point.x===x&&point.y===y);
    return element({draftAction:'place',side,x:String(x),y:String(y),...(index>=0?{index:String(index)}:{})},index>=0);
  }
  const troop=(side,index)=>{const point=context.draftFor(side).positions[index];return cell(side,point.x,point.y);};
  const button=dataset=>element(dataset);
  function drag(pointerId,side,index,target){
    const origin=troop(side,index);dispatch('pointerdown',event(pointerId,origin,10,10));
    context.hit=target;dispatch('pointermove',event(pointerId,origin,30,40));return origin;
  }
  return {context,calls,captures,timers,dispatch,event,cell,troop,button,drag};
}

test('small pointer movement retains normal taps without creating or capturing a drag',()=>{
  const {context,calls,dispatch,event,troop}=fixture(),before=plain(context.draftFor('player').positions),origin=troop('player',1);
  dispatch('pointerdown',event(1,origin,10,10));
  const move=dispatch('pointermove',event(1,origin,17,10));
  dispatch('pointerup',event(1,origin,17,10));
  assert.equal(move.prevented,undefined);assert.equal(calls.ghosts.length,0);assert.deepEqual(calls.capture,[]);
  assert.equal(context.deploymentPointers.size,0);assert.deepEqual(plain(context.draftFor('player').positions),before);
});

test('both players can drag simultaneously while roster selection changes without crossing their home rows',()=>{
  const {context,calls,captures,dispatch,event,cell,troop,button}=fixture();
  const player=context.draftFor('player'),other=context.draftFor('ai'),army=plain([player.army,other.army]);
  const p1=troop('player',1),p2=troop('ai',3);
  dispatch('pointerdown',event(11,p1,10,200));dispatch('pointerdown',event(12,p2,300,10));
  dispatch('pointerdown',event(13,troop('player',2),30,200));
  assert.equal(context.deploymentPointers.size,2,'a second finger on the same army cannot steal its existing drag');
  context.hit=cell('player',0,6);dispatch('pointermove',event(11,p1,20,150));
  context.hit=cell('ai',8,0);dispatch('pointermove',event(12,p2,300,60));
  dispatch('click',event(99,button({draftAction:'select',side:'player',index:'4'})));
  dispatch('click',event(98,button({draftAction:'select',side:'ai',index:'2'})));
  context.hit=cell('player',0,6);dispatch('pointerup',event(11,p1,20,150));
  context.hit=cell('ai',8,0);dispatch('pointerup',event(12,p2,300,60));
  assert.deepEqual(plain(player.positions[1]),{x:0,y:6});assert.deepEqual(plain(other.positions[3]),{x:8,y:0});
  assert.deepEqual(plain([player.army,other.army]),army);assert.equal(context.deploymentPointers.size,0);assert.equal(captures.size,0);
  assert.deepEqual(calls.capture,[11,12]);assert.deepEqual(calls.release,[11,12]);assert.ok(calls.ghosts.every(ghost=>ghost.removed));
});

test('dropping on a friendly creature swaps positions while an enemy home-row drop is rejected',()=>{
  const {context,dispatch,event,cell,troop,drag}=fixture(),draft=context.draftFor('player');
  const original=plain(draft.positions),target=troop('player',3),origin=drag(21,'player',1,target);
  dispatch('pointerup',event(21,origin,30,40));
  assert.deepEqual(plain(draft.positions[1]),original[3]);assert.deepEqual(plain(draft.positions[3]),original[1]);
  const afterSwap=plain(draft.positions),badTarget=cell('ai',0,0),second=drag(22,'player',1,badTarget);
  dispatch('pointerup',event(22,second,50,20));assert.deepEqual(plain(draft.positions),afterSwap);
});

test('the compatibility click after a drag cannot move the selected creature a second time',()=>{
  const {context,dispatch,event,cell,drag}=fixture(),draft=context.draftFor('player');
  const origin=drag(31,'player',1,cell('player',0,6));dispatch('pointerup',event(31,origin,30,40));
  const placed=plain(draft.positions);
  dispatch('click',event(31,cell('player',8,6),80,50));assert.deepEqual(plain(draft.positions),placed);
  context.now=1600;dispatch('click',event(31,cell('player',8,6),80,50));assert.deepEqual(plain(draft.positions[1]),{x:8,y:6});
});

test('pointer cancellation removes ghosts, capture and target decoration without placing or replaying a click',()=>{
  const {context,calls,captures,dispatch,event,cell,drag}=fixture(),draft=context.draftFor('player'),before=plain(draft.positions);
  const target=cell('player',0,6),origin=drag(41,'player',1,target);
  assert.ok(target.classList.contains('deployment-drag-over'));
  dispatch('pointercancel',event(41,origin,30,40));
  assert.equal(target.classList.contains('deployment-drag-over'),false);assert.equal(context.deploymentPointers.size,0);assert.equal(captures.size,0);
  assert.ok(calls.ghosts.every(ghost=>ghost.removed));dispatch('click',event(41,target));assert.deepEqual(plain(draft.positions),before);
});

test('starting battle cleans up both held drags and preserves the exact preview terrain and purchased formation',()=>{
  const {context,calls,captures,cell,drag}=fixture();
  context.draftFor('player');const player=context.draftFor('player');applyDraftAction(player,{type:'buy',role:'scout'});
  const preview=createGame(context.buildBattleOptions()),formation=plain(player.positions);
  drag(51,'player',1,cell('player',0,6));drag(52,'ai',2,cell('ai',8,0));
  context.start();
  assert.equal(context.scene,'battle');assert.equal(context.countdownRemainingMs,3000);assert.equal(context.deploymentPointers.size,0);assert.equal(captures.size,0);
  assert.ok(calls.ghosts.every(ghost=>ghost.removed));assert.deepEqual(context.game.tiles,preview.tiles);
  assert.deepEqual(context.game.units.filter(unit=>unit.side==='player').map(({x,y})=>({x,y})),formation);
  assert.equal(context.game.units.filter(unit=>unit.side==='player').length,player.army.length);
});

test('going back to army setup or hiding the page clears every in-progress deployment drag',()=>{
  for(const leave of ['back','hidden']){
    const {context,calls,captures,dispatch,event,cell,button,drag}=fixture();
    const before=plain(context.draftFor('player').positions);drag(61,'player',1,cell('player',0,6));
    if(leave==='back')dispatch('click',event(77,button({command:'setup-back'})));
    else{context.document.hidden=true;dispatch('visibilitychange');}
    assert.equal(context.deploymentPointers.size,0);assert.equal(captures.size,0);assert.ok(calls.ghosts.every(ghost=>ghost.removed));
    assert.deepEqual(plain(context.draftFor('player').positions),before);if(leave==='back')assert.equal(context.setupStep,'army');
  }
});

test('campaign setup uses its mission seed, rules and army in both preview and actual battle',()=>{
  const {context}=fixture();context.setupMode='campaign';context.campaignLevel=3;
  const draft=context.draftFor('player');applyDraftAction(draft,{type:'select',index:1});applyDraftAction(draft,{type:'place',x:0,y:6});
  const preview=createGame(context.buildBattleOptions());context.start();
  assert.equal(context.game.campaignId,CAMPAIGN[2].id);assert.equal(context.game.opponentMode,'ai');
  assert.deepEqual(context.game.tiles,preview.tiles);assert.deepEqual(context.game.units,preview.units);
  assert.equal(context.game.difficulty,CAMPAIGN[2].difficulty);assert.equal(context.game.mode,CAMPAIGN[2].mode);
});
