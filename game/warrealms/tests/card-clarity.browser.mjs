// Headless regression against the real shell, card definitions and event handlers.
// Firebase is stubbed and all external traffic blocked; no preview or live writes.
import http from 'node:http';
import fs from 'node:fs/promises';
import {createRequire} from 'node:module';
import assert from 'node:assert/strict';
const require=createRequire(import.meta.url);
const {chromium}=require(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES ? `${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright` : 'playwright');
const root=new URL('../../',import.meta.url);
let html=await fs.readFile(new URL('warrealms.html',root),'utf8');
html=html.replace(/https:\/\/www.gstatic.com\/firebasejs\/10.7.1\/firebase-[a-z]+.js/g,'/firebase-stub.js');
const setup=`
  boot = async () => {}; recoverActiveArmoryPage = async () => {};
  loadCardArt = async () => {}; hydrateCardArt = () => {};
  firebaseUser={uid:'test'};
  const mkPlayer=(id)=>({id,uid:id==='me'?'test':'other',name:id,hand:[],draw:[],discard:[],played:[],bases:[],attachments:[],pendingChoices:[],transformProgress:{},health:60,trade:0,combat:0});
  state={phase:'playing',turn:0,turnSerial:3,players:[mkPlayer('me'),mkPlayer('enemy')],scrapped:[],log:[],visualEvents:[]};
  const me=state.players[0];
  const heatCard=Object.values(ALL_CARD_MAP).find(card=>card.heat?.actions?.length);
  me.played.push(makeBattleEntry(heatCard.id,me.id,{instanceId:'heat',heat:4}));
  const baseCard=Object.values(ALL_CARD_MAP).find(card=>card.type==='base'&&card.charge?.actions?.length);
  const attachmentCard=Object.values(ALL_CARD_MAP).find(card=>card.type==='attachment'&&card.attachment?.action);
  for(let n=0;n<4;n++) {
    me.bases.push(makeBattleEntry(baseCard.id,me.id,{instanceId:'base'+n,charges:10,constructionRemaining:0}));
    me.attachments.push(makeBattleEntry(attachmentCard.id,me.id,{instanceId:'attachment'+n,attachedTo:'base'+n}));
  }
  const transformCard=Object.values(ALL_CARD_MAP).find(card=>card.transform?.choose?.length>1&&card.type==='base');
  const evolution=makeBattleEntry(transformCard.id,me.id,{instanceId:'evolution'});me.bases.push(evolution);
  const choices=transformChoiceOptions(transformCard.transform).map(option=>({cardId:option.into,instanceId:evolution.instanceId,label:option.label,description:option.description,effect:{__resolveTransformChoice:{sourceCardId:transformCard.id,sourceInstanceId:evolution.instanceId,into:option.into,preserve:option.preserve||transformCard.transform.preserve||{},destination:baseTransformDestination(transformCard,transformCard.transform,option)||'',trigger:transformCard.transform.trigger}}}));
  const choice={id:'evolve-test',cardId:transformCard.id,source:'transform:test',options:choices};
  let actions=[]; performAction=async action=>{actions.push(action)};
  renderBattle=()=>{};
  document.querySelectorAll('.overlay,.view').forEach(node=>node.classList.add('hidden'));
  document.querySelector('#battleView').classList.remove('hidden');
  document.body.classList.add('battleMode');document.documentElement.classList.add('battleMode');
  function paint(){
    document.querySelector('#handRow').innerHTML=battleCardHtml(me.played[0],{directAction:'play-card',dataAttributes:'data-playable="1" data-hand-index="0"'});
    document.querySelector('#ownCards').innerHTML=me.bases.map(entry=>baseGroupHtml(me,entry,{mine:true,canAct:true})).join('');
    refreshCardInspection();animateNewAttachmentLinks();
  }
  paint();
  window.clarityTest={actions,paint,state,me,choice,openCardInfo,closeModal,renderPendingChoice,cardAbilityStatuses,cardAllyStatuses,reduceGame,getCard,baseGroupHtml,
    replaceSnapshot(){state=JSON.parse(JSON.stringify(state)); this.state=state; this.me=state.players[0];},
    showChoices(){me.pendingChoices=[choice];renderPendingChoice();},
    clearChoices(){me.pendingChoices=[];renderPendingChoice();},
    setHeat(value){me.played[0].heat=value;refreshCardInspection();},
    inspectHeat(){openCardInfo(heatCard.id,'',false,'heat');},
    compare(){return evolutionComparisonHtml(choice,choices[0]);}
  };`;
html=html.replace(/  void boot\(\)\.then\(\(\) => \{[\s\S]*?\n  \}\);/,setup);
const stub=`export const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({}),doc=()=>({}),collection=()=>({}),onAuthStateChanged=()=>()=>{},signInAnonymously=async()=>({}),getDoc=async()=>({exists:()=>false}),getDocs=async()=>({docs:[]}),setDoc=()=>{throw Error('Unexpected write')},updateDoc=setDoc,onSnapshot=()=>()=>{},query=()=>({}),where=()=>({}),runTransaction=setDoc,serverTimestamp=()=>0;`;
const server=http.createServer(async(req,res)=>{try{
 const url=new URL(req.url,'http://localhost');
 if(url.pathname==='/game/warrealms.html'){res.setHeader('Content-Type','text/html');res.end(html);return;}
 if(url.pathname==='/firebase-stub.js'){res.setHeader('Content-Type','text/javascript');res.end(stub);return;}
 if(!/^\/game\/warrealms(?:-pack)?\/[\w/.-]+\.(js|css)$/.test(url.pathname)){res.writeHead(404);res.end();return;}
 res.setHeader('Content-Type',url.pathname.endsWith('.css')?'text/css':'text/javascript');res.end(await fs.readFile(new URL(url.pathname.slice(6),root)));
 }catch(e){res.writeHead(500);res.end(String(e));}});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const browser=await chromium.launch({headless:true,executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE || undefined,args:['--no-sandbox']});
try{
 for(const width of [1280,390,320]){
  const page=await browser.newPage({viewport:{width,height:900}});
  const errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.route('**/*',route=>route.request().url().startsWith('http://127.0.0.1:')?route.continue():route.abort());
  await page.goto(`http://127.0.0.1:${server.address().port}/game/warrealms.html`);
  await page.waitForFunction(()=>window.clarityTest,{timeout:15000}).catch(e=>{throw Error(errors.join('\n')||String(e))});
  const initial=await page.evaluate(()=>JSON.stringify(clarityTest.state));
  // Enter on nested Info must not reach the article's Play action.
  await page.locator('#handRow [data-act="card-info"]').focus();await page.keyboard.press('Enter');
  assert.equal(await page.locator('#modalOverlay').evaluate(node=>node.classList.contains('hidden')),false);
  assert.equal(await page.evaluate(()=>clarityTest.actions.length),0);
  await page.keyboard.press('Escape');
  assert.equal(await page.evaluate(()=>JSON.stringify(clarityTest.state)),initial);
  // Artwork and status rail do not overlap.
  assert.ok(await page.locator('#handRow .gameCard').evaluate(node=>node.querySelector('.cardStatusTray').getBoundingClientRect().top>=node.querySelector('.battleCardVisual').getBoundingClientRect().bottom-1));
  // Every collapsed component is inspectable, and actionable where rules allow.
  assert.equal(await page.locator('.wrAttachmentTab').count(),4);
  assert.equal(await page.locator('.wrAttachmentTab [data-act="card-info"]').count(),4);
  assert.equal(await page.locator('.wrAttachmentTab [data-act="use-attachment"]').count(),4);
  await page.locator('[data-base-group="base0"] [data-act="toggle-base-group"]').click();
  assert.equal(await page.locator('[data-base-group="base0"]').evaluate(node=>node.classList.contains('wrBaseExpanded')),true);
  await page.evaluate(()=>clarityTest.paint());
  assert.equal(await page.locator('[data-base-group="base0"]').evaluate(node=>node.classList.contains('wrBaseExpanded')),true);
  await page.evaluate(()=>clarityTest.inspectHeat());
  await page.evaluate(()=>clarityTest.setHeat(9));
  assert.match(await page.locator('[data-live-inspection]').innerText(),/Heat 9/);
  await page.keyboard.press('Escape');
  await page.evaluate(()=>clarityTest.showChoices());
  const beforePreview=await page.evaluate(()=>JSON.stringify(clarityTest.state));
  assert.ok(await page.locator('.wrEvolutionCompare table').count()>=2);
  await page.locator('#choiceGrid [data-act="card-info"]').first().click();
  await page.keyboard.press('Escape');
  assert.equal(await page.evaluate(()=>JSON.stringify(clarityTest.state)),beforePreview);
  assert.equal(await page.evaluate(()=>clarityTest.actions.length),0);
  assert.ok(await page.locator('.choiceModal').evaluate(node=>node.scrollWidth<=node.clientWidth+1));
  assert.ok(await page.locator('.wrEvolutionCompare').first().evaluate(node=>node.scrollWidth<=node.clientWidth+1));
  assert.deepEqual(errors,[]);
  console.log(`PASS ${width}px: safe keyboard inspection, unobstructed art, 4 collapsed attachments, persistent expansion, live inspection, non-mutating branch previews, comparison layout.`);
  await page.close();
 }
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
