// Imported by published game documents. Play never waits on analytics.
const tag=document.querySelector('script[data-catnmice-game]');
const assigned=tag?.dataset.catnmiceGame;
const site='https://www.catnmice.com', truth='https://sean-pereksta.github.io';
const isOfficial=[site,'https://catnmice.com'].includes(location.origin);
const isTruth=location.origin===truth&&location.pathname.startsWith('/Truth/games/');
const trustedParent=(()=>{try{const ref=new URL(document.referrer);return [site,'https://catnmice.com'].includes(ref.origin)?ref.origin:null;}catch{return null;}})();
const surfaces='canvas,[data-gameplay-surface],#board,#gameBoard,#game-board,#battlefield,#hand,#playerHand,#player-hand,#playArea,.game-board,.chessboard,.game-grid,.game-cell,.hand-card,.battle-grid,.board-cell,.tile-grid';
const controls={
  biblegame:'#wordSubmitBtn,#referenceSubmitBtn,#bookOptions button',bibleroguelike:'#wordSubmitBtn,#referenceSubmitBtn,#bookOptions button',
  crowncouncil:'.cardRow button,.cardRow [data-pick]',catmobile:'#moveJoy,#buildJoy,#grid',grass:'#rollBtn,#acceptRollBtn,#rerollBtn,#endTurnBtn,#quickEndTurnBtn,#shopEndTurnBtn',
  knightwarfare:'#submitAnswer,#mapStage #gridLayer',quipclash:'[data-submit-answers],[data-action="vote"]',rock:'#map',
  roguecards:'#grid,#benchBoard',rogueduel:'#grid,#draftChoices,#relicChoices',warrealms:'#endTurnBtn,#handRow,#attackTargetsBoard',
  trumprev:'#boardGrid,#battleGrid',microwars2:'#mapView',spelltower:'#endTurnBtn',cardrealms:'#attackAuthorityBtn,#endTurnBtn',cardrealmsmulti:'#attackAuthorityBtn,#endTurnBtn'
};
let recorded=false;
function key(){
  if(assigned==='biblegame'&&(new URLSearchParams(location.search).get('mode')==='roguelike'||localStorage.getItem('gh.lastGameType')==='bibleroguelike'&&new URLSearchParams(location.search).has('lobby')))return 'bibleroguelike';
  return assigned;
}
async function played(){
  if(recorded||!assigned||!isOfficial&&!isTruth||navigator.userAgent.includes('Electron/'))return;
  recorded=true;
  const game=key();
  try{
    if(isTruth&&trustedParent&&parent!==self){parent.postMessage({type:'catnmice-gameplay-started',game},trustedParent);return;}
    const {recordGameplay}=await import('./web-account.mjs');
    await recordGameplay(game,isOfficial);
  }catch{recorded=false;}
}
const explicit=tag?.dataset.confirmed==='engine';
if(explicit){
  window.addEventListener('catnmice-game-started',played);
  if(document.documentElement.dataset.catnmicePlaying==='true')void played();
}else{
  window.addEventListener('pointerdown',event=>{
    if(!event.isTrusted||event.button!==0||event.target.closest('a,dialog,input,textarea,select,nav,[role="dialog"]'))return;
    const surface=event.target.closest(controls[assigned]?surfaces+','+controls[assigned]:surfaces);
    if(surface&&!/preview|avatar|portrait|thumbnail|icon|logo/i.test(surface.id+' '+surface.className))void played();
  },{capture:true,passive:true});
  window.addEventListener('keydown',event=>{
    if(!event.isTrusted||event.repeat||!['ArrowUp','ArrowDown','ArrowLeft','ArrowRight','KeyW','KeyA','KeyS','KeyD','Space'].includes(event.code)||event.target.closest('button,input,textarea,select,dialog,nav,[role="dialog"]'))return;
    const view=[...document.querySelectorAll('canvas')].find(e=>e.clientWidth>=200&&e.clientHeight>=120&&e.checkVisibility?.()!==false&&!/preview|avatar|portrait|thumbnail/i.test(e.id+' '+e.className));
    if(view){const box=view.getBoundingClientRect();if(document.elementFromPoint(box.x+box.width/2,box.y+box.height/2)===view)void played();}
  },true);
}
