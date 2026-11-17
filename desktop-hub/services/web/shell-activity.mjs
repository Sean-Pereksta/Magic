import {recordGameplay,linkGuest,sync} from './web-account.mjs';
const frame=document.querySelector('#contentFrame');
const truth='https://sean-pereksta.github.io';
const truthRoutes={bannerlands:'bannerlands',monstermallexpanded:'monster-mall-expanded',pacmanblackout:'pacman',pirates:'pirates',academystandards:'academy-standards',operationwhiskerfall:'operation-whiskerfall'};
window.addEventListener('message',event=>{
  if(event.source!==frame?.contentWindow||event.data?.type!=='catnmice-gameplay-started')return;
  const id=event.data.game;
  const allowed=event.origin===truth&&Object.hasOwn(truthRoutes,id)||
    /^https:\/\/[a-z0-9-]+\.trycloudflare\.com$/.test(event.origin)&&['bannerlands','cat','trashtactics'].includes(id);
  // Cross-origin documents cannot read site storage. The official lobby set
  // the catalog key during the user's launch action.
  if(!allowed||localStorage.getItem('gh.lastGameType')!==id)return;
  void recordGameplay(id,true).catch(()=>{});
});
function bindProfile(){
  let doc;
  try{doc=frame.contentDocument;if(!doc||!/^\/lobby\//.test(doc.location.pathname))return;}catch{return;}
  const place=doc.getElementById('profileDetails');
  if(!place||doc.getElementById('catnmice-link-guest'))return;
  const button=doc.createElement('button');button.type='button';button.className='btn';button.id='catnmice-link-guest';button.textContent='Link this browser’s guest play to my profile';
  button.addEventListener('click',async()=>{button.disabled=true;try{await linkGuest();await sync();button.textContent='Guest play linked';}catch(e){button.textContent='Could not link guest play. Try again';button.disabled=false;}});
  place.after(button);
}
frame?.addEventListener('load',bindProfile);bindProfile();
