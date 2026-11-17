import {recordGameplay} from './web-account.mjs';
const website='https://www.catnmice.com';
let recorded=false;
export async function recordCommunity(game) {
  if(recorded||!['bannerlands','cat','trashtactics'].includes(game)||navigator.userAgent.includes('Electron/')||!/^https:\/\/[a-z0-9-]+\.trycloudflare\.com$/.test(location.origin))return;
  recorded=true;
  try{
    if(parent!==self&&document.referrer&&[website,'https://catnmice.com'].includes(new URL(document.referrer).origin)){
      parent.postMessage({type:'catnmice-gameplay-started',game},new URL(document.referrer).origin);
      return;
    }
    await recordGameplay(game,false);
  }catch{recorded=false;}
}
