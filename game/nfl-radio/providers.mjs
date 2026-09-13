import {team} from './core.mjs';
import {sirius} from './provider-data.mjs';

export const PROVIDERS = {
 auto:{name:'Best available',free:true,requiresLogin:false,description:'Tries verified direct game audio first, then free/no-login station players and official free listening pages. Login/subscription services are kept as last-resort fallbacks.'},
 iheart:{name:'iHeartRadio',free:true,requiresLogin:false,description:'Uses a mapped official iHeart station widget when one is available. No Catnmice login is required; station schedules and geographic restrictions still apply.',infoUrl:'https://www.iheart.com/'},
 tunein:{name:'TuneIn',free:true,requiresLogin:false,description:'Uses a mapped official TuneIn station embed when available. Free station listening is preferred; premium or restricted feeds are never treated as free.',infoUrl:'https://tunein.com/radio/sports/'},
 audacy:{name:'Audacy',free:true,requiresLogin:false,description:'Opens a mapped official Audacy station page as a free listening option when available. Game rights and local restrictions remain with Audacy.',infoUrl:'https://www.audacy.com/sports'},
 westwood:{name:'Westwood One',free:true,requiresLogin:false,description:'Uses Westwood One as a free national fallback for games it is carrying. Availability depends on the published national schedule.',infoUrl:'https://www.westwoodonesports.com/'},
 sirius:{name:'SiriusXM',free:false,requiresLogin:true,description:'Home and away calls for all 32 teams. Opens the official SiriusXM player; a compatible subscription and provider login are required.',loginUrl:'https://www.siriusxm.com/player/',infoUrl:'https://www.siriusxm.com/sports/nfl'},
 nfl:{name:'NFL+',free:false,requiresLogin:true,description:'Every game’s home, away and national calls in the US. Opens NFL+; sign in and select Live Audio there.',loginUrl:'https://www.nfl.com/plus/',infoUrl:'https://support.nfl.com/hc/en-us/articles/35869715432468-What-games-can-I-listen-to-with-live-audio-on-NFL'}
};

// Only explicit provider-supported embeds belong here. A widget is a listening
// surface, not proof that the current NFL game is available in that market.
export const widgets = [
 {id:'iheart-det',team:'DET',name:'97.1 The Ticket',provider:'iheart',kind:'flagship',language:'en',url:'https://www.iheart.com/live/971-the-ticket-10827/',embedUrl:'https://www.iheart.com/live/971-the-ticket-10827/?embed=true&cid=oembed&keyid%5B0%5D=97.1%20The%20Ticket&sc=live_widget',checkedAt:'2026-09-13'},
 {id:'iheart-phi',team:'PHI',name:'SportsRadio 94WIP',provider:'iheart',kind:'flagship',language:'en',url:'https://www.iheart.com/live/sportsradio-94wip-10934/',embedUrl:'https://www.iheart.com/live/sportsradio-94wip-10934/?embed=true&cid=oembed&keyid%5B0%5D=SportsRadio%2094WIP&sc=live_widget',checkedAt:'2026-09-13'}
];

const providerKey=value=>{
 const key=String(value||'').toLowerCase();
 if(key.includes('iheart'))return 'iheart';
 if(key.includes('tunein'))return 'tunein';
 if(key.includes('audacy'))return 'audacy';
 if(key.includes('westwood'))return 'westwood';
 if(key.includes('sirius'))return 'sirius';
 if(key.includes('nfl'))return 'nfl';
 return key||'official';
};

export function validWidget(widget){
 try{
  const url=new URL(widget.embedUrl), provider=providerKey(widget.provider);
  if(provider==='iheart')return url.origin==='https://www.iheart.com'&&/^\/live\/[a-z0-9-]+-\d+\/$/.test(url.pathname)&&url.searchParams.get('embed')==='true';
  if(provider==='tunein')return url.origin==='https://tunein.com'&&/^\/embed\/player\/s\d+\/?$/.test(url.pathname);
  return false;
 }catch{return false;}
}
export function widgetsFor(teamId,preferred,provider='auto'){
 const wanted=provider==='auto'?widgets:widgets.filter(w=>providerKey(w.provider)===provider);
 const matches=wanted.filter(w=>w.team===teamId&&validWidget(w));
 return matches.sort((a,b)=>(a.id===preferred?-1:0)-(b.id===preferred?-1:0));
}
export function widgetFor(teamId,preferred,provider='auto'){return widgetsFor(teamId,preferred,provider)[0];}
export function siriusEvent(game){return sirius.events.find(e=>e.home===team(game.home)?.name&&e.away===team(game.away)?.name&&Math.abs(Date.parse(e.date)-Date.parse(game.date))<60000);}
export function listeningLink(provider,game,teamId,role){
 if(provider==='nfl')return {provider:'nfl',auth:'required',cost:'subscription',url:PROVIDERS.nfl.loginUrl,label:'Open NFL+ live audio',detail:'LOGIN · Provider subscription required. Choose this game’s Live Audio on NFL+.'};
 if(provider!=='sirius'||!team(teamId))return null;
 if(role==='national'||role==='spanish'){
  const event=siriusEvent(game), url=event?.[role];
  if(!url)return null;
  return {provider:'sirius',auth:'required',cost:'subscription',url,label:`Open SiriusXM ${role} broadcast`,detail:'LOGIN · Scheduled provider broadcast. Subscription and provider login required.'};
 }
 const item=sirius.teams[team(teamId).name];
 if(!item)return null;
 return {provider:'sirius',auth:'required',cost:'subscription',url:item.url,label:`Open ${team(teamId).name} on SiriusXM`,detail:`LOGIN · Team audio · streaming channel ${item.channel}. Subscription and provider login required.`};
}

const SAFE_HOSTS=new Set([
 'sxm.app.link','www.siriusxm.com','www.nfl.com','www.iheart.com','iheart.com','tunein.com','www.tunein.com',
 'www.audacy.com','audacy.com','www.westwoodonesports.com','westwoodonesports.com','www.packers.com','packers.com',
 'www.chiefs.com','chiefs.com','www.philadelphiaeagles.com','philadelphiaeagles.com'
]);
// URL validation also guards a future catalog refresh from creating arbitrary
// navigation destinations. Provider login always happens on the provider origin.
export function safeListeningUrl(value){
 try{const u=new URL(value);return u.protocol==='https:'&&SAFE_HOSTS.has(u.hostname)&&!u.username&&!u.password;}catch{return false;}
}

export function catalogProvider(item){
 if(item?.provider)return providerKey(item.provider);
 try{return providerKey(new URL(item?.sourceUrl).hostname);}catch{return 'official';}
}
const roleMatch=(item,game,teamId,role)=>{
 const target=role==='home'?game.home:role==='away'?game.away:teamId;
 if(role==='national')return item.kind==='national';
 if(role==='spanish')return item.language==='es'&&(!item.team||item.team===target);
 return item.team===target||item.kind==='national';
};
const catalogRank=item=>item.kind==='flagship'?40:item.kind==='affiliate'?50:item.kind==='national'?60:70;

// Resolver order after direct streams: supported free embeds -> free official
// station/provider pages -> authenticated providers. Direct streams are handled
// by feedQueue/RadioPlayer because only that layer can verify actual playback.
export function providerCandidates(catalog,game,teamId,role,preferred){
 const result=[];
 for(const widget of widgetsFor(role==='home'?game.home:role==='away'?game.away:teamId,preferred)){
  const provider=providerKey(widget.provider);
  result.push({type:'widget',provider,auth:'none',cost:'free',rank:widget.id===preferred?10:provider==='iheart'?20:30,label:`${widget.name} · ${PROVIDERS[provider]?.name||provider}`,detail:'FREE · No Catnmice login required. Provider controls game availability and geographic restrictions.',widget});
 }
 for(const item of catalog||[]){
  if(!item||item.access==='subscription'||!safeListeningUrl(item.sourceUrl)||!roleMatch(item,game,teamId,role))continue;
  const provider=catalogProvider(item);
  result.push({type:'external',provider,auth:'none',cost:'free',rank:catalogRank(item),url:item.sourceUrl,label:`${item.name} · FREE`,detail:`FREE · No Catnmice login required. ${item.note||'Availability is controlled by the broadcaster.'}`,catalogId:item.id});
 }
 const target=role==='home'?game.home:role==='away'?game.away:teamId;
 const siriusLink=listeningLink('sirius',game,target,role);if(siriusLink)result.push({type:'external',rank:100,...siriusLink});
 const nflLink=listeningLink('nfl',game,target,role);if(nflLink)result.push({type:'external',rank:110,...nflLink});
 return result.sort((a,b)=>a.rank-b.rank||a.label.localeCompare(b.label));
}
export function providerOptions(game,teamId){return ['sirius','nfl'].map(p=>({provider:p,...listeningLink(p,game,teamId)}));}
