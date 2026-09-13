import {team} from './core.mjs';
import {sirius} from './provider-data.mjs';
export const PROVIDERS = {
 auto:{name:'Best available',description:'Direct game feeds first, then a supported station widget. Provider options appear when neither is available.'},
 sirius:{name:'SiriusXM',description:'Home and away calls for all 32 teams. Opens the official SiriusXM player; a compatible subscription and provider login are required.',loginUrl:'https://www.siriusxm.com/player/',infoUrl:'https://www.siriusxm.com/sports/nfl'},
 nfl:{name:'NFL+',description:'Every game’s home, away and national calls in the US. Opens NFL+; sign in and select Live Audio there.',loginUrl:'https://www.nfl.com/plus/',infoUrl:'https://support.nfl.com/hc/en-us/articles/35869715432468-What-games-can-I-listen-to-with-live-audio-on-NFL'}
};
// Explicitly supplied iHeart oEmbed endpoints. Widget permission is not a promise
// of NFL game availability: the provider owns playback, ads and geographic checks.
export const widgets = [
 {id:'iheart-det',team:'DET',name:'97.1 The Ticket',provider:'iHeart',kind:'flagship',language:'en',url:'https://www.iheart.com/live/971-the-ticket-10827/',embedUrl:'https://www.iheart.com/live/971-the-ticket-10827/?embed=true&cid=oembed&keyid%5B0%5D=97.1%20The%20Ticket&sc=live_widget',checkedAt:'2026-09-13'},
 {id:'iheart-phi',team:'PHI',name:'SportsRadio 94WIP',provider:'iHeart',kind:'flagship',language:'en',url:'https://www.iheart.com/live/sportsradio-94wip-10934/',embedUrl:'https://www.iheart.com/live/sportsradio-94wip-10934/?embed=true&cid=oembed&keyid%5B0%5D=SportsRadio%2094WIP&sc=live_widget',checkedAt:'2026-09-13'}
];
export function validWidget(widget){
 try{const url=new URL(widget.embedUrl);return url.origin==='https://www.iheart.com'&&/^\/live\/[a-z0-9-]+-\d+\/$/.test(url.pathname)&&url.searchParams.get('embed')==='true';}catch{return false;}
}
export function widgetFor(teamId,preferred){return widgets.find(w=>w.team===teamId&&w.id===preferred)||widgets.find(w=>w.team===teamId);}
export function siriusEvent(game){return sirius.events.find(e=>e.home===team(game.home)?.name&&e.away===team(game.away)?.name&&Math.abs(Date.parse(e.date)-Date.parse(game.date))<60000);}
export function listeningLink(provider,game,teamId,role){
 if(provider==='nfl')return {url:PROVIDERS.nfl.loginUrl,label:'Open NFL+ live audio',detail:'Provider login required. Choose this game’s Live Audio on NFL+.'};
 if(provider!=='sirius'||!team(teamId))return null;
 if(role==='national'||role==='spanish'){
  const event=siriusEvent(game), url=event?.[role];
  if(!url)return null;
  return {url,label:`Open SiriusXM ${role} broadcast`,detail:'Scheduled provider broadcast. Subscription and provider login required.'};
 }
 const item=sirius.teams[team(teamId).name];
 if(!item)return null;
 return {url:item.url,label:`Open ${team(teamId).name} on SiriusXM`,detail:`Team audio · streaming channel ${item.channel}. Subscription and provider login required.`};
}
// URL validation also guards a future catalog refresh from creating arbitrary
// navigation destinations. Provider login always happens on the provider origin.
export function safeListeningUrl(value){
 try{const u=new URL(value);return u.protocol==='https:'&&['sxm.app.link','www.siriusxm.com','www.nfl.com','www.iheart.com','www.westwoodonesports.com'].includes(u.hostname)&&!u.username&&!u.password;}catch{return false;}
}
export function providerOptions(game,teamId){return ['sirius','nfl'].map(p=>({provider:p,...listeningLink(p,game,teamId)}));}
