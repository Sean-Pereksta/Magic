// Quiet, bounded attack notices. The victim supplies the location; no attacker
// position, target lookup, wall-clock time or browser input state is involved.
const alerts=new WeakMap();
export const ATTACK_ALERT_LIFETIME=6,ATTACK_ALERT_LIMIT=5;
function stateFor(game){let state=alerts.get(game);const now=Number.isFinite(game.time)?game.time:0;if(!state||now<state.time){state={items:[],nextId:1,time:now};alerts.set(game,state);}state.time=now;state.items=state.items.filter(item=>now-item.lastHit<=ATTACK_ALERT_LIFETIME);return state;}
export function recordAttackAlert(game,event){
 const target=event?.target;
 if(!game||event?.type!=='hit'||!(event.amount>0)||target?.team!==0||!Number.isFinite(target.x)||!Number.isFinite(target.y))return false;
 const state=stateFor(game),now=state.time;
 let item=state.items.find(item=>Math.hypot(item.x-target.x,item.y-target.y)<260);
 if(item){item.lastHit=now;if(now-item.pulseAt>=.8){item.pulseAt=now;item.x=target.x;item.y=target.y;}}
 else{item={id:state.nextId++,x:target.x,y:target.y,pulseAt:now,lastHit:now};state.items.push(item);}
 if(state.items.length>ATTACK_ALERT_LIMIT)state.items.sort((a,b)=>b.lastHit-a.lastHit||b.id-a.id).length=ATTACK_ALERT_LIMIT;
 return true;
}
export function getAttackAlerts(game){if(!game)return [];return stateFor(game).items.map(item=>Object.freeze({...item}));}
export function resetAttackAlerts(game){alerts.delete(game);}
export function drawAttackAlerts(c,game,{width,height,sx=width/game.width,sy=height/game.height}){
 const items=getAttackAlerts(game);if(!items.length)return;const now=Number.isFinite(game.time)?game.time:0;
 c.save();c.beginPath();c.rect(0,0,width,height);c.clip();
 for(const item of items){
  const age=now-item.lastHit,fade=Math.min(1,(ATTACK_ALERT_LIFETIME-age)/1.4),x=item.x*sx,y=item.y*sy,phase=((now-item.pulseAt)%1.2)/1.2;
  c.globalAlpha=Math.max(0,fade);c.fillStyle='#ee4941';c.strokeStyle='#fff0ce';c.lineWidth=.8;c.beginPath();c.arc(x,y,2.1,0,Math.PI*2);c.fill();c.stroke();
  for(let ring=0;ring<2;ring++){const p=(phase+ring*.5)%1;c.globalAlpha=Math.max(0,fade*(1-p)*.82);c.strokeStyle='#f34b43';c.lineWidth=1.5;c.beginPath();c.arc(x,y,3+p*7,0,Math.PI*2);c.stroke();}
 }
 c.restore();
}
