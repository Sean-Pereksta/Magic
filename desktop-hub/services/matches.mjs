import {randomUUID,randomBytes} from 'node:crypto';
import {accountKey,seasonAt} from './ledger.mjs';

const ticket=()=>randomBytes(32).toString('base64url');
const fail=(status,message)=>Object.assign(new Error(message),{status});
export function manageMatches(ledger,createEngine,now=Date.now) {
  const entrants=new Map(),matches=new Map(),tickets=new Map(),sessions=new Map();
  const remove=entry=>{if(entrants.get(entry.player.uid)===entry)entrants.delete(entry.player.uid);tickets.delete(entry.handoff);sessions.delete(entry.cookie);};
  function describe(entry) {
    if(entry.failed)throw fail(503,'The match server could not open this game.');
    if(entry.receipt)return {state:'complete',receipt:entry.receipt};
    if(entry.seat){
      if(!entry.handoff){entry.handoff=ticket();tickets.set(entry.handoff,entry);}
      return {state:'ready',handoff:entry.handoff};
    }
    return {state:'waiting',game:entry.game};
  }
  async function enqueue(player,game) {
    if(!['bannerlands','catandmouse'].includes(game))throw fail(400,'Choose a ranked queue.');
    let existing=entrants.get(player.uid);
    if(existing&&existing.game===game){existing.seen=now();return describe(existing);}
    if(existing?.match)throw fail(409,'Finish or leave your current ranked session.');
    if(existing)remove(existing);
    if(entrants.size>=250)throw fail(503,'The match queue is full.');
    const rival=[...entrants.values()].find(e=>e.game===game&&!e.match&&!e.failed&&e.seen>now()-30000&&accountKey(e.player)!==accountKey(player));
    const entry={game,player,seen:now()};entrants.set(player.uid,entry);
    if(!rival)return describe(entry);
    const entries=[rival,entry],players=entries.map(e=>e.player);
    const match={id:randomUUID(),game,players,entries,season:seasonAt(new Date(now())),created:now()};
    matches.set(match.id,match);
    for(const e of entries)e.match=match;
    try{
      match.engine=await createEngine(match);
      for(const [i,e] of entries.entries())e.seat=match.engine.seats[i];
    }catch(error){
      matches.delete(match.id);
      for(const e of entries){remove(e);e.failed=true;}
      throw fail(503,'The match server could not start this queue.');
    }
    return describe(entry);
  }
  function claim(key) {
    const e=tickets.get(key);
    if(!e)throw fail(410,'This game invitation has expired.');
    tickets.delete(key);e.handoff=null;
    if(e.cookie)sessions.delete(e.cookie);
    e.cookie=ticket();sessions.set(e.cookie,e);
    const {url,...seat}=e.seat;
    return {cookie:e.cookie,game:e.game,...seat};
  }
  function cancel(player) {
    const entry=entrants.get(player.uid);
    if(entry?.match&&!entry.receipt)throw fail(409,'Your ranked match is already reserved.');
    if(entry)remove(entry);
  }
  async function removeMatch(match){
    matches.delete(match.id);for(const e of match.entries)remove(e);
    await match.engine?.close();
  }
  async function tick(){
    for(const entry of entrants.values())if(!entry.match&&entry.seen<now()-30000)remove(entry);
    for(const m of matches.values()) {
      if(!m.engine)continue;
      if(!m.completed&&m.engine.started()&&!m.played) {
        const ids=Object.keys(ledger.counts().games);
        const catalogGame=m.game==='catandmouse'&&!ids.includes(m.game)&&ids.includes('cat')?'cat':m.game;
        for(const player of m.players)ledger.activity(accountKey(player),[catalogGame]);
        m.played=true;
      }
      const outcome=!m.completed&&m.engine.outcome();
      if(outcome){
        const receipt=ledger.record({...m,...outcome});m.completed=now();
        for(const e of m.entries)e.receipt=receipt;
      }
      if((m.completed&&now()-m.completed>5*60000)||(!m.played&&now()-m.created>10*60000)||now()-m.created>6*3600000)await removeMatch(m);
    }
  }
  return {enqueue,cancel,claim,session:key=>sessions.get(key),tick,close:()=>Promise.all([...matches.values()].map(removeMatch))};
}
