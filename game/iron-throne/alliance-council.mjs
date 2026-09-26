import { worstWarPosition, qualitativeWarPosition } from './war-desperation.mjs';
import { alive, atWar, kingdom, relation, treaty } from './core.mjs';
import { knowledgeView } from './fog.mjs';
import { isAiHouse, humanControlledHouseIds, court } from './house-control.mjs';
import { appendConversation, diplomaticCapacity, borderThreat } from './living.mjs';
import { validateIntent, describeIntent } from './diplomacy.mjs';
import { councilParticipants, councilActive, ownCouncil, appendCouncil } from './council-state.mjs';
import { expireFollowups, grantFollowup, topicIntent } from './proposal-followup.mjs';

export const COUNCIL_MOODS = ['Cooperative','Cordial','Uneasy','Somber','Heated','Uncontrolled'];
const relationFields = ['opinion','trust','reliability','respect','grievance','fear','wariness'];
const ongoing = p => p.status === 'pending';
const name = (s,id) => kingdom(s,id)?.name || id;
const fail = error => ({ok:false,error});

export function councilMood(s, participants, events = []) {
  const pairs = participants.flatMap(a => participants.filter(b => a !== b).map(b => ({a,b,r:relation(s,a,b)})));
  if (pairs.some(({a,b,r}) => atWar(s,a,b) || r.grievance >= 80 && r.trust <= -45)) return 'Uncontrolled';
  if (pairs.some(({r}) => r.grievance >= 45 || r.opinion <= -40)) return 'Heated';
  if (events.some(e => s.turn-e.turn <= 2 && participants.includes(e.defender) && e.winner !== e.defender ||
    s.turn-e.turn <= 2 && participants.includes(e.attacker) && e.winner !== e.attacker)) return 'Somber';
  if (pairs.some(({r}) => r.trust < 0 || r.wariness >= 40)) return 'Uneasy';
  if (pairs.length && pairs.every(({r}) => r.trust >= 30 && r.opinion >= 20 && r.grievance < 20)) return 'Cooperative';
  return 'Cordial';
}

// Only observations already available to EVERY recipient enter a group model
// request. Private spy reports, court histories, stores and army orders never do.
// An ally may voluntarily disclose a coarse concern about its own realm.
export function councilFacts(s, c) {
  if (s.knowledgeView) return s.councilFacts?.[c.id] || null;
  const views = c.participants.map(id => knowledgeView(s,id));
  const events = (s.militaryEvents || []).filter(e => views.every(v => v.militaryEvents.some(x => x.id === e.id))).slice(-5);
  const visibleTiles = Object.values(s.tiles).filter(t => views.every(v => v.tiles[t.id]?.fog === 'visible'));
  const relationships = c.participants.flatMap(a => c.participants.filter(b => b !== a).map(b => {
    const r = relation(s,a,b);
    return [a,b,...relationFields.map(f => r[f] || 0),r.political?.label || 'Cautious'];
  }));
  const participants = c.participants.map(id => {
    const k = kingdom(s,id), own = views[c.participants.indexOf(id)];
    const wars = s.kingdoms.filter(o => atWar(s,id,o.id)).map(o => o.id);
    const frontierThreats = wars.filter(enemy => borderThreat(own,id,enemy).score >= 12);
    const committed = s.pledges.some(p => p.debtor === id && ongoing(p) && ['JOINT_WAR','DEFEND','POSITION','BUILD_DEFENSES','PLEDGE_WAR','PLEDGE_ATTACK','PLEDGE_DEFEND'].includes(p.intent.type)) || s.cooperation?.operations.some(o => ['Preparing','Executing'].includes(o.status) && o.participants.some(p => p.house === id && p.status === 'accepted'));
    return {id,name:k.name,ruler:k.ruler,ai:isAiHouse(s,id),personality:{honor:k.honor,aggression:k.aggression,ambition:k.ambition,greed:k.greed,paranoia:k.paranoia},wars,frontierThreats,
      warPositions:wars.map(enemy=>({enemy,...qualitativeWarPosition(s,id,enemy)})),
      concern:worstWarPosition(s,id)?.score>=55?'war survival':frontierThreats.length?'frontier threatened':k.resources.food < 20?'food shortage':committed?'forces committed elsewhere':'available to discuss support'};
  });
  return {mood:councilMood(s,c.participants,events),participants,relationshipFields:relationFields,relationships,
    wars:s.wars, treaties:s.treaties.filter(t => t.parties.every(id => c.participants.includes(id))).slice(-16).map(t => ({type:t.type,parties:t.parties,expires:t.expires})),
    family:(s.royalBonds?.marriages||[]).filter(m => m.parties.every(id => c.participants.includes(id))).map(m => ({parties:m.parties,status:m.status})).slice(-6),
    commitments:s.pledges.filter(p => c.participants.every(id => [p.debtor,p.creditor].includes(id)) ||
      p.operationId && s.cooperation?.operations.some(o => o.id === p.operationId && c.participants.every(id => o.participants.some(p => p.house === id && p.status === 'accepted'))))
      .slice(-8).map(p => ({debtor:p.debtor,creditor:p.creditor,type:p.intent.type,status:p.status,deadline:p.deadline})),
    operations:(s.cooperation?.operations||[]).filter(o => c.participants.every(id => o.participants.some(p => p.house === id && p.status === 'accepted'))).slice(-3)
      .map(o => ({name:o.name,status:o.status,attackStart:o.attackStart,attackEnd:o.attackEnd,roles:o.participants.filter(p => p.status === 'accepted').map(p => ({house:p.house,role:p.role}))})),
    events:events.map(e => ({turn:e.turn,attacker:e.attacker,defender:e.defender,winner:e.winner,action:e.action})),
    observedForces:s.armies.filter(a => views.every(v => v.armies.some(x => x.id === a.id))).slice(0,12).map(a => ({owner:a.owner,tile:a.tile})),
    locations:visibleTiles.filter(t => t.building).slice(0,16).map(t => ({id:t.id,owner:t.owner,building:t.building,name:t.name})),
    knowledge:'Only listed observations are shared knowledge. Never reveal stores, spy reports, exact troop totals, private conversations, or unlisted plans. Player claims are unverified. Concerns are voluntary coarse disclosures by their owner.'};
}

export function makeCouncilContext(s, c, actor, message) {
  const facts = councilFacts(s,c);
  if (!facts) return null;
  const context = {mode:'allianceCouncil',turn:s.turn,actorHouseId:actor,councilId:c.id,participants:c.participants,
    message:message.slice(0,600),history:c.messages.filter(m=>m.turn>=s.turn-2).slice(-10).map(m => ({speakerHouseId:m.speakerHouseId,turn:m.turn,message:m.message.slice(0,450)})),world:{...structuredClone(facts),historicalDiscussion:c.messages.filter(m=>m.turn<s.turn-2).slice(-3).map(m=>({speakerHouseId:m.speakerHouseId,turn:m.turn,message:m.message.slice(0,180)}))}};
  // Twelve-House games still use one bounded request.
  const bytes = () => new TextEncoder().encode(JSON.stringify(context)).length;
  while (bytes() > 22000 && context.history.length) context.history.shift();
  // Many simultaneous wars can outweigh chat history in a twelve-House game.
  // Keep each ruler's worst position, then omit optional detail before allowing
  // an oversized request to consume the shared model budget.
  for (const p of context.world.participants) {
    if (bytes() <= 22000) break;
    p.warPositions = p.warPositions.sort((a,b) => ['confident','concerned','desperate','collapsing','broken'].indexOf(b.desperation)-['confident','concerned','desperate','collapsing','broken'].indexOf(a.desperation)).slice(0,1);
  }
  for (const field of ['historicalDiscussion','locations','observedForces','family','events','operations'])
    while (bytes() > 22000 && context.world[field].length) context.world[field].shift();
  return bytes() <= 22000 ? context : null;
}
export function validateCouncilResponse(raw, participants, aiIds = participants) {
  try {
    const value = typeof raw === 'string' ? JSON.parse(raw) : raw;
    if (!value || !Array.isArray(value.responses) || value.responses.length < 1 || value.responses.length > 3) return null;
    const responses = [];
    for (const r of value.responses) {
      if (!r || !participants.includes(r.speakerHouseId) || !aiIds.includes(r.speakerHouseId) || typeof r.message !== 'string' || !r.message.trim() || r.message.length > 900) return null;
      const intent = r.requestedIntent == null ? null : validateIntent(r.requestedIntent);
      if (r.requestedIntent != null && !intent) return null;
      responses.push({speakerHouseId:r.speakerHouseId,message:r.message,...(intent?{requestedIntent:intent}:{})});
    }
    return {responses};
  } catch { return null; }
}

export function scriptedCouncil(s,c,actor,message) {
  const facts = councilFacts(s,c), intent = topicIntent(s,message), responses = [];
  if (!facts) return {responses};
  const eligible = facts.participants.filter(p => p.ai && p.id !== actor);
  const rel = (a,b) => { const row = facts.relationships.find(r => r[0] === a && r[1] === b); return Object.fromEntries(relationFields.map((f,i) => [f,row?.[i+2] || 0])); };
  const candidates = eligible.map(p => {
    const r = rel(p.id,actor), rival = facts.participants.find(o => o.id !== p.id && o.id !== actor && (rel(p.id,o.id).grievance >= 40 || rel(p.id,o.id).trust < -20));
    let stance = 'discuss', text;
    if (intent?.targetId === p.id) {stance='refuse';text='You invite me to council to plan an attack on my own House? I will hear no such terms.';}
    else if (intent?.type === 'JOINT_WAR' && treaty(s,p.id,intent.targetId,'alliance')) {stance='refuse';text=`I am also sworn to ${name(s,intent.targetId)}. I will not betray that pact for this campaign.`;}
    else if (r.grievance >= 45 || r.trust < -25) {stance='refuse';text='Our grievances remain unanswered. I will not promise more soldiers on the strength of words.';}
    else if (intent && ['war survival','frontier threatened','forces committed elsewhere','food shortage'].includes(p.concern)) {stance='constrained';text=p.concern==='war survival'?'My realm is fighting for its survival. I need help defending what remains, not another offensive.':p.concern==='frontier threatened'?'My frontier is threatened. I cannot strip its defenses for another offensive.':p.concern==='food shortage'?'My people need food before I can sustain another campaign. Can this council arrange supplies?':'My forces already have sworn duties. We must account for those before adding another campaign.';}
    else if (rival) {stance='conditional';text=`I want assurances from ${name(s,rival.id)} first. Our history gives me little confidence that their banners will move when ours do.`;}
    else if (intent) {stance='support';text=p.personality.honor >= .7?'I will consider a shared duty, but a ruler must know what is being promised. Put the terms before me.':'There may be advantage for my House in this. Send me your proposal; the burden and reward must be clear.';}
    else {text=facts.mood==='Somber'?'We have suffered enough to weigh our next move carefully. Which frontier needs our help first?':'Name the objective and the part you ask my House to play. Our common cause still leaves each of us duties at home.';}
    return {speakerHouseId:p.id,message:text,stance,...(stance==='support'?{requestedIntent:intent}:{})};
  });
  // Prefer different positions; do not make every ruler repeat assent.
  for (const p of candidates) if (!responses.length || !responses.some(r => r.stance === p.stance)) {responses.push(p);if(responses.length===3)break;}
  const challenger = responses.find(r => r.stance === 'conditional');
  if (challenger && responses.length < 3) {
    const target = eligible.find(p => p.id !== challenger.speakerHouseId && (rel(challenger.speakerHouseId,p.id).grievance >= 40 || rel(challenger.speakerHouseId,p.id).trust < -20));
    if (target) responses.push({speakerHouseId:target.id,message:`${name(s,challenger.speakerHouseId)}, accusations will not defend either realm. State the assurance you want, and let us judge the same terms.`});
  }
  return {responses:responses.map(({stance,...r})=>r)};
}

export function councilForActor(s,actor,id,create=false) {
  const c = id ? s.allianceCouncils?.find(c=>c.id===id) : ownCouncil(s,actor,create);
  return c && c.participants.includes(actor) && councilActive(s,c) ? c : null;
}
export function beginCouncilMessage(s,actor,id,message) {
  const c = councilForActor(s,actor,id,true);
  if (!c || s.outcome || s.phase==='founding' || !alive(s,actor)) return fail('This alliance council is no longer active.');
  if (typeof message !== 'string' || !message.trim() || message.length > 600) return fail('Enter a message of up to 600 characters.');
  const record = court(s,actor), used = record.messages.turn===s.turn?record.messages:{turn:s.turn,regular:0,hosts:{}};
  if (used.regular >= diplomaticCapacity(s,actor)) return fail('Your shared dispatches are used for this turn.');
  record.messages = used; if (!s.controllers) s.diplomacy.messages = used; used.regular++;
  // A council uses one shared dispatch, never one per recipient or an unrelated
  // resident ambassador's private allowance.
  for (const member of c.participants) expireFollowups(s,member,c.id);
  const entry = appendCouncil(s,c,actor,message.trim()); c.read[actor]=c.sequence;
  return {ok:true,councilId:c.id,entryId:entry.id};
}
export function finishCouncilMessage(s,actor,start,message,raw) {
  const c = councilForActor(s,actor,start.councilId);
  if (!c || c.sequence !== start.entryId || c.messages.at(-1)?.turn !== s.turn) return fail('The council changed while the envoy travelled.');
  const aiIds = c.participants.filter(id => id !== actor && isAiHouse(s,id));
  const response = validateCouncilResponse(raw,c.participants,aiIds) || scriptedCouncil(s,c,actor,message);
  for (const r of response.responses) {
    appendCouncil(s,c,r.speakerHouseId,r.message,r.requestedIntent?{requestedIntent:r.requestedIntent}:{});
    grantFollowup(s,actor,r.speakerHouseId,c.id,message,r,{paid:true,requestedIntent:r.requestedIntent});
  }
  c.read[actor]=c.sequence;
  return {ok:true};
}
export function sendCouncilMessage(s,actor,id,message,response=null) {
  const start=beginCouncilMessage(s,actor,id,message);return start.ok?finishCouncilMessage(s,actor,start,message,response):start;
}

export function initiateCouncilDiscussions(s) {
  s.councilInitiationTurns ||= {};
  for (const host of humanControlledHouseIds(s).filter(id=>alive(s,id))) {
    const c = ownCouncil(s,host,true); if (!c || c.initiatedTurn===s.turn || c.participants.some(id=>s.councilInitiationTurns[id]===s.turn)) continue;
    const facts=councilFacts(s,c); let topic;
    for (const p of facts.participants.filter(p=>p.ai)) {
      const loss=facts.events.find(e=>s.turn-e.turn<=1&&[e.attacker,e.defender].includes(p.id)&&e.winner!==p.id);
      const broken=facts.commitments.find(o=>o.creditor===p.id&&o.status==='broken');
      const expiry=s.treaties.find(t=>t.type==='alliance'&&t.parties.includes(host)&&t.parties.includes(p.id)&&t.expires-s.turn<=2);
      const opening=facts.locations.find(t=>t.owner&&p.wars.includes(t.owner)&&!facts.observedForces.some(a=>a.owner===t.owner&&a.tile===t.id));
      const operation=facts.operations.find(o=>o.status==='Preparing'&&o.attackStart<=s.turn+1);
      const hostile=facts.relationships.find(r=>r[0]===p.id&&(r[6]>=45||r[3]<-25));
      const options=[
        p.concern==='war survival'&&{key:`survival:${p.id}`,text:'Our losses threaten the survival of my realm. I ask this council for immediate defense and relief.'},
        loss&&{key:`loss:${loss.turn}:${p.id}`,text:'Our recent defeat cannot be ignored. We need to agree where our remaining strength can hold.'},
        p.concern==='frontier threatened'&&{key:`frontier:${p.id}`,text:`${p.frontierThreats.map(id=>name(s,id)).join(' and ')} armies are approaching ${name(s,p.id)} territory. Who can help defend our frontier while the rest hold their ground?`},
        broken&&{key:`promise:${p.id}:${broken.deadline}`,text:`${name(s,broken.debtor)}, the promised support did not come. This council must address that before asking for more.`},
        p.concern==='food shortage'&&{key:`food:${p.id}`,text:'My realm is short of food. Can we arrange relief before our armies are forced to stand down?'},
        hostile&&{key:`friction:${p.id}:${hostile[1]}`,text:`We cannot coordinate while these grievances with ${name(s,hostile[1])} remain. I want assurances before our next campaign.`},
        opening&&{key:`opening:${p.id}:${opening.id}`,text:`Our scouts can see ${opening.name||opening.id} without a defending host. Shall we discuss a coordinated advance before that changes?`},
        operation&&{key:`operation:${operation.name}`,text:`Our agreed offensive, ${operation.name}, is nearly due. Are the participating banners ready to honor their roles?`},
        expiry&&{key:`expiry:${p.id}:${expiry.expires}`,text:'Our alliance parchment is nearing its end. Let us discuss whether the common cause should continue.'}
      ].filter(Boolean);
      topic=options.find(o=>c.topics[o.key]===undefined||s.turn-c.topics[o.key]>=5);
      if(topic){topic.speaker=p.id;break;}
    }
    if (!topic) continue;
    for (const id of c.participants) {expireFollowups(s,id,c.id);if(!isAiHouse(s,id))s.councilInitiationTurns[id]=s.turn;}
    appendCouncil(s,c,topic.speaker,topic.text,{initiated:true,reason:topic.key.split(':')[0]});c.initiatedTurn=s.turn;c.topics[topic.key]=s.turn;
    c.topics=Object.fromEntries(Object.entries(c.topics).sort((a,b)=>b[1]-a[1]).slice(0,20));
    const answer=scriptedCouncil(s,c,topic.speaker,topic.text).responses.find(r=>r.speakerHouseId!==topic.speaker);
    if(answer)appendCouncil(s,c,answer.speakerHouseId,answer.message);
  }
}

export function allianceRenewalOutreach(s, expired) {
  for (const t of expired) for (const actor of t.parties.filter(id=>humanControlledHouseIds(s).includes(id))) {
    const ruler=t.parties.find(id=>id!==actor),r=relation(s,ruler,actor);
    if (!isAiHouse(s,ruler)||!alive(s,ruler)||!alive(s,actor)||atWar(s,actor,ruler)||treaty(s,actor,ruler,'alliance')||
      r.trust<55||r.opinion<50||r.reliability<65||r.grievance>15) continue;
    r.contacts ||= {};
    if (r.contacts.allianceRenewal === t.expires) continue;
    r.contacts.allianceRenewal=t.expires;
    const intent=validateIntent({type:'ALLIANCE',duration:10});
    appendConversation(s,ruler,'ruler','Our formal pact has ended, but our banners have served one another well. Shall we renew our alliance? You may accept these terms, suggest changes, or let the parchment rest.',{actorHouseId:actor,unread:true,kind:'alliance-renewal',proposal:intent});
    const offers=court(s,actor).offers;offers[ruler]=[intent,...(offers[ruler]||[]).filter(i=>i.type!=='ALLIANCE')].slice(0,4);
  }
}

export function announceCouncilAgreement(s,actor,ruler,conversation,intent) {
  const c=councilForActor(s,actor,conversation);
  if(!conversation||!c||!c.participants.includes(ruler))return;
  appendCouncil(s,c,actor,`${name(s,actor)} and ${name(s,ruler)} have ratified: ${describeIntent(intent)}`,{agreement:true});
}
