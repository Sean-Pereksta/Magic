import { alive, armiesOf, atWar, distance, kingdom, pair, relation, settlements, strength, treaty } from './core.mjs';
import { planningView } from './ai-knowledge.mjs';
import { appendConversation, contact } from './living.mjs';
import { court, isAiHouse, humanControlledHouseIds } from './house-control.mjs';

const clamp=(n,min=0,max=100)=>Math.max(min,Math.min(max,n));
const power=armies=>armies.reduce((n,a)=>n+strength(a)*(a.confidence??1),0);
export const DESPERATION_BANDS=['Confident','Concerned','Desperate','Collapsing','Broken'];
const bandFor=n=>n>=90?'Broken':n>=75?'Collapsing':n>=55?'Desperate':n>=30?'Concerned':'Confident';
const capitalOf=(s,id)=>Object.values(s.tiles).find(t=>t.capital===id)?.id||s.founding?.houses[id]?.capital||settlements(s,id)[0]?.id||null;

export function recordWarBaseline(s,a,b,{legacy=false}={}) {
  if(s.knowledgeView)return null;
  s.warBaselines ||= {};
  const id=pair(a,b);
  if(s.warBaselines[id])return s.warBaselines[id];
  const started=legacy?(s.diplomacy.warHistory||[]).findLast(w=>[w.attacker,w.defender].includes(a)&&[w.attacker,w.defender].includes(b))?.turn??s.turn:s.turn;
  const sides={};
  for(const house of [a,b]){
    const current=settlements(s,house).length;
    // For an old save, only recorded captures can reconstruct prior holdings.
    // Missing history is never invented to make capitulation easier.
    const captures=legacy?s.militaryEvents.filter(e=>e.turn>=started&&e.action==='capture'&&['city','town'].includes(s.tiles[e.tile]?.building)):[];
    const lost=captures.filter(e=>e.defender===house).length,gained=captures.filter(e=>e.attacker===house).length;
    sides[house]={settlements:Math.max(current,current+lost-gained),militaryStrength:power(armiesOf(s,house)),capital:capitalOf(s,house)};
  }
  return s.warBaselines[id]={started,sides,outreach:{},legacy};
}
export function ensureWarBaselines(s) {
  if(s.knowledgeView)return;
  s.warBaselines ||= {};
  for(const id of Object.keys(s.warBaselines))if(!s.wars.includes(id))delete s.warBaselines[id];
  for(const war of s.wars){const [a,b]=war.split(':');recordWarBaseline(s,a,b,{legacy:true});}
}

export function warDesperation(s,house,enemy) {
  if(!atWar(s,house,enemy)||!kingdom(s,house)||!kingdom(s,enemy))return null;
  // Assessment is for the ruler's own knowledge. It is not a secret enemy census.
  // Projected clients receive qualitative positions separately from the controller.
  if(s.knowledgeView)return null;
  const baseline=s.warBaselines?.[pair(house,enemy)] || recordWarBaseline(s,house,enemy,{legacy:true});
  const start=baseline.sides[house],view=planningView(s,house),towns=settlements(s,house),own=power(armiesOf(s,house));
  const enemies=s.kingdoms.filter(k=>atWar(s,house,k.id)).map(k=>k.id);
  const observed=power(armiesOf(view,enemy));
  const totalPressure=power(view.armies.filter(a=>enemies.includes(a.owner)));
  // Count useful, observed allied forces close enough to assist, already at war
  // with this opponent, and belonging to a trusted ally. Paper pacts add nothing.
  const help=power(view.armies.filter(a=>a.owner!==house&&!enemies.includes(a.owner)&&
    treaty(s,house,a.owner,'alliance')&&atWar(s,a.owner,enemy)&&relation(s,house,a.owner).trust>=30&&
    towns.some(t=>distance(view.tiles[a.tile],t)<=6)))*.7;
  const effective=own+help,ratio=observed/Math.max(1,effective);
  const lost=clamp(1-towns.length/Math.max(1,start.settlements),0,1),armyLost=clamp(1-own/Math.max(1,start.militaryStrength),0,1);
  const capital=s.tiles[start.capital],fallen=!!capital&&capital.owner!==house;
  const nearCapital=capital?view.armies.filter(a=>enemies.includes(a.owner)&&!a.remembered&&distance(view.tiles[a.tile],capital)<=3):[];
  const threatened=nearCapital.length>0,besiegers=capital?nearCapital.filter(a=>distance(view.tiles[a.tile],capital)<=1):[];
  const recent=view.militaryEvents.filter(e=>e.turn>=baseline.started&&s.turn-e.turn<=4&&[e.attacker,e.defender].includes(house));
  const siege=!fallen&&!!capital&&(besiegers.length>0&&power(besiegers)>effective || threatened&&recent.some(e=>e.tile===capital.id&&e.action==='siege'&&s.turn-e.turn<=1));
  const battles=recent.filter(e=>e.action==='battle'&&e.winner);
  let defeats=0;for(const event of [...battles].reverse()){if(event.winner===house)break;defeats++;}
  const captures=recent.filter(e=>e.action==='capture'&&e.defender===house).length;
  let score=lost*30+armyLost*25+clamp((ratio-1)/4,0,1)*20+(fallen?18:siege?10:threatened?6:0)+Math.min(3,defeats)*3+Math.min(3,captures)*2+Math.min(3,enemies.length-1)*2;
  // Severe, observed collapse can break a one-city House too. Merely having one
  // city from the start, or a larger unobserved opponent, cannot satisfy this.
  const destroyed=armyLost>=.85||own<=5&&ratio>=20;
  const majorCollapse=fallen||lost>=.6&&start.settlements>=3||destroyed&&(threatened||lost>=.4)||siege&&lost>=.5;
  if(majorCollapse&&ratio>=10&&destroyed)score=Math.max(score,90);
  if(majorCollapse&&ratio>=20&&destroyed&&(siege||fallen||lost>=.6))score=Math.max(score,97);
  if(fallen&&ratio>=3&&(lost>=.5||armyLost>=.75))score=Math.max(score,92);
  if(lost>=.6&&ratio>=5&&armyLost>=.75)score=Math.max(score,90);
  // Real relief or a competitive restored army must immediately undo a crisis
  // floor. Rounded bands and outreach high-water marks avoid notification churn.
  if(ratio<2&&!fallen)score=Math.min(score,74);
  if(ratio<1.25)score=Math.min(score,54);
  score=Math.round(clamp(score));
  const actualRatio=power(armiesOf(s,enemy))/Math.max(1,effective);
  const decisive=fallen?ratio>=3&&actualRatio>=3:ratio>=5&&actualRatio>=5;
  return {score,band:bandFor(score),house,enemy,settlementLoss:lost,armyLoss:armyLost,ownStrength:own,observedEnemyStrength:observed,alliedAssistance:help,
    ratio,capitalStatus:fallen?'captured':siege?'under siege':threatened?'threatened':'secure',defeats,recentCaptures:captures,multipleEnemies:enemies.length,
    majorCollapse,decisive,eligible:score>=90&&majorCollapse&&decisive&&alive(s,house)&&alive(s,enemy),totalPressure};
}
export function submissionResistance(s,house,enemy) {
  const k=kingdom(s,house),r=relation(s,house,enemy),rep=kingdom(s,enemy).reputation||{};
  return Math.round(clamp(20+k.honor*15+k.ambition*20+k.aggression*8+k.paranoia*10+
    (r.grievance||0)*.2+Math.max(0,-r.trust)*.2-Math.max(0,r.trust)*.2-((r.reliability??50)-50)*.15-
    Math.min(40,r.fear||0)*.05+Math.min(20,(rep.broken||0)*4+(rep.envoysKilled||0)*8)-Math.min(8,(rep.kept||0)*2),10,98));
}
export function capitulationCheck(s,house,enemy,intent={}) {
  const war=warDesperation(s,house,enemy);
  if(!war?.eligible)return {eligible:false,willing:false,war,reason:'Negotiate peace before requesting allegiance. Capitulation requires material military collapse.'};
  const resistance=submissionResistance(s,house,enemy);
  // Actual relief in the ratified settlement buys breathing room; unverified
  // promises of mercy do not act as a new persuasion currency.
  const relief=Math.min(20,(intent.giveAmount||0)/5);
  const pressure=30+(war.score-90)*3+(war.capitalStatus==='captured'?15:0)+relief;
  return {eligible:true,willing:pressure>=resistance,war,resistance,
    reason:pressure>=resistance?'Our military position is catastrophic. These terms preserve our House; ratification ends this war and places us under your crown.':
      relation(s,house,enemy).trust<0?'You have broken our armies. What assurance have I that kneeling will preserve my people? Offer real relief and terms worthy of trust.':'You have broken our armies, but not our resolve. Give our House a dignified settlement and material means to survive.'};
}
export function qualitativeWarPosition(s,house,enemy) {
  if(s.knowledgeView)return s.warPositions?.[`${house}:${enemy}`]||null;
  const w=warDesperation(s,house,enemy);if(!w)return null;
  return {warPosition:w.score>=90?'catastrophic':w.score>=75?'collapsing':w.score>=55?'dire':w.score>=30?'costly':'competitive',
    desperation:w.band.toLowerCase(),territorialSituation:w.settlementLoss>=.6?'most pre-war settlements lost':w.settlementLoss>0?'some pre-war settlements lost':'holdings largely intact',
    militarySituation:w.ratio>=5?'observed enemy forces overwhelmingly exceed forces available to us':w.ratio>=2?'observed enemy forces have a clear advantage':'the observed military balance remains competitive or uncertain',
    capitalStatus:w.capitalStatus,recentTrend:w.defeats>=2?'repeated defeats':w.defeats?'a recent defeat':'no sustained recent defeats',
    alliedAssistance:w.alliedAssistance>0?'meaningful allied forces are within reach':'no meaningful allied relief currently within reach',
    capitulationEligibility:w.eligible?'possible':'unavailable',submissionAttitude:submissionResistance(s,house,enemy)>=70?'highly resistant':submissionResistance(s,house,enemy)>=45?'resistant':'open to survival terms'};
}
export function worstWarPosition(s,house) {
  return s.kingdoms.filter(k=>atWar(s,house,k.id)).map(k=>warDesperation(s,house,k.id)).filter(Boolean).sort((a,b)=>b.score-a.score)[0]||null;
}
export function warOpening(position) {
  return !position?null:position.desperation==='broken'?'Our armies have been shattered. I will not pretend the field remains ours. The survival of my House must be part of any terms.':
    position.desperation==='collapsing'?'Our position is collapsing. Enough blood has been spent; state the terms under which our people may endure.':
    position.desperation==='desperate'?'We have paid dearly for this war. I would hear serious terms for peace.':
    position.desperation==='concerned'?'You have gained ground, but this war is not decided. We can discuss a reasonable peace.':null;
}
export function desperateDiplomacy(s) {
  ensureWarBaselines(s);
  for(const house of s.kingdoms.filter(k=>isAiHouse(s,k.id)&&alive(s,k.id)).map(k=>k.id)) {
    for(const enemy of s.kingdoms.filter(k=>atWar(s,house,k.id)&&alive(s,k.id)).map(k=>k.id)){
      const w=warDesperation(s,house,enemy),baseline=s.warBaselines[pair(house,enemy)],level=DESPERATION_BANDS.indexOf(w.band);
      if(level<2||level<=(baseline.outreach[house]??0))continue;
      baseline.outreach[house]=level;
      const position=qualitativeWarPosition(s,house,enemy),type=w.eligible?'VASSALAGE':'PEACE';
      const proposal={type,duration:10,giveResource:'gold',giveAmount:0,receiveResource:'food',receiveAmount:0,targetId:''};
      if(humanControlledHouseIds(s).includes(enemy)){
        appendConversation(s,house,'ruler',`${warOpening(position)} ${w.eligible?'If there is a place for our House beneath your crown, send your terms of allegiance.':'Send your proposal for peace.'}`,
          {actorHouseId:enemy,unread:true,kind:'war-desperation',proposal});
        court(s,enemy).offers[house]=[proposal];
      }
      for(const ally of s.kingdoms.filter(k=>k.id!==house&&treaty(s,k.id,house,'alliance')&&humanControlledHouseIds(s).includes(k.id)))
        contact(s,house,`war-relief:${enemy}`,`Our war with ${kingdom(s,enemy).name} threatens the survival of our House. Can you help defend our remaining settlements or send supplies?`,4,ally.id);
    }
  }
}
export function validateWarBaselines(s) {
  if(s.warBaselines===undefined&&!s.wars.length)return;
  s.warBaselines ||= {};
  const fail=()=>{throw Error('Damaged war baseline data.');};
  if(typeof s.warBaselines!=='object'||Array.isArray(s.warBaselines)||Object.keys(s.warBaselines).length>66)fail();
  for(const [id,w]of Object.entries(s.warBaselines)){
    const parties=id.split(':');
    if(parties.length!==2||parties[0]===parties[1]||parties.some(p=>!kingdom(s,p))||!w||!Number.isInteger(w.started)||w.started<0||w.started>s.turn||!w.sides||!w.outreach)fail();
    for(const house of parties){const b=w.sides[house];if(!b||!Number.isInteger(b.settlements)||b.settlements<0||b.settlements>1000||!Number.isFinite(b.militaryStrength)||b.militaryStrength<0||b.militaryStrength>1e9||b.capital!==null&&!s.tiles[b.capital])fail();}
    if(Object.entries(w.outreach).some(([house,n])=>!parties.includes(house)||!Number.isInteger(n)||n<0||n>4))fail();
  }
  ensureWarBaselines(s);
}
