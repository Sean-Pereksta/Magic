'use strict';
const { createHash } = require('node:crypto');
const BASE_TROOPS=['rifleman','vrocket','vguard','miner','medic','suppressor','shieldcap','marksman','emp','phaser','blade','storm','guardian','wardmaker','architect','golem','runebinder','hellspawn','roach','defiler','blood','spitter','matron','viper','carrion','burrower'];
const UNLOCKS={6:'signalhawk',12:'chronoweaver',18:'siphonleech',20:'railbreaker',24:'miragewarden',26:'hiveherald'};
const NEW_TROOPS=Object.values(UNLOCKS);
const ALL_TROOPS=[...BASE_TROOPS,...NEW_TROOPS];
const FORGE_COSTS={1:{cards:10,dust:100},2:{cards:30,dust:350}};
const RANKING={sameOpponentResultsPerDay:3,earlyForfeitPenalty:25};
const REWARDS={pvp:{cards:8,dust:20,cap:10},bot:{cards:4,dust:10,cap:10},campaign:{cards:6,dust:20,cap:30},boss:{cards:12,dust:40,cap:30},replay:{cards:2,dust:5,cap:5},endless:{cards:4,dust:10,cap:10}};
const ROUND_WIN_CARDS=3,ROUND_WIN_DAILY_CAP=70;
const FORGE_PACKS=[
  {id:'vanir',name:'Vanir Reinforcements',faction:'Vanir',dust:50,keys:['rifleman','vguard','medic']},
  {id:'aesir',name:'Aesir Reinforcements',faction:'Aesir',dust:50,keys:['phaser','blade','guardian']},
  {id:'helions',name:'Helion Reinforcements',faction:'Helions',dust:50,keys:['hellspawn','roach','defiler']},
  {id:'war-chest',name:'War Chest',faction:null,dust:90,keys:['vrocket','marksman','storm','wardmaker','spitter','viper']}
].map(({keys,...pack})=>({...pack,cards:keys.map(troopKey=>({troopKey,quantity:5})),totalCards:keys.length*5}));
class ForgeError extends Error {
  constructor(code,message,status=400){super(message);this.name='ForgeError';this.code=code;this.status=status;}
}
const copy = value=>JSON.parse(JSON.stringify(value));
function fail(condition,code,message,status=400){if(!condition)throw new ForgeError(code,message,status);}
function onlyKeys(value,keys) { fail(value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>keys.includes(key)),'INVALID_ARGUMENT','Only documented input fields are accepted.');return value; }
function safeId(value,label='Identifier') { fail(typeof value==='string'&&/^[A-Za-z0-9_-]{1,128}$/.test(value),'INVALID_ARGUMENT',label+' is invalid.');return value; }
function safeUsername(value){fail(typeof value==='string'&&value.length>0&&value.length<=80&&!/[\/\x00-\x1F]/.test(value),'INVALID_ARGUMENT','Username is invalid.');return value;}
function newProfile(uid,username,mmr=1000,now=Date.now()) {
  const rating=Number(mmr);
  return {schemaVersion:1,uid,username,crowncouncilElo:Number.isFinite(rating)&&rating>=0?Math.round(rating):1000,forgeDust:0,
    troops:Object.fromEntries(ALL_TROOPS.map(key=>[key,{forgeTier:1,cards:0,blueprints:0,unlocked:BASE_TROOPS.includes(key)}])),
    campaign:{missions:{},endlessUnlocked:false,endlessBest:0},wins:0,losses:0,activeSessionId:null,rewardTarget:null,rewardDays:{},createdAt:now,updatedAt:now};
}
function upgrade(profile,troopKey,now=Date.now()) {
  fail(ALL_TROOPS.includes(troopKey),'INVALID_ARGUMENT','Unknown troop.');
  fail(!profile.activeSessionId,'MATCH_ACTIVE','Finish or abandon the active match before upgrading.',409);
  const unit=profile.troops[troopKey];fail(unit?.unlocked,'TROOP_LOCKED','Unlock this troop before upgrading.');
  const cost=FORGE_COSTS[unit.forgeTier];fail(cost,'MAX_TIER','This troop is already Elite.');
  fail(unit.cards>=cost.cards&&profile.forgeDust>=cost.dust,'INSUFFICIENT_RESOURCES','More matching cards and Forge Dust are required.',409);
  const next=copy(profile);next.troops[troopKey].cards-=cost.cards;next.forgeDust-=cost.dust;next.troops[troopKey].forgeTier++;next.updatedAt=now;
  return {profile:next,transaction:{troopKey,fromTier:unit.forgeTier,toTier:unit.forgeTier+1,cardsSpent:cost.cards,dustSpent:cost.dust,completedAt:now}};
}
function buyPack(profile,packId,now=Date.now()){
  const pack=FORGE_PACKS.find(item=>item.id===packId);fail(pack,'INVALID_PACK','Choose a card pack.');
  fail(!profile.activeSessionId,'MATCH_ACTIVE','Finish your current match before buying packs.',409);
  fail(pack.cards.every(card=>profile.troops[card.troopKey]?.unlocked),'TROOP_LOCKED','Unlock the troops in this pack first.');
  fail(Number.isSafeInteger(profile.forgeDust)&&profile.forgeDust>=pack.dust,'INSUFFICIENT_RESOURCES',`This pack requires ${pack.dust} Forge Dust.`,409);
  const next=copy(profile);next.forgeDust-=pack.dust;for(const card of pack.cards)next.troops[card.troopKey].cards+=card.quantity;next.updatedAt=now;
  return {profile:next,transaction:{packId:pack.id,dustSpent:pack.dust,cards:copy(pack.cards),completedAt:now}};
}
function missionNumber(id) {
  if(Number.isInteger(id)&&id>=1&&id<=30)return id;
  const canonical=/^forge-(\d{2})$/.exec(String(id));
  if(canonical){const n=Number(canonical[1]);fail(n>=1&&n<=30,'INVALID_MISSION','Unknown campaign mission.');return n;}
  const match=/^(?:c|ch)?([1-5])(?:m|[-_])([1-6])$/.exec(String(id));
  if(match)return (Number(match[1])-1)*6+Number(match[2]);
  const n=Number(String(id).replace(/^mission[-_]/,''));
  fail(Number.isInteger(n)&&n>=1&&n<=30,'INVALID_MISSION','Unknown campaign mission.');return n;
}
function missionId(n) { return `forge-${String(n).padStart(2,'0')}`; }
function canStart(profile,{mode,missionId:requested}) {
  fail(['campaign','bot','endless','pvp'].includes(mode),'INVALID_MODE','Unsupported match mode.');
  fail(!profile.activeSessionId,'MATCH_ACTIVE','Resume or abandon your active match.',409);
  if(mode==='campaign') {
    const n=missionNumber(requested);
    fail(n===1||profile.campaign.missions[missionId(n-1)]?.completed,'MISSION_LOCKED','Complete the previous mission first.',409);
    return {missionNumber:n,missionId:missionId(n)};
  }
  if(mode==='endless')fail(profile.campaign.endlessUnlocked,'MODE_LOCKED','Complete the campaign to unlock the Last Crucible challenge.');
  return {missionNumber:null,missionId:null};
}
function cardRewards(profile,count,seed) {
  const next=copy(profile),cards=[],unlocks=[];
  for(let i=0;i<count;i++) {
    const hash=parseInt(createHash('sha256').update(seed+':'+i).digest('hex').slice(0,8),16);
    let pool=ALL_TROOPS.filter(k=>next.troops[k].unlocked&&next.troops[k].forgeTier<3);
    if(!pool.length)pool=BASE_TROOPS;
    if(hash%7===0)pool=NEW_TROOPS.filter(k=>!next.troops[k].unlocked);
    if(!pool.length)pool=BASE_TROOPS;
    // Every third drop progresses a player-selected unlocked troop. Remaining
    // drops favor the five least collected troop types for duplicate protection.
    const target=next.rewardTarget;
    let key;
    if(i%3===0&&target&&next.troops[target]?.unlocked&&next.troops[target].forgeTier<3)key=target;
    else { pool.sort((a,b)=>(next.troops[a].cards+next.troops[a].blueprints)-(next.troops[b].cards+next.troops[b].blueprints));key=pool[hash%Math.min(5,pool.length)]; }
    const unit=next.troops[key],blueprint=!unit.unlocked;
    if(blueprint){unit.blueprints++;if(unit.blueprints>=15){unit.unlocked=true;unit.blueprints-=15;unlocks.push(key);}}
    else unit.cards++;
    const existing=cards.find(c=>c.troopKey===key&&c.blueprint===blueprint);if(existing)existing.quantity++;else cards.push({troopKey:key,quantity:1,blueprint});
  }
  return {profile:next,cards,unlocks};
}
function ratingDecision(profiles,session,now=Date.now()) {
  const ratings=profiles.map(p=>Math.max(0,Number(p.crowncouncilElo)||0)),delta=[0,0];
  const decision={delta,sourceMmr:ratings,counted:false,reason:null};
  if(session.mode!=='pvp'||!session.ranked)return decision;
  const day=new Date(now).toISOString().slice(0,10);
  const repeated=profiles.some((p,side)=>(p.rewardDays[day]?.['rated_opponent_'+session.participants[1-side]]||0)>=RANKING.sameOpponentResultsPerDay);
  if(repeated){decision.reason='Daily rated-result limit against this opponent reached.';return decision;}
  decision.counted=true;
  const winner=session.winnerSide,loser=1-winner;
  const early=session.forfeitUid&&(!(session.completedBattles>0)||now-session.createdAt<20000);
  if(early){delta[loser]=-Math.min(RANKING.earlyForfeitPenalty,ratings[loser]);decision.reason='Early surrender: quitter penalty only; no winner rating gain.';return decision;}
  const requested=Math.max(10,Math.round(25*(1+(ratings[loser]-ratings[winner])/4000)));
  // Ranked transfers cannot create points from an opponent already at the floor.
  const transfer=Math.min(requested,ratings[loser]);delta[winner]=transfer;delta[loser]=transfer?-transfer:0;
  if(!transfer)decision.reason='Opponent rating is at the floor; no rating transfer.';
  return decision;
}
function settle(profile,session,side,{now=Date.now(),opponentUid=null}={}) {
  fail(session.phase==='game-over'&&session.serverValidated===true,'RESULT_UNVERIFIED','Only an authoritative completed session can produce rewards.',409);
  fail(session.participants[side]===profile.uid,'PERMISSION_DENIED','This result does not belong to this profile.',403);
  const won=session.winnerSide===side;
  let next=copy(profile);next.activeSessionId=null;next.updatedAt=now;
  if(won)next.wins++;else next.losses++;
  const day=new Date(now).toISOString().slice(0,10);
  const ledger=copy(next.rewardDays[day]||{});
  // Keep a bounded seven-day anti-farming window.
  next.rewardDays=Object.fromEntries(Object.entries(next.rewardDays).filter(([key])=>key>=new Date(now-7*86400000).toISOString().slice(0,10)));
  let kind=session.mode;
  const num=session.missionNumber;
  const first=session.mode==='campaign'&&!next.campaign.missions[session.missionId]?.completed;
  if(session.mode==='campaign')kind=first?(num%6===0?'boss':'campaign'):'replay';
  let config=REWARDS[kind]||REWARDS.bot;
  let eligible=won&&(ledger[kind]||0)<config.cap&&(!session.forfeitUid||session.completedBattles>0&&now-session.createdAt>=20000);
  if(session.mode==='pvp'&&opponentUid){if((ledger['opponent_'+opponentUid]||0)>=2)eligible=false;}
  const receipt={sessionId:session.id,uid:profile.uid,won,mode:session.mode,missionId:session.missionId||null,cards:[],forgeDust:0,unlocks:[],stars:0,reason:won&&!eligible?(session.forfeitUid&&(!(session.completedBattles>0)||now-session.createdAt<20000)?'An early surrender does not award cards or Forge Dust.':'Daily reward limit reached.'):null,completedAt:now,mmrDelta:0};
  if(won&&session.mode==='campaign') {
    const old=next.campaign.missions[session.missionId]||{};
    const lives=session.lives[side]; const stars=lives>=3?3:lives>=2?2:1;
    next.campaign.missions[session.missionId]={completed:true,stars:Math.max(old.stars||0,stars),clears:(old.clears||0)+1,lastCompletedAt:now};receipt.stars=stars;
    if(UNLOCKS[num]&&!next.troops[UNLOCKS[num]].unlocked){next.troops[UNLOCKS[num]].unlocked=true;receipt.unlocks.push(UNLOCKS[num]);}
    if(num===30)next.campaign.endlessUnlocked=true;
  }
  if(won&&session.mode==='endless')next.campaign.endlessBest=Math.max(next.campaign.endlessBest||0,session.challengeLevel||1);
  const playedRounds=Number.isInteger(session.roundWins?.[side])?Math.max(0,Math.min(7,session.roundWins[side])):0;
  const roundEligible=!session.forfeitUid||session.completedBattles>0&&now-session.createdAt>=20000;
  const rewardedRounds=roundEligible?Math.min(playedRounds,Math.max(0,ROUND_WIN_DAILY_CAP-(ledger.round||0))):0;
  receipt.roundWins=playedRounds;receipt.roundCards=rewardedRounds*ROUND_WIN_CARDS;
  const cardCount=(eligible?config.cards:0)+receipt.roundCards;
  if(cardCount){const grant=cardRewards(next,cardCount,session.id+':'+profile.uid);next=grant.profile;receipt.cards=grant.cards;receipt.unlocks.push(...grant.unlocks);}
  if(rewardedRounds)ledger.round=(ledger.round||0)+rewardedRounds;
  if(eligible) {
    receipt.forgeDust=config.dust;next.forgeDust+=config.dust;ledger[kind]=(ledger[kind]||0)+1;
    if(opponentUid)ledger['opponent_'+opponentUid]=(ledger['opponent_'+opponentUid]||0)+1;
  }
  next.rewardDays[day]=ledger;
  // Human ranked results only. No bot result can alter competitive rating.
  if(session.mode==='pvp'&&session.ranked) {
    const decision=session.ratingDecision;
    fail(decision&&Array.isArray(decision.delta),'RATING_UNVERIFIED','Ranked transfer needs the authoritative pair decision.',409);
    receipt.mmrDelta=decision.delta[side];receipt.ratingReason=decision.reason;
    next.crowncouncilElo=Math.max(0,next.crowncouncilElo+receipt.mmrDelta);
    if(decision.counted){const key='rated_opponent_'+session.participants[1-side];ledger[key]=(ledger[key]||0)+1;next.rewardDays[day]=ledger;}
  }
  return {profile:next,receipt};
}
module.exports={ForgeError,fail,onlyKeys,safeId,safeUsername,newProfile,upgrade,buyPack,settle,canStart,ratingDecision,RANKING,missionNumber,missionId,cardRewards,FORGE_COSTS,FORGE_PACKS,REWARDS,ROUND_WIN_CARDS,ROUND_WIN_DAILY_CAP,ALL_TROOPS,BASE_TROOPS,NEW_TROOPS};
