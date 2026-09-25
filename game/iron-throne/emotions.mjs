// Personal feelings are directional, bounded simulation state, never model output.
export const FEELINGS = {
  affection:'Affection', fondness:'Fondness', admiration:'Admiration', gratitude:'Gratitude',
  loyalty:'Loyalty', protectiveness:'Protectiveness', attachment:'Personal attachment',
  obligation:'Obligation', jealousy:'Jealousy', envy:'Envy', pride:'Pride',
  humiliation:'Humiliation', bitterness:'Bitterness', rivalry:'Personal rivalry',
  suspicion:'Suspicion', betrayedFriendship:'Betrayed friendship'
};
export const BONDS = ['Trusted Confidant','Brothers/Sisters in Arms','Life Debt','Patron','Protected House','Personal Rival','Betrayed Friend'];
const clamp = n => Math.max(0,Math.min(100,Math.round(n*100)/100));
const house = (s,id) => s.kingdoms.find(k=>k.id===id);
const relation = (s,a,b) => house(s,a)?.relations[b];
const hostile = (s,a,b) => s.wars.includes([a,b].sort().join(':'));
const allied = (s,a,b) => s.treaties.some(t=>t.type==='alliance'&&t.expires>s.turn&&t.parties.includes(a)&&t.parties.includes(b));
export function emptyPersonal(turn=0) {
  return {feelings:Object.fromEntries(Object.keys(FEELINGS).map(k=>[k,0])),bonds:[],history:[],
    rescued:false,started:turn,lastTick:turn,positiveTurns:[],deedTurns:[],campaigns:[],aidTurns:[],privateTurns:[],lastSpeech:-1};
}
export function initializeEmotions(s) {
  for(const k of s.kingdoms)for(const r of Object.values(k.relations)){if(r.personal===undefined)r.personal=emptyPersonal(s.turn);};
}
const uniqueTurn = (list,turn) => {if(!list.includes(turn))list.push(turn);if(list.length>32)list.shift();};
export function refreshBonds(s,observer,subject) {
  const r=relation(s,observer,subject),p=r?.personal;if(!p)return;
  const f=p.feelings,age=s.turn-p.started,b=[];
  if(f.betrayedFriendship>=35)b.push('Betrayed Friend');
  if(f.rivalry>=35&&r.respect>=35)b.push('Personal Rival');
  if(!hostile(s,observer,subject)&&f.betrayedFriendship<25&&r.grievance<45){
    if(age>=12&&r.trust>=70&&r.reliability>=65&&p.privateTurns.length>=4&&p.deedTurns.length>=6&&f.attachment>=25)b.push('Trusted Confidant');
    if(p.campaigns.length>=2&&r.trust>=45&&f.loyalty>=30)b.push('Brothers/Sisters in Arms');
    if(f.obligation>=55&&p.rescued)b.push('Life Debt');
    if(p.aidTurns.length>=5&&f.gratitude>=30&&r.dependency>=15)b.push('Protected House');
    const other=relation(s,subject,observer)?.personal;
    if(other?.aidTurns.length>=5&&f.protectiveness>=25)b.push('Patron');
  }
  p.bonds=b;
}
const EFFECTS = {
  courtesy:{fondness:.6}, cooperation:{fondness:1,affection:.6,attachment:.5,loyalty:.5},
  aid:{gratitude:3,admiration:1,fondness:1}, relief:{gratitude:12,obligation:6,affection:2},
  support:{protectiveness:2,attachment:1}, promise:{loyalty:4,admiration:3,affection:2,attachment:2},
  private:{attachment:2,loyalty:1}, campaign:{loyalty:12,attachment:6,admiration:5,pride:4},
  rescue:{gratitude:45,obligation:65,admiration:25,affection:8},
  restraint:{gratitude:15,admiration:10,affection:3},
  victory:{admiration:3,pride:4,rivalry:4}, defeat:{humiliation:7,rivalry:7,admiration:3,envy:4},
  capitalLost:{humiliation:35,bitterness:25,rivalry:15,suspicion:12},
  insult:{humiliation:4,bitterness:3,suspicion:2}, threat:{suspicion:5,bitterness:2},
  coercion:{humiliation:18,bitterness:10,suspicion:7},
  abandonment:{bitterness:20,suspicion:15,loyalty:-15,affection:-10,attachment:-8},
  betrayal:{bitterness:40,suspicion:40,loyalty:-60,affection:-40,attachment:-30},
  jealousy:{jealousy:5,envy:2}, marriage:{affection:12,attachment:15,loyalty:10,protectiveness:10},
  marriageBroken:{bitterness:60,humiliation:35,suspicion:50,loyalty:-70,affection:-60,attachment:-50}
};
const POSITIVE = new Set(['cooperation','aid','relief','promise','private','campaign','rescue','restraint']);
export function emotionalEvent(s,observer,subject,kind,{key=`${kind}:${s.turn}`,text='',scale=1}={}) {
  if(s.projectionOnly||s.knowledgeView||observer===subject||!EFFECTS[kind])return false;
  const r=relation(s,observer,subject),k=house(s,observer);if(!r)return false;
  const p=r.personal??=emptyPersonal(s.turn),f=p.feelings;
  if(p.history.some(e=>e.key===key))return false;
  // Multiple messages, split gifts, and duplicate callbacks cannot farm a turn.
  if(POSITIVE.has(kind)&&p.history.some(e=>e.kind===kind&&e.turn===s.turn))return false;
  if(kind==='courtesy'){
    if(p.lastSpeech===s.turn||r.trust<0||r.grievance>25)return false;
    p.lastSpeech=s.turn;
    if(f.fondness>=8)return false;
  }
  const close=f.affection>=35||f.loyalty>=35||f.attachment>=35||p.bonds.some(b=>['Trusted Confidant','Brothers/Sisters in Arms','Life Debt'].includes(b));
  scale=Math.max(.1,Math.min(2,scale));
  for(const [feeling,delta] of Object.entries(EFFECTS[kind])){
    let weight=1;
    if(['promise','campaign','restraint'].includes(kind))weight=.65+(k.honor||0)*.65;
    if(['aid','relief'].includes(kind))weight=.8+(k.greed||0)*.35;
    if(feeling==='admiration'&&['victory','defeat'].includes(kind))weight=.7+(k.ambition||0)*.7;
    if(feeling==='humiliation')weight=.7+(k.ambition||0)*.5+(k.honor||0)*.3;
    if(feeling==='suspicion')weight=.6+(k.paranoia||0)*.8;
    f[feeling]=clamp(f[feeling]+delta*weight*scale);
  }
  if(['betrayal','marriageBroken'].includes(kind)&&close)f.betrayedFriendship=clamp(f.betrayedFriendship+65);
  if(POSITIVE.has(kind)){
    uniqueTurn(p.positiveTurns,s.turn);
    if(kind!=='cooperation')uniqueTurn(p.deedTurns,s.turn);
  }
  if(['aid','relief'].includes(kind))uniqueTurn(p.aidTurns,s.turn);
  if(kind==='rescue')p.rescued=true;
  if(kind==='private')uniqueTurn(p.privateTurns,s.turn);
  if(kind==='campaign'&&!p.campaigns.includes(key))p.campaigns=[...p.campaigns,key].slice(-8);
  p.history.push({key:String(key).slice(0,100),kind,turn:s.turn,text:String(text||kind).slice(0,220)});
  p.history=p.history.slice(-32);
  refreshBonds(s,observer,subject);
  return true;
}
export function updateEmotions(s) {
  initializeEmotions(s);
  for(const k of s.kingdoms)for(const [other,r] of Object.entries(k.relations)){
    const p=r.personal;if(p.lastTick>=s.turn)continue;
    p.lastTick=s.turn;
    // A peaceful treaty alone cannot manufacture intimate friendship or loyalty.
    if(allied(s,k.id,other)&&r.trust>=30&&r.reliability>=55&&r.grievance<25&&p.deedTurns.length>=2&&!hostile(s,k.id,other))
      emotionalEvent(s,k.id,other,'cooperation',{text:'Dependable cooperation continued between our allied courts.'});
    for(const name of ['gratitude','humiliation','jealousy','envy','pride'])p.feelings[name]=clamp(p.feelings[name]-.15);
    for(const name of ['affection','loyalty','attachment'])if(hostile(s,k.id,other)||r.grievance>=50)p.feelings[name]=clamp(p.feelings[name]-.5);
    if(s.turn%4===0&&r.grievance<15&&r.trust>=45)for(const name of ['suspicion','bitterness'])p.feelings[name]=clamp(p.feelings[name]-.4);
    refreshBonds(s,k.id,other);
  }
}
export function personalContext(s,observer,subject) {
  const p=relation(s,observer,subject)?.personal;
  if(!p)return {feelings:[],bonds:[],memories:[]};
  return {feelings:Object.entries(p.feelings).filter(([,n])=>n>=8).sort((a,b)=>b[1]-a[1]).slice(0,5).map(([key,n])=>`${n>=55?'Deep':n>=25?'Established':'Growing'} ${FEELINGS[key].toLowerCase()}`),
    bonds:[...p.bonds],memories:p.history.filter(e=>e.kind!=='courtesy'&&e.kind!=='cooperation').slice(-3).map(e=>`Turn ${e.turn}: ${e.text}`)};
}
export function personalOpening(s,observer,subject) {
  const p=relation(s,observer,subject)?.personal;if(!p)return '';
  const f=p.feelings;
  if(f.betrayedFriendship>=35)return 'Do not speak to me as though the friendship we once had survived what you did.';
  if(f.bitterness>=30)return 'Our treaties have not erased what passed between us.';
  if(f.gratitude>=25){const e=[...p.history].reverse().find(e=>['rescue','relief','aid','restraint'].includes(e.kind));return e?`I have not forgotten: ${e.text}`:'My House remembers what yours did for us.';}
  if(p.bonds.includes('Brothers/Sisters in Arms'))return 'We have bled for the same cause more than once. That matters to me.';
  if(f.affection>=25||f.attachment>=25)return 'You have become a welcome presence at my court, beyond the business of our Houses.';
  if(f.rivalry>=25&&f.admiration>=15)return 'I would sooner have your strength beside me than across the battlefield.';
  if(f.suspicion>=25)return 'I listen, but confidence will have to be earned through deeds.';
  if(f.fondness>=8)return 'It is good to hear from you again. Let us speak plainly.';
  return '';
}
export function personalWillingness(s,observer,subject) {
  const p=relation(s,observer,subject)?.personal;if(!p)return 0;
  const f=p.feelings;
  return Math.max(-35,Math.min(25,f.loyalty*.12+f.gratitude*.08+f.affection*.08+f.obligation*.08-f.bitterness*.16-f.betrayedFriendship*.2));
}
export function validateEmotions(s) {
  initializeEmotions(s);
  const obj=v=>v&&typeof v==='object'&&!Array.isArray(v),turn=n=>Number.isInteger(n)&&n>=0&&n<=s.turn;
  const fail=()=>{throw new Error('Damaged personal relationship data.');};
  for(const k of s.kingdoms)for(const r of Object.values(k.relations)){
    const p=r.personal;
    if(!obj(p)||typeof p.rescued!=='boolean'||!obj(p.feelings)||Object.keys(p.feelings).length!==Object.keys(FEELINGS).length||Object.keys(FEELINGS).some(key=>!Number.isFinite(p.feelings[key])||p.feelings[key]<0||p.feelings[key]>100)||!turn(p.started)||!turn(p.lastTick)||!(p.lastSpeech===-1||turn(p.lastSpeech)))fail();
    if(!Array.isArray(p.bonds)||p.bonds.length>7||new Set(p.bonds).size!==p.bonds.length||p.bonds.some(b=>!BONDS.includes(b)))fail();
    for(const key of ['positiveTurns','deedTurns','aidTurns','privateTurns'])if(!Array.isArray(p[key])||p[key].length>32||p[key].some(t=>!turn(t))||new Set(p[key]).size!==p[key].length)fail();
    if(!Array.isArray(p.campaigns)||p.campaigns.length>8||p.campaigns.some(x=>typeof x!=='string'||x.length>100))fail();
    if(!Array.isArray(p.history)||p.history.length>32||p.history.some(e=>!obj(e)||!turn(e.turn)||!Object.hasOwn(EFFECTS,e.kind)||typeof e.key!=='string'||e.key.length>100||typeof e.text!=='string'||e.text.length>220))fail();
  }
}
