// New regional guardians opt in at creation. Ordinary and saved legacy battles
// retain their original director. Every telegraph names an executable move.
import {MOVES,SPECIES} from './data.mjs';
const alive=p=>p&&p.hp>0;
export function configureGuardian(b,landId='jungle'){
  const e=b?.enemy;if(!e||b.guardianPattern)return b;
  const native=SPECIES[e.species]?.types||[],attacks=Object.entries(MOVES).filter(([,m])=>m.power&&!m.signature&&!m.types&&native.includes(m.type)&&!m.switch&&(m.minStage||0)<=(e.stage||0)&&(m.minLevel||1)<=e.level).sort((a,c)=>(c[1].power||0)-(a[1].power||0));
  const heavy=attacks[0]?.[0]||'heavyPalm',shield=Object.keys(MOVES).find(id=>MOVES[id].shield&&native.includes(MOVES[id].type)&&!MOVES[id].switch)||'ironGuard';
  e.learned=[...new Set([...(e.learned||[]),'focus',heavy,shield,'breathe'])];e.loadout=['focus',heavy,shield,'breathe'];
  e.guardianMaxHp=e.hp;b.guardian=true;b.guardianPattern={version:1,landId:String(landId),heavy,shield,phase:1};return b;
}
export function planGuardian(s){
  const b=s?.run?.battle,g=b?.guardianPattern,e=b?.enemy;if(!g||g.version!==1||!e)return null;
  // Reserve support creatures keep their role even if the leading guardian fell.
  if(e.id!==b.enemies?.[0]?.id)return null;
  const maximum=e.guardianMaxHp||Math.max(e.hp,1),phase=e.hp<maximum*.45?2:1;
  const available=id=>e.loadout.includes(id)&&MOVES[id]&&(MOVES[id].cost||0)<=e.energy&&!(e.cooldowns?.[id]>b.round)&&!(MOVES[id].uses&&(e.uses?.[id]||0)>=MOVES[id].uses)&&(!MOVES[id].switch||!e.status?.root&&b.enemies.some(p=>p.id!==e.id&&alive(p)));
  const slot=((b.round||1)-1)%(phase===2?3:4),desired=slot===0?'focus':slot===1?g.heavy:phase===1&&slot===2?g.shield:'breathe';
  // Charge only when the planned follow-up can be funded after natural recovery.
  const funded=desired!=='focus'||e.energy-(MOVES.focus.cost||0)+1>=(MOVES[g.heavy]?.cost||0);
  const id=available(desired)&&funded?desired:available('breathe')?'breathe':'strike',m=MOVES[id];
  let intentLabel=m.name,intentDetail=m.desc;
  if(id==='focus'){intentLabel=`Charging ${MOVES[g.heavy].name}`;intentDetail=`Uses ${m.name} now. A heavy ${MOVES[g.heavy].type||'physical'} attack is planned next round; brace, weaken it, or switch to a resistant companion.`;}
  else if(id===g.heavy){intentLabel=`Heavy attack · ${m.name}`;intentDetail=`${m.desc} The guardian's charge strengthens this attack when still active.`;}
  else if(id===g.shield){intentLabel=`Shield phase · ${m.name}`;intentDetail=`${m.desc} A shield-breaking technique or an energy-recovery action can help.`;}
  else if(id==='breathe'){intentLabel='Recovering energy';intentDetail='The guardian uses Deep Breath. Take this opening to heal, prepare, or attack.';}
  return{type:'move',id,intentLabel,intentDetail,phase,phaseLabel:phase===2?'Final resolve':'Guardian pattern'};
}
// Capture a stable maximum after the core applies the guardian's final stats.
export function initializeGuardianHealth(b){if(b?.guardianPattern&&b.enemy&&!b.enemy.guardianMaxHp)b.enemy.guardianMaxHp=b.enemy.hp;return b;}
