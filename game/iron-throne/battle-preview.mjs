import { localHouseId } from './house-control.mjs';
import { kingdom, projectedBattleLosses } from './core.mjs';
import { buildingLevel, buildingSpec, fortMaximum, wallMaximum } from './economy.mjs';
const escape=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
export function battlePreview(s,armyId,targetId) {
  if(!s.armies.some(a=>a.id===armyId&&a.owner===localHouseId(s)))return '';
  const forecast=projectedBattleLosses(s,armyId,targetId);if(!forecast)return '';
  const range=r=>`${r.low}–${r.high}`;
  const t=s.tiles[targetId],fortified=wallMaximum(t)||fortMaximum(t);
  const defenses=[wallMaximum(t)?`${escape(buildingSpec('wall',buildingLevel(t,'wall')).name)} — ${t.walls} / ${wallMaximum(t)}`:'',fortMaximum(t)?`${escape(buildingSpec('fort',buildingLevel(t,'fort')).name)} — ${t.fortIntegrity??fortMaximum(t)} / ${fortMaximum(t)}`:''].filter(Boolean).join('<br>');
  const empty=forecast.kind==='capture';
  return `<section class="battle-preview" aria-label="Projected battle losses">${fortified?`<p>${defenses}</p><p><strong>Defender Protection: +${Math.round(forecast.fortifications.bonus*100)}%</strong> <span class="fine">from fortifications${empty?' · no defending troops':''}</span></p>`:''}<strong>${empty?'Undefended · capture on arrival':fortified?'Projected assault losses · one clash':'Projected battle losses · one clash'}</strong><div class="loss-ranges"><span>You <b>−${range(forecast.yours)}</b></span><span>Them <b>−${range(forecast.theirs)}</b></span></div><p class="fine">${empty?'Fortifications cannot prevent occupation without soldiers. Remaining walls transfer intact.':`Troops lost against ${escape(kingdom(s,forecast.enemyOwner)?.name)}. Includes current terrain, formations and fortifications; movement, reinforcements and battle rolls can change the result.${fortified?' Assault immediately, or bombard walls and forts to reduce their protection first.':''}${forecast.multipleDefenders?' First defending army only; other armies must also be defeated before occupation.':''}`}</p></section>`;
}
