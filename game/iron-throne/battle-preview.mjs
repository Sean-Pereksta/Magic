import { PLAYER, kingdom, projectedBattleLosses } from './core.mjs';
const escape=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
export function battlePreview(s,armyId,targetId) {
  if(!s.armies.some(a=>a.id===armyId&&a.owner===PLAYER))return '';
  const forecast=projectedBattleLosses(s,armyId,targetId);if(!forecast)return '';
  const range=r=>`${r.low}–${r.high}`;
  return `<section class="battle-preview" aria-label="Projected battle losses"><strong>${forecast.kind==='siege'?'Projected siege losses · next siege turn':'Projected battle losses · one clash'}</strong><div class="loss-ranges"><span>You <b>−${range(forecast.yours)}</b></span><span>Them <b>−${range(forecast.theirs)}</b></span></div><p class="fine">Troops lost against ${escape(kingdom(s,forecast.enemyOwner)?.name)}. Estimated with current forces; movement, reinforcements and battle rolls can change the result.${forecast.kind==='siege'?' Fortifications must fall before a field assault.':''}${forecast.multipleDefenders?' First defending army only; other armies may fight separately.':''}</p></section>`;
}
