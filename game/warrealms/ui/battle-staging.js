const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[c]));
let transitioning = false;

export function battleStagingSummary(opponent, difficulty) {
  const detail = {};
  window.dispatchEvent(new CustomEvent('warrealms:battle-summary', { detail }));
  return `<div class="wrBattleStaging"><section><span>YOUR COMMANDER</span><h3>${escape(detail.commander || 'Your Realm')}</h3><strong>${escape(detail.deck || 'Select an active deck in the Armory')}</strong><p>${detail.count || 0} cards</p><details><summary>Inspect deck cards</summary>${(detail.cards || []).map(card => `<button type="button" data-act="card-info" data-card-id="${escape(card.id)}">${escape(card.name)} · Info</button>`).join('')}</details></section><b class="wrStagingVS">VS</b><section><span>ENEMY COMMANDER</span><h3>${escape(opponent?.name || 'Unknown Warbot')}</h3><strong>${escape(opponent?.deck || 'Random strategy deck')}</strong><p>${escape(opponent?.factions || 'Faction pairing revealed at battle start')}</p></section></div><p class="wrStagingRules">Solo · ${escape(difficulty)} · Shared market<br>Opening cards: ${escape(detail.opening || 'Standard draw')} · Discounts: ${escape(detail.discounts || 'None')}</p>`;
}

export async function battleStartTransition(root) {
  if (transitioning) return false;
  transitioning = true;
  let overlay;
  const buttons = [...(root?.querySelectorAll("button") || [])].map(button => [button, button.disabled]);
  try {
    const reduced = matchMedia('(prefers-reduced-motion: reduce)').matches || document.documentElement.classList.contains('wrReduceMotion');
    root?.querySelectorAll('button').forEach(button => { button.disabled = true; });
    overlay = document.createElement('div');
    overlay.className = 'wrBattleStartTransition';
    overlay.setAttribute('aria-hidden', 'true');
    const selected = root?.querySelector('.wrBattleStaging');
    if (selected) overlay.append(selected.cloneNode(true));
    else overlay.textContent = 'VS';
    overlay.querySelectorAll('details').forEach(node => node.remove());
    document.body.append(overlay);
    await new Promise(resolve => setTimeout(resolve, reduced ? 0 : 420));
    return !root?.hidden;
  } finally {
    overlay?.remove();
    buttons.forEach(([button, disabled]) => { if (button.isConnected) button.disabled = disabled; });
    transitioning = false;
  }
}
