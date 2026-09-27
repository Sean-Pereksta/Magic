import { marriageReadiness } from './marriage.mjs';

/** Player-initiated shortcuts. They only draft a message; they neither send an
 * envoy, replace an existing draft, nor accept any agreement. No global state. */
export function renderCourtOptions(state, rulerId, actorHouseId) {
  const anchor = document.getElementById('quick-promises');
  const composer = document.getElementById('chat-message');
  if (!anchor || !composer) return;
  let controls = document.getElementById('court-knowledge-options');
  if (!controls) {
    controls = document.createElement('div'); controls.id = 'court-knowledge-options'; controls.className = 'button-row';
    anchor.parentElement.insertAdjacentElement('afterend', controls);
    for (const [id, label] of [['court-marriage', 'Discuss marriage'], ['court-intelligence', 'Ask about threats']]) {
      const button = document.createElement('button'); button.id = id; button.type = 'button'; button.textContent = label; controls.append(button);
    }
    const details = document.createElement('details'); details.id = 'court-marriage-readiness'; details.className = 'fine';
    const summary = document.createElement('summary'); details.append(summary);
    const body = document.createElement('p'); body.className = 'fine'; details.append(body); controls.insertAdjacentElement('afterend', details);
  }
  const readiness = marriageReadiness(state, rulerId, actorHouseId);
  const details = document.getElementById('court-marriage-readiness');
  details.querySelector('summary').textContent = `Marriage: ${readiness.label}`;
  details.querySelector('p').textContent = [readiness.reason, ...(readiness.requirements || []).filter(r => !r.met).map(r => r.label)].join(' ');
  const draft = text => {
    if (composer.value.trim()) {
      details.open = true; details.querySelector('p').textContent = 'Your existing message has been kept. Finish or clear that draft before using a conversation shortcut.';
      composer.focus(); return;
    }
    composer.value = text; composer.dispatchEvent(new Event('input', { bubbles: true })); composer.focus();
  };
  const marriage = document.getElementById('court-marriage');
  marriage.textContent = readiness.status === 'married' ? 'Discuss family bond' : 'Discuss marriage';
  marriage.disabled = !!state.outcome;
  marriage.onclick = () => draft(readiness.status === 'married'
    ? 'Let us discuss our marriage bond and the obligations between our Houses.'
    : 'I would like to discuss a royal marriage between our Houses. Which available adult family members would your court consider, and what would you need before agreeing?');
  const intelligence = document.getElementById('court-intelligence');
  intelligence.disabled = !!state.outcome;
  intelligence.onclick = () => draft('Has anyone approached your court about plotting against or overthrowing my House? What are you willing to disclose?');
  // Keep the existing, fully negotiated marriage controls. Bound family members
  // remain visible but cannot accidentally be selected as a new match.
  for (const [id, roles] of [['marriage-actorMember', readiness.available?.speaker], ['marriage-rulerMember', readiness.available?.ruler]]) {
    const select = document.getElementById(id);
    if (!select || !roles) continue;
    for (const option of select.options) option.disabled = !roles.includes(option.value);
    if (!roles.includes(select.value) && roles.length) select.value = roles[0];
  }
}
