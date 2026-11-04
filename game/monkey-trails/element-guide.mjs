import { CONFIG, ELEMENTS, effectiveness, moveEffect } from './core.mjs';
const ids = Object.keys(ELEMENTS);
const mult = (value) => Number(value.toFixed(3)) + '×';
const options = (selected) =>
  ids
    .map((id) => `<option value="${id}" ${id === selected ? 'selected' : ''}>${ELEMENTS[id].name}</option>`)
    .join('');
export function counterList(first, second = '') {
  const defenders = [first, second].filter(Boolean);
  return ids.map((id) => ({ id, value: effectiveness(id, defenders) })).sort((a, b) => b.value - a.value);
}
export function counterResults(first, second) {
  const rows = counterList(first, second);
  return `<h3>Attacking ${[...new Set([first, second].filter(Boolean))].map((id) => ELEMENTS[id].name).join(' / ')}</h3><ul class="counter-list">${rows.map(({ id, value }) => `<li><span>${ELEMENTS[id].name}</span><b class="match-${value > 1 ? 'strong' : value < 1 ? 'resist' : 'even'}">${mult(value)}</b></li>`).join('')}</ul>`;
}
export function elementGuide(first = 'water', second = '') {
  return `<section class="element-help"><span class="eyebrow">FIELD GUIDE · COMBAT</span><h2>What counters what?</h2><p>Choose the opponent’s elements. Attacks with the highest damage multiplier appear first.</p><div class="element-pickers"><label>Opponent element<select id="element-first">${options(first)}</select></label><label>Second element<select id="element-second"><option value="">None</option>${options(second)}</select></label></div><div id="counter-results" aria-live="polite">${counterResults(first, second)}</div><h3>All eight elements</h3><p>Read across from the attacking element to the defender. <strong>${mult(CONFIG.elemental.strong)}</strong> is strong, <strong>${mult(CONFIG.elemental.resisted)}</strong> is resisted, and <strong>1×</strong> is neutral.</p><div class="element-table" tabindex="0" aria-label="Scrollable element matchup table"><table><caption>Attack rows × defending columns</caption><thead><tr><th scope="col">Attack ↓</th>${ids.map((id) => `<th scope="col">${ELEMENTS[id].name}</th>`).join('')}</tr></thead><tbody>${ids
    .map(
      (at) =>
        `<tr><th scope="row">${ELEMENTS[at].name}</th>${ids
          .map((d) => {
            const n = effectiveness(at, [d]);
            return `<td class="match-${n > 1 ? 'strong' : n < 1 ? 'resist' : 'even'}">${mult(n)}</td>`;
          })
          .join('')}</tr>`,
    )
    .join(
      '',
    )}</tbody></table></div><h3>Two elements in one matchup</h3><p>A dual-element defender multiplies the two matchups, bounded between ${mult(CONFIG.elemental.min)} and ${mult(CONFIG.elemental.max)}. Fire against Leaf / Water deals ${mult(effectiveness('fire', ['leaf']))} × ${mult(effectiveness('fire', ['water']))} = <strong>${mult(effectiveness('fire', ['leaf', 'water']))}</strong> base damage.</p><p>A split-element move averages its two multipliers. A Fire / Water move against Fire gets <strong>${mult(moveEffect({ types: ['fire', 'water'] }, ['fire']))}</strong>. Neutral moves use 1×.</p></section>`;
}
