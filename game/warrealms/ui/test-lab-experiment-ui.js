import { TEST_LAB_DECK_PRESETS } from './test-lab-deck-presets.js';
import { EXTENDED_TEST_LAB_STRATEGIES } from './test-lab-strategy-extension.js';
import { getTestLabCard, getTestLabCards } from './test-lab-simulator.js';
import { groupDeckResults } from './test-lab-experiments.js';

const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[c]));
let decks = [], mutations = {}, results = [];
const styles = EXTENDED_TEST_LAB_STRATEGIES;
let saved = [];
try { saved = JSON.parse(sessionStorage.getItem('warrealms-test-decks') || '[]'); } catch {}
const available = [...TEST_LAB_DECK_PRESETS, ...(Array.isArray(saved) ? saved : [])];
const fields = ['cost', 'effect.trade', 'effect.combat', 'heat.overload.at', 'heat.overload.effect.trade', 'heat.overload.effect.combat'];
const valueAt = (object, path) => path.split('.').reduce((value, key) => value?.[key], object);

export function experimentOptions() {
  return structuredClone({ decks, mutations });
}

function inspect(deck) {
  const dialog = document.createElement('dialog');
  dialog.className = 'wrExperimentDialog';
  const counts = new Map();
  for (const id of deck.cardIds) counts.set(id, (counts.get(id) || 0) + 1);
  dialog.innerHTML = `<h2>${escape(deck.name)}</h2><div class="wrInspectCards">${[...counts].map(([id, count]) => {
    const card = getTestLabCard(id);
    return `<article><strong>${count} × ${escape(card?.name || id)}</strong><p>Cost ${card?.cost ?? 0} · ${escape(card?.faction)}</p><img loading="lazy" src="https://pub-71006ef154924b3e854de5a08b52c1e6.r2.dev/${encodeURIComponent(card?.image || id + '.png')}" alt="${escape(card?.name)}"><p>${escape(card?.text)}</p><p>${escape(card?.heatText)}</p><p>${escape(card?.allyText)}</p></article>`;
  }).join('')}</div><button type="button">Back</button>`;
  dialog.querySelector('button').onclick = () => dialog.close();
  dialog.addEventListener('close', () => dialog.remove());
  document.body.append(dialog); dialog.showModal();
}

function renderDecks() {
  document.getElementById('wrExperimentDecks').innerHTML = decks.map((deck, index) => `<fieldset data-deck="${index}"><legend>${escape(deck.name)}</legend><div class="wrStyleChecks">${styles.map(style => `<label><input type="checkbox" value="${style.id}" ${deck.styles.includes(style.id) ? 'checked' : ''}>${escape(style.name)}</label>`).join('')}</div><div class="wrExperimentTools"><button type="button" data-all>Select All / Use All Playstyles</button><button type="button" data-clear>Clear All</button><button type="button" data-random>Random</button><button type="button" data-info>Inspect Deck</button><button type="button" data-remove>Remove</button></div></fieldset>`).join('');
}

function renderMutation() {
  const id = document.getElementById('wrMutationCard').value;
  const card = getTestLabCard(id);
  document.getElementById('wrMutationFields').innerHTML = fields.filter(path => Number.isFinite(valueAt(card, path))).map(path => `<label>${escape(path)}<span>${valueAt(card, path)} → <input type="number" min="0" max="100" step="1" data-path="${path}" value="${mutations[id]?.[path] ?? valueAt(card, path)}"></span></label>`).join('');
  document.getElementById('wrMutationCount').textContent = `${Object.keys(mutations).length} mutated cards · lab only`;
}

export function renderExperimentResults(rows = []) {
  results = rows;
  const mode = document.getElementById('wrDeckGroup')?.value || 'combined';
  const target = document.getElementById('wrDeckResults');
  if (target) target.innerHTML = groupDeckResults(rows, mode).map(row => `<tr><td>${escape(row.deck)}</td><td>${escape(row.style)}</td><td>${row.games}</td><td>${row.winRate.toFixed(1)}%</td><td>${row.wins} / ${row.losses} / ${row.draws}</td></tr>`).join('') || '<tr><td colspan="5">Add decks and run a test to compare results.</td></tr>';
}

export function installExperiments() {
  const panel = document.createElement('section');
  panel.className = 'panel wrExperiments';
  panel.innerHTML = `<div class="panelHead"><h3>Decks & Playstyles</h3><button type="button" id="wrAddDeck">Add Deck</button></div><div class="panelBody"><p>Each checked deck/playstyle is tested against the other selected variants. A single variant uses Bot B as its opponent. Games are shared across matchups, with seats alternating. Decks contribute to the shared market, as in normal Warrealms.</p><div id="wrExperimentDecks"></div><details><summary>Mutation Testing</summary><p>Temporary card changes for this lab only. The accelerated simulator estimates conditional effects, including Heat payoff; it is not a full live-game rules simulation.</p><label>Card <select id="wrMutationCard">${getTestLabCards().map(card => `<option value="${escape(card.id)}">${escape(card.name)}</option>`).join('')}</select></label><div id="wrMutationFields"></div><button type="button" id="wrResetMutation">Reset Mutation</button><button type="button" id="wrResetAllMutations">Reset All</button><span id="wrMutationCount"></span></details></div>`;
  document.querySelector('.runControls').parentElement.before(panel);
  const resultPanel = document.createElement('section');
  resultPanel.className = 'panel wrExperiments';
  resultPanel.innerHTML = `<div class="panelHead"><h3>Deck Comparison</h3><select id="wrDeckGroup"><option value="combined">Deck + Playstyle</option><option value="deck">Deck</option><option value="style">Playstyle</option></select></div><div class="panelBody" style="overflow:auto"><table class="strategyTable"><thead><tr><th>Deck</th><th>Playstyle</th><th>Games</th><th>Win Rate</th><th>W / L / D</th></tr></thead><tbody id="wrDeckResults"></tbody></table></div>`;
  document.querySelector('.resultsPanel').before(resultPanel);
  const css = document.createElement('style');
  css.textContent = `.wrExperiments{margin:12px 0}.wrExperiments .panelBody{padding:12px;font-size:12px;line-height:1.5}.wrExperiments button,.wrExperiments select,.wrExperimentDialog button{background:#172534;color:#f0e6c4;border:1px solid #59677b;border-radius:7px;padding:8px;cursor:pointer}.wrExperiments fieldset{border:1px solid #46586d;border-radius:10px;margin:10px 0;padding:12px}.wrStyleChecks,.wrExperimentTools{display:flex;flex-wrap:wrap;gap:12px;margin:8px 0}.wrStyleChecks label{display:flex;align-items:center;gap:5px}.wrExperiments input[type=checkbox]{width:18px;height:18px;accent-color:#d9bc5b}#wrMutationFields{display:flex;flex-wrap:wrap;gap:12px;margin:12px 0}#wrMutationFields label{display:grid}#wrMutationFields input{width:70px}.wrExperimentDialog{max-width:min(900px,92vw);max-height:85vh;overflow:auto;background:#101a25;color:#eee;border:1px solid #a38c55;border-radius:15px;padding:22px}.wrExperimentDialog::backdrop{background:#000b}.wrInspectCards{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px}.wrInspectCards img{max-width:100%;max-height:280px}.wrExperimentDialog .wrDeckChoice{display:flex;gap:12px;align-items:center;margin:10px 0}`;
  document.head.append(css);
  document.getElementById('wrAddDeck').onclick = () => {
    const dialog = document.createElement('dialog'); dialog.className = 'wrExperimentDialog';
    dialog.innerHTML = `<h2>Add Deck</h2><p>Choose a preset or a saved 50-card deck from the Armory.</p>${available.map((deck, index) => `<div class="wrDeckChoice"><span>${escape(deck.name)}</span><button type="button" data-add="${index}" ${decks.some(d => d.id === deck.id) ? 'disabled' : ''}>Add</button><button type="button" data-inspect="${index}">Info</button></div>`).join('')}<button type="button" data-close>Back</button>`;
    dialog.onclick = event => {
      const button = event.target.closest('button'); if (!button) return;
      if (button.hasAttribute('data-close')) return dialog.close();
      if (button.hasAttribute('data-inspect')) return inspect(available[Number(button.dataset.inspect)]);
      if (button.hasAttribute('data-add')) {
        const deck = available[Number(button.dataset.add)];
        decks.push({ id: deck.id, name: deck.name, cardIds: [...deck.cardIds], styles: [deck.strategyId || 'random'] });
        renderDecks(); dialog.close();
      }
    };
    dialog.addEventListener('close', () => dialog.remove()); document.body.append(dialog); dialog.showModal();
  };
  document.getElementById('wrExperimentDecks').onchange = event => {
    const field = event.target.closest('[data-deck]'); if (!field) return;
    decks[Number(field.dataset.deck)].styles = [...field.querySelectorAll('input:checked')].map(input => input.value);
  };
  document.getElementById('wrExperimentDecks').onclick = event => {
    const button = event.target.closest('button'), field = button?.closest('[data-deck]'); if (!field) return;
    const index = Number(field.dataset.deck), deck = decks[index];
    if (button.hasAttribute('data-info')) return inspect(deck);
    if (button.hasAttribute('data-all')) deck.styles = styles.filter(s => s.id !== 'random').map(s => s.id);
    if (button.hasAttribute('data-clear')) deck.styles = [];
    if (button.hasAttribute('data-random')) deck.styles = ['random'];
    if (button.hasAttribute('data-remove')) decks.splice(index, 1);
    renderDecks();
  };
  document.getElementById('wrMutationCard').onchange = renderMutation;
  document.getElementById('wrMutationFields').onchange = event => {
    const path = event.target.dataset.path, id = document.getElementById('wrMutationCard').value;
    if (!path || !event.target.checkValidity()) return;
    mutations[id] ||= {}; mutations[id][path] = Number(event.target.value); renderMutation();
  };
  document.getElementById('wrResetMutation').onclick = () => { delete mutations[document.getElementById('wrMutationCard').value]; renderMutation(); };
  document.getElementById('wrResetAllMutations').onclick = () => { mutations = {}; renderMutation(); };
  document.getElementById('wrDeckGroup').onchange = () => renderExperimentResults(results);
  renderDecks(); renderMutation(); renderExperimentResults();
}
