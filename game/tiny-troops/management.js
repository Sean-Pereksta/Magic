/* Accessible formation controls and previews layered over the native progression. */
(function () {
  'use strict';
  const R = window.TinyTroopsRules, M = window.TinyTroopsManagementRules, TP = window.TinyTroopsPolish;
  if (!R || !M || !TP) return;
  const el = id => document.getElementById(id), all = (s, root = document) => [...root.querySelectorAll(s)];
  const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const pendingPlacement = () => !!(S.placing || S.placingUpgrade || S.placingEffect || S.placingSpecialization);
  const army = () => [...S.squad, ...(S.bench || [])].filter(Boolean);
  const readiness = u => !!u && ((!u.evo && u.star >= 5) || (u.evo && !u.ascension && u.star >= 6));
  const art = u => u.ascension && u.ascEmoji || u.evo && u.evoEmoji || u.e, title = u => u.ascension && u.ascTitle || u.evo && u.evoTitle || u.n;
  let selected = null, dragging = null, managedTab = 'troops', evolution = null;
  function ensure() {
    if (!S.tt) return;
    S.bench = Array.from({ length: 3 }, (_, i) => S.bench?.[i] || null);
    S.tt.formations = M.sanitizePresets(S.tt.formations, 20);
    army().forEach(u => { if (!u.ttId) { TP.unitSeq = Math.max(TP.unitSeq || 0, S.tt.nextUnitId || 0) + 1; S.tt.nextUnitId = TP.unitSeq; u.ttId = S.tt.id + '-u' + TP.unitSeq; } });
  }
  const oldSanitize = R.sanitizeSnapshot;
  R.sanitizeSnapshot = function (input, roster) { const next = oldSanitize(input, roster); next.tt.formations = M.sanitizePresets(next.tt.formations, 20); return next; };
  function current(ref = selected) { return ref && (ref.where === 'board' ? S.squad : S.bench)?.[ref.index] === ref.unit ? ref.unit : null; }
  function reason() { return TP.recovery ? 'Restore the interrupted wave before changing formation.' : M.preparationReason(S); }
  function persist(label) { TP.structures = ''; render(); saveProfile(label); }
  function selectedRef(where, index) { ensure(); const unit = (where === 'board' ? S.squad : S.bench)[index]; return unit ? { where, index, unit } : null; }
  function cancelPlacement() {
    selected = null; dragging = null;
    if (pendingPlacement()) {
      if (typeof TP.cancelPlacement === 'function') TP.cancelPlacement();
      else { S.placing = S.placingEffect = null; S.placingUpgrade = S.placingSpecialization = false; close(); render(); }
    }
    refresh();
  }
  function select(where, index) {
    const ref = selectedRef(where, index); if (!ref) return false;
    if (reason()) { msg(reason()); if (ref.unit) detail(ref.unit); return false; }
    selected = ref; close(); refresh(); msg('Selected ' + ref.unit.n + '. Tap a square to move or swap, or X to cancel.'); return true;
  }
  function place(target) {
    const source = selected;
    if (!current(source)) { selected = null; refresh(); return false; }
    const result = M.move(S, source, target);
    if (!result.ok) { msg(result.reason); refresh(); return false; }
    S.squad = result.squad; S.bench = result.bench; if (result.bossSwap) S.bossSwapAvailable = false;
    selected = null; dragging = null; close(); persist(result.bossSwap ? 'post-boss formation swap' : 'formation move');
    msg(result.unit.n + ' moved.' + (result.bossSwap ? ' Post-boss switch used.' : '')); return true;
  }
  function autoArrange() {
    if (reason()) return msg(reason());
    S.squad = M.autoArrange(S.squad, R.role); selected = null; close(); persist('auto arranged'); msg('Formation arranged: defenders right, ranged troops behind, support in the middle.');
  }
  function saveFormation(index) {
    ensure(); if (!Number.isInteger(index) || index < 0 || index > 2) return;
    if (reason()) return msg(reason());
    S.tt.formations[index] = M.capture(S.squad, 'Formation ' + (index + 1)); saveProfile('formation preset saved'); openManagement('formation'); msg('Formation ' + (index + 1) + ' saved.');
  }
  function loadFormation(index) {
    ensure(); if (!Number.isInteger(index) || index < 0 || index > 2) return;
    if (reason()) return msg(reason());
    const result = M.restore(S, S.tt.formations[index]);
    if (!result.ok) return msg(result.reason);
    S.squad = result.squad; selected = null; close(); persist('formation preset loaded'); msg('Formation ' + (index + 1) + ' loaded.');
  }
  function status(u, where) {
    if (S.phase === 'battle') return where === 'bench' ? 'Reserve · sits out this battle' : u.dead || u.hp <= 0 ? 'Defeated this battle' : 'Deployed · in combat';
    if (reason()) return reason();
    return where === 'bench' ? (S.squad.some(u => !u) ? 'Ready · empty squares are free' : S.bossSwapAvailable ? 'Ready · post-boss switch available' : 'Board full · post-boss switch needed') : 'Deployed · tap another troop to swap';
  }
  function card(u, i, where, compact = false) {
    const attrs = `data-tt-mgmt-unit="${where}:${i}"`, isSelected = selected?.where === where && selected.index === i && current();
    if (!u) return `<button class="tt-reserve-card secondary tt-empty-reserve" ${attrs} aria-label="Empty bench slot ${i + 1}"><span>🪑</span><strong>Empty reserve</strong><small>Slot ${i + 1} of 3</small></button>`;
    const role = R.ROLE_INFO[R.role(u)], hp = Math.max(0, Math.ceil(u.hp || 0)), power = Math.round((u.atk || 0) * (u.spd || 1) + (u.maxHp || 0) / 6);
    return `<button class="tt-reserve-card secondary ${isSelected ? 'tt-selected' : ''}" ${attrs} draggable="${!reason()}" aria-pressed="${!!isSelected}" aria-label="${esc(u.n + ', ' + (u.star || 1) + ' stars, ' + status(u, where))}" title="${esc(status(u, where))}"><span class="tt-bench-emoji">${esc(art(u))}</span><strong>${esc(u.n)}</strong><span>${u.star || 1}★ · ${esc(role.emoji + ' ' + role.name)}</span><small>${esc(u.ascension ? 'Mastered' : u.evo ? 'Evolved' : 'Base')} · ♥ ${hp}/${Math.ceil(u.maxHp || 0)} · ⚔ ${power}</small><small class="tt-availability">${esc(compact && !reason() ? where === 'bench' ? S.squad.some(u => !u) ? 'Ready to deploy' : S.bossSwapAvailable ? 'Boss switch ready' : 'Requires empty space / boss switch' : 'Deployed' : status(u, where))}</small>${readiness(u) ? '<span class="tt-ready-badge">✨ Evolution ready</span>' : ''}</button>`;
  }
  function refresh() {
    if (!el('grid') || !S.tt) return;
    ensure(); if (selected && !current()) selected = null;
    let bar = el('ttBenchBar');
    if (!bar) { bar = document.createElement('section'); bar.id = 'ttBenchBar'; bar.setAttribute('aria-label', 'Troop bench and formation controls'); el('grid').closest('.arena')?.after(bar); if (!bar.isConnected) el('grid').parentElement.after(bar); }
    bar.hidden = S.tt.mode === 'MENU' || !!S.tt.finished;
    const scroll = bar.querySelector('.tt-bench-strip')?.scrollLeft || 0;
    const placing = pendingPlacement(), unit = current(), count = S.squad.filter(Boolean).length;
    const instruction = unit ? `Place ${unit.n} · tap a highlighted square; tap another troop to swap` : placing ? `Place ${S.placing?.n || S.placingEffect?.n || 'your selected card'} · tap a legal troop or space` : `${S.squad.length - count} deployment spaces open · ${S.bench.filter(Boolean).length}/3 reserves`;
    const html = `<div class="tt-mgmt-toolbar"><strong>🪑 Reserves</strong><span>${count}/${S.squad.length} deployed</span><button class="secondary" data-tt-mgmt="open">Expand team ↗</button></div><div class="tt-bench-strip" aria-label="Three reserve slots">${S.bench.map((u, i) => card(u, i, 'bench', true)).join('')}${S.placing ? `<button class="tt-reserve-card tt-selected" draggable="true" data-tt-mgmt-incoming="true"><span class="tt-bench-emoji">${esc(S.placing.e)}</span><strong>${esc(S.placing.n)}</strong><small>New recruit · drag to place</small></button>` : ''}</div><div class="tt-placement-status" aria-live="polite"><span>${esc(instruction)}</span>${unit || placing ? '<button class="secondary" data-tt-mgmt="cancel" aria-label="Cancel placement">✕ Cancel</button>' : '<button class="secondary" data-tt-mgmt="arrange">Auto Arrange</button>'}</div>`;
    // Do not replace the dragged card or steal focus on combat redraws.
    if (!dragging && bar.innerHTML !== html) { const focus = document.activeElement?.getAttribute('data-tt-mgmt-unit'); bar.innerHTML = html; bar.querySelector('.tt-bench-strip').scrollLeft = scroll; if (focus) bar.querySelector(`[data-tt-mgmt-unit="${focus}"]`)?.focus({ preventScroll: true }); }
    all('.cell', el('grid')).forEach((cell, i) => {
      cell.draggable = !!S.squad[i] && !reason();
      cell.classList.toggle('tt-selected', !!unit && selected.where === 'board' && selected.index === i);
      cell.classList.toggle('tt-legal-tile', !!unit && M.move(S, selected, { where: 'board', index: i }).ok);
      if (!dragging) { cell.classList.remove('tt-drop-preview'); delete cell.dataset.ttPreview; }
    });
  }
  function tabs(tab) {
    return '<nav class="tt-management-tabs" aria-label="Team sections">' + [['troops', 'Troops'], ['formation', 'Formation'], ['evolutions', 'Evolutions'], ['towers', 'Towers'], ['inventory', 'Inventory']].map(([key, name]) => `<button class="secondary ${key === tab ? 'selected' : ''}" data-tt-mgmt-tab="${key}" aria-pressed="${key === tab}">${name}</button>`).join('') + '</nav>';
  }
  function openManagement(tab = managedTab) {
    if (S.tt.mode === 'MENU') return msg('Resume a run to manage your team.');
    if (pendingPlacement()) { nativeBench(); if (el('drawer').classList.contains('open')) el('body').insertAdjacentHTML('afterbegin', '<button class="secondary tt-wide" data-tt-mgmt="cancel">✕ Cancel selected card</button>'); return; }
    managedTab = tab;
    if (tab === 'towers') return window.TinyTroopsEquipment?.openArmory ? window.TinyTroopsEquipment.openArmory() : window.openTowerManagement ? window.openTowerManagement() : msg('Select a tower on the battlefield to inspect its equipment.');
    if (tab === 'inventory') return window.TinyTroopsEquipment?.openInventory ? window.TinyTroopsEquipment.openInventory() : window.openTowerInventory ? window.openTowerInventory() : msg('Select a tower to open the equipment collection.');
    ensure(); let body = tabs(tab);
    if (tab === 'formation') {
      body += `<p class="tt-role-guide">← Backline: ranged · Middle: support / attackers · Frontline: defenders →</p><div class="tt-formation-grid">${S.squad.map((u, i) => `<button class="tt-formation-tile secondary" data-tt-mgmt-board="${i}" aria-label="${esc((u?.n || 'Empty') + ', row ' + (Math.floor(i / 4) + 1) + ', column ' + (i % 4 + 1))}"><span>${esc(u ? art(u) : '＋')}</span><small>${esc(u?.n || 'Empty')}</small></button>`).join('')}</div><button data-tt-mgmt="arrange" class="tt-wide">Auto Arrange</button><p class="hint">Tap a troop, then another square. Deployed troops can swap freely during preparation. Presets keep the current deployed team; a reserve swap still uses a post-boss switch.</p>`;
      body += S.tt.formations.map((p, i) => `<div class="tt-preset-row"><strong>${esc(p?.name || 'Formation ' + (i + 1))}</strong><span>${p ? p.board.filter(Boolean).length + ' troops saved' : 'Empty preset'}</span><button class="secondary" data-tt-mgmt-save="${i}">Save current</button><button ${p ? '' : 'disabled'} data-tt-mgmt-load="${i}">Load</button></div>`).join('');
    } else if (tab === 'evolutions') {
      body += '<p class="hint">Preview every available path freely. Training unlocks 5★ Evolution, 6★ Mastery, and the existing 7–13★ star specializations. No additional evolution materials are spent.</p>';
      body += army().map(u => `<button class="choice" data-tt-mgmt-evolution="${esc(M.id(u))}"><span class="be">${esc(art(u))}</span><span><strong>${esc(u.n)} · ${u.star || 1}★</strong><p>${esc(readiness(u) ? '✨ Ready to preview' : u.star < 5 ? 'Requires 5★ · ' + (u.starProg || 0) + '/' + starNeed(u) + ' training toward next star' : u.ascTitle || u.evoTitle || 'Existing star paths are preserved')}</p></span></button>`).join('');
      if (TP.pendingDecisionCount?.()) body += '<button class="secondary tt-wide" data-tt-mgmt="deferred">Review postponed choices</button>';
    } else {
      body += `<h3>Deployed · ${S.squad.filter(Boolean).length}/${S.squad.length}</h3><div class="tt-team-cards">${S.squad.map((u, i) => u ? card(u, i, 'board') : '').join('')}</div><h3>Reserves · ${S.bench.filter(Boolean).length}/3</h3><div class="tt-bench-strip">${S.bench.map((u, i) => card(u, i, 'bench')).join('')}</div><p class="hint">Select a troop for quick placement. Use Inspect for its abilities and equipment.</p>`;
    }
    show('Team management', reason() || 'Rearrange freely between waves. Every change preserves troop identity and progression.', body); el('drawer').dataset.ttView = 'management';
  }
  const nativeBench = window.openBench;
  window.openBench = function () { nativeBench(); if (el('drawer').classList.contains('open') && !el('body').querySelector('[data-tt-mgmt="open"]')) el('body').insertAdjacentHTML('beforeend', '<button class="secondary tt-wide" data-tt-mgmt="open">Expand full team management ↗</button>'); };
  if (el('benchBtn')) el('benchBtn').onclick = window.openBench;
  const nativeTap = tapCell;
  tapCell = window.tapCell = function (i) {
    if (current() && !pendingPlacement()) return place({ where: 'board', index: i });
    const wasPlacing = pendingPlacement(), result = nativeTap(i);
    if (!wasPlacing && !reason() && S.squad[i]) { selected = selectedRef('board', i); refresh(); }
    return result;
  };
  function evolutionContext() { return S.evoTarget ? { unit: S.evoTarget, options: S.evoChoices, stage: 5, kind: 'evolution' } : S.ascTarget ? { unit: S.ascTarget, options: S.ascChoices, stage: 6, kind: 'mastery' } : null; }
  function evoStats(u) { return `<div class="detailStats"><div class="stat">Health<b>${Math.round(u.maxHp)}</b></div><div class="stat">Attack<b>${Math.round(u.atk)}</b></div><div class="stat">Tempo<b>${Number(u.spd).toFixed(2)}</b></div><div class="stat">Shield<b>${Math.round(u.shield || 0)}</b></div></div>`; }
  function evolutionPreview(index) {
    const context = evolutionContext(); if (!context || !army().includes(context.unit) || S.phase === 'battle') return;
    const option = context.options?.[index]; if (!option) return msg('That path is unavailable. Choose another preview.');
    // Use the same native stat boost and label as the final transaction.
    const result = M.previewEvolution(context.unit, option, context.stage, context.stage === 5 ? (u, stage) => window.applySelectedUpgradeBoost?.(u, stage, 'evolution') : null);
    evolution = { ...context, index, option, preview: result.ok ? result.unit : null };
    const u = context.unit, next = result.unit;
    const body = `<div class="tt-evolution-options">${context.options.map((p, i) => `<button class="secondary tt-evolution-path ${index === i ? 'selected' : ''}" data-tt-evolution-preview="${i}" aria-pressed="${index === i}">${esc(p.e)} ${esc(p.n)}</button>`).join('')}</div><div class="tt-evolution-comparison"><section><span class="tt-eyebrow">CURRENT · ${u.star}★</span><div class="tt-evolution-art">${esc(art(u))}</div><h3>${esc(title(u))}</h3>${evoStats(u)}<p>${esc(u.a || u.ability || 'Native troop abilities')}</p></section><section><span class="tt-eyebrow">${context.stage}★ ${context.stage === 5 ? 'EVOLUTION' : 'MASTERY'}</span><div class="tt-evolution-art">${esc(next ? art(next) : option.e)}</div><h3>${esc(option.n)}</h3>${next ? evoStats(next) : '<p>' + esc(result.reason) + '</p>'}<p>${esc(option.desc || option.flavor || '')}</p></section></div><p class="tt-impact">✓ Requires ${context.stage}★${context.stage === 6 ? ' and a chosen evolution' : ''} · Ready<br>Cost: no additional coins or materials. Training already earned this upgrade.</p><p class="hint">Appearance: ${esc(art(u))} → ${esc(next ? art(next) : option.e)}. The selected path adds the abilities described above; existing effects and equipment remain.</p><div class="tt-replacement"><button ${result.ok ? '' : 'disabled'} data-tt-evolution-confirm="true">Confirm ${context.stage === 5 ? 'evolution' : 'mastery'} · ${esc(option.n)}</button><button class="secondary" data-tt-mgmt="later">Later · keep current troop</button></div>`;
    show(context.stage + '★ ' + (context.stage === 5 ? 'Evolution' : 'Mastery') + ': ' + u.n, 'Compare paths before confirming. Previewing and closing spend nothing.', body); el('drawer').dataset.ttView = 'evolution-preview';
  }
  function confirmEvolution() {
    const choice = evolution, context = evolutionContext();
    if (!choice || !context || choice.unit !== context.unit || choice.option !== context.options?.[choice.index] || !army().includes(choice.unit) || S.phase === 'battle' || S.tt.mode === 'MENU') return msg('This evolution changed. Reopen the troop to preview its current paths.');
    const check = M.previewEvolution(context.unit, choice.option, choice.stage, choice.stage === 5 ? (u, stage) => window.applySelectedUpgradeBoost?.(u, stage, 'evolution') : null);
    if (!check.ok) return msg(check.reason);
    const before = R.copy(context.unit);
    try {
      const apply = () => {
        choice.option.apply(context.unit);
        if (choice.stage === 5) window.applySelectedUpgradeBoost?.(context.unit, 5, 'evolution');
        if (choice.stage === 6 && context.unit.star === 6) context.unit.starProg = 0;
        context.unit.hp = context.unit.maxHp; context.unit.shield = (context.unit.shield || 0) + (choice.stage === 6 ? 70 : 35);
        if (![context.unit.maxHp, context.unit.atk, context.unit.spd].every(Number.isFinite)) throw new Error('Invalid evolved stats');
      };
      if (TP.applyUpgrade) { if (!TP.applyUpgrade(context.unit, apply)) return; } else apply();
      TP.completeDeferredChoice?.(context.unit, choice.kind, choice.stage);
      S.evoTarget = S.ascTarget = null; S.evoChoices = []; S.ascChoices = []; S.phase = 'recruit'; evolution = null;
      close(); persist(choice.kind); msg(context.unit.n + ' became ' + choice.option.n + '. Your formation is ready when you are.');
    } catch (error) {
      Object.keys(context.unit).forEach(key => delete context.unit[key]); Object.assign(context.unit, before);
      console.warn('Tiny Troops evolution transaction canceled', error); msg('That evolution could not be completed. Your troop and resources are unchanged.'); evolutionPreview(choice.index);
    }
  }
  const nativeOpenEvolution = openEvolution;
  openEvolution = window.openEvolution = function (u) {
    if (!u || !army().includes(u)) return msg('That troop is unavailable. Select a troop from your current army.');
    if (S.phase === 'battle' || S.battle && !S.battle.ttEnded) return msg('Evolution ready. Open Team → Evolutions between waves to choose a path.');
    if (!readiness(u)) return msg(u.star < 5 ? 'This troop unlocks evolution at 5★.' : 'This troop has chosen its evolution and mastery. Higher star paths remain available through training.');
    nativeOpenEvolution(u); if (evolutionContext()) evolutionPreview(0);
  };
  pickEvolution = window.pickEvolution = index => evolutionPreview(index);
  window.pickAscension = index => evolutionPreview(index);
  const nativeShow = show;
  show = window.show = function (heading, hint, html) {
    if (evolutionContext() && typeof html === 'string' && /onclick="pick(?:Evolution|Ascension)\(/.test(html)) return evolutionPreview(0);
    return nativeShow(heading, hint, html);
  };
  function later() { evolution = null; if (TP.deferChoice) TP.deferChoice(); else close(); refresh(); }
  const nativeClose = close;
  close = window.close = function () { if (evolutionContext() && el('drawer')?.dataset.ttView === 'evolution-preview' && TP.deferChoice) { evolution = null; TP.deferChoice(); refresh(); return; } evolution = null; return nativeClose(); };
  if (el('close')) el('close').onclick = close;
  if (el('shade')) el('shade').onclick = close;
  const nativeDetailHtml = detailHtml;
  detailHtml = window.detailHtml = function (u) {
    let body = nativeDetailHtml(u); if (u?.team !== 'ally' || !army().includes(u)) return body;
    const where = S.squad.includes(u) ? 'board' : 'bench', index = (where === 'board' ? S.squad : S.bench).indexOf(u);
    body += `<div class="tt-replacement"><button class="secondary" data-tt-mgmt-select="${where}:${index}">Move / swap</button><button class="secondary" data-tt-mgmt-evolution="${esc(M.id(u))}">${readiness(u) ? '✨ Evolution ready' : 'Evolution & progression'}</button><button class="secondary" data-tt-mgmt="open">Full team management</button></div>`; return body;
  };
  const nativeRender = render;
  render = window.render = function () { const result = nativeRender(); refresh(); return result; };
  function troopButton(value) { const [where, raw] = value.split(':'); return { where, index: Number(raw) }; }
  // Capture prevents the legacy delegated menus from also handling a management click.
  document.addEventListener('click', event => {
    const button = event.target.closest('[data-tt-mgmt],[data-tt-mgmt-tab],[data-tt-mgmt-unit],[data-tt-mgmt-board],[data-tt-mgmt-select],[data-tt-mgmt-save],[data-tt-mgmt-load],[data-tt-mgmt-evolution],[data-tt-evolution-preview],[data-tt-evolution-confirm]');
    if (!button || button.disabled) return;
    event.preventDefault(); event.stopImmediatePropagation(); const d = button.dataset;
    if (d.ttMgmtTab) return openManagement(d.ttMgmtTab);
    if (d.ttMgmtSave !== undefined) return saveFormation(Number(d.ttMgmtSave));
    if (d.ttMgmtLoad !== undefined) return loadFormation(Number(d.ttMgmtLoad));
    if (d.ttEvolutionPreview !== undefined) return evolutionPreview(Number(d.ttEvolutionPreview));
    if (d.ttEvolutionConfirm) return confirmEvolution();
    if (d.ttMgmtEvolution) { const u = army().find(u => M.id(u) === d.ttMgmtEvolution); return openEvolution(u); }
    if (d.ttMgmtSelect) { const ref = troopButton(d.ttMgmtSelect); return select(ref.where, ref.index); }
    if (d.ttMgmtBoard !== undefined) { const i = Number(d.ttMgmtBoard); if (current()) return place({ where: 'board', index: i }); return select('board', i); }
    if (d.ttMgmtUnit) {
      const ref = troopButton(d.ttMgmtUnit);
      if (pendingPlacement()) { nativeBench(); el('body').querySelector(`[data-tt-bench-slot="${ref.index}"]`)?.click(); return; }
      if (current() && selected.where === 'bench' && ref.where === 'bench' && S.bench[ref.index]) return select(ref.where, ref.index);
      if (current()) return place(ref);
      const u = (ref.where === 'board' ? S.squad : S.bench)[ref.index]; if (!u) return msg('Select a deployed troop to return it to this empty reserve slot.');
      return select(ref.where, ref.index);
    }
    ({ open: () => openManagement('troops'), cancel: cancelPlacement, arrange: autoArrange, later, deferred: () => TP.openDeferredChoice?.() })[d.ttMgmt]?.();
  }, true);
  document.addEventListener('dblclick', event => { const cell = event.target.closest('.cell'); if (cell && !pendingPlacement()) select('board', Number(cell.dataset.i)); });
  document.addEventListener('keydown', event => {
    if (event.key === 'Escape' && (current() || pendingPlacement())) { event.preventDefault(); event.stopImmediatePropagation(); cancelPlacement(); }
    else if (event.key === 'Escape' && evolutionContext() && el('drawer').dataset.ttView === 'evolution-preview') { event.preventDefault(); event.stopImmediatePropagation(); later(); }
  }, true);
  function targetAt(event) { const cell = event.target.closest('.cell,[data-tt-mgmt-board]'); if (cell) return { where: 'board', index: Number(cell.dataset.i ?? cell.dataset.ttMgmtBoard) }; const bench = event.target.closest('[data-tt-mgmt-unit^="bench:"]'); return bench ? troopButton(bench.dataset.ttMgmtUnit) : null; }
  function legalTarget(target) { if (!target) return false; if (dragging === 'incoming') return target.where === 'board' ? !S.squad[target.index] || S.squad[target.index].n === S.placing?.n : !S.bench[target.index] || S.bench[target.index].n === S.placing?.n; return !!current() && M.move(S, selected, target).ok; }
  document.addEventListener('dragstart', event => {
    const incoming = event.target.closest('[data-tt-mgmt-incoming]'), card = event.target.closest('[data-tt-mgmt-unit]'), cell = event.target.closest('.cell');
    if (incoming && S.placing) dragging = 'incoming';
    else { const ref = card ? troopButton(card.dataset.ttMgmtUnit) : cell ? { where: 'board', index: Number(cell.dataset.i) } : null; if (!ref || reason() || !selectedRef(ref.where, ref.index)) { event.preventDefault(); return; } dragging = 'unit'; selected = selectedRef(ref.where, ref.index); }
    if (event.dataTransfer) { event.dataTransfer.effectAllowed = 'move'; event.dataTransfer.setData('text/plain', 'tiny-troop'); } refresh();
  });
  document.addEventListener('dragover', event => {
    if (!dragging) return; const target = targetAt(event); all('.tt-drop-preview').forEach(node => { node.classList.remove('tt-drop-preview'); delete node.dataset.ttPreview; });
    if (!legalTarget(target)) { if (event.dataTransfer) event.dataTransfer.dropEffect = 'none'; return; }
    event.preventDefault(); if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
    const node = event.target.closest('.cell,[data-tt-mgmt-board],[data-tt-mgmt-unit]'); if (node) { node.classList.add('tt-drop-preview'); node.dataset.ttPreview = art(dragging === 'incoming' ? S.placing : current()); }
  });
  document.addEventListener('drop', event => {
    if (!dragging) return; const target = targetAt(event), allowed = legalTarget(target), type = dragging; event.preventDefault(); dragging = null;
    if (!allowed) { selected = null; refresh(); return msg('Placement canceled. Your formation is unchanged.'); }
    if (type === 'incoming') { if (target.where === 'board') tapCell(target.index); else { nativeBench(); el('body').querySelector(`[data-tt-bench-slot="${target.index}"]`)?.click(); } }
    else place(target); refresh();
  });
  document.addEventListener('dragend', () => { dragging = null; selected = null; refresh(); });
  window.addEventListener('popstate', () => { if (window.TinyTroopsNavigation) return; if (current() || pendingPlacement()) cancelPlacement(); else if (evolutionContext()) later(); });
  const beforeRestore = TP.beforeRestore;
  TP.beforeRestore = function () { selected = dragging = evolution = null; return beforeRestore?.(); };
  window.TinyTroopsManagement = { refresh, cancelPlacement, selectBoard: i => select('board', i), selectBench: i => select('bench', i), open: openManagement, autoArrange, saveFormation, loadFormation, confirmEvolution, previewEvolution: evolutionPreview, selected: () => current() ? { ...selected } : null };
  refresh();
})();
