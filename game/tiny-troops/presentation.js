/* Bounded presentation adapter. Combat never waits for, or depends on, an effect. */
(function () {
  'use strict';
  const $ = id => document.getElementById(id), R = window.TinyTroopsRules;
  const stylesheet = document.createElement('link'); stylesheet.rel = 'stylesheet'; stylesheet.href = 'tiny-troops/presentation.css?v=20261025'; document.head.appendChild(stylesheet);
  const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const motion = matchMedia('(prefers-reduced-motion: reduce)');
  let quality = 'full', selected = null, frame = null, epoch = '', warned = false;
  const pool = [], effects = new Map(), lastFloat = new WeakMap(), lastAbility = new WeakMap(), lastLabel = new WeakMap(), motions = new Set(), defeated = new WeakSet();
  try { quality = ['full', 'reduced', 'minimal'].includes(localStorage.getItem('ttEffects')) ? localStorage.getItem('ttEffects') : 'full'; } catch { /* Device preferences are optional. */ }
  const running = () => S.phase === 'battle' && S.tt?.mode === 'PLAYING' && !TinyTroopsPolish.recovery;
  const level = () => motion.matches ? 'minimal' : quality;
  const cap = () => level() === 'minimal' ? 8 : level() === 'reduced' ? 18 : innerWidth < 600 ? 28 : 48;
  function visual(action) { try { return action(); } catch (error) { if (!warned) { warned = true; console.warn('Battle presentation unavailable', error); } return null; } }
  function release(node) { effects.delete(node); node.remove(); node.className = ''; node.textContent = ''; node.removeAttribute('style'); if (pool.length < 48) pool.push(node); }
  function clear() { if (frame !== null) cancelAnimationFrame(frame); frame = null; [...effects.keys()].forEach(release); motions.forEach(animation => animation.cancel()); motions.clear(); document.querySelectorAll('.tt-attacking,.tt-under-attack').forEach(n => n.classList.remove('tt-attacking', 'tt-under-attack')); }
  function moveArt(node, x, y, hit = false) {
    if (level() !== 'full' || motions.size >= 16) return;
    const art = node?.querySelector('.emo,.ee'); if (!art?.animate) return;
    const base = getComputedStyle(art).transform, normal = base === 'none' ? '' : base;
    const animation = art.animate([{ transform: normal }, { transform: normal + ` translate(${x}px,${y}px)`, filter: hit ? 'brightness(1.5)' : 'none', offset: .4 }, { transform: normal }], { duration: 240, easing: 'ease-out' });
    motions.add(animation); animation.onfinish = animation.oncancel = () => motions.delete(animation);
  }
  function tick(now) {
    frame = null;
    if (!running()) return clear();
    effects.forEach((until, node) => { if (until <= now) release(node); });
    if (effects.size) frame = requestAnimationFrame(tick);
  }
  function emit(kind, point, duration, text, type, delta) {
    if (!running() || effects.size >= cap() || !point) return null;
    const node = pool.pop() || document.createElement('span');
    node.className = 'tt-vfx tt-vfx-' + kind + ' tt-element-' + (type || 'hit'); node.textContent = text || '';
    node.setAttribute('aria-hidden', 'true'); node.style.left = point.x + 'px'; node.style.top = point.y + 'px'; node.style.setProperty('--life', duration + 'ms');
    if (delta) { node.style.setProperty('--dx', delta.x + 'px'); node.style.setProperty('--dy', delta.y + 'px'); node.style.setProperty('--angle', Math.atan2(delta.y, delta.x) + 'rad'); }
    document.body.appendChild(node); effects.set(node, performance.now() + duration);
    if (frame === null) frame = requestAnimationFrame(tick); return node;
  }
  function typeOf(type) { return ({ burn: 'fire', slow: 'frost', ice: 'frost', lightning: 'storm', heal: 'healing', holy: 'healing', gear: 'siege' })[type] || (/^(fire|frost|storm|poison|healing|shield|shadow|siege)$/.test(type) ? type : 'hit'); }
  attackVisual = function (source, target, type = 'hit', label = '') {
    visual(() => {
      if (!source || !target || target.dead || !running() || level() === 'minimal') return;
      const from = elForUnit(source), to = elForUnit(target); if (!from?.isConnected || !to?.isConnected) return;
      const a = centerOf(from), b = centerOf(to), element = typeOf(type), ranged = R.has(source, 'RANGED', 'MAGIC') || source.kind === 'ranged';
      emit(ranged ? 'shot' : 'slash', a, level() === 'reduced' ? 230 : 380, ranged ? ({ fire: '🔥', frost: '❄', storm: 'ϟ', poison: '●', siege: '◆', healing: '✦', hit: '➶' })[element] || '✦' : '╱', element, { x: b.x - a.x, y: b.y - a.y });
      emit('impact', b, 420, element === 'shield' ? '◇' : '✧', element);
      const length = Math.max(1, Math.hypot(b.x - a.x, b.y - a.y)), dx = (b.x - a.x) / length, dy = (b.y - a.y) / length;
      if (!ranged) moveArt(from, dx * 7, dy * 7); moveArt(to, dx * 3, dy * 3, true);
      // Source/target highlights are CSS effects; stale nodes never reach combat.
      from.dataset.ttAttackAt = String(performance.now()); to.dataset.ttTargetAt = String(performance.now());
      const now = performance.now();
      if (label && element !== 'hit' && level() === 'full' && now - (lastAbility.get(source) || 0) > 3000) { lastAbility.set(source, now); emit('ability', a, 850, label.slice(0, 36), element); }
    });
  };
  placeFloat = function (node, text, color) {
    visual(() => {
      if (!node?.isConnected || !running()) return;
      const now = performance.now(); if (now - (lastFloat.get(node) || 0) < (level() === 'minimal' ? 550 : 160)) return;
      lastFloat.set(node, now); const value = String(text), healing = /^\+|heal/i.test(value) || /22c55e|16a34a|10b981/i.test(color || '');
      const kind = /crit|!|×/i.test(value) ? 'critical' : /block|shield|resist|immune/i.test(value) ? 'blocked' : healing ? 'healing' : 'damage';
      const effect = emit('number', centerOf(node), level() === 'minimal' ? 450 : 760, value, kind); if (effect && color) effect.style.color = color;
    });
  };
  floatAlly = (i, text, color) => placeFloat(TinyTroopsPolish.allyNodes?.[i], text, color);
  floatEnemy = (i, text, color) => placeFloat(TinyTroopsPolish.enemyNodes?.[i], text, color);
  bossVfx = function (kind, node) { visual(() => { if (node?.isConnected && level() !== 'minimal') emit('area', centerOf(node), 520, '◎', /ice/i.test(kind) ? 'frost' : /poison/i.test(kind) ? 'poison' : /holy|heal/i.test(kind) ? 'healing' : 'fire'); }); };
  intentTag = function (node, label, type = 'hit') { visual(() => { if (!node?.isConnected || level() === 'minimal' || !label || performance.now() - (lastLabel.get(node) || 0) < 700) return; lastLabel.set(node, performance.now()); emit('ability', centerOf(node), 800, String(label).slice(0, 36), typeOf(type)); }); };
  TinyTroopsPolish.addLegacyFx = (node, life) => visual(() => { if (level() === 'full' && node && !node.classList.contains('regionBanner')) { const point = { x: parseFloat(node.style.left) || innerWidth / 2, y: parseFloat(node.style.top) || innerHeight / 2 }; emit('impact', point, Math.min(life || 400, 600), '✧', typeOf(node.className)); } });
  const legacyFlash = TinyTroopsPolish.legacyFlash; TinyTroopsPolish.legacyFlash = function () { if (level() !== 'minimal') return visual(() => legacyFlash.apply(this, arguments)); };

  const ribbon = document.createElement('section'); ribbon.id = 'ttBattleRibbon'; ribbon.setAttribute('aria-label', 'Battle progress');
  $('enemyRow').setAttribute('role', 'region'); $('enemyRow').setAttribute('aria-label', 'Enemy formation. Scroll vertically to inspect all enemies.'); $('enemyRow').tabIndex = 0;
  ribbon.innerHTML = '<div class="tt-force tt-friendly"><span>YOUR ARMY <b id="ttAlliedCount"></b></span><progress id="ttAlliedHealth" max="100" value="100" aria-label="Army health"></progress></div><span class="tt-versus">⚔</span><div class="tt-force tt-hostile"><span>ENEMY WAVE <b id="ttEnemyCount"></b></span><progress id="ttEnemyHealth" max="100" value="100" aria-label="Enemy health"></progress></div>';
  $('ttWavePreview').before(ribbon);
  const events = document.createElement('details'); events.id = 'ttBattleEvents'; events.innerHTML = '<summary>Battle events <span>↗</span></summary><ol></ol>'; $('ttIntel').appendChild(events);
  const gearSynergies = document.createElement('details'); gearSynergies.id = 'ttGearSynergies'; gearSynergies.hidden = true; $('ttIntel').appendChild(gearSynergies);
  const tools = document.createElement('div'); tools.className = 'tt-battle-tools'; tools.innerHTML = '<button class="secondary" id="ttTeamTools">☷ Team</button><button class="secondary" id="ttArmoryTools">⚙ Armory</button><button class="secondary" id="ttSavedChoices">☆ Choices</button><button class="secondary" id="ttVisualSettings" aria-label="Battle settings">⚙</button>';
  document.querySelector('.tt-control-row').appendChild(tools);
  document.querySelector('header .stats').appendChild($('benchBtn'));
  $('ttTeamTools').onclick = () => window.TinyTroopsManagement?.open('troops');
  $('ttArmoryTools').onclick = () => window.TinyTroopsEquipment?.openArmory();
  $('ttSavedChoices').onclick = () => TinyTroopsPolish.openDeferredChoice?.();
  const quick = document.createElement('section'); quick.id = 'ttQuickInspect'; quick.hidden = true; quick.setAttribute('aria-label', 'Selected troop'); document.querySelector('footer').prepend(quick);
  function keepBattlefieldVisible() {
    if (S.tt?.mode === 'MENU') return;
    const grid = $('grid'), main = document.querySelector('.main'), bounds = grid.getBoundingClientRect(), view = main.getBoundingClientRect();
    if (bounds.height < view.height && (bounds.bottom > view.bottom || bounds.top < view.top)) grid.scrollIntoView({ block: 'nearest', inline: 'nearest' });
  }
  const fullDetail = detail;
  function owned(unit) { return unit && [...S.squad, ...S.enemies, ...(S.bench || []), ...(S.tt?.wavePlan?.enemies || [])].includes(unit); }
  function quickHtml(unit) {
    const role = R.ROLE_INFO[R.role(unit)], health = Math.max(0, Math.ceil(unit.hp || 0));
    const effects = [unit.burn > 0 ? '🔥 Burning' : '', unit.poison > 0 ? '☠ Poisoned' : '', unit.slow > 0 ? '❄ Slowed' : '', unit.shield > 0 ? '🛡 ' + Math.round(unit.shield) : ''].filter(Boolean);
    return '<span class="tt-quick-art">' + esc(unit.e) + '</span><div class="tt-quick-copy"><b>' + esc(heroTitle(unit)) + '</b><span>' + role.emoji + ' ' + esc(role.name) + ' · ' + (unit.star || 1) + '★ · ' + esc(unit.evoTitle || unit.evo || 'Base') + '</span><span>♥ ' + health + '/' + Math.ceil(unit.maxHp || 1) + '　⚔ ' + Math.round(unit.atk || 0) + '　🛡 ' + Math.round(unit.armor || unit.damageDR * 100 || 0) + '</span><small>' + esc(unit.a || unit.ability || role.tip) + (effects.length ? ' · ' + esc(effects.join(' · ')) : '') + '</small></div><div class="tt-quick-actions"><button class="secondary" data-tt-quick="inspect">Inspect</button>' + (unit.team === 'ally' ? '<button class="secondary" data-tt-quick="equipment">Equip</button>' + (S.squad.includes(unit) && S.phase !== 'battle' ? '<button class="secondary" data-tt-quick="move">Move</button>' : '') : '') + '</div><button class="secondary tt-dismiss" data-tt-quick="close" aria-label="Close troop details">×</button>';
  }
  detail = function (unit) { if (!owned(unit)) return; selected = unit; refresh(); requestAnimationFrame(keepBattlefieldVisible); };
  quick.addEventListener('click', event => {
    const action = event.target.closest('[data-tt-quick]')?.dataset.ttQuick;
    if (action === 'close') { selected = null; refresh(); }
    if (action === 'inspect' && owned(selected)) fullDetail(selected);
    if (action === 'equipment' && owned(selected)) window.TinyTroopsEquipment?.openUnit?.(selected);
    if (action === 'move' && owned(selected)) window.TinyTroopsManagement?.selectBoard(S.squad.indexOf(selected));
  });
  function settings() {
    show('Battle settings', 'Choose how much visual feedback to show.', '<label class="tt-settings-label">Battle effects<select id="ttEffectQuality"><option value="full">Full · projectiles and elemental impacts</option><option value="reduced">Reduced · fewer effects</option><option value="minimal">Minimal · combat numbers</option></select></label><p class="hint">Your device’s reduced-motion preference always takes priority. Battles use the same rules at every setting.</p><button class="secondary tt-wide" id="ttSettingsDone">Done</button>');
    $('ttEffectQuality').value = quality;
    $('ttEffectQuality').onchange = event => { quality = event.target.value; clear(); try { localStorage.setItem('ttEffects', quality); } catch { /* Preference remains active in memory. */ } };
    $('ttSettingsDone').onclick = () => close();
  }
  $('ttVisualSettings').onclick = settings;
  function refresh() {
    visual(() => {
      const current = [S.tt?.id, S.battle?.id, S.tt?.mode, S.phase].join(':'); if (current !== epoch) { clear(); epoch = current; }
      if (!owned(selected) || S.tt?.mode === 'MENU') selected = null;
      quick.hidden = !selected;
      if (selected) { const html = quickHtml(selected); if (quick.innerHTML !== html) quick.innerHTML = html; }
      const combat = S.phase === 'battle', enemy = combat ? S.enemies : S.tt?.wavePlan?.enemies || [];
      const summary = units => { const list = units.filter(Boolean), live = list.filter(u => !u.dead && u.hp > 0); return { count: live.length, total: list.length, health: list.reduce((n, u) => n + Math.max(0, u.hp || 0), 0) / Math.max(1, list.reduce((n, u) => n + (u.maxHp || 1), 0)) * 100 }; };
      const a = summary(S.squad), e = summary(enemy);
      $('ttAlliedCount').textContent = a.count + '/' + a.total; $('ttEnemyCount').textContent = e.count + ' left';
      $('ttAlliedHealth').value = a.health; $('ttEnemyHealth').value = e.health;
      if (combat && running() && level() === 'full') [...S.squad, ...S.enemies].filter(Boolean).forEach(unit => { if ((unit.dead || unit.hp <= 0) && !defeated.has(unit)) { defeated.add(unit); const node = elForUnit(unit); if (node?.isConnected) emit('impact', centerOf(node), 600, '✧', unit.team === 'ally' ? 'healing' : 'fire'); } });
      const pending = TinyTroopsPolish.pendingDecisionCount?.() || 0; $('ttSavedChoices').hidden = !pending; $('ttSavedChoices').textContent = '☆ Choices ' + pending;
      const activeGear = window.TinyTroopsEquipment?.synergies() || []; gearSynergies.hidden = !activeGear.length;
      const gearHtml = '<summary>Equipment synergies · ' + activeGear.length + '</summary>' + activeGear.map(s => '<p><b>' + esc(s.name) + '</b> · ' + esc(s.description) + '</p>').join(''); if (gearSynergies.innerHTML !== gearHtml) gearSynergies.innerHTML = gearHtml;
      const log = (S.log || []).slice(0, 6).map(text => '<li>' + esc(typeof text === 'string' ? text : text.text || text.message || '') + '</li>').join('') || '<li>Battle events will appear here.</li>';
      const list = events.querySelector('ol'); if (list.innerHTML !== log) list.innerHTML = log;
      $('close').textContent = '×'; $('close').setAttribute('aria-label', 'Close panel'); $('close').title = 'Close · Escape';
      document.querySelectorAll('.cell').forEach((cell, index) => { cell.classList.toggle('tt-inspected', S.squad[index] === selected && !!selected); });
    });
  }
  const oldRender = render; render = function () { const result = oldRender.apply(this, arguments); refresh(); return result; };
  const oldEnd = end; end = function () { clear(); return oldEnd.apply(this, arguments); };
  const oldReset = reset; reset = function () { clear(); selected = null; return oldReset.apply(this, arguments); };
  const oldClose = close; close = function () { const result = oldClose.apply(this, arguments); if (innerWidth < 850) requestAnimationFrame(keepBattlefieldVisible); return result; };
  $('close').onclick = () => close(); $('shade').onclick = () => close();
  document.addEventListener('visibilitychange', () => { if (document.hidden) clear(); });
  document.addEventListener('keydown', event => { if (event.key === 'Escape' && !$('drawer').classList.contains('open')) { selected = null; refresh(); } });
  document.addEventListener('keydown', event => { if (event.key === 'Escape' && /INPUT|SELECT|TEXTAREA/.test(event.target.tagName) && $('drawer').classList.contains('open')) { event.preventDefault(); close(); } }, true);
  motion.addEventListener('change', clear);
  window.addEventListener('pagehide', clear);
  window.addEventListener('resize', () => requestAnimationFrame(keepBattlefieldVisible));
  window.TinyTroopsPresentation = { refresh, clear, settings, keepBattlefieldVisible, get quality() { return quality; }, get activeEffects() { return effects.size; }, get pooledEffects() { return pool.length; } };
  Object.assign(window, { render, detail, attackVisual, placeFloat, floatAlly, floatEnemy, bossVfx, intentTag, end, reset, close });
  refresh();
})();
