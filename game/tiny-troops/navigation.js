/* One temporary history entry makes browser Back dismiss the live game UI.
   Native X/confirmation paths are observed too, including private forceClose. */
(function () {
  'use strict';
  const KEY = '__tinyTroopsNavigationV1', APP = 'tiny-troops';
  const session = Date.now().toString(36) + '-' + Math.random().toString(36).slice(2);
  const node = id => document.getElementById(id);
  let sequence = 0, tracked = null, cleanup = null, scheduled = false, available = true, warned = false;
  function marker(state = history.state) {
    const value = state && typeof state === 'object' ? state[KEY] : null;
    return value?.app === APP && value.url === location.href && typeof value.session === 'string' && Number.isInteger(value.sequence) ? value : null;
  }
  function live() {
    if (node('drawer')?.classList.contains('open')) return 'dialog';
    if (typeof S === 'undefined' || S.tt?.mode === 'MENU' || node('mainMenu')?.classList.contains('open')) return null;
    if (S.placing || S.placingUpgrade || S.placingEffect || S.placingSpecialization || window.TinyTroopsManagement?.selected?.()) return 'placement';
    const quick = node('ttQuickInspect');
    return quick && !quick.hidden ? 'inspect' : null;
  }
  function warn(error) {
    if (!warned) { warned = true; console.warn('Tiny Troops browser navigation unavailable', error); }
  }
  function push() {
    const previous = history.state;
    const state = previous && typeof previous === 'object' && !Array.isArray(previous) ? { ...previous } : {};
    const entry = { app: APP, session, sequence: ++sequence, url: location.href, previous };
    state[KEY] = entry;
    try { history.pushState(state, '', location.href); tracked = entry; }
    catch (error) { available = false; warn(error); }
  }
  function remove(entry) {
    if (cleanup || !entry) return;
    cleanup = { session: entry.session, sequence: entry.sequence };
    try { history.back(); }
    catch (error) { cleanup = null; available = false; warn(error); }
  }
  function sync() {
    scheduled = false;
    if (!available || cleanup) return;
    const entry = marker(), active = live();
    if (entry) {
      tracked = entry;
      if (!active) remove(entry);
    } else {
      tracked = null;
      if (active) push();
    }
  }
  function queue() { if (!scheduled) { scheduled = true; queueMicrotask(sync); } }
  function settleViewport() {
    // Native history traversal restores scrolling after popstate. Let it finish
    // before applying the game's existing mobile battlefield visibility rule.
    requestAnimationFrame(() => requestAnimationFrame(() => {
      if (!node('drawer')?.classList.contains('open') && innerWidth < 850) window.TinyTroopsPresentation?.keepBattlefieldVisible?.();
    }));
  }
  function dismiss(kind) {
    if (kind === 'dialog') window.close();
    else if (kind === 'placement') {
      if (window.TinyTroopsManagement?.cancelPlacement) window.TinyTroopsManagement.cancelPlacement();
      else window.TinyTroopsPolish?.cancelPlacement?.();
    } else if (kind === 'inspect') node('ttQuickInspect')?.querySelector('[data-tt-quick="close"]')?.click();
  }
  // Management delegates its fallback Back handling to this history owner.
  // Internal cleanup reconciles live UI without canceling a newer selection.
  window.addEventListener('popstate', event => {
    const entry = marker(event.state), previous = tracked;
    if (!cleanup && !previous && !entry) return;
    event.stopImmediatePropagation();
    if (cleanup) {
      cleanup = null; tracked = entry;
      // The old panel may have been replaced while history.back was pending.
      // Reconcile its current state without dismissing anything a second time.
      sync(); settleViewport(); return;
    }
    if (entry) {
      tracked = entry;
      // Forward can revisit an entry whose panel has already been dismissed.
      // Skip that entry instead of manufacturing an empty dialog.
      sync(); settleViewport(); return;
    }
    tracked = null;
    const kind = live();
    if (kind) {
      try { dismiss(kind); }
      catch (error) { warn(error); }
    }
    // A dialog may sit above an unfinished placement. One Back closes the
    // dialog; the next Back cancels the remaining placement safely.
    sync(); settleViewport();
  }, true);
  const observer = new MutationObserver(queue);
  const observe = (target, options) => { if (target) observer.observe(target, options); };
  observe(node('drawer'), { attributes: true, attributeFilter: ['class', 'aria-hidden'] });
  observe(node('pick'), { attributes: true, attributeFilter: ['class'] });
  observe(node('grid'), { attributes: true, attributeFilter: ['class'], subtree: true, childList: true });
  observe(node('ttBenchBar'), { childList: true, subtree: true });
  observe(node('ttQuickInspect'), { attributes: true, attributeFilter: ['hidden'], childList: true, subtree: true });
  observe(node('mainMenu'), { attributes: true, attributeFilter: ['class'] });
  // These hooks also catch state changes that do not need a visible redraw.
  const previousRender = window.render;
  window.render = function () { const result = previousRender.apply(this, arguments); queue(); return result; };
  document.addEventListener('click', queue, true);
  document.addEventListener('keydown', queue, true);
  window.TinyTroopsNavigation = {
    sync,
    status: () => ({ active: live(), entry: !!marker(), cleaning: !!cleanup, available })
  };
  // A reload can retain our old sentinel. It still has a same-page predecessor,
  // so release it through the same guarded cleanup before tracking new UI.
  const existing = marker();
  if (existing && existing.session !== session) { tracked = existing; remove(existing); }
  else sync();
})();
