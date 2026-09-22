'use strict';
/* Final mobile HUD/travel tuning. Load after campaign content, before runtime-stability.
 * No saved-character stats, combat rules, timers or render loops are changed here.
 */
(() => {
  if (window.AWMobileTravel) return;
  const MOUNT_SPEED_MULTIPLIER = 1.6;
  const MOBILE_ZOOM = .86;
  const root = document.documentElement;
  const query = window.matchMedia('(pointer: coarse), (max-width: 760px)');
  const camera = window.AWPresentation?.camera;
  const tools = document.getElementById('buttons');
  const hud = document.getElementById('hud');
  const spells = document.getElementById('spells');
  let compact = false;
  let appliedZoom = 1;

  // Tune the shared catalogue, so stables, existing saves and every regional mount
  // use the same speed. The normal movement adapter still applies terrain bonuses
  // and restores player.speed; casting, damage and dodging still dismount normally.
  for (const mount of Object.values(window.AWCampaignData?.mounts || {})) {
    if (Number.isFinite(mount.speed) && mount.speed > 0) {
      mount.speed = Math.round(mount.speed * MOUNT_SPEED_MULTIPLIER * 1000) / 1000;
    }
  }

  let toggle = null;
  if (hud && tools) {
    toggle = document.createElement('button');
    toggle.id = 'awMobileMenuToggle';
    toggle.type = 'button';
    toggle.className = 'round-btn';
    toggle.textContent = '•••';
    toggle.title = 'Map, spellbook, mount and settings';
    toggle.setAttribute('aria-label', 'Open adventure tools');
    toggle.setAttribute('aria-controls', 'buttons');
    toggle.setAttribute('aria-expanded', 'false');
    hud.appendChild(toggle);
    toggle.addEventListener('click', () => setMenuOpen(!root.classList.contains('aw-mobile-tools-open')));
    tools.addEventListener('click', event => {
      // Delegate so tools added by later content packs also close the compact tray.
      if (event.target.closest('button')) setMenuOpen(false);
    });
    document.addEventListener('keydown', event => {
      if (event.key === 'Escape' && root.classList.contains('aw-mobile-tools-open')) {
        setMenuOpen(false);
        toggle.focus();
      }
    });
  }

  function setMenuOpen(open) {
    const expanded = compact && !!open;
    root.classList.toggle('aw-mobile-tools-open', expanded);
    toggle?.setAttribute('aria-expanded', String(expanded));
    toggle?.setAttribute('aria-label', expanded ? 'Close adventure tools' : 'Open adventure tools');
  }

  function syncSpellRows() {
    const count = Math.max(1, Math.min(5, spells?.querySelectorAll('.spell-slot').length || 3));
    // Updated only on a loadout change, never per frame. CSS switches to one row
    // in landscape; portrait reserves the second row for rare fourth/fifth slots.
    root.style.setProperty('--aw-mobile-spell-rows', String(Math.ceil(count / 3)));
  }

  function applyZoom() {
    const next = compact ? MOBILE_ZOOM : 1;
    if (camera) camera.zoom = camera.zoom / appliedZoom * next;
    appliedZoom = next;
  }

  function syncLayout() {
    compact = !!(isTouch || query.matches);
    root.classList.toggle('aw-mobile-compact', compact);
    setMenuOpen(false);
    syncSpellRows();
    applyZoom();
  }

  if (camera) {
    const baseTick = camera.tick;
    camera.tick = function(dt) {
      // Feed the original smoothing its unscaled zoom, then scale once. Multiplying
      // the already-scaled value every tick would progressively collapse the view.
      this.zoom /= appliedZoom;
      appliedZoom = 1;
      try { return baseTick.call(this, dt); }
      finally { applyZoom(); }
    };
  }
  query.addEventListener('change', syncLayout);
  if (spells) new MutationObserver(syncSpellRows).observe(spells, {
    childList: true, attributes: true, attributeFilter: ['data-spell-slots']
  });
  window.AWMobileTravel = {
    mountSpeedMultiplier: MOUNT_SPEED_MULTIPLIER,
    mobileZoom: MOBILE_ZOOM,
    syncLayout, syncSpellRows, setMenuOpen,
    get compact() { return compact; }
  };
  syncLayout();
})();
