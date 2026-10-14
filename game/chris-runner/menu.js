/* Progressive enhancement of the existing menu; gameplay retains its own handlers. */
(() => {
  'use strict';
  const runnerRoles = {
    chris: 'All-rounder', dawit: 'Speedster', jay: 'Dodge master',
    anthony: 'Parkour pro', kevon: 'Powerhouse'
  };
  const arrow = '<span class="runnerButtonArrow" aria-hidden="true">↗</span>';

  function getAbility(key) {
    return typeof RUNNER_ABILITIES !== 'undefined' ? RUNNER_ABILITIES[key] : null;
  }

  function decorateCards(grid) {
    grid.querySelectorAll('.characterCard').forEach((card, index) => {
      if (card.dataset.menuStyled) return;
      card.dataset.menuStyled = 'true';
      const key = card.dataset.character;
      const topline = document.createElement('span');
      topline.className = 'runnerCardTopline';
      const number = document.createElement('span');
      number.className = 'runnerCardNumber';
      number.textContent = `0${index + 1}`;
      number.setAttribute('aria-hidden', 'true');
      const role = document.createElement('span');
      role.className = 'runnerCardRole';
      role.textContent = runnerRoles[key] || 'Runner';
      topline.append(number, role);
      card.prepend(topline);

      const ability = getAbility(key);
      const stats = card.querySelector('.characterStats');
      if (ability && stats) {
        stats.replaceChildren();
        [['runnerCardDescription', ability.description],
          ['runnerCardStrength', `+ ${ability.strength}`],
          ['runnerCardWeakness', `− ${ability.weakness}`]].forEach(([className, text]) => {
          const line = document.createElement('span');
          line.className = className;
          line.textContent = text;
          stats.append(line);
        });
      }
    });
  }

  function updatePreview(grid) {
    const selected = grid.querySelector('[aria-pressed="true"]');
    const key = selected?.dataset.character;
    const ability = getAbility(key);
    const badge = document.getElementById('runnerPreviewBadge');
    const abilityLabel = document.getElementById('runnerPreviewAbility');
    if (badge) badge.textContent = runnerRoles[key] || 'Your runner';
    if (abilityLabel && ability) {
      abilityLabel.replaceChildren();
      const keycap = document.createElement('kbd');
      keycap.textContent = 'SPACE';
      abilityLabel.append(keycap, document.createTextNode(` ${ability.name} · ${ability.cost} energy`));
    }
  }

  function enhanceMenu() {
    const menu = document.getElementById('menuOverlay');
    const card = document.getElementById('menuCard');
    const grid = document.getElementById('characterGrid');
    if (!menu || !card || !grid || menu.dataset.modernMenu) return;
    menu.dataset.modernMenu = 'true';
    const title = card.querySelector('h1');
    if (title) {
      title.id ||= 'runnerMenuTitle';
      title.innerHTML = '<span>Chris</span><span class="runnerTitleAccent">Runner</span>';
      menu.setAttribute('role', 'dialog');
      menu.setAttribute('aria-labelledby', title.id);
      menu.setAttribute('aria-modal', 'true');
    }
    const subtitle = card.querySelector('h2');
    if (subtitle) subtitle.textContent = 'Pick your runner. Find your edge. Keep the chase alive.';
    const kicker = document.createElement('div');
    kicker.className = 'runnerMenuKicker';
    kicker.innerHTML = '<span class="runnerKickerLabel">THE CHASE IS ON</span><span class="runnerWorldBadge">16 WORLDS · 5 RUNNERS</span>';
    card.prepend(kicker);

    const buttons = [...document.querySelectorAll('#menuButtons button')];
    const solo = buttons.find(button => /startGame\(\s*1\s*\)/.test(button.getAttribute('onclick') || ''));
    const versus = buttons.find(button => /startGame\(\s*2\s*\)/.test(button.getAttribute('onclick') || ''));
    const replay = buttons.find(button => /openReplayLoadPicker/.test(button.getAttribute('onclick') || ''));
    if (solo) {
      solo.classList.add('runnerSoloButton');
      solo.innerHTML = `<span class="runnerButtonLabel">Run solo<span class="runnerButtonHint">One runner. Endless possibilities.</span></span>${arrow}`;
      solo.setAttribute('aria-label', 'Run solo, one player');
    }
    if (versus) {
      versus.classList.add('runnerVersusButton');
      versus.innerHTML = `<span class="runnerButtonLabel">Local versus<span class="runnerButtonHint">Two players. One keyboard.</span></span>${arrow}`;
      versus.setAttribute('aria-label', 'Local versus, two players on one keyboard');
    }
    if (replay) {
      replay.classList.add('runnerReplayButton');
      replay.textContent = '↺  Load a saved replay';
    }

    const controls = document.getElementById('menuControls');
    if (controls && !controls.querySelector('details')) {
      const details = document.createElement('details');
      const summary = document.createElement('summary');
      summary.className = 'runnerControlsSummary';
      summary.textContent = 'How to play · controls';
      const body = document.createElement('div');
      body.className = 'runnerControlsBody';
      while (controls.firstChild) body.append(controls.firstChild);
      const touch = document.createElement('div');
      touch.className = 'runnerMobileHint';
      touch.textContent = 'On touch screens, use the on-screen movement, sprint, and ability controls.';
      body.append(touch);
      details.append(summary, body);
      controls.append(details);
    }
    const preview = document.getElementById('characterPreviewWrap');
    if (preview) {
      const label = document.createElement('span');
      label.className = 'runnerPreviewKicker';
      label.textContent = 'YOUR STARTING LINE';
      const badge = document.createElement('span');
      badge.id = 'runnerPreviewBadge';
      badge.className = 'runnerPreviewBadge';
      preview.prepend(label, badge);
      const name = document.getElementById('previewCharacterName');
      if (name) { name.setAttribute('aria-live', 'polite'); name.setAttribute('aria-atomic', 'true'); }
      const ability = document.createElement('div');
      ability.id = 'runnerPreviewAbility';
      ability.className = 'runnerPreviewAbility';
      document.getElementById('characterPreviewInfo')?.append(ability);
    }

    decorateCards(grid);
    updatePreview(grid);
    grid.addEventListener('keydown', event => {
      const cards = [...grid.querySelectorAll('.characterCard')];
      const current = cards.indexOf(event.target.closest('.characterCard'));
      if (current < 0) return;
      let next;
      if (event.key === 'ArrowRight' || event.key === 'ArrowDown') next = (current + 1) % cards.length;
      else if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') next = (current + cards.length - 1) % cards.length;
      else if (event.key === 'Home') next = 0;
      else if (event.key === 'End') next = cards.length - 1;
      else return;
      event.preventDefault();
      event.stopPropagation();
      cards[next].focus();
      cards[next].click();
    });
    document.addEventListener('keydown', event => {
      if (event.key !== 'Tab' || !menu.getClientRects().length) return;
      const loading = document.getElementById('runnerLoadOverlay');
      if (loading && !loading.hidden && loading.getClientRects().length) return;
      const focusable = [...menu.querySelectorAll('button:not(:disabled), summary, input:not([disabled])')]
        .filter(element => element.getClientRects().length && getComputedStyle(element).visibility !== 'hidden');
      if (!focusable.length) return;
      const index = focusable.indexOf(document.activeElement);
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (index < 0 || (!event.shiftKey && document.activeElement === last) || (event.shiftKey && document.activeElement === first)) {
        event.preventDefault();
        (event.shiftKey ? last : first).focus();
      }
    }, true);
    // The game rebuilds its roster during initialization and updates pressed states
    // when a saved selection or replay changes. Keep decoration in sync with it.
    new MutationObserver(records => {
      if (records.some(record => record.type === 'childList')) decorateCards(grid);
      updatePreview(grid);
    }).observe(grid, { childList: true, subtree: true, attributes: true, attributeFilter: ['aria-pressed'] });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', enhanceMenu, { once: true });
  else enhanceMenu();
})();
