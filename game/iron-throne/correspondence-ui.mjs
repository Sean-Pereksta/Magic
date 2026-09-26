// Presentation only. The original proposal nodes and their delegated handlers
// remain authoritative. This module never evaluates, submits or ratifies a deal.
export function houseFromLabel(label, houses) {
  const name = String(label || '').replace(/^YOU\s*·\s*/i, '').trim().toLowerCase();
  return houses.find(h => h.id === name || h.name.toLowerCase() === name) || null;
}

export function installCorrespondence(doc, { portraits = {}, houses = [] } = {}) {
  const dialog = doc.getElementById('diplomacy');
  if (!dialog || dialog.dataset.correspondenceReady) return;
  const get = id => doc.getElementById(id);
  const messages = get('messages'), proposals = get('proposals');
  const desk = dialog.querySelector('.treaty-desk'), conversation = dialog.querySelector('.conversation');
  if (!messages || !proposals || !desk || !conversation) return;
  dialog.dataset.correspondenceReady = 'true';
  const win = doc.defaultView;
  const make = (tag, cls, text) => {
    const node = doc.createElement(tag);
    if (cls) node.className = cls;
    if (text !== undefined) node.textContent = text;
    return node;
  };
  const button = (label, cls = '') => {
    const node = make('button', cls, label); node.type = 'button'; return node;
  };
  const scroll = make('div', 'correspondence-scroll');
  scroll.setAttribute('role', 'log'); scroll.setAttribute('aria-label', 'Diplomatic correspondence');
  scroll.setAttribute('aria-live', 'polite'); scroll.setAttribute('aria-relevant', 'additions');
  scroll.tabIndex = 0;
  messages.before(scroll); scroll.append(messages);
  messages.removeAttribute('role');
  const cards = make('div', 'correspondence-cards'); scroll.append(cards);
  const latest = button('Latest correspondence ↓', 'correspondence-latest');
  latest.hidden = true; scroll.after(latest);
  // #messages is intentionally not the scroll container: app.mjs resets its
  // scrollTop on each render. Keeping a separate viewport protects readers.
  let pinned = true, savedTop = 0, previousHouse = '', previousLog = '';
  scroll.addEventListener('scroll', () => {
    savedTop = scroll.scrollTop;
    pinned = scroll.scrollHeight - scroll.clientHeight - savedTop < 48;
    if (pinned) latest.hidden = true;
  }, { passive: true });
  latest.addEventListener('click', () => {
    pinned = true; scroll.scrollTop = scroll.scrollHeight; latest.hidden = true;
  });

  desk.id = 'treaty-drawer'; desk.tabIndex = -1;
  desk.setAttribute('role', 'region'); desk.setAttribute('aria-labelledby', 'treaty-drawer-title');
  const heading = make('div', 'treaty-drawer-heading');
  const title = make('h3', '', 'The Treaty Desk'); title.id = 'treaty-drawer-title';
  const close = button('×', 'treaty-drawer-close'); close.setAttribute('aria-label', 'Close Treaty Desk');
  heading.append(title, close); desk.prepend(heading);
  const review = make('section', 'treaty-review');
  review.setAttribute('aria-label', 'Exact proposal terms'); review.append(proposals);
  get('offer-form').before(review);
  // Keep the original records/ambassador controls, listeners and IDs intact.
  if (get('council-records')) desk.append(get('council-records'));
  const actions = ['quick-offer', 'quick-request', 'quick-promises'].map(get).filter(Boolean);
  for (const action of actions) {
    action.setAttribute('aria-controls', desk.id); action.setAttribute('aria-expanded', 'false');
  }
  const narrow = win.matchMedia('(max-width: 760px)');
  let drawerOpen = false, returnFocus = null, forwarding = false;
  const background = [...dialog.children].filter(node => !node.contains(desk));
  // On phones the drawer covers the conversation, so prevent focus behind it.
  const inertNodes = [...background, conversation];
  function syncDrawer() {
    dialog.classList.toggle('treaty-open', drawerOpen);
    desk.inert = !drawerOpen; desk.setAttribute('aria-hidden', String(!drawerOpen));
    for (const action of actions) action.setAttribute('aria-expanded', String(drawerOpen));
    for (const node of inertNodes) node.inert = drawerOpen && narrow.matches;
    desk.setAttribute('role', drawerOpen && narrow.matches ? 'dialog' : 'region');
    if (drawerOpen && narrow.matches) desk.setAttribute('aria-modal', 'true');
    else desk.removeAttribute('aria-modal');
  }
  function openDrawer(trigger, focusTarget) {
    if (trigger && !desk.contains(trigger)) returnFocus = trigger;
    drawerOpen = true; syncDrawer();
    // Existing quick-action handlers may switch modal/compact mode and update
    // fields synchronously. Focus only after those handlers have finished.
    queueMicrotask(() => {
      if (!drawerOpen || !dialog.open) return;
      const target = focusTarget?.isConnected ? focusTarget : close;
      target.focus({ preventScroll: true });
      target.scrollIntoView?.({ block: 'nearest' });
    });
  }
  function closeDrawer(restore = true) {
    drawerOpen = false; syncDrawer();
    if (restore && dialog.open) {
      const target = returnFocus?.isConnected ? returnFocus : get('quick-offer');
      target?.focus({ preventScroll: true });
    }
  }
  close.addEventListener('click', () => closeDrawer());
  narrow.addEventListener('change', () => {
    syncDrawer();
    if (drawerOpen && narrow.matches && !desk.contains(doc.activeElement)) close.focus();
  });
  dialog.addEventListener('cancel', event => {
    if (drawerOpen) { event.preventDefault(); closeDrawer(); }
  });
  dialog.addEventListener('keydown', event => {
    // Stop the game's global Escape handler from redrawing a dialog that is
    // only closing its drawer. Other dialogs (e.g. Diagnostics) are untouched.
    if (event.key === 'Escape' && drawerOpen) {
      event.preventDefault(); event.stopPropagation(); closeDrawer(); return;
    }
    if (event.key !== 'Tab' || !drawerOpen || !narrow.matches) return;
    const focusable = [...desk.querySelectorAll('button, input, select, textarea, summary, [tabindex="0"]')]
      .filter(el => !el.disabled && !el.closest('[hidden]') && el.getClientRects().length);
    const first = focusable[0] || close, last = focusable.at(-1) || close;
    if (event.shiftKey && (doc.activeElement === first || doc.activeElement === desk)) {
      event.preventDefault(); last.focus();
    } else if (!event.shiftKey && doc.activeElement === last) {
      event.preventDefault(); first.focus();
    }
  });
  dialog.addEventListener('close', () => {
    // setCouncilMode closes and immediately reopens the same native dialog.
    // Those mode transitions must not discard the drawer or an unfinished form.
    if (!dialog.open) closeDrawer(false);
  });
  dialog.addEventListener('click', event => {
    const target = event.target.closest('button');
    if (!target || target.disabled || forwarding) return;
    if (actions.includes(target)) {
      openDrawer(target, target.id === 'quick-promises' ? get('council-records')?.querySelector('summary') : get('offer-type'));
    } else if (proposals.contains(target) && (target.dataset.modify !== undefined || target.dataset.counter !== undefined)) {
      openDrawer(target, target);
    }
  }, true);
  // Incoming trade review buttons live outside this dialog. Let the original
  // handler load its trade ID before revealing the exact terms.
  doc.addEventListener('click', event => {
    const target = event.target.closest('[data-trade-review], [data-trade-counter]');
    if (target && !target.disabled) queueMicrotask(() => {
      if (dialog.open) openDrawer(target, proposals.querySelector('button') || close);
    });
  });

  function portrait(house, label) {
    const frame = make('span', 'leader-portrait');
    frame.setAttribute('role', 'img'); frame.setAttribute('aria-label', label || house?.name || 'House symbol');
    frame.dataset.houseId = house?.id || '';
    const sigil = make('span', 'leader-sigil', house?.sigil || '♜');
    sigil.setAttribute('aria-hidden', 'true'); frame.append(sigil);
    const url = house && portraits[house.id];
    if (url) {
      const image = make('img'); image.alt = ''; image.setAttribute('aria-hidden', 'true');
      image.width = 64; image.height = 64; image.decoding = 'async';
      image.dataset.ironArt = ''; // existing global art fallback convention
      image.addEventListener('error', () => { image.hidden = true; frame.classList.remove('portrait-loaded'); });
      image.addEventListener('load', () => { if (image.naturalWidth) frame.classList.add('portrait-loaded'); });
      image.src = url; frame.append(image);
    }
    return frame;
  }
  function decorateMessages(ruler) {
    const mark = get('ruler-mark');
    if (mark && !mark.querySelector('.leader-portrait')) {
      const symbol = mark.textContent;
      mark.replaceChildren(portrait(ruler || { sigil: symbol }, get('ruler-name')?.textContent));
    }
    let previousSpeaker = null;
    for (const message of messages.children) {
      if (!message.classList.contains('message')) continue;
      const isPlayer = message.classList.contains('player');
      const system = ['council', 'system', 'event'].some(role => message.classList.contains(role));
      const house = isPlayer ? houseFromLabel(message.querySelector('small')?.textContent, houses) : ruler;
      const speaker = system ? null : `${isPlayer ? 'player' : 'ruler'}:${house?.id || 'unknown'}`;
      message.classList.toggle('message-continuation', !!speaker && speaker === previousSpeaker);
      message.classList.toggle('correspondence-player', isPlayer);
      message.classList.toggle('correspondence-system', system);
      if (!message.dataset.correspondenceMessage) {
        message.dataset.correspondenceMessage = 'true';
        const body = make('div', 'correspondence-body');
        while (message.firstChild) body.append(message.firstChild);
        message.append(body);
        if (!system) {
          const name = body.querySelector('small')?.textContent;
          message.prepend(portrait(house, name));
          if (house) { message.dataset.houseId = house.id; message.style.setProperty('--speaker-color', house.color); }
        }
      }
      previousSpeaker = speaker;
    }
  }
  function proposalFingerprint(source) {
    // Terms + status + counteroffer + stable human proposal IDs. A stale card
    // must never resolve by array index to a different live proposal.
    return source.textContent + '\n' + [...source.querySelectorAll('button')].map(b => JSON.stringify(b.dataset)).join('|');
  }
  function reviewSource(source, fingerprint, trigger, counter = false) {
    if (!proposals.contains(source) || proposalFingerprint(source) !== fingerprint) {
      trigger.disabled = true; trigger.textContent = 'Terms changed — review the latest card'; return;
    }
    const index = [...proposals.children].indexOf(source);
    forwarding = true;
    try {
      openDrawer(trigger, close);
      if (counter) {
        const original = source.querySelector('[data-counter]');
        if (!original || original.disabled) return;
        original.click(); // original handler validates and stores the counter
        source = proposals.children[index];
      }
      source?.querySelector('[data-modify]')?.click(); // loads exact terms; never ratifies
    } finally { forwarding = false; }
    if (!source || !proposals.contains(source)) return;
    proposals.querySelectorAll('.reviewing-proposal').forEach(el => el.classList.remove('reviewing-proposal'));
    source.classList.add('reviewing-proposal'); source.tabIndex = -1;
    queueMicrotask(() => {
      if (!drawerOpen || !source.isConnected) return;
      source.focus({ preventScroll: true }); source.scrollIntoView?.({ block: 'nearest' });
    });
  }
  let proposalHTML = '';
  function decorateProposals() {
    // Do not rebuild cards on unrelated header refreshes (preserve focus).
    const signature = [...proposals.children].map(proposalFingerprint).join('\n');
    const sourcesChanged = [...cards.children].some(card => !proposals.contains(card._proposalSource));
    if (signature === proposalHTML && !sourcesChanged) return;
    proposalHTML = signature; cards.replaceChildren();
    for (const source of proposals.children) {
      if (!source.classList.contains('proposal')) continue;
      const originalTitle = source.querySelector('h4')?.textContent || 'Proposed agreement';
      const isPromise = /promise|oath/i.test(originalTitle);
      const label = isPromise ? '◈ Proposed promise' : /marriage/i.test(originalTitle) ? 'Marriage arrangement' : 'Proposed agreement';
      const card = make('article', `correspondence-proposal${isPromise ? ' correspondence-promise' : ''}`);
      card._proposalSource = source;
      card.append(make('h4', '', label), make('small', '', originalTitle));
      // Copy text, not live controls or HTML from the model.
      for (const paragraph of [...source.children].filter(el => el.tagName === 'P').slice(0, 3)) {
        card.append(make('p', '', paragraph.textContent));
      }
      const fingerprint = proposalFingerprint(source), row = make('div', 'button-row');
      const reviewButton = button('Review Terms');
      reviewButton.addEventListener('click', () => reviewSource(source, fingerprint, reviewButton));
      row.append(reviewButton);
      const counter = source.querySelector('[data-counter]');
      if (counter) {
        card.append(make('p', 'correspondence-counter', source.querySelector('.counter-terms p')?.textContent || 'A counteroffer is available.'));
        const counterButton = button('Review counteroffer');
        counterButton.addEventListener('click', () => reviewSource(source, fingerprint, counterButton, true));
        row.append(counterButton);
      }
      card.append(row); cards.append(card);
    }
  }
  let observer;
  function refresh() {
    observer?.disconnect();
    const ruler = houseFromLabel(get('ruler-house')?.textContent, houses);
    const houseKey = ruler?.id || get('ruler-house')?.textContent || '';
    const changedHouse = houseKey !== previousHouse;
    if (changedHouse) {
      previousHouse = houseKey; pinned = true; savedTop = 0;
      // Changing courts should not unexpectedly inherit an open proposal panel.
      closeDrawer(false);
    }
    const logKey = [...messages.children].map(m => (m.querySelector('.correspondence-body') || m).textContent).join('\n') + '\n' + proposals.textContent;
    const changedLog = previousLog !== logKey; previousLog = logKey;
    decorateMessages(ruler); decorateProposals();
    if (pinned) scroll.scrollTop = scroll.scrollHeight;
    else {
      scroll.scrollTop = savedTop;
      if (changedLog) latest.hidden = false;
    }
    observe();
  }
  function observe() {
    observer.observe(messages, { childList: true, subtree: true });
    observer.observe(proposals, { childList: true, subtree: true });
    for (const id of ['ruler-house', 'ruler-mark']) {
      const node = get(id); if (node) observer.observe(node, { childList: true, subtree: true, characterData: true });
    }
  }
  observer = new win.MutationObserver(refresh);
  syncDrawer(); refresh();
  return { openDrawer, closeDrawer, refresh };
}
