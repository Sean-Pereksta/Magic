/* Compact HUD artwork and named settlement bearings. No command behavior changes. */
(() => {
  'use strict';
  const P = ATSRenderer.prototype, originalGuides = P.drawSettlementGuides;
  P.drawSettlementGuides = function (game) {
    this.settlementFinderLabels = [];
    if (!this.findSettlements) return originalGuides.call(this, game);
    const markers = this.settlementIndicators(game), c = this.ctx, small = this.w < 650;
    const nearest = markers.reduce((best, marker) => !best || marker.distance < best.distance ? marker : best, null);
    // All direction arrows remain visible. Label up to eight nearby destinations
    // plus attacked homes so a developed kingdom does not cover the playfield.
    const names = new Set(markers.slice().sort((a, b) => a.distance - b.distance).slice(0, 8).map(m => m.settlement));
    for (const marker of markers) {
      c.save(); c.translate(marker.x, marker.y);
      const points = marker.near ? [[-7, 3], [-7, -4], [0, -10], [7, -4], [7, 3]] : [[12, 0], [-7, -8], [-3, 0], [-7, 8]];
      if (!marker.near) c.rotate(marker.angle);
      c.beginPath(); points.forEach(([x, y], i) => i ? c.lineTo(x, y) : c.moveTo(x, y)); c.closePath();
      c.fillStyle = marker.settlement.attack ? '#efa183' : '#bbd8a8'; c.strokeStyle = '#142b27'; c.lineWidth = 2; c.fill(); c.stroke(); c.restore();
    }
    // Place the nearest home first. Name cards use independent inward lanes,
    // because the arrow spacing alone cannot fit variable-width village names.
    const ordered = markers.filter(m => names.has(m.settlement) || m.settlement.attack).sort((a, b) =>
      Number(b === nearest) - Number(a === nearest) || Number(!!b.settlement.attack) - Number(!!a.settlement.attack) || a.distance - b.distance);
    for (const marker of ordered) {
      const name = marker.settlement.name || 'Ape village', isNearest = marker === nearest, attacked = !!marker.settlement.attack;
      const side = marker.edge === 'left' ? 1 : marker.edge === 'right' ? -1 : 0;
      const x = marker.x + side * 17, y = marker.y + (marker.edge === 'top' ? 20 : marker.edge === 'bottom' ? -22 : marker.near ? 18 : -6);
      const text = (isNearest ? '◆ ' : '') + name;
      c.save(); c.font = (isNearest ? '600 ' : '500 ') + (small ? 10 : 11) + 'px system-ui';
      const nameWidth = c.measureText(text).width, nameFont = c.font;
      c.font = '8px system-ui';
      const measured = Math.max(nameWidth, isNearest ? c.measureText('NEAREST VILLAGE').width : 0, attacked ? c.measureText('UNDER ATTACK').width : 0);
      c.font = nameFont;
      const width = Math.min(this.w - 36, small ? 170 : 220, measured + 14), height = 19 + (isNearest ? 10 : 0) + (attacked ? 10 : 0);
      let left = side === 1 ? x - 6 : side === -1 ? x - width + 6 : x - width / 2;
      left = Math.max(6, Math.min(this.w - width - 6, left));
      let placed = null;
      const overlaps = box => this.settlementFinderLabels.some(other => box.x < other.x + other.width + 5 && box.x + box.width + 5 > other.x && box.y < other.y + other.height + 5 && box.y + box.height + 5 > other.y);
      for (let lane = 0; lane < 14 && !placed; lane++) {
        const inward = marker.edge === 'bottom' ? -1 : 1;
        const shift = side ? (lane ? Math.ceil(lane / 2) * (lane % 2 ? 1 : -1) * 44 : 0) : lane * 44 * inward;
        const top = Math.max(6, Math.min(this.h - height - 6, y - 12 + shift));
        const box = { x: left, y: top, width, height };
        if (!overlaps(box)) placed = box;
      }
      if (!placed) { c.restore(); continue; }
      const top = placed.y;
      if (Math.abs(top - (y - 12)) > 10) {
        c.beginPath(); c.moveTo(marker.x, marker.y); c.lineTo(Math.max(left + 4, Math.min(left + width - 4, marker.x)), top + height / 2);
        c.strokeStyle = attacked ? '#986c52' : '#516d5e'; c.lineWidth = .8; c.stroke();
      }
      c.fillStyle = 'rgba(5,20,24,.9)'; c.strokeStyle = attacked ? '#b17c61' : isNearest ? '#9a9764' : '#334f47'; c.lineWidth = 1;
      c.beginPath(); c.roundRect(left, top, width, height, 4); c.fill(); c.stroke();
      c.textAlign = 'left'; c.fillStyle = attacked ? '#f0b293' : isNearest ? '#ecd8a0' : '#c5d6be'; c.fillText(text, left + 7, top + 13, width - 14);
      c.font = '8px system-ui';
      if (isNearest) { c.fillStyle = '#a6bfae'; c.fillText('NEAREST VILLAGE', left + 7, top + 23); }
      if (attacked) { c.fillStyle = '#efa183'; c.fillText('UNDER ATTACK', left + 7, top + (isNearest ? 33 : 23)); }
      c.restore();
      this.settlementFinderLabels.push({ name, nearest: isNearest, attacked, ...placed });
    }
  };

  function portrait(canvas, species, king = false) {
    const library = window.ATSVisualAssets, data = library?.manifest?.characters;
    const kind = king ? 'king' : 'apes', row = king ? 0 : Math.max(0, data?.species.indexOf(species) ?? 0);
    const frame = data?.frames?.[kind]?.[row + ':0'], image = library?.get('characters-' + kind);
    if (!image || !frame) return;
    const c = canvas.getContext('2d'); canvas.width = 72; canvas.height = 72;
    const gradient = c.createRadialGradient(36, 28, 2, 36, 35, 36);
    gradient.addColorStop(0, '#536453'); gradient.addColorStop(1, '#12282a');
    c.fillStyle = gradient; c.fillRect(0, 0, 72, 72);
    const width = frame.w * .78, height = frame.h * .62;
    c.drawImage(image, frame.x + (frame.w - width) / 2, frame.y, width, height, 0, 0, 72, 72);
  }
  function mount(army) {
    const style = document.createElement('style');
    style.textContent = `
      #armyDock .army-row{background:linear-gradient(145deg,#13282aed,#08191ded);border-color:#57604b80;padding:3px;gap:3px;box-shadow:0 3px 16px #02090b80,inset 0 1px 0 #d9c59112}
      #armyDock button{background:linear-gradient(145deg,#233d36,#102924);border:1px solid #78957a1f;border-radius:4px}
      #armyDock button[aria-pressed=true]{box-shadow:inset 0 0 0 1px #d7bb71;border-color:#e3cc89;background:#4b5940}
      #armyDock .species-button{overflow:hidden;background:#142c2b;border-bottom:2px solid var(--species-color,#647f67)}
      #armyDock .species-button canvas{width:30px;height:30px;margin:0;border-radius:3px;opacity:.92}
      #armyDock .army-count{background:#071719e8;border-radius:3px;padding:2px;min-width:11px;text-align:center;text-shadow:0 1px 2px #000}
      #armyDock kbd{background:#071719c9;padding:1px 2px;border-radius:2px;right:1px;bottom:0}
      #armyDock .army-expand{border-color:#a48e5166;color:#dbc894}
      #armyDock.expanded .army-expand{background:#5b5a37;border-color:#d8bd76}
      .king-portrait{width:30px;height:30px;border:1px solid #a08d58;border-radius:50%;flex:none}
      .hud-stat-icon{width:17px;height:21px;grid-row:1/3;align-self:center;border-radius:3px;opacity:.9}
      .hud-stat{display:grid;grid-template-columns:17px auto;column-gap:5px;min-width:43px}.hud-stat strong{margin-top:2px;font-size:22px}
      .hud-cluster{gap:9px;padding:9px 11px}.king-status{min-width:124px}.hp-track{width:124px}
      @media(pointer:fine) and (min-width:801px){#armyDock:not(.expanded) .army-commands button:not([data-core]){display:none}#armyDock{gap:3px;bottom:10px}#armyDock .army-presets{position:static}#armyDock button{height:32px}#armyDock .species-button{height:36px}}
      @media(pointer:coarse),(max-width:800px){#armyDock button{min-width:44px;min-height:44px}#armyDock .species-button{width:42px;min-width:42px}#armyDock .army-species{gap:2px}.king-portrait{display:none}.hud-stat-icon{display:none}.hud-stat{display:block;min-width:30px}.king-status{min-width:98px}.hp-track{width:98px}.hud-cluster{gap:8px;padding:9px}.hud-stat strong{font-size:20px}}
    `;
    document.head.append(style);
    for (const button of army.commands.querySelectorAll('button')) {
      if (['call', 'nearestTarget', 'recall', 'recallField', 'hold', 'target', 'finder', 'build'].includes(button.dataset.armyCommand)) button.dataset.core = '';
    }
    const expand = army.root.querySelector('.army-expand');
    expand.setAttribute('aria-expanded', 'false');
    expand.title = 'More army commands'; expand.setAttribute('aria-label', 'More army commands');
    const observer = new MutationObserver(() => expand.setAttribute('aria-expanded', String(army.root.classList.contains('expanded'))));
    observer.observe(army.root, { attributes: true, attributeFilter: ['class'] });
    const king = document.createElement('canvas'); king.className = 'king-portrait'; king.setAttribute('aria-hidden', 'true');
    document.querySelector('.king-status').before(king);
    const counters = [...document.querySelectorAll('.hud-stat')].map(stat => {
      const canvas = document.createElement('canvas'); canvas.className = 'hud-stat-icon'; canvas.setAttribute('aria-hidden', 'true'); stat.prepend(canvas); return canvas;
    });
    window.ATSVisualAssets?.ready.then(() => {
      portrait(king, 'gorilla', true);
      const colors = ['#9ca9a8', '#c88a58', '#b4b48e', '#ddd4ac', '#c5aa79', '#89b8c3'];
      Object.entries(army.speciesButtons).forEach(([species, button], i) => {
        portrait(button.querySelector('canvas'), species); button.style.setProperty('--species-color', colors[i]);
      });
      portrait(counters[0], 'gorilla');
      const f = window.ATSVisualAssets.manifest.environment?.frames?.bramble, image = f && window.ATSVisualAssets.get(f.atlas);
      if (image) { const canvas = counters[1]; canvas.width = canvas.height = 48; canvas.getContext('2d').drawImage(image, ...f.rect, 0, 0, 48, 48); }
    });
    return { refreshPortrait: () => portrait(king, 'gorilla', true) };
  }
  window.ATSInterfaceArt = { mount, portrait };
})();
