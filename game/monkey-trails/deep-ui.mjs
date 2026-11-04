import { visits } from './deep-settlements.mjs';
import * as D from './deep-journal.mjs';
import { deepSpecies, hosts } from './deep-roster.mjs';
import { questArt, huntIllustration, emblem, motifs } from './deep-art.mjs';
import { portrait } from './art.mjs';
import { SPECIES, MOVES } from './data.mjs';
import { equipmentMoveCost } from './equipment-battle.mjs';
import { LANDS } from './lands.mjs';
const esc = (s) =>
  String(s ?? '').replace(
    /[&<>"']/g,
    (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c],
  );
const button = (act, text, id = '', disabled = false) =>
  `<button type="button" data-action="deep-${act}" data-id="${esc(id)}" ${disabled ? 'disabled' : ''}>${esc(text)}</button>`;
const landName = (id) => LANDS.find((l) => l.id === id)?.name || id;
export function deepPanel(s) {
  const j = D.journal(s),
    q = D.quests[j.pinned],
    x = D.activeQuest(s),
    story = D.quests.filter((q) => q.land === s.world.activeLand && D.done(s, q.id));
  return `<section class="panel deep-panel"><span class="eyebrow">FIELD JOURNAL</span><h3>Deep Expedition</h3>${q ? questArt(q.id, D.page(s, q.id).stage) : ''}${x ? `<h4>${q.name}</h4><p>${x.step.text}</p>${x.step.goal > 1 ? `<strong>${Math.floor(x.p.count)} / ${x.step.goal}</strong>` : ''}${x.step.kind === 'pace' ? (s.run.pace ? `<p>${Math.floor((Date.now() - s.run.pace.start) / 1000)} / 180 seconds · ${Math.round(s.run.pace.feet)} ft · ${(((s.run.pace.feet / Math.max(1, (Date.now() - s.run.pace.start) / 1000)) * 3600) / 5280).toFixed(1)} mph</p>` : button('pace', 'Start timed run')) : ''}${x.step.choices?.map((v, i) => button('choice', v, i, x.step.kind === 'camp' && s.run.phase !== 'camp')).join('') || ''}${['interrupt', 'block'].includes(x.step.kind) ? button('guardian', 'Challenge guardian', '', s.run.phase !== 'trail') : ''}` : q ? `<p>${q.name} · ${D.done(s, q.id) ? q.ending : 'Visit ' + landName(q.land)}</p>` : ''}${story.map((q) => `<p>${q.ending} ${esc(D.page(s, q.id).choice || '')}</p>`).join('')}${visits(
    s,
  )
    .map(
      (v) =>
        `<aside>${questArt(v.id, D.page(s, v.id).stage)}<p>${esc(v.scene)}</p>${button('visit', v.label, v.id, !v.ready)}</aside>`,
    )
    .join(
      '',
    )}${D.page(s, 7).recipe && s.run.phase === 'camp' ? button('drum', 'Craft shell drum') : ''}${button('journal', 'Open expedition journal')}${D.journal(s).target ? button('hunts', 'Investigate ' + SPECIES[j.target].name) : ''}${s.run.party.some((id) => hosts.includes(s.collection[id]?.species)) ? button('pairs', 'Manage sidekicks') : ''}</section>`;
}
export function mapLandmarks(s) {
  const places = D.quests.filter((q) => q.land === s.world.activeLand && D.done(s, q.id));
  if (!places.length) return '';
  return `<div class="deep-map-landmarks" aria-label="Restored landmarks">${places
    .map((q) => {
      const p = D.page(s, q.id);
      return `<span title="${esc(q.ending)}"><svg viewBox="-30 -30 60 60" aria-hidden="true">${emblem(q.id === 10 && p.choice === 'Nursery' ? 'nest' : q.id === 10 ? 'mail' : motifs[q.id])}</svg>${esc(p.choice || q.name)}</span>`;
    })
    .join('')}</div>`;
}
export function nestAtHome(s) {
  const p = D.page(s, 3);
  if (!D.done(s, 3) || p.choice !== 'Invite family home') return '';
  return `<section class="panel">${questArt(3, 4)}<h3>The youngster’s new home</h3><p>The youngster recognizes ${esc(s.collection[p.lead]?.nickname || SPECIES[s.collection[p.lead]?.species]?.name || 'its escort')} beside the nesting tree.</p></section>`;
}
export function book(s) {
  return `<h2>Deep Expedition journal</h2>${button('hunts', 'Mythic investigations')}${D.quests
    .map((q) => {
      const p = D.page(s, q.id);
      return `<details><summary>${D.done(s, q.id) ? '✓ ' : ''}${q.name} · ${p.stage}/${q.steps.length}</summary>${questArt(q.id, p.stage)}<p>${landName(q.land)} · ${q.steps[p.stage]?.text || q.ending}</p>${button('pin', 'Pin this chapter', q.id, !D.unlockedQuest(s, q) || D.done(s, q.id))}</details>`;
    })
    .join('')}`;
}
export function investigations(s) {
  return `<h2>Mythic investigations</h2>${deepSpecies
    .filter((p) => p.rarity === 'mythic')
    .map((p) => {
      const h = D.hunt(s, p.id),
        cue = D.trials[p.id][h.trial],
        ready = D.huntReady(s, p) && s.run.distance / 0.3048 >= p.minFeet,
        selected = D.journal(s).target === p.id;
      return `<article class="panel"><div class="deep-specimen">${portrait({ species: p.id })}</div><h3>${p.name}</h3>${huntIllustration(p, h.trial)}<p>${p.title} · ${landName(p.region)} · ${p.minFeet.toLocaleString()} actual ft</p><p>Earn the regional seal and two ordinary victories this expedition.${p.id === 'orrakai' ? ' Open the buried court first.' : ''}</p>${button('target', selected ? 'Investigation pinned' : 'Pin investigation', p.id, selected || !D.huntReady(s, p))}${selected ? `<p>${h.sign ? D.trailsigns[p.id] : h.checks + ' / 20 checks · ' + Math.floor(h.feet) + ' / 2,000 ft'}</p>${h.sign ? (cue ? `<p>${cue[0]}</p>${cue[1].map((v, i) => button('trial', v, i, !ready)).join('')}` : button('mythic', D.trialLessons[p.id] && !h.proven ? D.trialLessons[p.id] : 'Challenge ' + p.name, '', !ready || s.run.phase !== 'trail')) : ''}` : ''}</article>`;
    })
    .join('')}`;
}
export function pairs(s) {
  const setup = s.run.phase === 'camp' || (s.run.phase === 'trail' && !s.run.distance);
  return `<h2>Attach Sidekick</h2><p>Choose one conscious Sanctuary monkey outside the lineup for each host. Set pairs before walking or at camp.</p>${s.run.party
    .map((id) => s.collection[id])
    .filter((p) => hosts.includes(p.species))
    .map(
      (p) =>
        `<article><div class="deep-pair-portraits"><div>${portrait(p)}</div>${D.rider(s, p) ? `<div>${portrait(D.rider(s, p))}</div>` : ''}</div><h3>${SPECIES[p.species].name}</h3><select id="sidekick-${esc(p.id)}"><option value="">No rider</option>${Object.values(
          s.collection,
        )
          .filter((r) => !s.run.party.includes(r.id) && r.hp > 0)
          .map(
            (r) =>
              `<option value="${esc(r.id)}" ${D.rider(s, p)?.id === r.id ? 'selected' : ''}>${esc(r.nickname || SPECIES[r.species].name)} · Lv ${r.level}</option>`,
          )
          .join('')}</select>${button('attach', 'Save pair', p.id, !setup)}</article>`,
    )
    .join('')}`;
}
export function pairTurn(s, move, technique = '') {
  const p = s.run.monkey,
    r = D.rider(s, p);
  const targets = (actor) =>
    [...s.run.party.map((id) => s.collection[id]), ...(r ? [r] : [])]
      .filter((q) => q.hp > 0)
      .map(
        (q) =>
          `<option value="${esc(q.id)}" ${actor.id === q.id ? 'selected' : ''}>${esc(q.nickname || SPECIES[q.species].name)}</option>`,
      )
      .join('');
  return `<h2>Confirm paired turn</h2><p>${SPECIES[p.species].name} uses ${MOVES[move].name} first. Offensive abilities target the active foe.</p><label>Host ally target<select id="host-ally">${targets(p)}</select></label><h3>${SPECIES[r.species].name} · ${r.energy} energy</h3><label>Sidekick ability<select id="rider-move">${r.loadout.map((id) => `<option value="${id}" ${(r.cooldowns[id] || 0) > s.run.battle.round || r.energy < equipmentMoveCost(s.run.battle, r, MOVES[id]) ? 'disabled' : ''}>${MOVES[id].name} · ${MOVES[id].cost || 0} energy</option>`).join('')}</select></label><label>Sidekick ally target<select id="rider-ally">${targets(r)}</select></label>${button(technique ? 'technique' : 'turn', 'Confirm both abilities', technique || move)}`;
}
