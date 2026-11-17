import {trashTacticsRank} from './rank-tiers.mjs';

function element(container, tag, className, content) {
  const node = container.ownerDocument.createElement(tag);
  node.className = className;
  if (content !== undefined) node.textContent = content;
  container.append(node);
  return node;
}

function appendProfile(container, profile, className) {
  const rank = trashTacticsRank(profile);
  const card = element(container, 'div', className);
  card.style.setProperty('--tt-rank-color', rank.color);

  const name = profile.displayName || profile.username;
  const initials = name.trim().split(/\s+/).slice(0, 2).map(word => [...word][0]).join('').toUpperCase();
  const emblem = element(card, 'span', 'tt-rank-emblem', initials);
  emblem.setAttribute('aria-hidden', 'true');
  const info = element(card, 'div', 'tt-rank-info');
  element(info, 'strong', 'tt-rank-name', name);
  element(info, 'span', 'tt-rank-tier', `${rank.title} · ${profile.rating} Elo`);
  if (profile.games > 0) {
    element(info, 'span', 'tt-rank-record', `${profile.wins} wins · ${profile.losses} losses · ${profile.draws} draws`);
  }
}

export function renderRankProfile(container, profile) {
  container.replaceChildren();
  if (profile) appendProfile(container, profile, 'tt-rank-profile');
}

export function renderLeaderboard(container, profiles) {
  container.replaceChildren();
  if (!profiles.length) {
    element(container, 'p', 'tt-rank-empty', 'No ranked matches recorded yet.');
    return;
  }
  const list = element(container, 'ol', 'tt-rank-standings');
  for (const profile of profiles) {
    const row = element(list, 'li', 'tt-rank-standing');
    appendProfile(row, profile, 'tt-rank-profile tt-rank-profile-compact');
  }
}
