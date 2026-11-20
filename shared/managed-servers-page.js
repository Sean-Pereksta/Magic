'use strict';
(() => {
  const query = new URLSearchParams(location.search);
  const game = query.get('game');
  const button = document.querySelector('button');
  const status = document.querySelector('[role=status]');
  if (!window.CatnmiceServers || !['bannerlands', 'trashtactics', 'cat', 'lastsignal'].includes(game)) {
    status.textContent = 'Open multiplayer from the game menu to find its servers.';
    return;
  }
  button.hidden = false;
  button.onclick = () => window.CatnmiceServers.open(game);
  window.CatnmiceServers.open(game, { live: query.get('live') === '1' });
})();
