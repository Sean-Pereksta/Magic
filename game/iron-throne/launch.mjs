// This module stays small so the menu artwork appears while the game loads.
const get = id => document.getElementById(id);
export function launchProgress(message, completed, total = 4) {
  get('launch-loading').hidden = false;
  get('launch-menu').hidden = true;
  get('new-game-form').hidden = true;
  get('launch-error').hidden = true;
  get('art-status').textContent = message;
  get('art-progress').max = total;
  get('art-progress').value = completed;
  get('launch-loading').style.setProperty('--progress', `${completed / total * 100}%`);
}
export function launchMenu() {
  get('launch-loading').hidden = true;
  get('launch-error').hidden = true;
  get('new-game-form').hidden = true;
  get('launch-menu').hidden = false;
}
export function launchFailure(error, retry, returnToMenu = launchMenu) {
  get('launch-loading').hidden = true;
  get('launch-menu').hidden = true;
  get('new-game-form').hidden = true;
  get('launch-error').hidden = false;
  get('launch-error-message').textContent = error?.message || 'The campaign could not be loaded.';
  get('launch-retry').onclick = retry;
  get('launch-return').onclick = returnToMenu;
}
export const paintLaunch = () => new Promise(resolve => requestAnimationFrame(() => setTimeout(resolve, 0)));
