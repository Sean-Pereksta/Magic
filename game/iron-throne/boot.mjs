import { launchProgress, launchFailure } from './launch.mjs';
const welcome = document.getElementById('welcome');
welcome.showModal();
launchProgress('Preparing the main menu', 0);
try {
  await import('./app.mjs');
} catch (error) {
  launchFailure(error, () => location.reload(), () => {
    const url = new URL(location.href);
    url.search = '';
    location.assign(url);
  });
}
