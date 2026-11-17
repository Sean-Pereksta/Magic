import {MAP_BRIEFS,mapScreenshotUrl} from './skirmish-menu.js';

const select = document.getElementById('mapSelect');
const layout = document.getElementById('mapBriefPreview');
const legend = layout.closest('figure').querySelector('.map-preview-legend');
const media = document.createElement('div');
media.className = 'map-preview-media';

const scenery = document.createElement('img');
scenery.id = 'mapScenePreview';
scenery.className = 'map-scene-preview';
scenery.hidden = true;

const toggle = document.createElement('button');
toggle.type = 'button';
toggle.className = 'map-preview-switch';
toggle.setAttribute('aria-controls', layout.id + ' ' + scenery.id);
toggle.hidden = true;

layout.before(media);
media.append(scenery, layout, toggle);

function showTerrain(show) {
  scenery.hidden = !show;
  layout.hidden = show;
  legend.hidden = show;
  toggle.textContent = show ? 'View layout' : 'View terrain';
}

let selection = 0;
async function showSelection() {
  const current = ++selection;
  const map = select.value;
  const screenshot = mapScreenshotUrl(map);
  toggle.hidden = true;
  showTerrain(false);
  if (!screenshot) return;
  scenery.alt = `${MAP_BRIEFS[map].name}. ${MAP_BRIEFS[map].scene}`;
  scenery.src = screenshot;
  try {
    await scenery.decode();
  } catch {
    // The tactical layout remains available if the preview cannot load.
    return;
  }
  if (current !== selection) return;
  toggle.hidden = false;
  showTerrain(true);
}

toggle.addEventListener('click', () => showTerrain(scenery.hidden));
select.addEventListener('change', showSelection);
void showSelection();
