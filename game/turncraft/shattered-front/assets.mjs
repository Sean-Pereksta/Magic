// Expansion art is independent of the established unit/terrain asset paths.
const DEFAULT_BASE =
  'https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/';
let manifest = { assets: {} };
export function assetBase() {
  const value =
    globalThis.TURNCRAFT_EXPANSION_ASSET_BASE ||
    localStorage.getItem('turncraft.expansionAssetBase') ||
    DEFAULT_BASE;
  try {
    const url = new URL(value, location.href);
    return /^(https?:)$/.test(url.protocol)
      ? url.href.replace(/\/?$/, '/')
      : DEFAULT_BASE;
  } catch {
    return DEFAULT_BASE;
  }
}
export function assetUrl(id, fallbackPath) {
  const entries = manifest.assets || manifest.images || {};
  const entry = Array.isArray(entries)
    ? entries.find((item) => item.id === id || item.logicalId === id)
    : entries[id];
  const path = typeof entry === 'string' ? entry : entry?.path || fallbackPath;
  return path ? new URL(path, assetBase()).href : '';
}
export async function loadManifest() {
  try {
    const response = await fetch(
      new URL('../shattered-front-assets.json', import.meta.url)
    );
    if (response.ok) manifest = await response.json();
  } catch {
    /* Existing faction illustrations keep the interface available offline. */
  }
  return manifest;
}
export function artwork(
  id,
  path,
  { alt = '', fallback = '', className = '' } = {}
) {
  const image = document.createElement('img');
  image.alt = alt;
  image.className = className;
  image.loading = 'lazy';
  image.decoding = 'async';
  image.src = assetUrl(id, path);
  image.addEventListener('error', () => {
    if (fallback && image.dataset.fallback !== 'true') {
      image.dataset.fallback = 'true';
      image.src = fallback;
    } else image.hidden = true;
  });
  return image;
}
