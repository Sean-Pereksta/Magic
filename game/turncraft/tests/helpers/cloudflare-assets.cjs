const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const index = require('../../assets/visual-asset-index.json');
const base = new URL(index.baseUrl);
const cache = process.env.TURNCRAFT_ASSET_CACHE || path.join(os.tmpdir(), 'turncraft-artwork', base.host);

function assetPath(url) {
  const parsed = new URL(url);
  const key = parsed.pathname.slice(1);
  if (parsed.origin !== base.origin || parsed.search || parsed.hash || !Object.hasOwn(index.images, key)) return null;
  const file = index.images[key].localPath ? path.resolve(__dirname, '../../../..', index.images[key].localPath) : path.join(cache, ...key.split('/'));
  if (!fs.existsSync(file)) throw new Error('Missing artwork cache: run python3 scripts/cache-visual-assets.py before the checks');
  return file;
}

function enablePixelInspection() {
  // Only the isolated image fixtures need CORS so the artwork QA can inspect pixels.
  const NativeImage = window.Image;
  window.Image = function (...args) {
    const image = new NativeImage(...args);
    image.crossOrigin = 'anonymous';
    return image;
  };
  window.Image.prototype = NativeImage.prototype;
  Object.setPrototypeOf(window.Image, NativeImage);
}

module.exports = {assetPath, enablePixelInspection};
