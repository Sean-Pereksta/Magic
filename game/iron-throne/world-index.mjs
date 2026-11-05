// Campaign grids have fixed coordinates. Cache their key order, while reading
// each tile again so ownership, construction and replacement tiles stay current.
// Weak keys keep this transient index out of saves and worker messages.
const gridKeys = new WeakMap();

export function worldTileKeys(s) {
  let keys = gridKeys.get(s.tiles);
  if (!keys) {
    keys = Object.keys(s.tiles);
    if (keys.length === s.width * s.height) gridKeys.set(s.tiles, keys);
  }
  return keys;
}

export function worldTiles(s) {
  return worldTileKeys(s).map(id => s.tiles[id]);
}
