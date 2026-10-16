import { ART_BASE } from "./config.mjs";
export async function loadAssets(progress = () => {}) {
  let cache;
  try {
    cache = await caches.open("mountonline-art-v2");
  } catch {}
  async function get(path) {
    const url = ART_BASE + path + "?v=2";
    let response = await cache?.match(url);
    if (!response) {
      response = await fetch(url);
      if (!response.ok) throw new Error(`Artwork could not load: ${path}`);
      try {
        await cache?.put(url, response.clone());
      } catch {}
    }
    return response;
  }
  const manifest = await (await get("manifest.json")).json(),
    images = [];
  let loaded = 0;
  const paths = [...manifest.atlases.map((a) => a.file), manifest.hero];
  await Promise.all(
    paths.map(async (path, index) => {
      const blob = await (await get(path)).blob();
      let bitmap;
      if (typeof createImageBitmap === "function")
        bitmap = await createImageBitmap(blob);
      else {
        bitmap = new Image();
        const url = URL.createObjectURL(blob);
        bitmap.src = url;
        try {
          await bitmap.decode();
        } finally {
          URL.revokeObjectURL(url);
        }
      }
      images[index] = bitmap;
      progress(++loaded / paths.length);
    }),
  );
  return { manifest, images };
}
