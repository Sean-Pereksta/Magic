// Build from the three image-generated RGBA source atlases, in troop/elite/world order.
// No image synthesis: this step only sizes, encodes, and records fixed-cell crop metadata.
const fs = require("node:fs"),
  path = require("node:path"),
  sharp = require("sharp");
async function main() {
  const sources = process.argv.slice(2);
  if (sources.length !== 3)
    throw new Error(
      "Usage: node scripts/build-mountonline-assets.cjs infantry.png elite.png world.png",
    );
  const root = path.resolve(__dirname, "../graphics/mountonline"),
    files = [
      ["units/levies.webp", 1280, 1120, 8, 7],
      ["units/elite.webp", 1280, 1120, 8, 7],
      ["world/atlas.webp", 1024, 1024, 4, 4],
    ],
    manifest = {
      version: 2,
      frameInset: 2,
      animation: {
        idle: [0],
        move: [1, 2],
        attack: [3, 4],
        defend: [5],
        dying: [6, 7],
      },
      atlases: [],
    };
  for (let i = 0; i < 3; i++) {
    const [file, width, height, columns, rows] = files[i];
    await sharp(sources[i])
      .resize(width, height, { fit: "fill" })
      .webp({ quality: 84, alphaQuality: 100, effort: 6 })
      .toFile(path.join(root, file));
    manifest.atlases.push({
      file,
      width,
      height,
      columns,
      rows,
      frameWidth: width / columns,
      frameHeight: height / rows,
      bytes: fs.statSync(path.join(root, file)).size,
    });
  }
  const hero = path.resolve(
    __dirname,
    "../lobby/assets/game-tiles/mountonline.webp",
  );
  fs.copyFileSync(hero, path.join(root, "ui/hero.webp"));
  manifest.hero = "ui/hero.webp";
  fs.writeFileSync(
    path.join(root, "manifest.json"),
    JSON.stringify(manifest, null, 2) + "\n",
  );
  console.log(JSON.stringify(manifest, null, 2));
}
main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
