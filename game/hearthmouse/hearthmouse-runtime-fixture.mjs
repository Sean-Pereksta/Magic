// Uses the real bundled Three.js and actor factories without WebGL or a browser.
import { readFileSync } from "node:fs";
import vm from "node:vm";

export function bundledRuntime() {
  const html = readFileSync(new URL("./index.html", import.meta.url), "utf8");
  const source = html.split('<script type="module">')[1].split('</script>')[0];
  const gradient = { addColorStop() {} };
  const canvasContext = new Proxy({
    createLinearGradient: () => gradient, createRadialGradient: () => gradient,
    createImageData: (w, h) => ({ data: new Uint8ClampedArray(w * h * 4) }),
    getImageData: (x, y, w, h) => ({ data: new Uint8ClampedArray(w * h * 4) }),
  }, { get: (target, key) => target[key] ?? (() => {}) });
  const context = { console, setTimeout, clearTimeout, performance, TextDecoder, TextEncoder, URL, atob, btoa, AbortController,
    window: {}, document: { createElement: () => ({ relList: { supports: () => true }, getContext: () => canvasContext }) } };
  context.self = context;
  vm.runInNewContext(source.slice(0, source.lastIndexOf('var bu=')) + ';window.testFactories={cat:ml,mouse:pl,world:Ul,Raycaster:Za};', context);
  return { I: context.window.HearthmouseInternals, ...context.window.testFactories };
}
