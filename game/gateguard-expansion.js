"use strict";
(() => {
  const parts = [
    "gateguard-expansion/part-01.txt",
    "gateguard-expansion/part-02.txt",
    "gateguard-expansion/part-03.txt",
    "gateguard-expansion/part-04.txt",
    "gateguard-expansion/part-05.txt",
    "gateguard-expansion/part-06.txt",
    "gateguard-expansion/part-07.txt",
  ];
  Promise.all(parts.map(async (path) => {
    const response = await fetch(path, { cache: "no-cache" });
    if (!response.ok) throw new Error(`Expansion part failed: ${path}`);
    return response.text();
  }))
    .then((chunks) => {
      const script = document.createElement("script");
      script.textContent = chunks.join("");
      document.body.appendChild(script);
    })
    .catch((error) => {
      console.error("Gate Guard expansion failed to load", error);
      const notice = document.createElement("div");
      notice.textContent = "ROGUELITE EXPANSION FAILED TO LOAD";
      notice.style.cssText = "position:fixed;z-index:9999;left:8px;right:8px;bottom:8px;padding:10px;border:2px solid #a21f2b;background:#fff;color:#a21f2b;font:700 11px monospace;text-align:center";
      document.body.appendChild(notice);
    });
})();
