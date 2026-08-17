const MODEL_FILES = Object.freeze({
  rat: "./models/rat.glb",
  cat: "./models/cat.glb",
});

const JSON_CHUNK = 0x4e4f534a;
const BIN_CHUNK = 0x004e4942;
const GLB_MAGIC = 0x46546c67;
const decoder = new TextDecoder();

function bytesToDataUrl(bytes, mimeType = "application/octet-stream") {
  const chunkSize = 0x8000;
  let binary = "";
  for (let offset = 0; offset < bytes.length; offset += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(offset, Math.min(bytes.length, offset + chunkSize)));
  }
  return `data:${mimeType};base64,${btoa(binary)}`;
}

function dataUrlToBuffer(uri) {
  const comma = uri.indexOf(",");
  if (comma < 0) throw new Error("Malformed data URI in glTF buffer");
  const header = uri.slice(0, comma);
  const data = uri.slice(comma + 1);
  if (/;base64/i.test(header)) {
    const binary = atob(data);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index++) bytes[index] = binary.charCodeAt(index);
    return bytes.buffer;
  }
  return new TextEncoder().encode(decodeURIComponent(data)).buffer;
}

function parseGlb(arrayBuffer) {
  if (arrayBuffer.byteLength < 20) throw new Error("GLB is too small to contain a valid glTF 2.0 asset");
  const view = new DataView(arrayBuffer);
  if (view.getUint32(0, true) !== GLB_MAGIC) throw new Error("Model is not a binary glTF (.glb) file");
  const version = view.getUint32(4, true);
  if (version !== 2) throw new Error(`Unsupported GLB version ${version}`);
  const declaredLength = view.getUint32(8, true);
  if (declaredLength > arrayBuffer.byteLength) throw new Error("GLB file is truncated");

  let json = null;
  let binChunk = null;
  let offset = 12;
  while (offset + 8 <= declaredLength) {
    const chunkLength = view.getUint32(offset, true);
    const chunkType = view.getUint32(offset + 4, true);
    const start = offset + 8;
    const end = start + chunkLength;
    if (end > declaredLength) throw new Error("GLB chunk extends past the end of the file");
    if (chunkType === JSON_CHUNK) {
      const text = decoder.decode(new Uint8Array(arrayBuffer, start, chunkLength)).replace(/[\u0000\u0020]+$/g, "");
      json = JSON.parse(text);
    } else if (chunkType === BIN_CHUNK && !binChunk) {
      binChunk = arrayBuffer.slice(start, end);
    }
    offset = end;
  }
  if (!json) throw new Error("GLB does not contain a JSON chunk");
  return { json, binChunk };
}

async function fetchArrayBuffer(url) {
  const response = await fetch(url, { cache: "force-cache" });
  if (!response.ok) throw new Error(`Could not load ${url} (${response.status})`);
  return response.arrayBuffer();
}

async function resolveBuffers(json, binChunk, modelUrl) {
  const definitions = json.buffers ?? [];
  const buffers = new Array(definitions.length);
  for (let index = 0; index < definitions.length; index++) {
    const definition = definitions[index] ?? {};
    if (index === 0 && !definition.uri && binChunk) {
      buffers[index] = binChunk;
    } else if (typeof definition.uri === "string" && definition.uri.startsWith("data:")) {
      buffers[index] = dataUrlToBuffer(definition.uri);
    } else if (typeof definition.uri === "string") {
      buffers[index] = await fetchArrayBuffer(new URL(definition.uri, modelUrl).href);
    } else {
      throw new Error(`glTF buffer ${index} has no data source`);
    }
  }
  return buffers;
}

async function resolveImages(json, buffers, modelUrl) {
  const images = json.images ?? [];
  const imageUris = new Array(images.length).fill(null);
  for (let index = 0; index < images.length; index++) {
    const image = images[index] ?? {};
    if (typeof image.uri === "string" && image.uri.startsWith("data:")) {
      imageUris[index] = image.uri;
      continue;
    }
    if (typeof image.uri === "string") {
      const imageUrl = new URL(image.uri, modelUrl).href;
      const response = await fetch(imageUrl, { cache: "force-cache" });
      if (!response.ok) throw new Error(`Could not load glTF image ${imageUrl} (${response.status})`);
      const bytes = new Uint8Array(await response.arrayBuffer());
      imageUris[index] = bytesToDataUrl(bytes, response.headers.get("content-type") || image.mimeType || "application/octet-stream");
      continue;
    }
    if (Number.isInteger(image.bufferView)) {
      const view = json.bufferViews?.[image.bufferView];
      if (!view) throw new Error(`glTF image ${index} references a missing bufferView`);
      const source = buffers[view.buffer];
      if (!source) throw new Error(`glTF image ${index} references a missing buffer`);
      const byteOffset = view.byteOffset ?? 0;
      const byteLength = view.byteLength ?? 0;
      imageUris[index] = bytesToDataUrl(new Uint8Array(source, byteOffset, byteLength), image.mimeType || "image/png");
    }
  }
  return imageUris;
}

async function loadModel(kind, relativePath) {
  const modelUrl = new URL(relativePath, self.location.href);
  const source = await fetchArrayBuffer(modelUrl.href);
  const { json, binChunk } = parseGlb(source);
  const unsupported = [];
  const usedExtensions = new Set(json.extensionsUsed ?? []);
  if (usedExtensions.has("KHR_draco_mesh_compression")) unsupported.push("KHR_draco_mesh_compression");
  if (usedExtensions.has("EXT_meshopt_compression")) unsupported.push("EXT_meshopt_compression");
  if (unsupported.length) {
    throw new Error(`${kind}.glb uses unsupported compressed geometry: ${unsupported.join(", ")}`);
  }
  const buffers = await resolveBuffers(json, binChunk, modelUrl);
  const imageUris = await resolveImages(json, buffers, modelUrl);
  return { kind, json, buffers, imageUris };
}

async function loadAllModels() {
  const entries = Object.entries(MODEL_FILES);
  const loaded = await Promise.all(entries.map(([kind, path]) => loadModel(kind, path)));
  const models = Object.fromEntries(loaded.map((model) => [model.kind, model]));
  const transfers = [];
  for (const model of loaded) {
    for (const buffer of model.buffers) if (buffer instanceof ArrayBuffer) transfers.push(buffer);
  }
  self.postMessage({ type: "hearthmouse-models-ready", models }, transfers);
}

self.addEventListener("message", (event) => {
  if (event.data?.type !== "load-hearthmouse-models") return;
  loadAllModels().catch((error) => {
    self.postMessage({
      type: "hearthmouse-models-error",
      message: error instanceof Error ? error.message : String(error),
    });
  });
});
