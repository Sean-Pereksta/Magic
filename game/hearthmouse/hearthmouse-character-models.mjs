const MODEL_WORKER_URL = new URL("./hearthmouse-model-loader.worker.mjs", import.meta.url);
const DECORATE_INTERVAL_MS = 100;
const MODEL_LOAD_TIMEOUT_MS = 15000;

const COMPONENT_INFO = Object.freeze({
  5120: { ArrayType: Int8Array, bytes: 1, read: (view, offset) => view.getInt8(offset) },
  5121: { ArrayType: Uint8Array, bytes: 1, read: (view, offset) => view.getUint8(offset) },
  5122: { ArrayType: Int16Array, bytes: 2, read: (view, offset) => view.getInt16(offset, true) },
  5123: { ArrayType: Uint16Array, bytes: 2, read: (view, offset) => view.getUint16(offset, true) },
  5125: { ArrayType: Uint32Array, bytes: 4, read: (view, offset) => view.getUint32(offset, true) },
  5126: { ArrayType: Float32Array, bytes: 4, read: (view, offset) => view.getFloat32(offset, true) },
});

const TYPE_COMPONENTS = Object.freeze({
  SCALAR: 1,
  VEC2: 2,
  VEC3: 3,
  VEC4: 4,
  MAT2: 4,
  MAT3: 9,
  MAT4: 16,
});

const ATTRIBUTE_NAMES = Object.freeze({
  POSITION: "position",
  NORMAL: "normal",
  TANGENT: "tangent",
  TEXCOORD_0: "uv",
  TEXCOORD_1: "uv1",
  COLOR_0: "color",
});

const HEAD_NAMES = Object.freeze(["head", "face", "muzzle", "snout", "nose"]);
const BODY_NAMES = Object.freeze(["body", "torso", "chest", "spine"]);
const TEXTURE_WRAP = Object.freeze({ 10497: 1000, 33071: 1001, 33648: 1002 });

let loadedModels = null;
let loadError = null;
let installTimer = 0;
let constructors = null;

function loadModelPayloads() {
  return new Promise((resolve, reject) => {
    const worker = new Worker(MODEL_WORKER_URL, { type: "module", name: "hearthmouse-model-loader" });
    const timeout = window.setTimeout(() => {
      worker.terminate();
      reject(new Error("Timed out while loading Hearthmouse character models"));
    }, MODEL_LOAD_TIMEOUT_MS);

    const finish = () => {
      window.clearTimeout(timeout);
      worker.terminate();
    };

    worker.addEventListener("message", (event) => {
      if (event.data?.type === "hearthmouse-models-ready") {
        finish();
        resolve(event.data.models);
      } else if (event.data?.type === "hearthmouse-models-error") {
        finish();
        reject(new Error(event.data.message || "Could not load Hearthmouse character models"));
      }
    });
    worker.addEventListener("error", (event) => {
      finish();
      reject(event.error ?? new Error(event.message || "Hearthmouse model worker failed"));
    }, { once: true });
    worker.postMessage({ type: "load-hearthmouse-models" });
  });
}

function firstWorldMesh(engine) {
  let sample = null;
  engine?.world?.root?.traverse?.((object) => {
    if (!sample && object?.isMesh && object.geometry?.attributes?.position) sample = object;
  });
  return sample;
}

function firstWorldTexture(engine) {
  let sample = null;
  engine?.world?.root?.traverse?.((object) => {
    if (sample || !object?.material) return;
    const materials = Array.isArray(object.material) ? object.material : [object.material];
    for (const material of materials) {
      const texture = material?.map ?? material?.normalMap ?? material?.roughnessMap;
      if (texture) {
        sample = texture;
        break;
      }
    }
  });
  return sample;
}

function deriveConstructors(engine) {
  if (constructors) return constructors;
  const I = window.HearthmouseInternals;
  const mesh = firstWorldMesh(engine);
  if (!I?.Group || !I?.Mesh || !I?.MeshStandardMaterial || !I?.Vector3 || !mesh) return null;

  mesh.geometry.computeBoundingBox?.();
  const geometryPrototype = Object.getPrototypeOf(mesh.geometry.constructor.prototype);
  const attributePrototype = Object.getPrototypeOf(mesh.geometry.attributes.position.constructor.prototype);
  const BufferGeometry = geometryPrototype?.constructor;
  const BufferAttribute = attributePrototype?.constructor;
  const Box3 = mesh.geometry.boundingBox?.constructor;
  if (!BufferGeometry || !BufferAttribute || !Box3) return null;

  const texture = firstWorldTexture(engine);
  const texturePrototype = texture ? Object.getPrototypeOf(texture.constructor.prototype) : null;
  const Texture = texturePrototype?.constructor ?? null;

  constructors = {
    Group: I.Group,
    Mesh: I.Mesh,
    MeshStandardMaterial: I.MeshStandardMaterial,
    Vector3: I.Vector3,
    BufferGeometry,
    BufferAttribute,
    Box3,
    Texture,
    srgbColorSpace: texture?.colorSpace ?? "srgb",
  };
  return constructors;
}

function accessorCache(model) {
  return model.__accessorCache ?? (model.__accessorCache = new Map());
}

function textureCache(model) {
  return model.__textureCache ?? (model.__textureCache = new Map());
}

function readElement(view, byteOffset, componentType) {
  const info = COMPONENT_INFO[componentType];
  if (!info) throw new Error(`Unsupported glTF component type ${componentType}`);
  return info.read(view, byteOffset);
}

function readAccessor(model, accessorIndex) {
  const cache = accessorCache(model);
  if (cache.has(accessorIndex)) return cache.get(accessorIndex);

  const accessor = model.json.accessors?.[accessorIndex];
  if (!accessor) throw new Error(`glTF accessor ${accessorIndex} is missing`);
  const componentInfo = COMPONENT_INFO[accessor.componentType];
  const componentCount = TYPE_COMPONENTS[accessor.type];
  if (!componentInfo || !componentCount) throw new Error(`Unsupported glTF accessor ${accessorIndex}`);

  const result = new componentInfo.ArrayType(accessor.count * componentCount);
  if (Number.isInteger(accessor.bufferView)) {
    const bufferView = model.json.bufferViews?.[accessor.bufferView];
    const buffer = model.buffers?.[bufferView?.buffer];
    if (!bufferView || !buffer) throw new Error(`glTF accessor ${accessorIndex} references missing data`);
    const view = new DataView(buffer);
    const elementBytes = componentInfo.bytes * componentCount;
    const stride = bufferView.byteStride ?? elementBytes;
    const baseOffset = (bufferView.byteOffset ?? 0) + (accessor.byteOffset ?? 0);
    for (let element = 0; element < accessor.count; element++) {
      const elementOffset = baseOffset + element * stride;
      for (let component = 0; component < componentCount; component++) {
        result[element * componentCount + component] = readElement(
          view,
          elementOffset + component * componentInfo.bytes,
          accessor.componentType,
        );
      }
    }
  }

  const sparse = accessor.sparse;
  if (sparse?.count) {
    const indexViewDef = model.json.bufferViews?.[sparse.indices?.bufferView];
    const valueViewDef = model.json.bufferViews?.[sparse.values?.bufferView];
    const indexBuffer = model.buffers?.[indexViewDef?.buffer];
    const valueBuffer = model.buffers?.[valueViewDef?.buffer];
    const sparseIndexInfo = COMPONENT_INFO[sparse.indices?.componentType];
    if (!indexViewDef || !valueViewDef || !indexBuffer || !valueBuffer || !sparseIndexInfo) {
      throw new Error(`glTF accessor ${accessorIndex} has invalid sparse data`);
    }
    const indexView = new DataView(indexBuffer);
    const valueView = new DataView(valueBuffer);
    const indexStart = (indexViewDef.byteOffset ?? 0) + (sparse.indices.byteOffset ?? 0);
    const valueStart = (valueViewDef.byteOffset ?? 0) + (sparse.values.byteOffset ?? 0);
    for (let sparseElement = 0; sparseElement < sparse.count; sparseElement++) {
      const destination = readElement(
        indexView,
        indexStart + sparseElement * sparseIndexInfo.bytes,
        sparse.indices.componentType,
      );
      for (let component = 0; component < componentCount; component++) {
        result[destination * componentCount + component] = readElement(
          valueView,
          valueStart + (sparseElement * componentCount + component) * componentInfo.bytes,
          accessor.componentType,
        );
      }
    }
  }

  const packed = Object.freeze({
    array: result,
    itemSize: componentCount,
    normalized: !!accessor.normalized,
    componentType: accessor.componentType,
  });
  cache.set(accessorIndex, packed);
  return packed;
}

function makeAttribute(model, accessorIndex, C) {
  const packed = readAccessor(model, accessorIndex);
  return new C.BufferAttribute(packed.array, packed.itemSize, packed.normalized);
}

function sequentialIndices(vertexCount) {
  const ArrayType = vertexCount > 65535 ? Uint32Array : Uint16Array;
  const indices = new ArrayType(vertexCount);
  for (let index = 0; index < vertexCount; index++) indices[index] = index;
  return indices;
}

function trianglesForMode(indices, mode) {
  if (mode === undefined || mode === 4) return indices;
  if (mode !== 5 && mode !== 6) throw new Error(`Unsupported glTF primitive mode ${mode}`);
  const output = [];
  if (mode === 5) {
    for (let index = 0; index + 2 < indices.length; index++) {
      if (index % 2 === 0) output.push(indices[index], indices[index + 1], indices[index + 2]);
      else output.push(indices[index + 1], indices[index], indices[index + 2]);
    }
  } else {
    for (let index = 1; index + 1 < indices.length; index++) output.push(indices[0], indices[index], indices[index + 1]);
  }
  const ArrayType = output.some((value) => value > 65535) ? Uint32Array : Uint16Array;
  return new ArrayType(output);
}

function makeTexture(model, textureIndex, C, colorTexture = false) {
  if (!Number.isInteger(textureIndex) || !C.Texture) return null;
  const key = `${textureIndex}:${colorTexture ? "srgb" : "linear"}`;
  const cache = textureCache(model);
  if (cache.has(key)) return cache.get(key);

  const textureDefinition = model.json.textures?.[textureIndex];
  const sourceIndex = textureDefinition?.extensions?.EXT_texture_webp?.source ?? textureDefinition?.source;
  const uri = model.imageUris?.[sourceIndex];
  if (!textureDefinition || !uri) return null;

  const image = new Image();
  const texture = new C.Texture(image);
  texture.flipY = false;
  if (colorTexture) texture.colorSpace = C.srgbColorSpace;
  const sampler = model.json.samplers?.[textureDefinition.sampler];
  if (sampler) {
    if (sampler.wrapS in TEXTURE_WRAP) texture.wrapS = TEXTURE_WRAP[sampler.wrapS];
    if (sampler.wrapT in TEXTURE_WRAP) texture.wrapT = TEXTURE_WRAP[sampler.wrapT];
  }
  image.addEventListener("load", () => { texture.needsUpdate = true; }, { once: true });
  image.src = uri;
  cache.set(key, texture);
  return texture;
}

function applyMaterialTextureTransform(texture, textureInfo) {
  const transform = textureInfo?.extensions?.KHR_texture_transform;
  if (!texture || !transform) return;
  if (Array.isArray(transform.offset)) texture.offset?.set?.(transform.offset[0] ?? 0, transform.offset[1] ?? 0);
  if (Array.isArray(transform.scale)) texture.repeat?.set?.(transform.scale[0] ?? 1, transform.scale[1] ?? 1);
  if (Number.isFinite(transform.rotation)) texture.rotation = -transform.rotation;
}

function makeMaterial(model, materialIndex, C, hasVertexColors = false) {
  const definition = model.json.materials?.[materialIndex] ?? {};
  const pbr = definition.pbrMetallicRoughness ?? {};
  const base = pbr.baseColorFactor ?? [1, 1, 1, 1];
  const material = new C.MeshStandardMaterial({
    roughness: Number.isFinite(pbr.roughnessFactor) ? pbr.roughnessFactor : 1,
    metalness: Number.isFinite(pbr.metallicFactor) ? pbr.metallicFactor : 1,
    transparent: definition.alphaMode === "BLEND" || (base[3] ?? 1) < 0.999,
    opacity: base[3] ?? 1,
    side: definition.doubleSided ? 2 : 0,
    vertexColors: hasVertexColors,
  });
  material.name = definition.name || `gltf-material-${materialIndex ?? "default"}`;
  material.color?.setRGB?.(base[0] ?? 1, base[1] ?? 1, base[2] ?? 1);
  if (definition.alphaMode === "MASK") material.alphaTest = definition.alphaCutoff ?? 0.5;

  const mapInfo = pbr.baseColorTexture;
  material.map = makeTexture(model, mapInfo?.index, C, true);
  applyMaterialTextureTransform(material.map, mapInfo);

  const metalRoughInfo = pbr.metallicRoughnessTexture;
  const metalRough = makeTexture(model, metalRoughInfo?.index, C, false);
  if (metalRough) {
    material.metalnessMap = metalRough;
    material.roughnessMap = metalRough;
    applyMaterialTextureTransform(metalRough, metalRoughInfo);
  }

  const normalInfo = definition.normalTexture;
  material.normalMap = makeTexture(model, normalInfo?.index, C, false);
  applyMaterialTextureTransform(material.normalMap, normalInfo);
  if (material.normalMap && Number.isFinite(normalInfo?.scale)) material.normalScale?.setScalar?.(normalInfo.scale);

  const emissive = definition.emissiveFactor ?? [0, 0, 0];
  material.emissive?.setRGB?.(emissive[0] ?? 0, emissive[1] ?? 0, emissive[2] ?? 0);
  const emissiveInfo = definition.emissiveTexture;
  material.emissiveMap = makeTexture(model, emissiveInfo?.index, C, true);
  applyMaterialTextureTransform(material.emissiveMap, emissiveInfo);

  const occlusionInfo = definition.occlusionTexture;
  material.aoMap = makeTexture(model, occlusionInfo?.index, C, false);
  if (material.aoMap && Number.isFinite(occlusionInfo?.strength)) material.aoMapIntensity = occlusionInfo.strength;
  material.needsUpdate = true;
  return material;
}

function makePrimitive(model, primitive, C, primitiveIndex) {
  if (primitive.extensions?.KHR_draco_mesh_compression || primitive.extensions?.EXT_meshopt_compression) {
    throw new Error("Compressed glTF geometry is not supported by the local Hearthmouse bridge");
  }
  const geometry = new C.BufferGeometry();
  for (const [semantic, accessorIndex] of Object.entries(primitive.attributes ?? {})) {
    const attributeName = ATTRIBUTE_NAMES[semantic];
    if (!attributeName) continue;
    geometry.setAttribute(attributeName, makeAttribute(model, accessorIndex, C));
  }
  const position = geometry.attributes?.position;
  if (!position) throw new Error("glTF mesh primitive has no POSITION attribute");

  let indices = Number.isInteger(primitive.indices)
    ? readAccessor(model, primitive.indices).array
    : sequentialIndices(position.count);
  indices = trianglesForMode(indices, primitive.mode);
  geometry.setIndex(new C.BufferAttribute(indices, 1, false));
  if (!geometry.attributes.normal) geometry.computeVertexNormals?.();
  geometry.computeBoundingBox?.();
  geometry.computeBoundingSphere?.();

  const material = makeMaterial(model, primitive.material, C, !!geometry.attributes.color);
  const mesh = new C.Mesh(geometry, material);
  mesh.name = `gltf-primitive-${primitiveIndex}`;
  mesh.castShadow = true;
  mesh.receiveShadow = true;
  mesh.frustumCulled = true;
  return mesh;
}

function applyNodeTransform(group, node) {
  if (Array.isArray(node.matrix) && node.matrix.length === 16) {
    group.matrix.fromArray(node.matrix);
    group.matrix.decompose(group.position, group.quaternion, group.scale);
    return;
  }
  if (Array.isArray(node.translation)) group.position.fromArray(node.translation);
  if (Array.isArray(node.rotation)) group.quaternion.fromArray(node.rotation);
  if (Array.isArray(node.scale)) group.scale.fromArray(node.scale);
}

function buildModel(model, C) {
  const json = model.json;
  const nodeDefinitions = json.nodes ?? [];
  const nodes = nodeDefinitions.map((definition, index) => {
    const group = new C.Group();
    group.name = definition?.name || `gltf-node-${index}`;
    group.userData.__hearthmouseGltfNodeIndex = index;
    applyNodeTransform(group, definition ?? {});
    return group;
  });

  for (let nodeIndex = 0; nodeIndex < nodeDefinitions.length; nodeIndex++) {
    const definition = nodeDefinitions[nodeIndex] ?? {};
    const node = nodes[nodeIndex];
    if (Number.isInteger(definition.mesh)) {
      const meshDefinition = json.meshes?.[definition.mesh];
      if (!meshDefinition) throw new Error(`glTF node ${nodeIndex} references a missing mesh`);
      for (let primitiveIndex = 0; primitiveIndex < (meshDefinition.primitives?.length ?? 0); primitiveIndex++) {
        const primitive = makePrimitive(model, meshDefinition.primitives[primitiveIndex], C, primitiveIndex);
        primitive.name = meshDefinition.name
          ? `${meshDefinition.name}-${primitiveIndex}`
          : `gltf-mesh-${definition.mesh}-${primitiveIndex}`;
        node.add(primitive);
      }
    }
  }

  for (let nodeIndex = 0; nodeIndex < nodeDefinitions.length; nodeIndex++) {
    const definition = nodeDefinitions[nodeIndex] ?? {};
    for (const childIndex of definition.children ?? []) {
      if (nodes[childIndex]) nodes[nodeIndex].add(nodes[childIndex]);
    }
  }

  const scene = new C.Group();
  scene.name = `hearthmouse-${model.kind}-gltf`;
  const sceneDefinition = json.scenes?.[json.scene ?? 0] ?? json.scenes?.[0];
  const rootNodeIndices = sceneDefinition?.nodes ?? nodeDefinitions
    .map((_, index) => index)
    .filter((index) => !nodeDefinitions.some((node) => node?.children?.includes(index)));
  for (const nodeIndex of rootNodeIndices) if (nodes[nodeIndex]) scene.add(nodes[nodeIndex]);
  scene.updateMatrixWorld(true);
  return scene;
}

function findNamedObject(root, needles) {
  let match = null;
  let score = -1;
  root.traverse((object) => {
    const name = String(object?.name ?? "").toLowerCase();
    if (!name) return;
    for (let index = 0; index < needles.length; index++) {
      if (!name.includes(needles[index])) continue;
      const candidateScore = needles.length - index + (object.isMesh ? 2 : 0);
      if (candidateScore > score) {
        match = object;
        score = candidateScore;
      }
    }
  });
  return match;
}

function centerOfObject(object, C, out) {
  if (!object) return null;
  object.updateWorldMatrix?.(true, true);
  const box = new C.Box3().setFromObject(object);
  if (!box.isEmpty()) return box.getCenter(out);
  object.getWorldPosition?.(out);
  return out;
}

function inferForwardCorrection(content, C) {
  const fullBox = new C.Box3().setFromObject(content);
  if (fullBox.isEmpty()) return 0;
  const center = fullBox.getCenter(new C.Vector3());
  const head = findNamedObject(content, HEAD_NAMES);
  const headCenter = centerOfObject(head, C, new C.Vector3());
  if (!headCenter) return 0;
  const dx = headCenter.x - center.x;
  const dz = headCenter.z - center.z;
  if (Math.hypot(dx, dz) < 1e-5) return 0;
  const yawThatPointsMinusZTowardHead = Math.atan2(-dx, -dz);
  return -yawThatPointsMinusZTowardHead;
}

function fitModelToRig(modelContent, rigRoot, kind, C) {
  rigRoot.updateWorldMatrix?.(true, true);
  const targetBox = new C.Box3().setFromObject(rigRoot);
  const targetSize = targetBox.getSize(new C.Vector3());
  const rootWorld = rigRoot.getWorldPosition(new C.Vector3());
  const rootScale = rigRoot.getWorldScale(new C.Vector3());
  const localTargetHeight = targetSize.y / Math.max(1e-6, Math.abs(rootScale.y));
  const localTargetBottom = (targetBox.min.y - rootWorld.y) / Math.max(1e-6, Math.abs(rootScale.y));

  const wrapper = new C.Group();
  wrapper.name = `hearthmouse-${kind}-model`;
  wrapper.userData.__hearthmouseGlbVisual = true;
  const orientation = new C.Group();
  orientation.name = `${kind}-model-orientation`;
  orientation.add(modelContent);
  wrapper.add(orientation);

  orientation.updateMatrixWorld(true);
  orientation.rotation.y = inferForwardCorrection(orientation, C);
  orientation.updateMatrixWorld(true);

  let modelBox = new C.Box3().setFromObject(orientation);
  const modelSize = modelBox.getSize(new C.Vector3());
  if (!(modelSize.y > 1e-6) || !(localTargetHeight > 1e-6)) throw new Error(`${kind}.glb has invalid dimensions`);
  const scale = localTargetHeight / modelSize.y;
  wrapper.scale.setScalar(scale);

  orientation.updateMatrixWorld(true);
  modelBox = new C.Box3().setFromObject(orientation);
  const body = findNamedObject(orientation, BODY_NAMES);
  const bodyCenter = centerOfObject(body, C, new C.Vector3()) ?? modelBox.getCenter(new C.Vector3());
  wrapper.position.x = -bodyCenter.x * scale;
  wrapper.position.z = -bodyCenter.z * scale;
  wrapper.position.y = localTargetBottom - modelBox.min.y * scale;
  wrapper.userData.__hearthmouseModelScale = scale;
  return wrapper;
}

function hasAncestorNamedFood(object, stopAt) {
  for (let current = object; current && current !== stopAt; current = current.parent) {
    if (String(current.name ?? "").toLowerCase().startsWith("food-")) return true;
  }
  return false;
}

function hidePrimitiveCharacterGeometry(rigRoot, glbWrapper) {
  rigRoot.traverse((object) => {
    if (object === glbWrapper || glbWrapper.children?.includes(object)) return;
    for (let parent = object; parent && parent !== rigRoot; parent = parent.parent) {
      if (parent === glbWrapper || parent.userData?.__hearthmouseGlbVisual) return;
    }
    if (hasAncestorNamedFood(object, rigRoot)) return;
    if (object?.isMesh || object?.isLine || object?.isPoints || object?.isSprite) object.visible = false;
  });
}

function decorateRig(rig, model, kind, engine) {
  const root = rig?.root;
  if (!root || root.userData?.__hearthmouseGlbModelAttached || root.userData?.__hearthmouseGlbModelError) return false;
  if (model.__buildError) {
    root.userData.__hearthmouseGlbModelError = model.__buildError;
    return false;
  }
  const C = deriveConstructors(engine);
  if (!C) return false;

  try {
    const content = buildModel(model, C);
    const wrapper = fitModelToRig(content, root, kind, C);
    root.add(wrapper);
    hidePrimitiveCharacterGeometry(root, wrapper);
    root.userData.__hearthmouseGlbModelAttached = kind;
    root.userData.__hearthmouseGlbModel = wrapper;
    return true;
  } catch (error) {
    const message = String(error?.message ?? error);
    model.__buildError = message;
    root.userData.__hearthmouseGlbModelError = message;
    console.warn(`Hearthmouse could not apply ${kind}.glb; keeping the geometric fallback.`, error);
    return false;
  }
}

export function applyCharacterModels(engine = window.hearthmouseEngine, models = loadedModels) {
  if (!engine || engine.disposed || !models) return 0;
  let applied = 0;
  for (const mouse of engine.mice ?? []) {
    if (models.rat && decorateRig(mouse?.rig, models.rat, "rat", engine)) applied++;
  }
  for (const cat of engine.cats ?? []) {
    if (models.cat && decorateRig(cat?.rig, models.cat, "cat", engine)) applied++;
  }
  return applied;
}

function installLoop() {
  if (typeof window === "undefined" || installTimer) return;
  const tick = () => {
    const engine = window.hearthmouseEngine;
    if (loadedModels && engine && !engine.disposed) applyCharacterModels(engine, loadedModels);
    installTimer = window.setTimeout(tick, DECORATE_INTERVAL_MS);
  };
  tick();
}

async function start() {
  if (typeof window === "undefined" || typeof Worker === "undefined") return;
  try {
    loadedModels = await loadModelPayloads();
    applyCharacterModels(window.hearthmouseEngine, loadedModels);
  } catch (error) {
    loadError = error;
    console.warn("Hearthmouse GLB models could not be loaded. The existing geometric actors remain active.", error);
  } finally {
    installLoop();
  }
}

export function hearthmouseModelStatus() {
  return {
    ready: !!loadedModels,
    error: loadError ? String(loadError?.message ?? loadError) : null,
    rat: !!loadedModels?.rat,
    cat: !!loadedModels?.cat,
  };
}

if (typeof window !== "undefined") {
  start();
  window.addEventListener("beforeunload", () => {
    if (installTimer) window.clearTimeout(installTimer);
    installTimer = 0;
  }, { once: true });
}
