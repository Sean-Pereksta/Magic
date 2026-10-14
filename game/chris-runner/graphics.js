/* Chris Runner cartoon-v1: self-contained GLB assets, loaded once before play.
 * This small reader deliberately supports only our embedded, unskinned PBR pack.
 * Geometry/materials belong to this cache: cleanup must skip runnerAssetShared.
 * Use create(id, { ownMaterials: true }) before recoloring an individual actor.
 */
(function (global) {
  'use strict';

  const BASE_URL = 'https://pub-b0a47452b499401bb0642836b2fe3501.r2.dev/';
  // The user uploads only the six model folders to R2. Keep the immutable
  // manifest with the game, resolved from this script rather than the page URL.
  const scriptURL = global.document && global.document.currentScript && global.document.currentScript.src;
  const MANIFEST_URL = scriptURL ? new URL('asset-manifest.json', scriptURL).href : new URL('chris-runner/asset-manifest.json', global.location.href).href;
  const EXPECTED_ASSETS = 262;
  const CONCURRENCY = 5;
  const TIMEOUT_MS = 40000;
  const ATTEMPTS = 3;
  const MAX_MANIFEST_BYTES = 512 * 1024;
  const MAX_ASSET_BYTES = 8 * 1024 * 1024;
  const COMPONENTS = { 5120: Int8Array, 5121: Uint8Array, 5122: Int16Array, 5123: Uint16Array, 5125: Uint32Array, 5126: Float32Array };
  const WIDTHS = { SCALAR: 1, VEC2: 2, VEC3: 3, VEC4: 4, MAT4: 16 };
  const templates = new Map();
  const entries = new Map();
  const inFlightBytes = new Map();
  const listeners = new Set();
  let manifest = null;
  let running = null;
  let completedBytes = 0;
  let status = { phase: 'idle', loaded: 0, total: EXPECTED_ASSETS, loadedBytes: 0, totalBytes: 0, error: null };

  function assert(condition, message) {
    if (!condition) throw new Error(message);
  }

  function integer(value, minimum = 0) {
    return Number.isSafeInteger(value) && value >= minimum;
  }

  function finiteArray(value, length) {
    return Array.isArray(value) && value.length === length && value.every(Number.isFinite);
  }

  function emit(changes = {}) {
    let partial = 0;
    for (const value of inFlightBytes.values()) partial += value;
    status = { ...status, ...changes, loaded: templates.size, loadedBytes: completedBytes + partial };
    const snapshot = { ...status };
    for (const listener of listeners) {
      try { listener(snapshot); } catch (error) { console.warn('Runner graphics progress callback failed', error); }
    }
  }

  function assetURL(path) {
    assert(typeof path === 'string' && /^[a-z0-9-]+\/[a-z0-9-]+\.glb$/.test(path), 'Unsafe graphics path');
    const result = new URL(path, BASE_URL);
    assert(result.origin === new URL(BASE_URL).origin && result.href.startsWith(BASE_URL), 'Graphics URL left the asset pack');
    return result.href;
  }

  function validateManifest(value) {
    assert(value && value.format === 'chris-runner-glb-pack' && value.version === 'cartoon-v1', 'This is not the Chris Runner cartoon asset pack');
    assert(value.assetCount === EXPECTED_ASSETS && Array.isArray(value.assets) && value.assets.length === EXPECTED_ASSETS, 'The graphics manifest must list all 262 models');
    const ids = new Set(), paths = new Set();
    let bytes = 0;
    for (const entry of value.assets) {
      assert(entry && typeof entry.id === 'string' && /^[a-z]+:[a-z0-9-]+$/.test(entry.id), 'Invalid graphics ID');
      assert(['characters', 'troops', 'tiles', 'obstacles', 'props', 'pickups'].includes(entry.category), 'Invalid graphics category');
      assert(entry.id.startsWith(entry.category + ':') && entry.path.startsWith(entry.category + '/'), 'Graphics category does not match its path');
      assetURL(entry.path);
      assert(!ids.has(entry.id) && !paths.has(entry.path), 'Duplicate graphics ID or path');
      assert(integer(entry.bytes, 20) && entry.bytes <= MAX_ASSET_BYTES, 'Invalid graphics byte count');
      assert(typeof entry.sha256 === 'string' && /^[a-f0-9]{64}$/.test(entry.sha256), 'Missing graphics checksum');
      assert(entry.bounds && finiteArray(entry.bounds.min, 3) && finiteArray(entry.bounds.max, 3) && entry.bounds.max.every((v, i) => v >= entry.bounds.min[i]), 'Invalid graphics bounds');
      ids.add(entry.id); paths.add(entry.path); bytes += entry.bytes;
    }
    assert(bytes === value.totalGLBBytes && bytes <= 100 * 1024 * 1024, 'Invalid graphics pack size');
    return value;
  }

  async function fetchBytes(url, limit, progress) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
    try {
      const response = await fetch(url, { mode: 'cors', credentials: 'omit', signal: controller.signal, redirect: 'error', cache: 'default' });
      assert(response.ok, `Graphics download returned HTTP ${response.status}`);
      const declared = Number(response.headers.get('content-length'));
      assert(!Number.isFinite(declared) || declared <= limit, 'Graphics response is too large');
      if (!response.body || !response.body.getReader) {
        const bytes = new Uint8Array(await response.arrayBuffer());
        assert(bytes.byteLength <= limit, 'Graphics response is too large');
        if (progress) progress(bytes.byteLength);
        return bytes;
      }
      const reader = response.body.getReader();
      const chunks = [];
      let length = 0;
      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          length += value.byteLength;
          assert(length <= limit, 'Graphics response is too large');
          chunks.push(value);
          if (progress) progress(length);
        }
      } catch (error) {
        await reader.cancel().catch(() => {});
        throw error;
      }
      const bytes = new Uint8Array(length);
      let offset = 0;
      for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
      return bytes;
    } catch (error) {
      if (error.name === 'AbortError') throw new Error('Graphics download timed out; check your connection and try again');
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  async function withRetries(task) {
    let lastError;
    for (let attempt = 0; attempt < ATTEMPTS; attempt++) {
      try { return await task(); } catch (error) { lastError = error; }
      if (attempt + 1 < ATTEMPTS) await new Promise(resolve => setTimeout(resolve, 500 * (2 ** attempt)));
    }
    throw lastError;
  }

  async function verifyBytes(bytes, entry) {
    assert(bytes.byteLength === entry.bytes, `${entry.path}: incomplete graphics download`);
    // Modern browsers expose SHA-256 on HTTPS; exact size and GLB validation also
    // remain active in local HTTP development, where SubtleCrypto may be absent.
    if (global.crypto && global.crypto.subtle) {
      const digest = new Uint8Array(await global.crypto.subtle.digest('SHA-256', bytes));
      const hex = Array.from(digest, value => value.toString(16).padStart(2, '0')).join('');
      assert(hex === entry.sha256, `${entry.path}: graphics checksum did not match`);
    }
  }

  function readGLB(buffer) {
    const THREE = global.THREE;
    assert(THREE && THREE.BufferGeometry, 'Three.js must load before Runner graphics');
    assert(buffer instanceof ArrayBuffer && buffer.byteLength >= 20, 'Invalid GLB buffer');
    const view = new DataView(buffer);
    assert(view.getUint32(0, true) === 0x46546c67 && view.getUint32(4, true) === 2 && view.getUint32(8, true) === buffer.byteLength, 'Invalid GLB header');
    let json, bin;
    for (let offset = 12; offset < buffer.byteLength;) {
      assert(offset + 8 <= buffer.byteLength, 'Truncated GLB chunk header');
      const size = view.getUint32(offset, true), type = view.getUint32(offset + 4, true);
      offset += 8;
      assert(size % 4 === 0 && offset + size <= buffer.byteLength, 'Truncated GLB chunk');
      if (type === 0x4e4f534a) {
        assert(!json, 'Duplicate GLB JSON chunk');
        json = JSON.parse(new TextDecoder().decode(new Uint8Array(buffer, offset, size)));
      } else if (type === 0x004e4942) {
        assert(!bin, 'Duplicate GLB binary chunk');
        bin = buffer.slice(offset, offset + size);
      } else throw new Error('Unsupported GLB chunk');
      offset += size;
    }
    assert(json && json.asset && json.asset.version === '2.0' && bin, 'Missing GLB scene or geometry');
    assert(Array.isArray(json.buffers) && json.buffers.length === 1 && !json.buffers[0].uri && integer(json.buffers[0].byteLength, 1) && json.buffers[0].byteLength <= bin.byteLength && bin.byteLength - json.buffers[0].byteLength <= 3, 'Only embedded GLB geometry is supported');
    assert(!json.images && !json.textures && !json.skins && !(json.extensionsRequired || []).length, 'Unsupported external or compressed GLB resource');
    assert(Array.isArray(json.accessors) && Array.isArray(json.bufferViews) && Array.isArray(json.meshes) && Array.isArray(json.nodes) && Array.isArray(json.scenes), 'Incomplete GLB scene');
    const allocated = [];
    try {
      const accessorCache = new Map();
      function accessor(index) {
        if (accessorCache.has(index)) return accessorCache.get(index);
        assert(integer(index) && json.accessors[index], 'Invalid GLB accessor');
        const a = json.accessors[index], b = json.bufferViews[a.bufferView], Type = COMPONENTS[a.componentType], width = WIDTHS[a.type];
        assert(!a.sparse && b && (b.buffer || 0) === 0 && Type && width && integer(a.count, 1), 'Unsupported GLB accessor');
        const offset = (b.byteOffset || 0) + (a.byteOffset || 0), rowBytes = width * Type.BYTES_PER_ELEMENT, stride = b.byteStride || rowBytes;
        assert(integer(b.byteOffset || 0) && integer(a.byteOffset || 0) && integer(b.byteLength, 1) && integer(stride, rowBytes) && stride % Type.BYTES_PER_ELEMENT === 0 && offset % Type.BYTES_PER_ELEMENT === 0, 'Invalid GLB accessor layout');
        const end = offset + (a.count - 1) * stride + rowBytes;
        assert(end <= (b.byteOffset || 0) + b.byteLength && end <= json.buffers[0].byteLength, 'GLB accessor exceeds its geometry buffer');
        const values = new Type(a.count * width);
        if (stride === rowBytes) values.set(new Type(bin, offset, a.count * width));
        else for (let i = 0; i < a.count; i++) values.set(new Type(bin, offset + i * stride, width), i * width);
        if (Type === Float32Array) assert(values.every(Number.isFinite), 'Non-finite GLB geometry');
        accessorCache.set(index, values);
        return values;
      }
      const materials = (json.materials || []).map(m => {
        const p = m.pbrMetallicRoughness || {}, c = p.baseColorFactor || [1, 1, 1, 1];
        assert(!p.baseColorTexture && !p.metallicRoughnessTexture && !m.normalTexture && !m.emissiveTexture && finiteArray(c, 4), 'Only embedded GLB PBR colors are supported');
        const material = new THREE.MeshStandardMaterial({ color: new THREE.Color(c[0], c[1], c[2]), opacity: c[3], transparent: m.alphaMode === 'BLEND', alphaTest: m.alphaMode === 'MASK' ? (m.alphaCutoff ?? 0.5) : 0, roughness: p.roughnessFactor ?? 1, metalness: p.metallicFactor ?? 1, side: m.doubleSided ? THREE.DoubleSide : THREE.FrontSide });
        allocated.push(material);
        if (m.emissiveFactor) { assert(finiteArray(m.emissiveFactor, 3), 'Invalid GLB emissive color'); material.emissive.fromArray(m.emissiveFactor); }
        material.name = m.name || '';
        material.userData.runnerAssetShared = true;
        return material;
      });
      const defaultMaterial = () => {
        const material = new THREE.MeshStandardMaterial({ color: 0xffffff });
        material.userData.runnerAssetShared = true;
        allocated.push(material);
        return material;
      };
      const meshes = json.meshes.map(m => {
        assert(Array.isArray(m.primitives) && m.primitives.length, 'Missing GLB mesh primitives');
        return m.primitives.map(p => {
          assert((p.mode ?? 4) === 4 && p.attributes && p.attributes.POSITION !== undefined && !p.targets && !p.extensions, 'Only static GLB triangles are supported');
          const geometry = new THREE.BufferGeometry();
          allocated.push(geometry);
          for (const [semantic, index] of Object.entries(p.attributes)) {
            const spec = { POSITION: ['position', 'VEC3'], NORMAL: ['normal', 'VEC3'], TEXCOORD_0: ['uv', 'VEC2'], COLOR_0: ['color', null] }[semantic];
            assert(spec, 'Unsupported GLB vertex attribute');
            const values = accessor(index), a = json.accessors[index];
            assert(!spec[1] || a.type === spec[1], 'Invalid GLB attribute width');
            geometry.setAttribute(spec[0], new THREE.BufferAttribute(values, WIDTHS[a.type], !!a.normalized));
          }
          for (const a of Object.values(geometry.attributes)) assert(a.count === geometry.attributes.position.count, 'GLB attribute count mismatch');
          if (p.indices !== undefined) {
            const values = accessor(p.indices), a = json.accessors[p.indices];
            assert(a.type === 'SCALAR' && [5121, 5123, 5125].includes(a.componentType) && values.length % 3 === 0 && values.every(index => index < geometry.attributes.position.count), 'Invalid GLB triangle indices');
            geometry.setIndex(new THREE.BufferAttribute(values, 1));
          } else assert(geometry.attributes.position.count % 3 === 0, 'Invalid GLB triangle count');
          if (!geometry.attributes.normal) geometry.computeVertexNormals();
          geometry.computeBoundingBox(); geometry.computeBoundingSphere();
          geometry.userData.runnerAssetShared = true;
          assert(p.material === undefined || materials[p.material], 'Invalid GLB material reference');
          const mesh = new THREE.Mesh(geometry, materials[p.material] || defaultMaterial());
          mesh.name = m.name || '';
          mesh.castShadow = true; mesh.receiveShadow = true;
          return mesh;
        });
      });
      const nodes = json.nodes.map(n => {
        assert(!n.skin && !n.camera, 'Unsupported GLB node');
        const group = new THREE.Group();
        group.name = n.name || '';
        group.userData = n.extras && typeof n.extras === 'object' ? { ...n.extras } : {};
        if (n.matrix) {
          assert(finiteArray(n.matrix, 16), 'Invalid GLB matrix');
          group.matrix.fromArray(n.matrix); group.matrix.decompose(group.position, group.quaternion, group.scale);
        } else {
          if (n.translation) { assert(finiteArray(n.translation, 3), 'Invalid GLB translation'); group.position.fromArray(n.translation); }
          if (n.rotation) { assert(finiteArray(n.rotation, 4), 'Invalid GLB rotation'); group.quaternion.fromArray(n.rotation); }
          if (n.scale) { assert(finiteArray(n.scale, 3), 'Invalid GLB scale'); group.scale.fromArray(n.scale); }
        }
        if (n.mesh !== undefined) { assert(meshes[n.mesh], 'Invalid GLB mesh reference'); meshes[n.mesh].forEach(mesh => group.add(mesh.clone())); }
        return group;
      });
      const parents = new Map();
      json.nodes.forEach((n, i) => {
        assert(!n.children || Array.isArray(n.children), 'Invalid GLB node children');
        for (const child of n.children || []) {
          assert(integer(child) && nodes[child] && child !== i && !parents.has(child), 'Invalid GLB hierarchy');
          parents.set(child, i);
        }
      });
      for (let i = 0; i < nodes.length; i++) {
        const visited = new Set([i]);
        for (let parent = parents.get(i); parent !== undefined; parent = parents.get(parent)) {
          assert(!visited.has(parent), 'Cyclic GLB hierarchy'); visited.add(parent);
        }
      }
      for (const [child, parent] of parents) nodes[parent].add(nodes[child]);
      const sceneSpec = json.scenes[json.scene ?? 0];
      assert(sceneSpec && Array.isArray(sceneSpec.nodes), 'Invalid GLB active scene');
      const scene = new THREE.Group();
      for (const index of sceneSpec.nodes) { assert(nodes[index] && !parents.has(index), 'Invalid GLB scene root'); scene.add(nodes[index]); }
      const clips = (json.animations || []).map(animation => {
        const tracks = animation.channels.map(channel => {
          const sampler = animation.samplers[channel.sampler], target = channel.target;
          assert(sampler && target && nodes[target.node] && ['translation', 'rotation', 'scale'].includes(target.path) && ['LINEAR', 'STEP'].includes(sampler.interpolation || 'LINEAR'), 'Unsupported GLB animation');
          const times = accessor(sampler.input), values = accessor(sampler.output);
          assert(json.accessors[sampler.input].type === 'SCALAR' && values.length === times.length * (target.path === 'rotation' ? 4 : 3) && times.every((time, index) => time >= 0 && (!index || time > times[index - 1])), 'Invalid GLB animation samples');
          const property = { translation: 'position', rotation: 'quaternion', scale: 'scale' }[target.path];
          const Track = target.path === 'rotation' ? THREE.QuaternionKeyframeTrack : THREE.VectorKeyframeTrack;
          return new Track(nodes[target.node].uuid + '.' + property, times, values, sampler.interpolation === 'STEP' ? THREE.InterpolateDiscrete : THREE.InterpolateLinear);
        });
        return new THREE.AnimationClip(animation.name || 'Animation', -1, tracks);
      });
      scene.updateMatrixWorld(true);
      const primitives = [];
      scene.traverse(object => {
        if (object.isMesh) primitives.push(Object.freeze({ geometry: object.geometry, material: object.material, matrix: object.matrixWorld.clone() }));
      });
      return { scene, clips, primitives: Object.freeze(primitives) };
    } catch (error) {
      allocated.forEach(resource => resource.dispose());
      throw error;
    }
  }

  function entryFor(idOrPath) {
    const entry = entries.get(idOrPath);
    assert(entry, `Unknown Runner graphic: ${idOrPath}`);
    return entry;
  }

  function recordFor(idOrPath) {
    const entry = entryFor(idOrPath), record = templates.get(entry.path);
    assert(record, `Runner graphic has not loaded: ${entry.path}`);
    return record;
  }

  async function preload() {
    emit({ phase: 'manifest', error: null });
    if (!manifest) {
      const bytes = await withRetries(() => fetchBytes(MANIFEST_URL, MAX_MANIFEST_BYTES));
      manifest = validateManifest(JSON.parse(new TextDecoder().decode(bytes)));
      for (const entry of manifest.assets) { entries.set(entry.id, entry); entries.set(entry.path, entry); }
    }
    emit({ phase: 'loading', total: manifest.assetCount, totalBytes: manifest.totalGLBBytes });
    const queue = manifest.assets.filter(entry => !templates.has(entry.path));
    const errors = [];
    let cursor = 0;
    async function worker() {
      // A missing individual file must not block the remaining pack, but an
      // unreachable host must not spend three timeouts on all 262 models.
      // Let already-dispatched work settle; Retry will queue every missing file.
      while (cursor < queue.length && errors.length < CONCURRENCY) {
        const entry = queue[cursor++];
        try {
          const record = await withRetries(async () => {
            inFlightBytes.set(entry.path, 0); emit();
            const bytes = await fetchBytes(assetURL(entry.path), entry.bytes, count => { inFlightBytes.set(entry.path, count); emit(); });
            await verifyBytes(bytes, entry);
            return readGLB(bytes.buffer);
          });
          record.scene.userData.runnerAsset = entry;
          templates.set(entry.path, record);
          completedBytes += entry.bytes;
        } catch (error) {
          errors.push({ path: entry.path, message: error.message });
        } finally {
          inFlightBytes.delete(entry.path); emit();
        }
      }
    }
    await Promise.all(Array.from({ length: Math.min(CONCURRENCY, queue.length) }, worker));
    if (errors.length) {
      const stopped = cursor < queue.length ? ` Loading paused after repeated errors; ${manifest.assetCount - templates.size} models are still waiting.` : '';
      const error = new Error(`${errors.length} model${errors.length === 1 ? '' : 's'} could not load.${stopped} Check the asset upload and Cloudflare CORS settings, then retry. (${errors[0].path}: ${errors[0].message})`);
      error.failures = errors;
      throw error;
    }
    assert(templates.size === EXPECTED_ASSETS, 'The graphics pack is incomplete');
    emit({ phase: 'ready', error: null });
  }

  function start(onProgress) {
    if (typeof onProgress === 'function') { listeners.add(onProgress); onProgress({ ...status }); }
    if (status.phase === 'ready') return Promise.resolve();
    if (!running) {
      running = preload().catch(error => { emit({ phase: 'error', error: error.message }); throw error; }).finally(() => { running = null; });
    }
    return running;
  }

  function create(idOrPath, options = {}) {
    const entry = entryFor(idOrPath), record = recordFor(idOrPath), clone = record.scene.clone(true);
    const sourceNodes = [], cloneNodes = [], ids = new Map();
    record.scene.traverse(object => sourceNodes.push(object));
    clone.traverse(object => cloneNodes.push(object));
    sourceNodes.forEach((object, index) => ids.set(object.uuid, cloneNodes[index].uuid));
    clone.userData.runnerAsset = entry;
    clone.userData.clips = record.clips.map(clip => {
      const result = clip.clone();
      for (const track of result.tracks) {
        const split = track.name.lastIndexOf('.'), id = track.name.slice(0, split);
        track.name = ids.get(id) + track.name.slice(split);
      }
      return result;
    });
    if (options.ownMaterials) {
      const materials = new Map();
      const own = source => {
        if (!materials.has(source)) { const material = source.clone(); delete material.userData.runnerAssetShared; materials.set(source, material); }
        return materials.get(source);
      };
      clone.traverse(object => { if (object.isMesh) object.material = Array.isArray(object.material) ? object.material.map(own) : own(object.material); });
    }
    return clone;
  }

  global.RunnerGraphics = Object.freeze({
    baseURL: BASE_URL,
    manifestURL: MANIFEST_URL,
    get ready() { return status.phase === 'ready'; },
    get status() { return { ...status }; },
    get manifest() { return manifest; },
    start,
    retry: start,
    create,
    asset: entryFor,
    template: idOrPath => recordFor(idOrPath).scene,
    primitives: idOrPath => recordFor(idOrPath).primitives,
    // Removing a progress listener is useful when replacing the startup screen.
    unsubscribe: callback => listeners.delete(callback)
  });
})(window);
