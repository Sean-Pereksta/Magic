import { mergeStaticMeshes } from "./hearthmouse-habitat.mjs";
import { registerFrameVisualStage } from "./hearthmouse-performance-manager.mjs";

const STATIC_NAMES = /^(?:living-floor|kitchen-floor|north-wall|south-wall|west-wall|east-wall|ceiling|partition|north-baseboard|south-baseboard|west-baseboard|east-baseboard|living-rug|sofa-|coffee-table-|bookcase|book-|east-lower-cabinets|north-lower-cabinets|east-counter|north-counter|island-|dining-table-|chair-|window-frame-|pantry-shelf-|pantry-box-|reading-chair-|window-bench-|study-desk|study-bed|study-mattress|study-rug|laundry-folding-table|laundry-table-leg)/;
const EXCLUDE = /door|nest|stockpile|mouse|cat|hole|tunnel|refuge|shelter|gap|night-|food|trap|vacuum|dog|lamp|light|curtain|climb|household/i;
const stateByWorld = new WeakMap();

export function staticBatchEligible(node, root, dynamic = new Set()) {
  if (!node?.isMesh || node.isSkinnedMesh || node.morphTargetInfluences || Array.isArray(node.material) || dynamic.has(node)) return false;
  if (node.children?.length || node.material?.transparent || node.material?.opacity < 1 || node.material?.onBeforeCompile?.__custom) return false;
  if (!node.geometry?.attributes?.normal || !node.geometry.attributes.uv || node.userData?.__hearthmousePersistentStructure) return false;
  if (EXCLUDE.test(node.name) || node.userData?.interactive || node.userData?.dynamic || node.userData?.__staticVisualBatch) return false;
  if (node.parent === root) return STATIC_NAMES.test(node.name);
  // Only explicit construction-time marks qualify deeper room furniture/decor.
  return node.userData?.staticScenery === true;
}

export function batchStaticScenery(engine, I) {
  const root = engine.world.root;
  let state = stateByWorld.get(root);
  if (!state) stateByWorld.set(root, state = { sources: new Map(), groups: new Map(), signature: "" });
  const dynamic = new Set();
  for (const prop of engine.__expansion?.dynamicProps?.values() ?? []) dynamic.add(prop.mesh);
  for (const door of engine.__expansion?.roomDoors ?? []) dynamic.add(door.mesh);
  root.traverse(node => {
    if (!staticBatchEligible(node, root, dynamic)) return;
    if (!state.sources.has(node) && node.visible) state.sources.set(node, { baseVisible: true });
  });
  const groups = new Map(); let signature = "";
  for (const [node] of state.sources) {
    if (!node.parent || node.userData.__hearthmouseStructureRemoved || dynamic.has(node)) continue;
    const key = `${node.parent.uuid}:${node.material.uuid}:${node.castShadow}:${node.receiveShadow}:${node.renderOrder}:${node.layers.mask}`;
    let nodes = groups.get(key); if (!nodes) groups.set(key, nodes = []); nodes.push(node);
    signature += `${node.id}:${node.geometry.uuid}:${key};`;
  }
  if (signature === state.signature) return state.groups.size;
  const next = new Map();
  for (const [key, nodes] of groups) {
    if (nodes.length < 2) continue;
    const first = nodes[0], parent = first.parent;
    const batch = new I.Mesh(mergeStaticMeshes(I, nodes, parent), first.material);
    batch.name = "static-furniture-visual-batch"; batch.userData.__staticVisualBatch = true;
    batch.castShadow = first.castShadow; batch.receiveShadow = first.receiveShadow;
    batch.renderOrder = first.renderOrder; batch.layers.mask = first.layers.mask;
    // A batch inherits only its own parent's visibility. Base house geometry is
    // permanently on; expanded decor retains room visibility/unlock semantics.
    batch.frustumCulled = false; batch.updateMatrix(); batch.matrixAutoUpdate = false;
    next.set(key, { batch, nodes, parent });
  }
  for (const { batch, nodes } of state.groups.values()) {
    batch.removeFromParent(); batch.geometry.dispose();
    for (const node of nodes) { node.visible = !node.userData.__hearthmouseStructureRemoved; delete node.userData.__staticBatched; }
  }
  for (const { batch, nodes, parent } of next.values()) {
    parent.add(batch);
    for (const node of nodes) { node.visible = false; node.userData.__staticBatched = true; node.updateMatrix(); node.matrixAutoUpdate = false; }
  }
  state.groups = next; state.signature = signature;
  const manager = engine.__expansion?.performanceManager;
  if (manager) manager.stats.staticDrawsSaved = [...next.values()].reduce((n, group) => n + group.nodes.length - 1, 0);
  return next.size;
}

registerFrameVisualStage("bake-static-scenery", engine => {
  const I = globalThis.window?.HearthmouseInternals;
  if (!I || !engine.__livingHouse?.root) return;
  const revision = `${engine.snapshot.night}:${engine.__expansion?.routeRevision}:${engine.world.root.children.length}`;
  if (engine.__staticSceneryRevision === revision) return;
  batchStaticScenery(engine, I); engine.__staticSceneryRevision = `${engine.snapshot.night}:${engine.__expansion?.routeRevision}:${engine.world.root.children.length}`;
}, 0.8);
