import { registerCharacterVisualStage } from "./hearthmouse-performance-manager.mjs";

const RAT_SKIN_MATERIAL_PATTERN = /^(pink|skin)$/i;
const RAT_SKIN_COLOR = Object.freeze([0.64000004529953, 0.368318110704422, 0.32935419678688]);
const restoredControllers = new WeakSet();

function restoreRatSkin(controller) {
  if (!controller || controller.kind !== "rat" || restoredControllers.has(controller)) return false;
  if (!controller.__hearthmouseProceduralPolish?.mouseTintApplied) return false;

  let restored = false;
  controller.wrapper?.traverse?.((object) => {
    if (!object?.isMesh || !object.material) return;
    const materials = Array.isArray(object.material) ? object.material : [object.material];
    for (const material of materials) {
      const materialName = String(material?.name ?? "").trim();
      if (!material?.color?.setRGB || !RAT_SKIN_MATERIAL_PATTERN.test(materialName)) continue;
      material.color.setRGB(...RAT_SKIN_COLOR);
      material.needsUpdate = true;
      material.userData ??= {};
      material.userData.__hearthmouseOriginalRatSkinRestored = true;
      restored = true;
    }
  });

  if (restored) {
    controller.wrapper.userData.__hearthmouseOriginalRatSkinRestored = true;
    restoredControllers.add(controller);
  }
  return restored;
}

registerCharacterVisualStage("restore-rat-skin", (actor) => {
  const controller = actor?.rig?.root?.userData?.__hearthmouseGlbController;
  if (controller) restoreRatSkin(controller);
}, 30);
