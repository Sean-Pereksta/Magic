const POLL_INTERVAL_MS = 40;
const MAX_POLISH_ATTEMPTS = 250;

function nameOf(object) {
  return String(object?.name ?? "").toLowerCase();
}

function looksLikeWhisker(object) {
  const name = nameOf(object);
  return name.includes("whisk") || name.includes("vibriss") || name.includes("moustache");
}

function looksLikeEar(object) {
  const name = nameOf(object);
  return name.includes("ear") && !name.includes("heart");
}

function looksLikeEye(object) {
  const name = nameOf(object);
  return name.includes("eye");
}

function looksLikeNose(object) {
  const name = nameOf(object);
  return name.includes("nose") || name.includes("snout") || name.includes("muzzle");
}

function looksLikeTail(object) {
  return nameOf(object).includes("tail");
}

function looksLikeHead(object) {
  const name = nameOf(object);
  return name.includes("head") && !name.includes("ahead");
}

function looksLikeBody(object) {
  const name = nameOf(object);
  return name.includes("body") || name.includes("torso") || name.includes("chest");
}

function tuneMaterial(object) {
  const materials = Array.isArray(object?.material) ? object.material : object?.material ? [object.material] : [];
  for (const material of materials) {
    if (!material) continue;
    if (looksLikeEye(object)) {
      material.roughness = Math.min(material.roughness ?? 0.45, 0.32);
      material.metalness = 0;
    } else if (looksLikeNose(object)) {
      material.roughness = Math.max(material.roughness ?? 0.5, 0.52);
      material.metalness = 0;
    }
  }
}

function polishPart(object) {
  if (!object?.scale) return;

  if (looksLikeWhisker(object)) {
    // The old whiskers dominate the face in third person. Keep them readable,
    // but make them feel like fine mouse vibrissae rather than antennae.
    object.scale.x *= 0.58;
    object.scale.y *= 0.58;
    object.scale.z *= 0.58;
    if (object.material) {
      const materials = Array.isArray(object.material) ? object.material : [object.material];
      for (const material of materials) {
        if (material?.opacity != null) {
          material.transparent = true;
          material.opacity = Math.min(material.opacity, 0.72);
        }
      }
    }
  } else if (looksLikeEar(object)) {
    object.scale.x *= 0.88;
    object.scale.y *= 0.9;
    object.scale.z *= 0.88;
  } else if (looksLikeHead(object)) {
    // Slightly less bulbous and a little longer through the muzzle axis.
    object.scale.x *= 0.94;
    object.scale.y *= 0.93;
    object.scale.z *= 1.03;
  } else if (looksLikeBody(object)) {
    // A lower, longer body reads much more naturally from the chase camera.
    object.scale.x *= 0.95;
    object.scale.y *= 0.88;
    object.scale.z *= 1.08;
  } else if (looksLikeTail(object)) {
    object.scale.x *= 0.84;
    object.scale.y *= 0.84;
    object.scale.z *= 0.94;
  } else if (looksLikeEye(object)) {
    object.scale.multiplyScalar(0.9);
  } else if (looksLikeNose(object)) {
    object.scale.multiplyScalar(0.88);
  }

  tuneMaterial(object);
}

function candidatePlayerRigs(engine) {
  const candidates = [
    engine?.player?.rig,
    engine?.playerRig,
    engine?.mouseRig,
    engine?.player?.mesh,
    engine?.playerMesh,
  ];
  return candidates.filter(Boolean);
}

export function polishThirdPersonMouse(engine = globalThis.window?.hearthmouseEngine) {
  if (!engine) return false;
  const rigs = candidatePlayerRigs(engine);
  if (!rigs.length) return false;

  let polished = false;
  for (const rig of rigs) {
    const root = rig.root ?? rig;
    if (!root?.traverse || root.userData?.__hearthmousePlayerVisualPolished) continue;
    root.userData ??= {};
    root.userData.__hearthmousePlayerVisualPolished = true;
    root.traverse(polishPart);

    // Lower the visible third-person shell just a touch so the mouse feels
    // planted on the floor instead of standing high on its legs.
    if (root.position && !root.userData.__hearthmousePlayerVisualYOffsetApplied) {
      root.position.y -= 0.012;
      root.userData.__hearthmousePlayerVisualYOffsetApplied = true;
    }
    polished = true;
  }
  return polished;
}

function installWhenReady(attempt = 0) {
  if (typeof window === "undefined") return;
  if (polishThirdPersonMouse(window.hearthmouseEngine)) return;
  if (attempt < MAX_POLISH_ATTEMPTS) window.setTimeout(() => installWhenReady(attempt + 1), POLL_INTERVAL_MS);
}

if (typeof window !== "undefined") installWhenReady();
