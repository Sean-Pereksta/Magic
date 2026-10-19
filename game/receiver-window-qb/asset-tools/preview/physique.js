import * as THREE from 'three';

/** Configure an independently loaded/cloned athlete before placing it in gameplay.
 * Mesh targets, rest joints and translation tracks use the same additive profile.
 * This changes anatomy; live ball tracking still requires the game's IK/contact code.
 */
export function createPhysiqueController(model, clips, entry) {
  const baseHeight = Number(entry.heightM ?? entry.measurements?.height_m);
  const width = Number(entry.proportions?.width ?? entry.measurements?.authoring_width_scale);
  const depth = Number(entry.proportions?.depth ?? entry.measurements?.authoring_depth_scale);
  if (!(baseHeight > 0) || !(width > 0) || !(depth > 0)) throw new Error('Missing model proportions for physique adaptation.');
  const stature = baseHeight / 1.88;
  const originalScale = model.scale.clone();
  const skeletons = new Set(), bones = new Set(), meshes = [];
  model.traverse(o => { if (o.isSkinnedMesh) { meshes.push(o); skeletons.add(o.skeleton); for (const b of o.skeleton.bones) bones.add(b); } });
  // Three r160 SkeletonUtils clones the bones but shares boneInverses. Rebinding
  // must own this array and its matrices, or another athlete's proportions change.
  for (const skeleton of skeletons) skeleton.boneInverses = skeleton.boneInverses.map(matrix => matrix.clone());
  model.updateMatrixWorld(true);
  const modelInverse = model.matrixWorld.clone().invert();
  const bindWorld = new Map();
  for (const skeleton of skeletons) skeleton.bones.forEach((bone, i) => {
    bindWorld.set(bone, skeleton.boneInverses[i].clone().invert());
  });
  const bindPosition = new Map(), bindInModel = new Map(), bindLocal = new Map(), originalTrackValues = new Map();
  for (const bone of bones) {
    const world = bindWorld.get(bone);
    bindInModel.set(bone, modelInverse.clone().multiply(world));
    bindPosition.set(bone, new THREE.Vector3().setFromMatrixPosition(world).applyMatrix4(modelInverse));
    const parentWorld = bindWorld.get(bone.parent) ?? bone.parent?.matrixWorld ?? new THREE.Matrix4();
    const local = parentWorld.clone().invert().multiply(world);
    const position = new THREE.Vector3(), quaternion = new THREE.Quaternion(), scale = new THREE.Vector3();
    local.decompose(position, quaternion, scale); bindLocal.set(bone, { position, quaternion, scale });
  }
  const byName = new Map([...bones].map(b => [b.name, b]));
  for (const clip of clips) for (const track of clip.tracks) {
    const binding = THREE.PropertyBinding.parseTrackName(track.name);
    if (binding.propertyName === 'position' && byName.has(binding.nodeName)) originalTrackValues.set(track, { bone: byName.get(binding.nodeName), values: track.values.slice() });
  }
  const clamp = value => THREE.MathUtils.clamp(Number(value) || 0, 0, 1);
  function weights(profile) {
    const strength = clamp((Number(profile.strength ?? 60) - 60) / 35);
    return {
      Muscularity: strength,
      ShoulderWidth: clamp(strength * .4 + clamp(profile.shoulders) * .6),
      ChestSize: clamp(strength * .65 + clamp(profile.chest) * .35),
      ArmThickness: clamp(strength + clamp(profile.arms)),
      LegPower: clamp(strength * .35 + clamp(profile.legs) * .65),
      BodyWeight: clamp(profile.weight), Wingspan: clamp(profile.reach), HandSize: clamp(profile.hands), Reach: clamp(profile.extension),
    };
  }
  // Model coordinates are glTF X right, Y up, Z forward. These equations are
  // identical to the author's shape displacements in Blender's Z-up coordinates.
  function displacement(point, values) {
    const x = point.x / width, z = point.y / stature, y = -point.z / depth, sign = x >= 0 ? 1 : -1;
    const result = new THREE.Vector3();
    const shoulder = clamp((z - 1.04) / .36) * clamp((1.60 - z) / .08) * clamp(Math.abs(x) / .15) * values.ShoulderWidth;
    result.x += sign * .030 * shoulder * width;
    if (Math.abs(x) > .22 && z > .74) {
      const f = clamp((Math.abs(x) - .22) / .23), amount = .045 * values.Wingspan + .025 * values.Reach;
      result.x += sign * amount * f * width; result.y -= amount * .55 * f * stature;
    }
    if (Math.abs(x) > .465 && z < 1.005) {
      result.x += (x - sign * .480) * .20 * width * values.HandSize;
      result.y += (z - .976) * .20 * stature * values.HandSize;
      result.z -= (y + .018) * .20 * depth * values.HandSize;
    }
    return result;
  }
  function apply(profile = {}) {
    const requested = Number(profile.heightM ?? baseHeight);
    if (!Number.isFinite(requested)) throw new Error('Athlete height must be finite.');
    const requestedHeight = THREE.MathUtils.clamp(requested, 1.55, 2.2);
    const values = weights(profile);
    // Rebind at reference scale, then apply stature once to the complete model.
    model.scale.copy(originalScale); model.updateMatrixWorld(true);
    const transformed = new Map();
    for (const bone of bones) {
      const rest = bindLocal.get(bone); bone.position.copy(rest.position); bone.quaternion.copy(rest.quaternion); bone.scale.copy(rest.scale);
      const localPoint = bindPosition.get(bone), matrix = model.matrixWorld.clone().multiply(bindInModel.get(bone));
      matrix.setPosition(localPoint.clone().add(displacement(localPoint, values)).applyMatrix4(model.matrixWorld));
      transformed.set(bone, matrix);
    }
    const localDeltas = new Map();
    for (const bone of bones) {
      const parentWorld = transformed.get(bone.parent) ?? bone.parent?.matrixWorld ?? new THREE.Matrix4();
      const matrix = parentWorld.clone().invert().multiply(transformed.get(bone));
      matrix.decompose(bone.position, bone.quaternion, bone.scale);
      localDeltas.set(bone, bone.position.clone().sub(bindLocal.get(bone).position));
    }
    model.updateMatrixWorld(true);
    for (const skeleton of skeletons) skeleton.calculateInverses();
    for (const mesh of meshes) {
      // Rebinding after placement must update the mesh's bind matrix as well;
      // otherwise the skinning transform cancels its actor's world translation.
      mesh.bind(mesh.skeleton, mesh.matrixWorld);
      for (const [name, value] of Object.entries(values)) {
        const index = mesh.morphTargetDictionary?.[name]; if (index !== undefined) mesh.morphTargetInfluences[index] = value;
      }
      mesh.boundingBox = null; mesh.boundingSphere = null;
    }
    for (const [track, original] of originalTrackValues) {
      const delta = localDeltas.get(original.bone), v = track.values, base = original.values;
      for (let i = 0; i < v.length; i += 3) { v[i] = base[i] + delta.x; v[i + 1] = base[i + 1] + delta.y; v[i + 2] = base[i + 2] + delta.z; }
    }
    model.scale.copy(originalScale).multiplyScalar(requestedHeight / baseHeight); model.updateMatrixWorld(true);
    return { heightM: requestedHeight, weights: values };
  }
  return { apply, bones: byName, baseHeightM: baseHeight };
}
