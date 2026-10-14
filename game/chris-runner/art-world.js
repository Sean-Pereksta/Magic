/* Cloudflare GLB world art. Collision and the seeded section planner stay in
 * the game; cosmetic placements are indexed and never consume gameRandom.
 * Cached geometry/materials remain owned by RunnerGraphics. Chunk cleanup
 * disposes each InstancedMesh, but skips resources marked runnerAssetShared.
 */
(function (global) {
  'use strict';

  const REALMS = new Set(['forest', 'city', 'rooftops', 'construction', 'dream', 'industrial', 'tunnel', 'ruins', 'snow', 'desert', 'carnival', 'volcanic', 'crystal', 'neon', 'harbor', 'clockwork']);
  const TILE = 6;
  const realmName = value => REALMS.has(value) ? value : 'forest';
  const graphics = () => global.RunnerGraphics;
  const dimensions = () => ({
    road: typeof ROAD_WIDTH === 'number' ? ROAD_WIDTH : 32,
    terrain: typeof TERRAIN_WIDTH === 'number' ? TERRAIN_WIDTH : 78,
    chunk: typeof CHUNK_LENGTH === 'number' ? CHUNK_LENGTH : 42
  });

  // One draw per source primitive for all copies of a path in this group.
  // The source primitive matrix preserves nested GLB node transforms.
  class Batches {
    constructor(group) {
      this.group = group;
      this.paths = new Map();
      this.transform = new global.THREE.Object3D();
    }

    add(path, x, y, z, scale = [1, 1, 1], rotationY = 0, castShadow = true) {
      this.transform.position.set(x, y, z);
      this.transform.rotation.set(0, rotationY, 0);
      this.transform.scale.set(...scale);
      this.transform.updateMatrix();
      const key = path + (castShadow ? ':shadow' : ':flat');
      if (!this.paths.has(key)) this.paths.set(key, {path, castShadow, matrices: []});
      this.paths.get(key).matrices.push(this.transform.matrix.clone());
    }

    finish() {
      const THREE = global.THREE, matrix = new THREE.Matrix4();
      for (const {path, matrices, castShadow} of this.paths.values()) {
        for (const primitive of graphics().primitives(path)) {
          primitive.geometry.userData.runnerAssetShared = true;
          for (const material of (Array.isArray(primitive.material) ? primitive.material : [primitive.material])) material.userData.runnerAssetShared = true;
          const mesh = new THREE.InstancedMesh(primitive.geometry, primitive.material, matrices.length);
          matrices.forEach((placement, index) => mesh.setMatrixAt(index, matrix.multiplyMatrices(placement, primitive.matrix)));
          mesh.name = path;
          mesh.userData.runnerAssetPath = path;
          mesh.castShadow = castShadow;
          mesh.receiveShadow = true;
          mesh.instanceMatrix.needsUpdate = true;
          mesh.computeBoundingBox();
          mesh.computeBoundingSphere();
          this.group.add(mesh);
        }
      }
      return this.group;
    }
  }

  function group(name) {
    const result = new global.THREE.Group();
    result.name = name;
    result.userData.runnerWorldArt = true;
    return result;
  }

  function ground(width, length, biome) {
    biome = realmName(biome);
    // Ground historically spans the complete scenery width, beyond the track.
    const terrainWidth = Math.max(width, dimensions().terrain);
    const columns = Math.ceil(terrainWidth / TILE), rows = Math.ceil(length / TILE);
    const result = group(biome + ' terrain'), batch = new Batches(result);
    for (let row = 0; row < rows; row++) for (let column = 0; column < columns; column++) {
      batch.add(`tiles/${biome}-terrain.glb`, (column - (columns - 1) / 2) * TILE, 0, (row - (rows - 1) / 2) * TILE, [1, 1, length / (rows * TILE)], 0, false);
    }
    // Nonstandard lengths remain centered and continuous as well.
    if (length !== rows * TILE) {
      for (const item of batch.paths.values()) item.matrices.forEach(matrix => { matrix.elements[14] *= length / (rows * TILE); });
    }
    return batch.finish();
  }

  function road(chunk, zStart, biome) {
    biome = realmName(biome);
    const {road: roadWidth, chunk: length} = dimensions();
    const result = group(biome + ' road'), batch = new Batches(result);
    const rows = Math.ceil(length / TILE), rowLength = length / rows;
    // The edge GLB has road on its inner half and shoulder on its outer half.
    // Four center tiles cover 24 m; at the default width, each 8 m edge has
    // 4 m of road and 4 m of shoulder, preserving the track boundary at ±16.
    const interiorColumns = Math.max(1, Math.ceil(roadWidth / TILE) - 2);
    const interiorWidth = Math.min(interiorColumns * TILE, roadWidth - 2);
    const centerWidth = interiorWidth / interiorColumns;
    const edgeRoadWidth = (roadWidth - interiorWidth) / 2;
    const transition = Math.floor(zStart / length) % 6 === 0;
    for (let row = 0; row < rows; row++) {
      const z = zStart + (row + .5) * rowLength;
      for (let column = 0; column < interiorColumns; column++) {
        const kind = transition && row === 0 ? 'transition' : 'center';
        batch.add(`tiles/${biome}-${kind}.glb`, (column - (interiorColumns - 1) / 2) * centerWidth, .095, z, [centerWidth / TILE, 1, rowLength / TILE], 0, false);
      }
      for (const side of [-1, 1]) {
        batch.add(`tiles/${biome}-edge.glb`, side * roadWidth / 2, .095, z, [edgeRoadWidth / 3, 1, rowLength / TILE], side < 0 ? Math.PI : 0, false);
      }
    }
    chunk.add(batch.finish());
    return result;
  }

  function architecture(chunk, zStart, biome) {
    biome = realmName(biome);
    const result = group(biome + ' architecture'), batch = new Batches(result);
    const length = dimensions().chunk;
    const index = Math.floor(zStart / length);
    for (const side of [-1, 1]) {
      for (let i = 0; i < 3; i++) {
        const z = zStart + 7 + i * 14, faceRoad = -side * Math.PI / 2;
        const variation = .86 + ((index * 7 + i * 3 + (side + 1)) % 7) * .055;
        if (biome === 'city') {
          batch.add('props/city-building.glb', side * 27, 0, z, [1, variation, 1], faceRoad);
          if (i !== 1) batch.add('props/street-light.glb', side * 19.8, 0, z + 4, [1, 1, 1], side < 0 ? 0 : Math.PI);
        } else if (biome === 'rooftops') {
          batch.add('props/rooftop-building.glb', side * 27, -8.2, z, [1, 1, 1], faceRoad);
        } else if (biome === 'construction') {
          batch.add('props/construction-crane.glb', side * 28, 0, z, [1, variation, 1], side < 0 ? Math.PI : 0);
          batch.add('props/traffic-barrier.glb', side * 21, 0, z + 3, [.7, .85, 1], Math.PI / 2);
        } else if (biome === 'industrial') {
          batch.add('props/industrial-tank.glb', side * 28, 0, z, [1, variation, 1], faceRoad);
        } else if (biome === 'ruins') {
          batch.add('props/ruins-column.glb', side * 26, 0, z, [1, variation, 1], faceRoad);
          batch.add('props/ruins-column.glb', side * 33, 0, z + 5, [.72, .68, .72], faceRoad);
        }
      }
      if (biome === 'rooftops') {
        for (let i = 0; i < 7; i++) batch.add('props/rooftops-railing.glb', side * 21.5, 0, zStart + 3 + i * 6, [6 / 5.4, 1, 1], Math.PI / 2);
        batch.add('props/rooftops-vent.glb', side * 18.7, 0, zStart + 20);
      }
      if (biome === 'tunnel') {
        for (let i = 0; i < 7; i++) batch.add('props/tunnel-wall.glb', side * 23, 0, zStart + 3 + i * 6, [1, 1.16, 1], side < 0 ? Math.PI : 0);
      }
    }
    if (biome === 'tunnel') for (const z of [4, 18, 32]) for (const x of [-18, -6, 6, 18]) {
      batch.add('props/tunnel-ceiling-beam.glb', x, 14, zStart + z);
    }
    if (biome === 'city') batch.add('props/city-sign.glb', index % 2 ? -19.7 : 19.7, 0, zStart + 30, [.8, .8, .8], Math.PI);
    chunk.add(batch.finish());
    return result;
  }

  function addWeather(result, zStart, biome) {
    const THREE = global.THREE;
    if (biome === 'harbor') {
      const geometry = new THREE.PlaneGeometry(7.5, dimensions().chunk);
      const material = new THREE.MeshStandardMaterial({color: 0x3297b5, roughness: .21, metalness: .24});
      for (const side of [-1, 1]) {
        const water = new THREE.Mesh(geometry, material);
        water.rotation.x = -Math.PI / 2;
        water.position.set(side * 19.75, .03, zStart + dimensions().chunk / 2);
        water.userData.water = true;
        water.receiveShadow = true;
        result.add(water);
      }
      const rain = group('harbor rain');
      const count = typeof isMobileDevice !== 'undefined' && isMobileDevice ? 12 : 24;
      const streakGeometry = new THREE.BoxGeometry(.035, 1.8, .035);
      const streakMaterial = new THREE.MeshBasicMaterial({color: 0xb3e8f2, transparent: true, opacity: .6});
      const streaks = new THREE.InstancedMesh(streakGeometry, streakMaterial, count);
      const transform = new THREE.Object3D();
      for (let i = 0; i < count; i++) {
        transform.position.set(((i * 17) % 43) - 21, 6 + i % 5, zStart + (i * 11) % 42);
        transform.rotation.z = -.35;
        transform.updateMatrix();
        streaks.setMatrixAt(i, transform.matrix);
      }
      streaks.instanceMatrix.needsUpdate = true;
      streaks.computeBoundingSphere();
      rain.add(streaks);
      rain.userData.rain = true;
      result.add(rain);
    }
    if (biome === 'volcanic') {
      const geometry = new THREE.OctahedronGeometry(.14, 0);
      const material = new THREE.MeshBasicMaterial({color: 0xffbb66});
      for (const side of [-1, 1]) for (let e = 0; e < 3; e++) {
        const ember = new THREE.Mesh(geometry, material);
        ember.position.set(side * (24 + e), 4 + e * 2, zStart + 9 + e * 12);
        ember.userData.ember = e;
        result.add(ember);
      }
    }
  }

  function realmScenery(chunk, zStart, biome) {
    biome = realmName(biome);
    const result = group(biome + ' realm scenery'), batch = new Batches(result);
    const index = Math.floor(zStart / dimensions().chunk);
    for (const side of [-1, 1]) for (let i = 0; i < 3; i++) {
      const z = zStart + 5 + i * 14, faceRoad = -side * Math.PI / 2;
      if (biome === 'carnival') {
        batch.add('props/carnival-booth.glb', side * 26, 0, z, [1, 1, 1], faceRoad);
        if (i === 1 && index % 3 === 0) batch.add('props/carnival-ferris-wheel.glb', side * 35, 0, z, [1, 1, 1], faceRoad);
        if (i === 1 && index % 3 === 1) batch.add('props/carnival-carousel.glb', side * 35, 0, z);
      } else if (biome === 'volcanic') {
        batch.add('props/volcanic-crag.glb', side * 28, 0, z, [1, 1 + i * .08, 1], i * .4);
        batch.add('props/volcanic-eruption-vent.glb', side * 24, 0, z + 5, [.7, .7, .7]);
      } else if (biome === 'crystal') {
        batch.add('props/crystal-cluster.glb', side * 26, 0, z, [1, 1 + i * .08, 1], side * .3);
        batch.add('props/crystal-rock.glb', side * 20.8, 0, z + 4);
        if (side === 1) for (const x of [-18, -6, 6, 18]) batch.add('props/crystal-ceiling.glb', x, 15, z);
      } else if (biome === 'neon') {
        batch.add('props/neon-tower.glb', side * 27, 0, z, [1, .85 + i * .23, 1], faceRoad);
        batch.add('props/neon-energy-gate.glb', side * 21, 0, z + 5, [.6, 1.4, 1], Math.PI / 2);
      } else if (biome === 'harbor') {
        batch.add('props/harbor-cargo-container.glb', side * 28, 0, z, [1, 1, 1], faceRoad);
        batch.add('props/harbor-pier.glb', side * 19.75, 0, z + 2);
        if (i === 1) batch.add('props/harbor-crane.glb', side * 36, 0, z, [1, 1, 1], side < 0 ? Math.PI : 0);
      } else if (biome === 'clockwork') {
        batch.add('props/clockwork-tower.glb', side * 27, 0, z, [1, 1, 1], faceRoad);
        // The authored gear's center is 3.4 m above its floor origin. Recenter
        // the visual inside its rotating parent to keep the axle stationary.
        const pivot = group('rotating clockwork gear');
        const gear = graphics().create('props/clockwork-gear.glb');
        gear.position.y = -3.4;
        pivot.add(gear);
        pivot.position.set(side * 23, 8, z);
        pivot.userData.spin = side * .35;
        result.add(pivot);
      }
    }
    if (biome === 'volcanic') for (const side of [-1, 1]) for (let i = 0; i < 7; i++) {
      batch.add('props/volcanic-lava-channel.glb', side * 19.5, 0, zStart + 3 + i * 6, [1, 1, 1], side * .04, false);
    }
    batch.finish();
    addWeather(result, zStart, biome);
    chunk.userData.realmScenery = result;
    chunk.add(result);
    return result;
  }

  function obstacle(biome, breakable, shape = 0) {
    biome = realmName(biome);
    shape = Math.abs(Math.trunc(shape) || 0) % 4;
    const result = group(biome + (breakable ? ' breakable' : ' solid') + ' obstacle');
    const path = biome==='clockwork' && shape===0 ? 'props/clockwork-piston.glb' : `obstacles/${biome}-${breakable ? 'breakable' : 'solid'}-${shape}.glb`;
    const art = graphics().create(path);
    art.userData.runnerObstacleVisual = true;
    if(biome==='clockwork' && shape===0){const bounds=graphics().asset(path).bounds;art.scale.y=(breakable?2.1:3.4)/(bounds.max[1]-bounds.min[1]);}
    result.add(art);
    Object.assign(result.userData, {type: 'wall', radiusX: 2.2, radiusZ: .7, breakable: !!breakable, vaultable: !!breakable, height: breakable ? 2.1 : 3.4, runnerAssetPath: path});
    return result;
  }

  function decor(chunk,models){const result=group('outer terrain decor'),batch=new Batches(result);for(const model of models){const a=model.userData.runnerAsset;batch.add(a.path,model.position.x,model.position.y,model.position.z,model.scale.toArray(),model.rotation.y,true);}chunk.add(batch.finish());return result;}
  global.RunnerWorldArt = Object.freeze({ground, road, architecture, realmScenery, obstacle, decor});
})(window);
