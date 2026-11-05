import { cardImage, backImage, drawSymbol } from './art.mjs';

export function createScene(canvas, { onInteract = () => {}, onHover = () => {} } = {}) {
  const T = window.THREE;
  if (!T) throw new Error('The 3D renderer could not load.');
  const renderer = new T.WebGLRenderer({ canvas, antialias: true, powerPreference: 'high-performance' });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.5));
  renderer.shadowMap.enabled = true;
  renderer.shadowMap.type = T.PCFSoftShadowMap;
  renderer.outputColorSpace = T.SRGBColorSpace;
  renderer.toneMapping = T.ACESFilmicToneMapping;
  renderer.toneMappingExposure = 1.35;
  const scene = new T.Scene();
  scene.background = new T.Color('#171c25');
  scene.fog = new T.FogExp2('#252326', .024);
  const camera = new T.PerspectiveCamera(61, 1, .1, 70);
  camera.position.set(0, 4.4, 7.6);
  camera.rotation.order = 'YXZ';
  let yaw = 0, pitch = -.29, looking = false, state, previousTime = 0;
  camera.rotation.set(pitch, yaw, 0);
  const objects = new Map(), textures = new Map(), hitObjects = [], creatures = [], candles = [], processedEvents = new Set();
  let handHints = new Set();
  const pointer = new T.Vector2(0, 0), raycaster = new T.Raycaster();
  let lastHoverKey = '', hovered;
  const loader = new T.TextureLoader();
  function texture(url) {
    if (!textures.has(url)) {
      const tx = loader.load(url); tx.colorSpace = T.SRGBColorSpace;
      tx.anisotropy = Math.min(4, renderer.capabilities.getMaxAnisotropy()); textures.set(url, tx);
    }
    return textures.get(url);
  }
  function mat(color, extra = {}) { return new T.MeshStandardMaterial({ color, roughness: .83, ...extra }); }
  const wood = mat('#64402a'), darkWood = mat('#3d2b25'), brass = mat('#b28b50', { metalness: .6, roughness: .48 });
  function mesh(geo, material, parent, x = 0, y = 0, z = 0) {
    const m = new T.Mesh(geo, material); m.position.set(x, y, z); m.castShadow = true; m.receiveShadow = true;
    (parent || scene).add(m); return m;
  }
  function box(w, h, d, material, parent, x, y, z) { return mesh(new T.BoxGeometry(w, h, d), material, parent, x, y, z); }
  function ball(w, h, d, material, parent, x, y, z) {
    const m = mesh(new T.SphereGeometry(1, 20, 14), material, parent, x, y, z); m.scale.set(w, h, d); return m;
  }
  function tube(radius, height, material, parent, x, y, z, radial = 14) { return mesh(new T.CylinderGeometry(radius, radius, height, radial), material, parent, x, y, z); }
  function cone(radius, height, material, parent, x, y, z) { return mesh(new T.ConeGeometry(radius, height, 9), material, parent, x, y, z); }
  function rod(a, b, radius, material, parent) {
    const from = new T.Vector3(...a), to = new T.Vector3(...b), delta = to.clone().sub(from);
    const m = tube(radius, delta.length(), material, parent, 0, 0, 0, 8);
    m.position.copy(from.add(to).multiplyScalar(.5)); m.quaternion.setFromUnitVectors(new T.Vector3(0, 1, 0), delta.normalize()); return m;
  }
  const hemi = new T.HemisphereLight('#c5d6ef', '#443124', 1.8); scene.add(hemi);
  const warm = new T.DirectionalLight('#ffe0b1', 2.3); warm.position.set(-3, 9, 5); warm.castShadow = true;
  warm.shadow.mapSize.set(1024, 1024); warm.shadow.camera.left = -10; warm.shadow.camera.right = 10;
  warm.shadow.camera.top = 10; warm.shadow.camera.bottom = -10; warm.shadow.normalBias = .035; scene.add(warm);
  const cool = new T.DirectionalLight('#91b4ec', 1.7); cool.position.set(1, 8, -9); scene.add(cool);
  const centerLight = new T.PointLight('#ffbd74', 36, 15, 2); centerLight.position.set(0, 5.5, 1); scene.add(centerLight);

  function woodTexture() {
    const c = document.createElement('canvas'); c.width = c.height = 1024;
    const ctx = c.getContext('2d'); ctx.fillStyle = '#865a3a'; ctx.fillRect(0, 0, 1024, 1024);
    let seed = 8283; const rnd = () => ((seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296);
    for (let row = 0; row < 9; row++) {
      ctx.fillStyle = `rgba(${row % 2 ? '40,24,13' : '211,157,92'},.11)`; ctx.fillRect(0, row * 122, 1024, 120);
      ctx.fillStyle = '#332014'; ctx.fillRect(0, row * 122, 1024, 2);
      for (let n = 0; n < 47; n++) {
        const y = row * 122 + rnd() * 120; ctx.strokeStyle = rnd() < .4 ? '#edba7629' : '#2b180f30'; ctx.lineWidth = rnd() * 1.8 + .3;
        ctx.beginPath(); ctx.moveTo(0, y); ctx.bezierCurveTo(300, y + rnd() * 14 - 7, 700, y + rnd() * 14 - 7, 1024, y + rnd() * 8 - 4); ctx.stroke();
      }
    }
    for (let n = 0; n < 16; n++) {
      const x = rnd() * 1024, y = rnd() * 1024;
      for (let r = 3; r < 18; r += 3) { ctx.strokeStyle = '#32201355'; ctx.beginPath(); ctx.ellipse(x, y, r * 2.7, r * .6, 0, 0, Math.PI * 2); ctx.stroke(); }
    }
    const tx = new T.CanvasTexture(c); tx.colorSpace = T.SRGBColorSpace; tx.anisotropy = 4; return tx;
  }
  const grain = woodTexture();
  const tableTop = mat('#cda878', { map: grain, roughness: .68 });
  const floor = box(26, .2, 28, mat('#7a5840', { map: grain }), null, 0, -.15, 0);
  floor.receiveShadow = true;
  // A compact timber room, with a bright moonlit window behind the guests.
  box(24, 10, .4, mat('#675047', { map: grain }), null, 0, 4.8, -10);
  box(.4, 10, 26, mat('#4b3d37', { map: grain }), null, -12, 4.8, 0);
  box(.4, 10, 26, mat('#4b3d37', { map: grain }), null, 12, 4.8, 0);
  for (const x of [-10, -5, 0, 5, 10]) box(.28, 10, .55, darkWood, null, x, 4.8, -9.7);
  for (const y of [1.1, 7.6, 9.3]) box(24, .3, .55, darkWood, null, 0, y, -9.65);
  for (const z of [-8, -2, 4, 10]) box(24, .5, .6, darkWood, null, 0, 9.4, z);
  const windowMat = new T.MeshBasicMaterial({ color: '#527cad' });
  box(4.6, 4.8, .08, windowMat, null, 1.5, 5.1, -9.55);
  for (const x of [-.9, 1.5, 3.9]) box(.14, 5.2, .25, darkWood, null, x, 5.1, -9.4);
  for (const y of [2.6, 5.1, 7.6]) box(5, .15, .25, darkWood, null, 1.5, y, -9.4);
  const moon = mesh(new T.CircleGeometry(.72, 40), new T.MeshBasicMaterial({ color: '#e3e8bc' }), null, 2.4, 6.3, -9.47);
  for (let i = 0; i < 24; i++) {
    const x = -.65 + ((i * 1.732) % 4.3), y = 3 + ((i * 1.139) % 4.2);
    mesh(new T.CircleGeometry(.018 + (i % 3) * .009, 5), new T.MeshBasicMaterial({ color: '#dbe3dc' }), null, x, y, -9.44);
  }
  // Rear shelves with glazed jars and bundles of herbs.
  for (const y of [3.5, 5.5, 7.5]) {
    box(4.7, .17, .8, darkWood, null, -6.7, y, -9.1);
    for (let i = 0; i < 5; i++) {
      const jar = tube(.19 + (i % 2) * .06, .5 + (i % 3) * .13, mat(['#788274', '#b68c64', '#847389'][i % 3]), null, -8.35 + i * .84, y + .4, -9.08);
      tube(.14, .09, brass, null, jar.position.x, y + .74 + (i % 3) * .065, -9.08);
    }
  }
  for (let i = 0; i < 4; i++) {
    rod([6.5 + i * .45, 8.6, -9], [6.4 + i * .45, 7.3 - (i % 2) * .3, -8.95], .025, brass);
    for (let j = 0; j < 5; j++) {
      const leaf = ball(.12, .28, .06, mat('#677858'), null, 6.2 + i * .45 + (j % 2) * .28, 7.4 + j * .15, -8.9);
      leaf.rotation.z = j % 2 ? -.6 : .6;
    }
  }
  // Massive round oak table and carved pedestal.
  mesh(new T.CylinderGeometry(5.5, 5.42, .35, 80), [wood, tableTop, darkWood], null, 0, 1.53, 0);
  tube(.7, 1.5, darkWood, null, 0, .64, 0, 14);
  for (let i = 0; i < 4; i++) {
    const a = i * Math.PI / 2;
    rod([0, .55, 0], [Math.cos(a) * 2.4, .1, Math.sin(a) * 2.4], .23, darkWood);
  }
  const ring = mesh(new T.TorusGeometry(4.6, .013, 5, 100), brass, null, 0, 1.715, 0); ring.rotation.x = -Math.PI / 2;
  const ring2 = mesh(new T.TorusGeometry(4.69, .009, 5, 100), brass, null, 0, 1.716, 0); ring2.rotation.x = -Math.PI / 2;
  const stampCanvas = document.createElement('canvas'); stampCanvas.width = stampCanvas.height = 256;
  const sc = stampCanvas.getContext('2d'); drawSymbol(sc, 'moth', 128, 128, 220, '#bb925b');
  const stampMat = new T.MeshBasicMaterial({ map: new T.CanvasTexture(stampCanvas), transparent: true, opacity: .5, depthWrite: false });
  for (let i = 0; i < 8; i++) {
    const a = i * Math.PI / 4, m = mesh(new T.PlaneGeometry(.6, .6), stampMat, null, Math.sin(a) * 4.1, 1.718, Math.cos(a) * 4.1);
    m.rotation.x = -Math.PI / 2; m.rotation.z = -a;
  }
  function chair(x, z, angle) {
    const g = new T.Group(); g.position.set(x, 0, z); g.rotation.y = angle; scene.add(g);
    box(1.75, .22, 1.7, wood, g, 0, 1.2, 0);
    for (const xx of [-.67, .67]) for (const zz of [-.6, .6]) box(.16, 1.2, .16, darkWood, g, xx, .6, zz);
    for (const xx of [-.7, .7]) box(.15, 2.2, .15, wood, g, xx, 2, -.65);
    box(1.5, .7, .18, wood, g, 0, 2.8, -.65);
    return g;
  }
  function candle(x, z, height = .68, parent = scene, baseY = 1.72) {
    tube(.27, .07, brass, parent, x, baseY + .04, z, 20);
    tube(.14, height, mat('#e8cf8f'), parent, x, baseY + height / 2 + .07, z, 16);
    for (let i = 0; i < 4; i++) ball(.028, .1 + i * .018, .03, mat('#ead6a1'), parent, x + Math.cos(i * 1.7) * .135, baseY + height - .05, z + Math.sin(i * 1.7) * .135);
    const flame = ball(.07, .18, .07, new T.MeshBasicMaterial({ color: '#ffd786' }), parent, x, baseY + height + .22, z);
    const light = new T.PointLight('#ffb65d', 11, 5, 2); light.position.copy(flame.position); parent.add(light);
    candles.push({ flame, light, seed: candles.length * 1.7 });
  }
  candle(-3.8, 2.2, .65); candle(3.85, 1.9, .83); candle(-2.85, -3.65, .5); candle(3.1, -3.55, .72);
  function mug(x, z) {
    const cup = tube(.24, .42, mat('#876c4c'), null, x, 1.95, z, 18);
    tube(.2, .008, mat('#2d1e15'), null, x, 2.167, z, 18);
    const handle = mesh(new T.TorusGeometry(.16, .045, 7, 16), brass, null, x + .26, 1.98, z); handle.rotation.y = Math.PI / 2;
    return cup;
  }
  mug(-4.3, -.8); mug(4.3, -.65); mug(.95, -4.4);

  function creature(player, x, z, kind) {
    const root = new T.Group(); root.position.set(x, 0, z); root.rotation.y = Math.atan2(-x, 6 - z); scene.add(root);
    chair(x, z, root.rotation.y);
    const body = new T.Group(); root.add(body);
    const skin = mat(kind === 'hare' ? '#8f9b62' : kind === 'moth' ? '#a78465' : '#ad7653');
    const pale = mat(kind === 'hare' ? '#bec397' : kind === 'moth' ? '#d5b793' : '#e4cc9c');
    const cloth = mat(kind === 'hare' ? '#53634c' : kind === 'moth' ? '#6b5265' : '#515c68');
    ball(.8, 1.05, .63, cloth, body, 0, 2.25, 0);
    for (const xx of [-.55, .55]) {
      const arm = ball(.24, .62, .25, skin, body, xx, 2.55, .33); arm.rotation.z = xx < 0 ? -.7 : .7;
      ball(.25, .17, .27, skin, body, xx * .95, 2.3, .72);
      for (let i = 0; i < 3; i++) ball(.055, .1, .11, pale, body, xx * .95 + (i - 1) * .095, 2.29, .93);
    }
    const head = new T.Group(); head.position.set(0, 3.75, .04); body.add(head);
    ball(.83, .79, .65, skin, head, 0, 0, 0);
    ball(.6, .34, .48, pale, head, 0, -.3, .45);
    // Nose, smile, and cheek tufts make the low-poly guests expressive.
    ball(.14, .09, .1, mat('#382c2a'), head, 0, -.23, .89);
    rod([-.22, -.41, .805], [0, -.46, .87], .018, mat('#493e32'), head);
    rod([0, -.46, .87], [.22, -.41, .805], .018, mat('#493e32'), head);
    for (const xx of [-.34, .34]) {
      ball(.24, .26, .12, pale, head, xx, .08, .58);
      ball(.17, .19, .09, mat(kind === 'moth' ? '#dfbc73' : '#e5d98c'), head, xx, .075, .681);
      const pupil = ball(.07, .13, .035, mat('#1b2220'), head, xx - .025, .06, .76);
      ball(.035, .04, .017, new T.MeshBasicMaterial({ color: '#fff6d5' }), head, xx - .045, .12, .793);
      const brow = ball(.28, .08, .1, skin, head, xx, .33, .6); brow.rotation.z = xx < 0 ? -.12 : .12;
    }
    if (kind === 'hare') {
      for (const xx of [-.47, .47]) {
        const ear = ball(.22, .95, .21, skin, head, xx, 1.02, -.03); ear.rotation.z = xx < 0 ? .22 : -.22;
        const inner = ball(.12, .73, .05, mat('#9f8b6e'), head, xx, 1.05, .165); inner.rotation.z = ear.rotation.z;
        rod([xx * .65, .5, -.2], [xx * 1.3, 1.3, -.36], .07, wood, head);
        rod([xx * 1.06, .98, -.3], [xx * 1.8, 1.34, -.31], .045, wood, head);
        rod([xx * 1.3, 1.25, -.36], [xx * 1.1, 1.65, -.3], .035, wood, head);
      }
      for (let i = 0; i < 11; i++) ball(.12, .08, .1, mat(i % 2 ? '#738c48' : '#a5a760'), head, Math.sin(i * 2.4) * .62, .57 + (i % 3) * .12, Math.cos(i * 1.2) * .3);
      for (const xx of [-.6, .6]) for (let i = 0; i < 3; i++) rod([xx, -.2, .65], [xx * 1.7, -.15 - i * .09, .8], .008, pale, head);
    } else if (kind === 'moth') {
      for (const xx of [-1, 1]) {
        const wing = ball(.8, 1.25, .1, mat('#806b7e'), body, xx * .88, 3.03, -.45); wing.rotation.z = xx * -.37;
        const patch = ball(.48, .7, .12, mat('#b29879'), body, xx * 1.13, 3.23, -.34); patch.rotation.z = xx * -.37;
        ball(.21, .3, .06, mat('#5b536b'), body, xx * 1.18, 3.33, -.2);
        rod([xx * .25, .62, .06], [xx * .58, 1.18, .04], .025, darkWood, head);
        ball(.09, .16, .07, pale, head, xx * .63, 1.2, .04);
      }
      // Fine brass spectacles.
      for (const xx of [-.34, .34]) mesh(new T.TorusGeometry(.28, .018, 6, 30), brass, head, xx, .08, .79);
      rod([-.07, .11, .8], [.07, .11, .8], .015, brass, head);
      ball(.5, .11, .2, mat('#d2b780'), body, 0, 2.93, .52);
      for (let i = 0; i < 3; i++) ball(.075, .075, .06, brass, body, 0, 2.65 - i * .25, .63);
    } else {
      for (const xx of [-.6, .6]) {
        const ear = cone(.32, .94, skin, head, xx, .72, -.03); ear.rotation.z = xx < 0 ? .36 : -.36;
        const inside = cone(.17, .65, pale, head, xx, .78, .16); inside.rotation.z = ear.rotation.z;
        for (let i = 0; i < 3; i++) { const feather = cone(.16, .54, pale, head, xx * 1.12, -.15 - i * .15, .15); feather.rotation.z = xx < 0 ? -1 : 1; }
      }
      const beak = cone(.2, .5, mat('#bf9851'), head, 0, -.25, .9); beak.rotation.x = Math.PI / 2;
      const scarf = tube(.63, .22, mat('#9b593e'), body, 0, 3.03, .06, 16);
      const tail = box(.3, .9, .12, mat('#9b593e'), body, .35, 2.62, .63); tail.rotation.z = -.18;
    }
    // Every modeled piece can be selected as a spell target.
    root.traverse(m => { if (m.isMesh) { m.userData.hit = { kind: 'player', player, id: player }; hitObjects.push(m); } });
    creatures.push({ root, body, head, player, phase: player * 1.8 });
  }
  creature(1, -5.15, -.75, 'hare'); creature(2, 0, -5.5, 'moth'); creature(3, 5.15, -.75, 'rook');

  const cardGeometry = new T.BoxGeometry(.73, 1.04, .026);
  const faceGeometry = new T.PlaneGeometry(.715, 1.025);
  const edgeMat = mat('#b9a17b', { roughness: .95 });
  const backMat = mat('#ffffff', { map: texture(backImage()), roughness: .85 });
  const goldOutline = new T.LineBasicMaterial({ color: '#f5d38a', transparent: true, opacity: .95 });
  const hintOutline = new T.LineBasicMaterial({ color: '#c4c9a1', transparent: true, opacity: .5 });
  const outlineGeo = new T.EdgesGeometry(new T.BoxGeometry(.77, 1.08, .034));
  const numberTextures = new Map();
  function numberTexture(number) {
    if (numberTextures.has(number)) return numberTextures.get(number);
    const c = document.createElement('canvas'); c.width = c.height = 128;
    const cx = c.getContext('2d');
    cx.fillStyle = '#1b241ff2'; cx.strokeStyle = '#cbb17a'; cx.lineWidth = 5;
    cx.beginPath(); cx.arc(64, 64, 53, 0, Math.PI * 2); cx.fill(); cx.stroke();
    cx.fillStyle = '#fff0c8'; cx.font = 'bold 76px Georgia, serif'; cx.textAlign = 'center'; cx.textBaseline = 'middle'; cx.fillText(String(number), 64, 68);
    const tx = new T.CanvasTexture(c); tx.colorSpace = T.SRGBColorSpace; numberTextures.set(number, tx); return tx;
  }
  function labelSprite(map, width, height) {
    const s = new T.Sprite(new T.SpriteMaterial({ map, transparent: true, depthWrite: false, depthTest: false }));
    s.scale.set(width, height, 1); s.renderOrder = 6; return s;
  }
  const burnList = [];
  function createCard(card, kind, player, index) {
    const group = new T.Group(); scene.add(group);
    const slab = mesh(cardGeometry, edgeMat, group);
    const frontMat = mat('#ffffff', { map: texture(cardImage(card.type)), roughness: .85 });
    const front = mesh(faceGeometry, frontMat, group, 0, 0, .014);
    const reverse = mesh(faceGeometry, backMat, group, 0, 0, -.014); reverse.rotation.y = Math.PI;
    const outline = new T.LineSegments(outlineGeo, goldOutline); outline.visible = false; group.add(outline);
    const number = labelSprite(numberTexture(index + 1), .27, .27); number.position.set(0, -.71, .04); number.visible = false; group.add(number);
    const obj = { card, group, front, slab, reverse, outline, number, kind, player, index, count: 7, target: new T.Vector3(), rotation: new T.Euler(), born: performance.now(), burning: false };
    group.position.set(0, 9 + (index % 7) * .3, 0);
    group.rotation.x = Math.PI / 2;
    for (const m of [slab, front, reverse, number]) { m.userData.cardObj = obj; hitObjects.push(m); }
    objects.set(card.id, obj); return obj;
  }
  function setCard(card, kind, player, index, count) {
    const obj = objects.get(card.id) || createCard(card, kind, player, index);
    if (obj.card.type !== card.type) obj.front.material.map = texture(cardImage(card.type));
    obj.card = card; obj.kind = kind; obj.player = player; obj.index = index; obj.count = count; obj.active = true;
    obj.number.visible = kind === 'hand';
    if (kind === 'hand') obj.number.material.map = numberTexture(index + 1);
    if (kind === 'center') {
      placeCenter(obj, count);
    } else if (player === 0) {
      obj.group.scale.setScalar(1);
      const offset = index - (count - 1) / 2;
      obj.target.set(offset * .65, 2.4 + .11 * (1 - Math.abs(offset) / 4), 3.72 + Math.abs(offset) * .055);
      obj.rotation.set(-.9, offset * -.025, offset * -.075);
    } else {
      obj.group.scale.setScalar(1);
      const angle = player === 1 ? -Math.PI / 2 : player === 2 ? Math.PI : Math.PI / 2;
      const offset = (index - (count - 1) / 2) * .27;
      const radius = 4.36;
      obj.target.set(Math.sin(angle) * radius + Math.cos(angle) * offset, 2.5, Math.cos(angle) * radius - Math.sin(angle) * offset);
      // Front faces the owner, printed backs face the local player.
      obj.rotation.set(.1, angle, (index - 3) * -.045);
    }
    return obj;
  }
  function placeCenter(obj, count) {
    const portrait = camera.aspect < .8, large = count > 16;
    const cols = large ? 6 : 4, rows = Math.ceil(count / cols), row = Math.floor(obj.index / cols);
    const rowCount = Math.min(cols, count - row * cols);
    const narrow = portrait ? Math.min(1, camera.aspect / (391 / 844)) : 1;
    const gapX = (portrait ? large ? .84 : 1.22 : large ? 1.15 : 1.35) * narrow;
    const gapZ = count > 12 ? 1.65 : 1.8;
    const rowRise = count > 12 && !large ? .35 : .3;
    const originZ = portrait ? large ? -1.65 : count > 12 ? -1.25 : -.65 : -.25;
    // The shallow staircase also separates rows on screen: a flat grid leaves
    // the back edge of a near card covering the next card at a seated eye level.
    obj.target.set((obj.index % cols - (rowCount - 1) / 2) * gapX, 1.85 + (rows - 1 - row) * rowRise, (row - (rows - 1) / 2) * gapZ + originZ);
    obj.rotation.set(Math.PI / 2 + .16, 0, Math.sin(obj.index * 3.7) * .08);
    obj.group.scale.setScalar((portrait ? large ? .8 : 1.03 : large ? 1 : 1.08) * narrow);
  }

  const particleCount = 500, particlePositions = new Float32Array(particleCount * 3), particleColors = new Float32Array(particleCount * 3);
  const particleLives = new Float32Array(particleCount), particleVelocity = new Float32Array(particleCount * 3);
  particlePositions.fill(-100);
  const particleGeo = new T.BufferGeometry(); particleGeo.setAttribute('position', new T.BufferAttribute(particlePositions, 3)); particleGeo.setAttribute('color', new T.BufferAttribute(particleColors, 3));
  const particles = new T.Points(particleGeo, new T.PointsMaterial({ size: .055, vertexColors: true, transparent: true, opacity: .9, depthWrite: false })); scene.add(particles);
  let particleCursor = 0;
  function burst(position) {
    for (let i = 0; i < 64; i++) {
      const at = particleCursor++ % particleCount, k = at * 3;
      particlePositions[k] = position.x + (Math.random() - .5) * .7;
      particlePositions[k + 1] = position.y + (Math.random() - .5) * .5;
      particlePositions[k + 2] = position.z + (Math.random() - .5) * .7;
      particleVelocity[k] = (Math.random() - .5) * 1.8; particleVelocity[k + 1] = .5 + Math.random() * 1.6; particleVelocity[k + 2] = (Math.random() - .5) * 1.8;
      particleLives[at] = .65 + Math.random() * 1.1;
      const ash = i % 4 === 0;
      particleColors[k] = ash ? .3 : 1; particleColors[k + 1] = ash ? .28 : .28 + Math.random() * .45; particleColors[k + 2] = ash ? .24 : .06;
    }
  }
  // Floating dust catches the warm room light.
  const dustPositions = new Float32Array(180 * 3);
  for (let i = 0; i < 180; i++) { dustPositions[i * 3] = Math.sin(i * 12.18) * 10; dustPositions[i * 3 + 1] = (i * .391) % 8 + 1; dustPositions[i * 3 + 2] = Math.cos(i * 5.76) * 10; }
  const dustGeo = new T.BufferGeometry(); dustGeo.setAttribute('position', new T.BufferAttribute(dustPositions, 3));
  const dust = new T.Points(dustGeo, new T.PointsMaterial({ color: '#e1c191', size: .023, transparent: true, opacity: .3, depthWrite: false })); scene.add(dust);

  function sync(next, events = []) {
    if (state && next !== state) {
      for (const obj of objects.values()) removeCard(obj);
      for (const obj of burnList) removeCard(obj);
      objects.clear(); burnList.length = 0; processedEvents.clear(); handHints.clear();
      particleLives.fill(0); particlePositions.fill(-100);
      yaw = 0; pitch = -.29; camera.rotation.set(pitch, yaw, 0);
      hovered = null; lastHoverKey = ''; onHover(null);
    }
    state = next;
    for (const obj of objects.values()) obj.active = false;
    // Keep consumed physical cards in the room until their burn animation finishes.
    for (const ev of events) {
      if (ev.id != null && processedEvents.has(ev.id)) continue;
      if (ev.id != null) processedEvents.add(ev.id);
      if (ev.type === 'burn') for (const card of ev.cards || []) {
        const obj = objects.get(typeof card === 'string' ? card : card.id);
        if (obj && !obj.burning) {
          obj.burning = true; obj.burnStart = performance.now(); obj.burnScale = obj.group.scale.x; obj.group.visible = true; obj.number.visible = false; obj.front.material = obj.front.material.clone();
          obj.front.material.emissive = new T.Color('#f76518'); obj.front.material.emissiveIntensity = .9;
          burst(obj.group.position); burnList.push(obj); objects.delete(obj.card.id);
        }
      }
    }
    (state.center || []).forEach((card, i) => setCard(card, 'center', null, i, state.center.length));
    (state.players || []).forEach((player, p) => player.hand.forEach((card, i) => setCard(card, p === 0 ? 'hand' : 'opponentCard', p, i, player.hand.length)));
    for (const [id, obj] of objects) if (!obj.active) { removeCard(obj); objects.delete(id); }
  }
  function removeCard(obj) {
    scene.remove(obj.group);
    obj.number.material.dispose();
    for (const m of [obj.slab, obj.front, obj.reverse, obj.number]) {
      const i = hitObjects.indexOf(m); if (i >= 0) hitObjects.splice(i, 1);
    }
    obj.front.material.dispose();
  }
  function hitTest(usePointer = false) {
    scene.updateMatrixWorld(); camera.updateMatrixWorld();
    raycaster.setFromCamera(looking && !usePointer ? new T.Vector2(0, 0) : pointer, camera);
    const hits = raycaster.intersectObjects(hitObjects, false);
    for (const h of hits) {
      if (!h.object.visible) continue;
      const obj = h.object.userData.cardObj;
      if (obj) {
        if (obj.burning || !obj.active || !obj.group.visible) continue;
        return { kind: obj.kind, id: obj.card.id, player: obj.player };
      }
      if (h.object.userData.hit) return h.object.userData.hit;
    }
    return null;
  }
  let drag = null;
  function look(dx, dy) {
    yaw -= dx * .002; pitch = Math.max(-1.15, Math.min(.7, pitch - dy * .002)); camera.rotation.set(pitch, yaw, 0);
  }
  function move(ev) {
    const rect = canvas.getBoundingClientRect();
    pointer.set(((ev.clientX - rect.left) / rect.width) * 2 - 1, -((ev.clientY - rect.top) / rect.height) * 2 + 1);
    if (drag && !looking && ev.pointerType !== 'touch') {
      if (Math.hypot(ev.clientX - drag.x, ev.clientY - drag.y) > 4) drag.moved = true;
      if (drag.moved) look(ev.clientX - drag.lastX, ev.clientY - drag.lastY);
      drag.lastX = ev.clientX; drag.lastY = ev.clientY;
    }
  }
  function press(ev) {
    // main.mjs owns touch gestures and taps, avoiding synthetic double flips.
    if (ev.pointerType === 'touch') { move(ev); return; }
    if (ev.button !== 0 && ev.button !== 2) return;
    if (!looking) move(ev);
    if (!looking && ev.button === 0) {
      drag = { x: ev.clientX, y: ev.clientY, lastX: ev.clientX, lastY: ev.clientY, moved: false };
      canvas.setPointerCapture?.(ev.pointerId);
      return;
    }
    const hit = hitTest();
    if (hit) { ev.preventDefault(); onInteract(hit, ev.button); }
  }
  function release(ev) {
    if (!drag || ev.pointerType === 'touch' || ev.button !== 0) return;
    const click = !drag.moved; drag = null;
    if (canvas.hasPointerCapture?.(ev.pointerId)) canvas.releasePointerCapture(ev.pointerId);
    if (click) { move(ev); const hit = hitTest(); if (hit) { ev.preventDefault(); onInteract(hit, 0); } }
  }
  function cancelDrag() { drag = null; }
  const stopMenu = ev => ev.preventDefault();
  canvas.addEventListener('pointermove', move); canvas.addEventListener('pointerdown', press); canvas.addEventListener('pointerup', release); canvas.addEventListener('pointercancel', cancelDrag); canvas.addEventListener('contextmenu', stopMenu);

  function tick(time = performance.now()) {
    const ms = time;
    const seconds = ms / 1000, dt = Math.min(.05, Math.max(.001, previousTime ? (ms - previousTime) / 1000 : .016)); previousTime = ms;
    const realNow = Date.now();
    camera.updateMatrixWorld();
    const handDistance = 2.7;
    const handHalfHeight = Math.tan(camera.fov * Math.PI / 360) * handDistance;
    const handWidth = handHalfHeight * camera.aspect * 2;
    const handScale = camera.aspect < .8 ? .38 : .52;
    const footerSpace = 58 * (handHalfHeight * 2) / Math.max(1, canvas.clientHeight);
    for (const obj of objects.values()) {
      let faceUp = false;
      if (obj.kind === 'center') {
        faceUp = state?.mode === 'lantern' || obj.card.faceUntil > realNow || (state?.revealUntil || 0) > realNow;
        obj.rotation.x = faceUp ? -Math.PI / 2 + .3 : Math.PI / 2 + .16;
      }
      const pending = state?.pendingGrab?.cardId === obj.card.id;
      const hover = hovered?.id === obj.card.id && hovered?.kind === obj.kind;
      const handChoice = obj.kind === 'hand' && !!state?.pendingGrab;
      const hint = obj.kind === 'hand' && handHints.has(obj.card.id);
      obj.outline.visible = pending || hover || faceUp || handChoice || hint;
      obj.outline.material = hint && !hover && !handChoice ? hintOutline : goldOutline;
      const bob = Math.sin(seconds * 1.4 + obj.index * 1.7) * .032;
      let handRotation;
      if (obj.kind === 'hand') {
        const offset = obj.index - (obj.count - 1) / 2;
        const gap = Math.max(.08, Math.min(handScale * .68, (handWidth * .92 - .73 * handScale) / Math.max(1, obj.count - 1)));
        const local = new T.Vector3(offset * gap, -handHalfHeight + .845 * handScale + footerSpace + .07 + (1 - Math.abs(offset) / 4) * .035, -handDistance + Math.abs(offset) * .016);
        obj.target.copy(local.applyQuaternion(camera.quaternion).add(camera.position));
        obj.group.scale.setScalar(handScale);
        handRotation = camera.quaternion.clone().multiply(new T.Quaternion().setFromEuler(new T.Euler(-.12, offset * -.012, offset * -.045)));
      }
      const target = obj.target.clone(); target.y += bob + (pending ? .35 : faceUp ? .15 : hover ? .08 : 0);
      const age = (performance.now() - obj.born) / 1000;
      obj.group.visible = true;
      const factor = 1 - Math.exp(-dt * (age < 1.5 ? 3.3 : 8));
      obj.group.position.lerp(target, factor);
      if (handRotation) obj.group.quaternion.slerp(handRotation, 1 - Math.exp(-dt * 14));
      else for (const axis of ['x', 'y', 'z']) obj.group.rotation[axis] += (obj.rotation[axis] - obj.group.rotation[axis]) * (1 - Math.exp(-dt * 11));
    }
    for (let i = burnList.length - 1; i >= 0; i--) {
      const obj = burnList[i], age = (performance.now() - obj.burnStart) / 1000;
      obj.group.position.y += dt * .45; obj.group.rotation.z += dt * .6;
      obj.group.scale.setScalar(Math.max(.001, (obj.burnScale || 1) * (1 - age / 1.05)));
      obj.front.material.emissiveIntensity = 1 + Math.sin(age * 30) * .5;
      if (age > 1.1) { removeCard(obj); burnList.splice(i, 1); }
    }
    for (let i = 0; i < particleCount; i++) {
      if (particleLives[i] <= 0) continue;
      const k = i * 3; particleLives[i] -= dt;
      particlePositions[k] += particleVelocity[k] * dt; particlePositions[k + 1] += particleVelocity[k + 1] * dt; particlePositions[k + 2] += particleVelocity[k + 2] * dt;
      particleVelocity[k + 1] -= dt * .4;
      if (particleLives[i] <= 0) particlePositions[k + 1] = -100;
    }
    particleGeo.attributes.position.needsUpdate = true; particleGeo.attributes.color.needsUpdate = true;
    for (const c of creatures) {
      c.body.position.y = Math.sin(seconds * 1.25 + c.phase) * .045;
      c.head.rotation.y = Math.sin(seconds * .4 + c.phase) * .1;
      c.head.rotation.z = Math.sin(seconds * .65 + c.phase) * .025;
    }
    for (const c of candles) { const flicker = Math.sin(seconds * 9 + c.seed) * .07 + Math.sin(seconds * 17 + c.seed) * .035; c.flame.scale.y = .18 * (1 + flicker); c.light.intensity = 11 + flicker * 15; }
    dust.rotation.y = seconds * .007; dust.position.y = Math.sin(seconds * .09) * .15;
    scene.updateMatrixWorld(); camera.updateMatrixWorld();
    hovered = hitTest();
    const hoverKey = hovered ? `${hovered.kind}:${hovered.player}:${hovered.id}` : '';
    if (hoverKey !== lastHoverKey) { lastHoverKey = hoverKey; onHover(hovered); }
    renderer.render(scene, camera);
  }
  function resize() {
    const width = Math.max(1, canvas.clientWidth || window.innerWidth), height = Math.max(1, canvas.clientHeight || window.innerHeight);
    renderer.setSize(width, height, false); camera.aspect = width / height;
    camera.fov = camera.aspect < 1 ? 76 : 61; camera.updateProjectionMatrix();
    for (const obj of objects.values()) if (obj.kind === 'center') placeCenter(obj, obj.count);
  }
  resize();
  return {
    sync, tick, resize,
    setHandHints(ids = []) { handHints = new Set(ids); },
    interactAt(x, y, button = 0) {
      const rect = canvas.getBoundingClientRect();
      pointer.set(((x - rect.left) / rect.width) * 2 - 1, -((y - rect.top) / rect.height) * 2 + 1);
      const hit = hitTest(true);
      if (hit) { onInteract(hit, button); return true; }
      return false;
    },
    setLooking(value) { looking = !!value; if (looking) drag = null; },
    look,
    dispose() {
      canvas.removeEventListener('pointermove', move); canvas.removeEventListener('pointerdown', press); canvas.removeEventListener('pointerup', release); canvas.removeEventListener('pointercancel', cancelDrag); canvas.removeEventListener('contextmenu', stopMenu);
      scene.traverse(m => { m.geometry?.dispose(); if (Array.isArray(m.material)) m.material.forEach(v => v.dispose()); else m.material?.dispose(); });
      textures.forEach(tx => tx.dispose()); numberTextures.forEach(tx => tx.dispose()); grain.dispose(); renderer.dispose();
    }
  };
}
