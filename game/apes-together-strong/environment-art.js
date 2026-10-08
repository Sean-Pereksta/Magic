/* Original atlas artwork layered onto the existing isometric renderer.
 * World geometry, navigation, sight, saves and simulation are never modified.
 * Leaf renderer hooks retain prison locks, siege platforms and selection cues.
 */
(() => {
  'use strict';
  const P = ATSRenderer.prototype;
  const base = {};
  for (const name of ['draw', 'drawObject', 'drawGroundChunk', 'drawRock', 'drawBerry', 'drawBuilding', 'drawTower', 'drawCage', 'drawRubble', 'drawWall', 'drawWallFaces', 'drawGate', 'drawLights', 'drawEffects', 'drawAtmosphere', 'drawMenu', 'drawAlarm', 'drawRadio', 'drawFuel']) base[name] = P[name];
  const TAU = Math.PI * 2;
  const replacedEffects = new Set(['muzzle', 'hit', 'smoke', 'smash', 'dust']);
  const artworkEffects = new Set(['muzzle', 'hit', 'smoke', 'smash', 'explosion', 'fire', 'blast', 'dust']);
  const cageFloor = [[24, 375], [322, 320], [472, 403], [173, 450]];
  const cageInterior = [[28, 97], [320, 55], [463, 111], [463, 402], [173, 446], [28, 375]];
  const cageFronts = [ [[10, 77], [177, 160], [177, 460], [10, 386]], [[173, 155], [486, 93], [486, 421], [173, 460]] ];
  const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
  function hash(a, b = 0) { let n = Math.imul((a | 0) ^ 71293, 374761393) ^ Math.imul((b | 0) + 19, 668265263); n = Math.imul(n ^ n >>> 13, 1274126177); return ((n ^ n >>> 16) >>> 0) / 4294967295; }
  function seed(id) { let n = 0; for (const c of String(id || '')) n = (Math.imul(n, 31) + c.charCodeAt(0)) | 0; return n; }
  function ellipse(c, x, y, w, h, color) { c.beginPath(); c.ellipse(x, y, w, h, 0, 0, TAU); c.fillStyle = color; c.fill(); }
  function line(c, x, y, u, v, color, width = 1) { c.beginPath(); c.moveTo(x, y); c.lineTo(u, v); c.strokeStyle = color; c.lineWidth = width; c.stroke(); }
  function polygon(c, points) { c.beginPath(); points.forEach((p, i) => i ? c.lineTo(p[0], p[1]) : c.moveTo(p[0], p[1])); c.closePath(); }
  function frame(name) { return window.ATSVisualAssets?.manifest?.environment?.frames?.[name]; }
  function image(id) { return window.ATSVisualAssets?.get?.(id) || window.ATSVisualAssets?.images?.[id]; }
  function available(name) { const f = frame(name); return f && image(f.atlas); }
  function art(c, name, width, height = width, x = 0, y = 0) {
    const f = frame(name), im = f && image(f.atlas);
    if (!im) return false;
    const [sx, sy, sw, sh] = f.rect, [ax, ay] = f.anchor;
    c.drawImage(im, sx, sy, sw, sh, x - ax / sw * width, y - ay / sh * height, width, height);
    return true;
  }
  // Mapping the material onto two existing face vectors keeps long walls and
  // narrow gates exactly aligned with the rectangles used by navigation.
  function material(c, name, origin, along, down, opacity = 1) {
    const f = frame(name), im = f && image(f.atlas);
    if (!im) return;
    c.save(); c.globalAlpha *= opacity;
    c.transform(along[0] / 512, along[1] / 512, down[0] / 512, down[1] / 512, origin[0], origin[1]);
    c.drawImage(im, ...f.rect, 0, 0, 512, 512); c.restore();
  }
  function profile(r) { return r.graphicsProfile || { foliage: 2, atmosphere: 2, shadows: 2, particleBudget: 280 }; }
  const iso = (x,y,z=0) => [(x-y)*.8,(x+y)*.42-z];
  function wallDimensions(o) {
    const w=o.w||70,h=o.h||14,axis=w>=h?'x':'y',tier=clamp(Math.round(o.wallTier||1),1,4);
    return {w,h,axis,tier,length:Math.max(w,h),thickness:Math.min(w,h),height:clamp(o.visualHeight||o.height||[35,35,52,76,104][tier],18,140)};
  }
  function closedDefense(o) { return o && (o.type==='wall'||o.type==='gate') && !o.dead && o.hp!==0 && o.solid!==false && !(o.type==='gate'&&(o.gateState==='open'||o.gateState==='destroyed'||o.forcedOpen)); }
  function wallLayout(o, neighbors=[]) {
    const d=wallDimensions(o),x=o.x||0,y=o.y||0,left=x-d.w/2,right=x+d.w/2,top=y-d.h/2,bottom=y+d.h/2;
    const joints=[];
    for(const side of [-1,1]) {
      const ex=x+(d.axis==='x'?side*d.length/2:0),ey=y+(d.axis==='y'?side*d.length/2:0);
      let joined=null;
      for(const n of neighbors) {
        if(n===o||n.id&&n.id===o.id||!closedDefense(n))continue;
        const nd=wallDimensions(n),nx=n.x||0,ny=n.y||0;
        // At most the one-unit overlap/tolerance used by existing blueprints.
        // Real service lanes and destroyed/open gates never get a bridge.
        if(ex<nx-nd.w/2-1||ex>nx+nd.w/2+1||ey<ny-nd.h/2-1||ey>ny+nd.h/2+1)continue;
        const candidate={object:n,dimensions:nd};
        if(!joined||nd.axis!==d.axis)joined=candidate;
      }
      const postSize=Math.min(12,d.thickness),j={side,kind:joined?(joined.dimensions.axis===d.axis?'straight':'corner'):'end',connected:!!joined,neighborId:joined?.object.id||null,drawPost:!joined,x:ex-x,y:ey-y,w:d.axis==='x'?Math.min(8,d.length):postSize,h:d.axis==='y'?Math.min(8,d.length):postSize};
      if(!joined){if(d.axis==='x')j.x-=side*j.w/2;else j.y-=side*j.h/2;}
      else if(j.kind==='corner') {
        const n=joined.object,nd=joined.dimensions,ix0=Math.max(left,n.x-nd.w/2),ix1=Math.min(right,n.x+nd.w/2),iy0=Math.max(top,n.y-nd.h/2),iy1=Math.min(bottom,n.y+nd.h/2);
        // The corner cap stays strictly within the actual shared footprint.
        j.x=(ix0+ix1)/2-x;j.y=(iy0+iy1)/2-y;j.w=Math.min(postSize,Math.max(0,ix1-ix0));j.h=Math.min(postSize,Math.max(0,iy1-iy0));
        const atNeighborEnd=Math.abs(Math.abs((nd.axis==='x'?ex-n.x:ey-n.y))-nd.length/2)<=d.thickness;
        j.drawPost=j.w>=2&&j.h>=2&&(!atNeighborEnd||String(o.id||'')<String(n.id||''));
      }
      joints.push(j);
    }
    return {...d,bounds:{left,right,top,bottom},joints};
  }
  function gateLayout(o) {
    const d=wallDimensions({...o,w:o.w||90,h:o.h||14}),open=!!(o.dead||o.forcedOpen||o.gateState==='open'||o.gateState==='destroyed');
    return {...d,open,posts:[-1,1].map(side=>({x:d.axis==='x'?side*(d.length/2+2):0,y:d.axis==='y'?side*(d.length/2+2):0})),opening:d.length};
  }
  // Texture coordinates follow the world axis in fixed-size material units.
  // Adjacent pieces no longer restart/stretch all the logs or stone blocks.
  function wallMaterial(c,name,from,to,start,length,height,opacity=.87) {
    const f=frame(name),im=f&&image(f.atlas);if(!im||length<=0||height<=0)return;
    const unit=64;
    c.save();c.globalAlpha*=opacity;c.transform((to[0]-from[0])/length,(to[1]-from[1])/length,0,1,from[0],from[1]-height);
    c.beginPath();c.rect(0,0,length,height);c.clip();
    const first=Math.floor(start/unit)*unit-start,top=height-Math.ceil(height/unit)*unit;
    for(let u=first;u<length;u+=unit)for(let v=top;v<height;v+=unit)c.drawImage(im,...f.rect,u,v,unit,unit);
    c.restore();
  }
  function defensePost(c,j,height,tier) {
    const hx=j.w/2,hy=j.h/2,point=(x,y,z=0)=>iso(x+j.x,y+j.y,z);
    const a=point(-hx,-hy,height),b=point(hx,-hy,height),e=point(hx,hy,height),d=point(-hx,hy,height),eb=point(hx,hy),db=point(-hx,hy),bb=point(hx,-hy);
    polygon(c,[db,eb,e,d]);c.fillStyle=tier>=3?'#737e75':'#837e5d';c.fill();
    polygon(c,[eb,bb,b,e]);c.fillStyle=tier>=3?'#445c58':'#4a5945';c.fill();
    polygon(c,[a,b,e,d]);c.fillStyle=tier>=3?'#b1b9a5':'#b9aa78';c.fill();
    line(c,db[0],db[1]-height*.75,eb[0],eb[1]-height*.75,tier>=3?'#a0aca0':'#444d41',2);
    line(c,db[0],db[1]-8,eb[0],eb[1]-8,tier>=3?'#9aa899':'#434f41',2);
  }
  function cageLayout(o) {
    const layers=window.ATSVisualAssets?.manifest?.environment?.cageLayers;
    const floor=layers?.floor||cageFloor,interior=layers?.interior||cageInterior,fronts=layers?.fronts||cageFronts;
    const width = clamp((o.w || 62) * 1.4, 72, 152), height = width * .77;
    const count = clamp(Math.floor(Number(o.count ?? o.prisoners ?? 3) || 0), 0, 4);
    const point = p => [(p[0] - 256) * width / 512, 9 + (p[1] - 390) * height / 512];
    // Bilinear positions sit on the painted cage floor, with near occupants
    // painted last. Their feet cannot drift onto the exterior ground plane.
    const slots = count === 1 ? [[.5,.58]] : count === 2 ? [[.31,.5],[.68,.59]] : count === 3 ? [[.28,.35],[.66,.38],[.48,.76]] : [[.27,.28],[.68,.29],[.30,.74],[.72,.73]];
    const occupants = count ? slots.map(([u,v]) => {
      const a = floor[0], b = floor[1], d = floor[3], e = floor[2];
      const x = (a[0] + (b[0] - a[0]) * u) * (1-v) + (d[0] + (e[0] - d[0]) * u) * v;
      const y = (a[1] + (b[1] - a[1]) * u) * (1-v) + (d[1] + (e[1] - d[1]) * u) * v;
      const p = point([x,y]); return { x:p[0], y:p[1], scale:width/335 };
    }).sort((a,b) => a.y-b.y) : [];
    return {width,height,count,floor:floor.map(point),interior:interior.map(point),fronts:fronts.map(face=>face.map(point)),occupants,lock:point([277,298]),badge:point([288,58])};
  }
  function init(r) {
    if (r._environmentArt) return r._environmentArt;
    const pool = Array.from({ length: 96 }, () => ({ life: 0 }));
    return r._environmentArt = { pool, cursor: 0, observations: new WeakMap(), cageLayouts: new WeakMap(), wallLayouts:new WeakMap(),wallLookupBudget:16,wallTextures:new Map(),wallTexturePixels:0,wallTextureBudget:4,focus: [], legacyEffects: [], world: null, menuReady: false, active: 0 };
  }
  P.draw = function (g, dt) {
    const v = init(this);
    if (v.world !== g.world) {
      v.world = g.world; v.observations = new WeakMap(); v.cageLayouts = new WeakMap();v.wallLayouts=new WeakMap();
      for (const particle of v.pool) particle.life = 0;
    }
    v.game = g; v.wallLookupBudget=16;v.wallTextureBudget=4;v.focus.length = 0; v.focus.push(g.king);
    // King plus twelve selected units use the actual atlas bounds. Clamp alpha
    // rather than multiplying the old King fade a second time.
    if (g.siege?.selected?.length) {
      for (const a of g.apes || []) {
        if (a.hp > 0 && g.siege.selected.includes(a.species) && (!g.siege.eligible || g.siege.eligible(a)) && (!g.siege.unitIds || g.siege.unitIds.includes(a.id))) v.focus.push(a);
        if (v.focus.length >= 13) break;
      }
    }
    return base.draw.call(this, g, dt);
  };
  P.drawObject = function (c, o) {
    const v = init(this);
    if (Number.isFinite(o.hp) && !['tree', 'berry'].includes(o.type)) {
      const old = v.observations.get(o);
      if (old && ((!old.dead && o.dead) || o.hp < old.hp)) this.emitStructureArtwork(o, !old.dead && o.dead);
      if (old) { old.hp = o.hp; old.dead = !!o.dead; }
      else v.observations.set(o, { hp: o.hp, dead: !!o.dead });
    }
    if (!o.dead && available('generator') && (o.type === 'generator' || o.type === 'prisonControl' || o.type === 'gateControl' && o.discovered)) {
      const power = o.type === 'generator';
      ellipse(c, 0, 3, power ? 29 : 12, 9, 'rgba(0,8,11,.32)');
      art(c, power ? 'generator' : 'control', power ? 77 : 47, power ? 68 : 57, 0, 5);
      if (o.hp > 0 && o.maxHp && o.hp < o.maxHp) this.health(c, o.hp / o.maxHp, power ? -67 : -57, 32, '#d7ad6a');
      return;
    }
    if (o.type === 'cage' && !o.dead && available('cage')) {
      this.drawCage(c, o);
      const layout = this.cageArtworkLayout(o);
      if (o.hp > 0 && o.maxHp && o.hp < o.maxHp) this.health(c, o.hp/o.maxHp, -layout.height*.77, 34, '#d7ae6b');
      if (o.prisonKind) {
        const locked = o.prisonLock !== 'chain' && !o.prisonUnlocked, p = layout.lock, color = locked ? '#e6a06e' : '#97cba0';
        c.save(); c.translate(p[0],p[1]); c.strokeStyle=color; c.lineWidth=1.3; c.beginPath(); c.arc(0,-2,2.2,Math.PI,TAU); c.stroke(); c.fillStyle=color; c.fillRect(-2.8,-2,5.6,5); c.restore();
        if (o.prisonReward === 'champion' || o.prisonReward === 'injuredLegend') {
          const b=layout.badge; polygon(c,[[b[0],b[1]-10],[b[0]+4,b[1]-6],[b[0],b[1]-2],[b[0]-4,b[1]-6]]); c.fillStyle='#e8d18c';c.fill();
        }
      }
      return;
    }
    if (o.type !== 'tree' || o.dead || !available('oak')) return base.drawObject.call(this, c, o);
    const n = seed(o.id), variants = window.ATSVisualAssets.manifest.environment.treeVariants;
    const variant = variants[(n >>> 0) % variants.length], s = clamp((o.r || 27) / 30, .58, 1.22);
    const width = variant === 'sapling' ? 147 : variant === 'conifer' ? 153 : 188;
    const height = variant === 'sapling' ? 183 : variant === 'conifer' ? 236 : 220;
    c.save();
    for (const a of v.focus) {
      const dx = ((o.x - a.x) - (o.y - a.y)) * .8, dy = ((o.x - a.x) + (o.y - a.y)) * .42;
      if (Math.abs(dx) < width * s * .46 && dy > 0 && dy < height * s * .9) { c.globalAlpha = Math.min(c.globalAlpha, .34); break; }
    }
    if (profile(this).shadows) ellipse(c, 0, 4, 35 * s, 12 * s, 'rgba(0,8,10,.35)');
    // Canopies sway about their ground anchor. The maximum trunk displacement
    // stays below one pixel; the navigation radius never follows visual sway.
    c.save();
    if (!this.reducedMotion && profile(this).foliage && this.detailLevel < 3) c.rotate(Math.sin(this.time * .65 + hash(n) * TAU) * .006);
    art(c, variant, width * s, height * s); c.restore();
    if (profile(this).foliage > 0 && this.detailLevel < 3 && hash(n, 6) > .34) {
      c.globalAlpha *= .86;
      art(c, hash(n, 7) > .83 ? 'fallenLog' : hash(n, 7) > .5 ? 'fern' : 'litter', 35 + hash(n, 8) * 23, 29, 20 * s, 5);
    }
    c.restore();
  };
  P.drawGroundChunk = function (c, world, bx, by, size, tiles, decoration) {
    base.drawGroundChunk.call(this, c, world, bx, by, size, tiles, decoration);
    if (!available('soil')) return;
    const span = size * tiles, cx = (bx + .5) * span, cy = (by + .5) * span;
    c.save(); polygon(c, [[0, -span * .42], [span * .8, 0], [0, span * .42], [-span * .8, 0]]); c.clip();
    for (let ix = bx * tiles; ix < bx * tiles + tiles; ix++) for (let iy = by * tiles; iy < by * tiles + tiles; iy++) {
      const x = (ix + .5) * size, y = (iy + .5) * size, px = ((x - cx) - (y - cy)) * .8, py = ((x - cx) + (y - cy)) * .42;
      const t = this.terrain(world, x, y), n = hash(ix, iy);
      if (t.bridge) continue;
      const name = t.water ? 'water' : t.road || t.biome === 'rocky' || t.biome === 'ruins' ? 'road' : t.biome === 'farmland' ? 'meadow' : 'soil';
      material(c, name, [px, py - size * .42], [size * .8, size * .42], [-size * .8, size * .42], t.water ? .42 : t.road ? .48 : .61);
      // A second independent organic decal obscures the tile rhythm. These
      // are floor details, not world objects or hidden movement blockers.
      if (!t.water && !t.road && profile(this).foliage && n > .48) {
        c.save(); c.globalAlpha *= .30 + n * .24;
        const xx = px + (hash(ix, iy + 14) - .5) * 42, yy = py + (hash(ix + 11, iy) - .5) * 18;
        art(c, n > .85 ? 'grass' : 'litter', 30 + n * 27, n > .85 ? 19 : 22, xx, yy);
        c.restore();
      }
    }
    c.restore();
  };
  P.drawRock = function (c, o) {
    if (!available('mossRock')) return base.drawRock.call(this, c, o);
    const s = clamp((o.r || 25) / 24, .7, 1.8); ellipse(c, 0, 3, 25 * s, 10 * s, 'rgba(0,8,11,.32)');
    art(c, 'mossRock', 69 * s, 61 * s, 0, 7);
  };
  P.drawBerry = function (c, o) {
    if (!available('bramble')) return base.drawBerry.call(this, c, o);
    c.save(); if (o.amount === 0) c.globalAlpha *= .53;
    ellipse(c, 0, 2, 22, 8, 'rgba(0,8,11,.25)');
    if (!this.reducedMotion && profile(this).foliage > 1) c.rotate(Math.sin(this.time * 1.1 + hash(seed(o.id)) * 6) * .016);
    art(c, 'bramble', 59, 51); c.restore();
  };
  P.drawBuilding = function (c, o, kind) {
    if (!available('guardhouse')) return base.drawBuilding.call(this, c, o, kind);
    const name = kind === 'barracks' ? 'tent' : kind === 'depot' ? 'depot' : 'guardhouse';
    const width = kind === 'depot' ? 170 : kind === 'barracks' ? 150 : 116;
    ellipse(c, 0, 9, width * .44, width * .16, 'rgba(0,7,12,.4)');
    art(c, name, width, kind === 'house' ? 119 : width * .82, 0, 13);
    if (o.powered !== false) this.glow(c, kind === 'barracks' ? 23 : 12, -17, 30, '229,180,90', .11);
    if (kind === 'barracks' && profile(this).foliage) { art(c, 'campfire', 26, 29, -width * .40, 19); this.glow(c, -width * .40, 9, 20, '243,168,78', .12); }
    this.drawArtworkDamage(c, o, width * .6, kind === 'house' ? 62 : 51);
  };
  P.drawTower = function (c, o) {
    if (!available('watchtower')) return base.drawTower.call(this, c, o);
    const h = clamp(o.height || 116, 80, 155), width = h * .83;
    ellipse(c, 0, 4, width * .27, 12, 'rgba(0,6,9,.36)'); art(c, 'watchtower', width, h + 28, 0, 4);
    if (o.powered !== false) this.glow(c, 2, -h + 4, 23, '247,220,157', .36);
    this.drawArtworkDamage(c, o, 36, h);
  };
  P.drawCage = function (c, o) {
    if (!available('cage')) return base.drawCage.call(this, c, o);
    const layout = this.cageArtworkLayout(o), {width,height} = layout;
    ellipse(c, 0, 5, width * .48, 18, 'rgba(0,7,12,.4)');
    // Back frame/floor, confined occupants, then only the near bars and door.
    // Repainting the whole cage after occupants would also paint rear bars on
    // top of faces and make otherwise correctly placed captives disappear.
    art(c, 'cage', width, height, 0, 9);
    c.save(); polygon(c,layout.interior); c.clip();
    for (let i = 0; i < layout.occupants.length; i++) {
      const p = layout.occupants[i];
      this.drawApeMini(c,p.x,p.y,p.scale,{id:o.id+'-captive-'+i,species:o.captiveSpecies || (o.prisonReward === 'champion' ? 'gorilla' : ['chimpanzee','orangutan','gorilla'][i%3]),coatVariant:i%3,dir:.25,state:'captive'});
    }
    c.restore();
    c.save();c.beginPath();
    for(const face of layout.fronts) {c.moveTo(...face[0]);for(let i=1;i<face.length;i++)c.lineTo(...face[i]);c.closePath();}
    c.clip();art(c,'cage',width,height,0,9);c.restore();
    this.drawArtworkDamage(c, o, width * .64, height * .54);
  };
  P.cageArtworkLayout = function(o) {
    const v=init(this),width=clamp((o.w||62)*1.4,72,152),count=clamp(Math.floor(Number(o.count??o.prisoners??3)||0),0,4);
    let layout=v.cageLayouts.get(o);
    if(!layout||layout.width!==width||layout.count!==count){layout=cageLayout(o);v.cageLayouts.set(o,layout)}
    return layout;
  };
  P.drawRubble = function (c, o) {
    if (!available('rubble')) return base.drawRubble.call(this, c, o);
    // Destroyed openings show only low debris, never the original solid face.
    const width = o.type === 'gate' || o.type === 'wall' ? clamp(Math.max(o.w || 70, o.h || 14) * .74, 42, 115) : 66;
    art(c, 'rubble', width, Math.min(40, width * .46), 0, 5);
  };
  P.drawAlarm = function (c, o) {
    if (!available('alarm')) return base.drawAlarm.call(this, c, o);
    const active = o.active || o.alert || o.alarming;
    ellipse(c, 0, 2, 11, 5, 'rgba(0,8,11,.3)'); art(c, 'alarm', 64, 69, 0, 3);
    const alpha = active ? this.reducedMotion ? .52 : .40 + (1 + Math.sin(this.time * 6)) * .15 : .045;
    this.glow(c, 0, -54, active ? 45 : 15, '255,72,44', alpha);
  };
  P.drawRadio = function (c, o) {
    if (!available('radio')) return base.drawRadio.call(this, c, o);
    ellipse(c, 0, 3, 19, 7, 'rgba(0,8,11,.3)'); art(c, 'radio', 119, 135, 0, 5);
    if (this.reducedMotion || Math.sin(this.time * 2.5) > .5) this.glow(c, 0, -115, 12, '248,99,64', .28);
  };
  P.drawFuel = function (c, o) {
    if (!available('barrels')) return base.drawFuel.call(this, c, o);
    ellipse(c, 0, 2, 25, 8, 'rgba(0,8,11,.3)'); art(c, 'barrels', 61, 52, 0, 6);
  };
  P.drawArtworkDamage = function (c, o, width, height) {
    if (!o.maxHp || o.hp >= o.maxHp * .85 || o.dead) return;
    const ratio = o.hp / o.maxHp, count = ratio < .3 ? 5 : ratio < .6 ? 3 : 1, n = seed(o.id);
    c.save(); c.lineJoin = 'miter';
    for (let i = 0; i < count; i++) {
      const x = (hash(n, i) - .5) * width * .85, y = -height * (.25 + hash(n, i + 12) * .6);
      c.beginPath(); c.moveTo(x - 3, y - 10); c.lineTo(x + 2, y); c.lineTo(x - 2, y + 5); c.lineTo(x + 3, y + 13); c.strokeStyle = 'rgba(5,13,15,.85)'; c.lineWidth = ratio < .3 ? 2.7 : 1.4; c.stroke();
      line(c, x + 3, y, x + 8, y - 4, 'rgba(186,159,119,.56)', .8);
    }
    c.restore();
  };
  P.drawWallFaces = function (c, o) {
    base.drawWallFaces.call(this, c, o);
  };
  P.wallArtworkLayout = function(o) {
    const v=init(this),world=v.world||v.game?.world;
    const revision=(world?.navRevision||0)+':'+(world?.chunkRevision||0),old=v.wallLayouts.get(o);
    if(old&&old.revision===revision)return old.layout;
    if(v.wallLookupBudget<=0)return old?.layout||wallLayout(o);
    v.wallLookupBudget--;
    const nearby=world?.getObjects?world.getObjects(o.x,o.y,Math.max(o.w||70,o.h||14)/2+40):[];
    const layout=wallLayout(o,nearby);v.wallLayouts.set(o,{revision,layout});return layout;
  };
  P.drawWall = function(c,o) {
    base.drawWall.call(this,c,o);
    if(!available('timber')||o.dead||o.hp<=0)return;
    const layout=this.wallArtworkLayout(o);
    const hx = (o.w || 70) / 2, hy = (o.h || 14) / 2, tier = clamp(Math.round(o.wallTier || 1), 1, 4);
    const h = clamp(o.visualHeight || o.height || [35, 35, 52, 76, 104][tier], 18, 140);
    const d = [(-hx - hy) * .8, (-hx + hy) * .42], e = [(hx - hy) * .8, (hx + hy) * .42], b = [(hx + hy) * .8, (hx - hy) * .42];
    const drawFaces=ctx=>{for (const [a, z, start, length] of [[d,e,(o.x||0)-hx,hx*2],[e,b,-((o.y||0)+hy),hy*2]]) {
      wallMaterial(ctx,tier>=3?'stone':'timber',[a[0],a[1]-2],[z[0],z[1]-2],start,length,h-7);
      line(ctx, a[0], a[1] - h + 3, z[0], z[1] - h + 3, tier >= 3 ? '#acb8ad' : '#8e9279', 2);
      if (tier < 3) line(ctx, a[0], a[1] - 10, z[0], z[1] - 10, '#6f7563', 3);
    }};
    const v=init(this),phase=n=>((n%64)+64)%64,key=[hx,hy,h,tier,phase((o.x||0)-hx),phase(-((o.y||0)+hy))].join(':');
    let texture=v.wallTextures.get(key);
    if(!texture&&v.wallTextureBudget>0){
      const w=Math.ceil((hx+hy)*1.6+8),height=Math.ceil(h+(hx+hy)*.84+8),pixels=w*height;
      if(pixels<=200000){
        v.wallTextureBudget--;const canvas=document.createElement('canvas');canvas.width=w;canvas.height=height;const sc=canvas.getContext('2d'),x=w/2,y=h+(hx+hy)*.42+4;sc.translate(x,y);drawFaces(sc);
        while(v.wallTextures.size>=128||v.wallTexturePixels+pixels>2000000){const oldest=v.wallTextures.keys().next().value;if(oldest===undefined)break;v.wallTexturePixels-=v.wallTextures.get(oldest).pixels;v.wallTextures.delete(oldest)}
        texture={canvas,x,y,pixels};v.wallTextures.set(key,texture);v.wallTexturePixels+=pixels;
      }
    }
    if(texture)c.drawImage(texture.canvas,-texture.x,-texture.y);else drawFaces(c);
    for(const j of layout.joints)if(j.drawPost)defensePost(c,j,h+(j.kind==='corner'?4:2),tier);
    this.drawArtworkDamage(c, o, (hx + hy) * 1.35, h);
    // Siege's walkable top remains above the face texture. Restore its vine
    // access marks in front so the climbing affordance is never painted over.
    if(o.walkable&&o.climbAccess){const height=o.walkHeight||h,side=(o.w||0)<(o.h||0)?1:-1;for(let i=0;i<3;i++){const x=(i-1)*7;line(c,x,0,x+side*5,-height,'#769368',2);for(let y=15;y<height;y+=18)line(c,x,-y,x+6,-y-6,'#95b777',2)}}
  };
  P.drawGate = function (c, o) {
    const layout=gateLayout(o);
    if(layout.open&&available('timber')) {
      if(o.dead||o.gateState==='destroyed')this.drawRubble(c,o);
      for(const p of layout.posts)defensePost(c,{...p,w:5,h:5},o.dead||o.gateState==='destroyed'?12:layout.height+7,layout.tier);
      return;
    }
    base.drawGate.call(this, c, o);
    if (o.dead || o.gateState === 'open' || o.forcedOpen || !available('timber')) return;
    const length = Math.max(o.w || 90, o.h || 14), vertical = (o.h || 14) > (o.w || 90), dx = length * .4 * (vertical ? -1 : 1), dy = length * .21;
    const tier = clamp(o.wallTier || 1, 1, 4), h = clamp(o.visualHeight || o.height || [44, 44, 58, 80, 108][tier], 18, 140);
    wallMaterial(c,tier>=3?'stone':'timber',[-dx,-dy-4],[dx,dy-4],(vertical?(o.y||0):(o.x||0))-length/2,length,h-10,.84);
    for (const lift of [h - 6, 12]) line(c, -dx, -dy - lift, dx, dy - lift, '#b0aa88', tier >= 3 ? 5 : 3);
    line(c, -dx, -dy - h + 8, dx, dy - 8, '#7c8676', 3); line(c, dx, dy - h + 8, -dx, -dy - 8, '#7c8676', 3);
    c.fillStyle = '#c0a46b'; c.fillRect(-4, -h * .49, 8, 10);
    this.drawArtworkDamage(c, o, Math.abs(dx) * 1.8, h);
  };
  P.emitStructureArtwork = function (o, collapse) {
    if (this.reducedMotion || !available('debris')) return;
    const v = init(this), budget = Math.min(v.pool.length, Math.floor(profile(this).particleBudget / 4));
    const count = Math.min(collapse ? 9 : 2, budget), n = seed(o.id) + Math.floor(this.time * 10);
    for (let i = 0; i < count; i++) {
      const p = v.pool[v.cursor++ % budget], a = hash(n, i) * TAU;
      p.x = o.x; p.y = o.y; p.vx = Math.cos(a) * (collapse ? 46 : 19); p.vy = Math.sin(a) * (collapse ? 46 : 19);
      p.born = this.time; p.life = collapse ? .8 + hash(n, i + 7) * .3 : .4;
      p.height = 8 + hash(n, i + 17) * (o.height || 40) * .45;
      p.kind = i % 3 ? 'debris' : 'dust'; p.size = collapse ? 18 + hash(n, i + 9) * 19 : 14;
      p.angle = a;
    }
  };
  P.drawEffects = function (c, effects) {
    if (!available('smoke')) return base.drawEffects.call(this, c, effects);
    const v = init(this), budget = Math.min(96, Math.floor(profile(this).particleBudget / 4));
    v.legacyEffects.length = 0;
    for (const e of effects) if (!replacedEffects.has(e.type)) v.legacyEffects.push(e);
    base.drawEffects.call(this, c, v.legacyEffects);
    let drawn = 0;
    c.save();
    for (const e of effects) {
      if (drawn >= budget || !artworkEffects.has(e.type)) continue;
      const p = this.project(e.x, e.y, e.z ?? (e.type === 'muzzle' ? 22 : e.type === 'hit' ? 16 : 3)), t = clamp(1 - e.life / (e.maxLife || 1), 0, 1);
      if (!this.visible(p, 90 * this.camera.zoom)) continue;
      const name = e.type === 'hit' ? 'sparks' : e.type === 'smash' ? 'debris' : ['blast', 'explosion'].includes(e.type) ? 'fire' : e.type;
      const size = e.type === 'muzzle' ? 39 : e.type === 'hit' ? 35 : e.type === 'smoke' ? 44 + t * 46 : 55 + t * 22;
      c.save(); c.translate(p.x, p.y); c.scale(this.camera.zoom, this.camera.zoom); c.globalAlpha *= (1 - t) * (e.type === 'smoke' ? .55 : .85);
      if (name === 'sparks' || name === 'muzzle' || name === 'fire') c.globalCompositeOperation = 'screen';
      if (e.type === 'muzzle') { const dir = e.dir ?? 0; c.rotate(Math.atan2((Math.cos(dir) + Math.sin(dir)) * .42, (Math.cos(dir) - Math.sin(dir)) * .8)); }
      if (name === 'smoke') c.translate(this.reducedMotion ? 0 : t * 8, -t * 27);
      art(c, name, size, size); c.restore(); drawn++;
    }
    for (const p of v.pool) {
      if (!p.life) continue;
      const age = this.time - p.born;
      if (age < 0 || age >= p.life) { p.life = 0; continue; }
      if (drawn >= budget || this.reducedMotion) continue;
      const q = this.project(p.x + p.vx * age, p.y + p.vy * age, Math.max(0, p.height + age * 42 - age * age * 110));
      if (!this.visible(q, 45)) continue;
      c.save(); c.translate(q.x, q.y); c.scale(this.camera.zoom, this.camera.zoom); c.globalAlpha *= (1 - age / p.life) * .8;
      if (p.kind === 'debris') c.rotate(p.angle + age * 1.4);
      art(c, p.kind, p.size, p.size); c.restore(); drawn++;
    }
    c.restore(); v.active = drawn;
    if (v.game?.performance?.counters) v.game.performance.counters.artworkParticles = drawn;
  };
  P.drawLights = function (world, lights, game) {
    base.drawLights.call(this, world, lights, game);
    if (profile(this).atmosphere < 1 || this.detailLevel >= 4) return;
    const c = this.ctx; let budget = profile(this).atmosphere > 1 ? 18 : 8;
    for (let i = 0; i < lights.length && budget > 0; i++) {
      const l = lights[i]; if (l.active === false || !l.range) continue;
      const cache = this.lightCache.get(l.id || i + ':' + l.kind);
      if (!cache?.points?.length) continue;
      const p = this.project(l.x, l.y, 4); if (!this.visible(p, l.range * this.camera.zoom)) continue;
      const col = l.kind === 'alarm' ? '235,85,59' : l.kind === 'heli' ? '172,214,221' : '241,221,164';
      c.save(); c.beginPath(); c.moveTo(p.x, p.y);
      for (const q of cache.points) { const xy = this.project(q.x, q.y); c.lineTo(xy.x, xy.y); }
      c.closePath(); c.clip();
      // Scattering is clipped strictly inside the same obstruction polygon.
      // No decorative cone can imply safety/danger outside gameplay lighting.
      this.glow(c, p.x, p.y, l.range * this.camera.zoom * .64, col, .10);
      c.restore(); budget--;
    }
  };
  P.drawAtmosphere = function (c, king, view, game) {
    base.drawAtmosphere.call(this, c, king, view, game);
    const amount = profile(this).atmosphere;
    if (!amount || this.detailLevel >= 3) return;
    const v = init(this);
    if (!v.mist) {
      v.mist = document.createElement('canvas'); v.mist.width = 256; v.mist.height = 96;
      const sc = v.mist.getContext('2d'), g = sc.createRadialGradient(128, 48, 2, 128, 48, 126);
      g.addColorStop(0, 'rgba(153,181,179,.17)'); g.addColorStop(.5, 'rgba(116,157,151,.055)'); g.addColorStop(1, 'rgba(116,157,151,0)'); sc.fillStyle = g; sc.fillRect(0, 0, 256, 96);
    }
    const time = this.reducedMotion ? 0 : this.time;
    c.save(); c.globalAlpha *= .48; c.globalCompositeOperation = 'screen';
    for (let i = 0; i < amount + 1; i++) {
      const x = this.w * hash(i, 77) + Math.sin(time * .035 + i * 2) * 50, y = this.h * (.34 + .53 * hash(i, 78));
      c.drawImage(v.mist, x - 240, y - 31, 480, 62);
    }
    c.restore();
  };
  P.drawMenu = function (time) {
    const v = init(this);
    if (!v.menuReady && available('oak')) {
      const variants = window.ATSVisualAssets.manifest.environment.treeVariants;
      for (let i = 0; i < this.trees.length; i++) {
        const cv = document.createElement('canvas'); cv.width = 166; cv.height = 205;
        art(cv.getContext('2d'), variants[i % variants.length], 166, 200, 83, 186); this.trees[i] = cv;
      }
      v.menuReady = true;
    }
    return base.drawMenu.call(this, time);
  };
  window.ATSEnvironmentArt = Object.freeze({ version: 1, poolCapacity: 96, frame, draw: art, cageLayout, wallLayout, gateLayout });
})();
