/* The authored GLBs keep their animation hierarchy; gameplay owns the outer rig. */
(function (global) {
  'use strict';
  const names = ['torso','head','leftArm','leftArmLower','leftArmEnd','rightArm','rightArmLower','rightArmEnd','leftLeg','leftLegLower','leftLegEnd','rightLeg','rightLegLower','rightLegEnd'];
  function actor(path, privateMaterials = true) {
    const model = global.RunnerGraphics.create(path, {ownMaterials: privateMaterials});
    const rig = new THREE.Group(); rig.add(model);
    rig.userData.runnerAsset = model.userData.runnerAsset;
    const parts = Object.fromEntries(names.map(name => [name, model.getObjectByName(name)]).filter(([,node]) => node));
    for (const name of ['leftLeg','rightLeg']) if (parts[name]) parts[name].userData.length ||= Math.abs(parts[name+'Lower']?.position.y || .82);
    return {rig, parts, model};
  }
  function materials(root, visit) {
    const seen = new Set(); root.traverse(node => {
      for (const material of (node.material ? Array.isArray(node.material) ? node.material : [node.material] : [])) {
        if (!seen.has(material)) { seen.add(material); visit(material); }
      }
    });
  }
  function character(profile, jerseyColor) {
    const made = actor('characters/'+profile.id+'.glb');
    made.rig.scale.fromArray(profile.modelScale);
    made.rig.userData.characterKey = profile.id;
    // Keep each runner's signature palette; orange marks the second player.
    if (jerseyColor === 0xea580c) materials(made.model, m => { if (m.name === 'jersey') m.color.setHex(jerseyColor); });
    return made;
  }
  function enemy(role, variant, realm, equipped) {
    const made = actor('troops/'+role+'.glb');
    made.rig.userData.role = role;
    made.rig.scale.y = 1 + (variant % 3 - 1) * .025;
    materials(made.model, m => {
      if (m.name === 'skin') m.color.setHex([0xd5a280,0x774e3c,0x402921][variant % 3]);
      if (realm === 'neon' && m.name === 'trim' && m.emissive) {m.emissive.setHex(0x35d8cd);m.emissiveIntensity=.4;}
    });
    let handTorch = made.model.getObjectByName('handTorch');
    if (equipped && !handTorch && made.parts.rightArmLower) {
      handTorch = global.RunnerGraphics.create('props/held-torch.glb');
      handTorch.name='handTorch'; handTorch.position.y=-.7; handTorch.scale.setScalar(.65);
      made.parts.rightArmLower.add(handTorch);
    }
    return {...made, handTorch};
  }
  function canine(spirit = false) {
    const made = actor('troops/'+(spirit?'spirit-dog':'dog')+'.glb', spirit);
    const legs = [0,1,2,3].map(i=>made.model.getObjectByName('leg'+i));
    for (const leg of legs) leg.userData.runnerBaseY=leg.position.y;
    const body=made.model.getObjectByName('body'),head=made.model.getObjectByName('head'),tail=made.model.getObjectByName('tail');
    let eyeMaterial;materials(made.model,m=>{if(m.name==='eyeMaterial')eyeMaterial=m;});
    const trails=[];
    if (spirit) {
      const shellMaterial=new THREE.MeshBasicMaterial({color:0xbaffff,side:THREE.BackSide,fog:false,toneMapped:false,depthWrite:false});
      const originals=[];made.model.traverse(n=>{if(n.isMesh)originals.push(n);});
      for(const mesh of originals){const shell=new THREE.Mesh(mesh.geometry,shellMaterial);shell.scale.setScalar(1.055);shell.userData.spiritOutline=true;mesh.add(shell);}
      const trailMaterial=new THREE.MeshBasicMaterial({color:0xb8a4ff,transparent:true,opacity:.16,depthWrite:false,fog:false});
      const trailGeometry=new THREE.SphereGeometry(.38,6,4);
      for(let i=0;i<3;i++){const trail=new THREE.Mesh(trailGeometry,trailMaterial);trail.position.set(0,1,-2.1-i*.6);made.rig.add(trail);trails.push(trail);}
    }
    return {...made,legs,body,head,tail,eyeMaterial,trails};
  }
  function prop(path) { return global.RunnerGraphics.create('props/'+path+'.glb'); }
  function pickup(type) {
    const kind={green:'energy',blue:'shield',gold:'distance',red:'risk',black:'attack'}[type];
    const model=global.RunnerGraphics.create('pickups/'+kind+'.glb');
    // Pickups were authored with floor origins; center them around the old orb.
    const entry=global.RunnerGraphics.asset('pickups/'+kind+'.glb');
    model.position.y=-(entry.bounds.max[1]-entry.bounds.min[1])*.5;
    return model;
  }
  global.RunnerActorArt=Object.freeze({actor,character,enemy,canine,prop,pickup});
})(window);
