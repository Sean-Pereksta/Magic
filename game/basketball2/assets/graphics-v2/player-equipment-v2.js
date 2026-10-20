/* Basketball 2 v2 athlete equipment and number fit.
 * Load after existing js/player-models.js, before the v2 loader/bootstrap.
 * Older assets retain their existing fit. All dimensions come from GLB extras.
 */
(function installBasketballV2Fit(){
  const originalEquipment=addBasketballModelEquipment;
  const originalNumbers=addBasketballPlayerNumbers;
  addBasketballModelEquipment=function(bones,sideBones,char,authored,materials,geometries){
    const fit=authored.equipmentSizing;
    if(authored.version!==2 || !fit)return originalEquipment(bones,sideBones,char,authored,materials,geometries);
    const appearance=getBasketballEquipmentAppearance(char),nodes=[],segments=authored.lod==='low'?12:20;
    function attach(region,bone,geometry,position,side=null,materialName=region){
      if(!bone){geometry.dispose();return null;}
      const spec=appearance.regions[region],material=new THREE.MeshStandardMaterial({color:spec.color,roughness:.94});
      material.name=materialName;materials.push(material);geometries.push(geometry);
      const mesh=new THREE.Mesh(geometry,material);mesh.name=`equipment_${region}_${side || 'center'}`;
      mesh.position.fromArray(position);mesh.visible=spec.enabled && (!side || spec.side==='both' || spec.side===side);
      // Small accessories do not justify additional shadow-map draws.
      mesh.castShadow=false;mesh.userData.basketballNoShadow=true;bone.add(mesh);nodes.push({node:mesh,region,side});return mesh;
    }
    const headGeometry=new THREE.TorusGeometry(fit.headband.radius,.006,4,segments);
    headGeometry.rotateX(Math.PI/2);headGeometry.scale(1,1,fit.headband.zScale);
    attach('headband',bones.get('head'),headGeometry,fit.headband.position);
    for(const side of ['left','right']){
      const suffix=sideBones[side],mirror=suffix==='L'?1:-1;
      const top=new THREE.Vector3(...fit.sleeve.topL),bottom=new THREE.Vector3(...fit.sleeve.bottomL);top.x*=mirror;bottom.x*=mirror;
      const direction=new THREE.Vector3().subVectors(top,bottom),center=new THREE.Vector3().addVectors(top,bottom).multiplyScalar(.5);
      const geometry=new THREE.CylinderGeometry(fit.sleeve.topRadius,fit.sleeve.bottomRadius,direction.length(),segments,1,true);
      geometry.applyQuaternion(new THREE.Quaternion().setFromUnitVectors(new THREE.Vector3(0,1,0),direction.normalize()));
      attach('sleeve',bones.get(`forearm_${suffix}`),geometry,center.toArray(),side);
      const bandPosition=fit.accessory.positionL.slice();bandPosition[0]*=mirror;
      const armBand=new THREE.CylinderGeometry(fit.accessory.radius,fit.accessory.radius*.96,fit.accessory.height,segments,1,true);
      armBand.rotateZ(mirror*-.30);
      attach('accessory',bones.get(`upper_arm_${suffix}`),armBand,bandPosition,side);
      // Right wristband is part of the shared skinned model; left remains modular.
      if(side==='left'){
        const wristGeometry=new THREE.CylinderGeometry(.034*(authored.anatomicalProportions?.depth || 1),.033*(authored.anatomicalProportions?.depth || 1),.028,segments,1,true);
        const wrist=bottom.clone();wrist.y-=.006;
        attach('wristband',bones.get(`forearm_${suffix}`),wristGeometry,wrist.toArray(),side,'wristband_left');
      }
    }
    return nodes;
  };
  addBasketballPlayerNumbers=function(bones,char,authored,materials,textures,ownedGeometries){
    const fit=authored.equipmentSizing?.numbers;
    if(authored.version!==2 || !fit)return originalNumbers(bones,char,authored,materials,textures,ownedGeometries);
    const chest=bones.get('chest');if(!chest || typeof document==='undefined')return;
    const canvas=document.createElement('canvas');canvas.width=128;canvas.height=128;
    const ctx=canvas.getContext('2d');if(!ctx)return;
    ctx.clearRect(0,0,128,128);ctx.fillStyle='#ffffff';ctx.font='bold 94px Arial';ctx.textAlign='center';ctx.textBaseline='middle';ctx.fillText(String(char.profile?.number || '23'),64,69,122);
    const texture=new THREE.CanvasTexture(canvas);texture.colorSpace=THREE.SRGBColorSpace;textures.push(texture);
    const material=new THREE.MeshStandardMaterial({map:texture,color:getBasketballEquipmentAppearance(char).regions.jersey.accent,alphaTest:.35,roughness:.95,polygonOffset:true,polygonOffsetFactor:-1,polygonOffsetUnits:-1});
    material.name='player_number_overlay';materials.push(material);
    // Both jersey numbers share one mesh/material and one draw call.
    const w=fit.width/2,h=fit.height/2,y=fit.centerY;
    const positions=[],normals=[],uv=[],indices=[];
    for(const [face,z] of [[1,fit.frontZ],[-1,fit.backZ]]){
      const offset=positions.length/3;
      positions.push(-w*face,y-h,z,w*face,y-h,z,w*face,y+h,z,-w*face,y+h,z);
      for(let i=0;i<4;i++)normals.push(0,0,face);
      uv.push(0,0,1,0,1,1,0,1);indices.push(offset,offset+1,offset+2,offset,offset+2,offset+3);
    }
    const geometry=new THREE.BufferGeometry();geometry.setAttribute('position',new THREE.Float32BufferAttribute(positions,3));geometry.setAttribute('normal',new THREE.Float32BufferAttribute(normals,3));geometry.setAttribute('uv',new THREE.Float32BufferAttribute(uv,2));geometry.setIndex(indices);ownedGeometries.push(geometry);
    const mesh=new THREE.Mesh(geometry,material);mesh.name='jersey_numbers';mesh.castShadow=false;mesh.userData.basketballNoShadow=true;chest.add(mesh);
  };
})();
