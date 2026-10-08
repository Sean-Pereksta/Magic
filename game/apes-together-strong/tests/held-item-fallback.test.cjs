'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
function harness(){
 class Renderer{};for(const name of ['drawPrimateGeometry','drawApe','drawApeSprite','drawHumanSprite','drawCorpses'])Renderer.prototype[name]=()=>{};
 Renderer.prototype.drawHuman=function(){this.legacyHumans=(this.legacyHumans||0)+1};
 const root=path.resolve(__dirname,'..'),c=vm.createContext({ATSRenderer:Renderer});c.window=c;
 c.ATSVisualAssets={manifest:{'held-items':JSON.parse(fs.readFileSync(path.join(root,'assets/visual/held-items/manifest.json'),'utf8')),characters:JSON.parse(fs.readFileSync(path.join(root,'assets/visual/characters/manifest.json'),'utf8'))},get:id=>({id})};
 for(const name of ['character-art','held-item-art'])vm.runInContext(fs.readFileSync(path.join(root,name+'.js'),'utf8'),c);
 return {c,r:new Renderer(),art:c.ATSHeldItemArt};
}
test('each missing held atlas sends weapon-free human bodies through the armed legacy fallback',()=>{
 for(const failed of ['held-combat','held-utility','held-stone']){const {c,r}=harness();c.ATSVisualAssets.get=id=>id===failed?null:{id};r.drawHuman({},Object.freeze({hp:100,type:'human',kind:'rifle'}));assert.equal(r.legacyHumans,1);assert.equal(r.characterArtworkDraws,undefined)}
});
test('ape stone and equipped spear leave the palm at the real throw release phase',()=>{
 const {art}=harness();
 for(const spear of [false,true]){const a=Object.freeze({id:'thrower',type:'ape',hp:100,equipment:Object.freeze({spear})}),id=spear?'spear':'stone';const before=art.resolve(a,{state:'throw',phase:.52}),after=art.resolve(a,{state:'throw',phase:.525});assert.ok(before.some(v=>v.id===id));assert.ok(!after.some(v=>v.id===id));if(spear)assert.ok(art.resolve(a,{state:'idle'}).some(v=>v.id==='spear'))}
});
test('unarmed nonworking crowds use a shared empty result and no actor texture cache',()=>{
 const {art}=harness(),a=Object.freeze({id:'idle',type:'ape',species:'gorilla',hp:100,state:'follow'});assert.equal(art.resolve(a,{}),art.resolve(a,{}));assert.equal(art.hasEquipment(a,{}),false);assert.equal(art.status().perActorCache,0);
});
