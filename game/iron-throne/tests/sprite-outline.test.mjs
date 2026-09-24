import test from 'node:test';
import assert from 'node:assert/strict';
import { SpriteOutlines } from '../sprite-outline.mjs';
import { WorldMap } from '../map.mjs';
import { ART } from '../asset-manifest.mjs';
function surface() {
 const calls=[];const context={calls,scale(...v){calls.push(['scale',...v]);},clearRect(){},fillRect(){calls.push(['fill',this.globalCompositeOperation,this.fillStyle]);},drawImage(...v){calls.push(['draw',...v]);}};
 return {getContext:()=>context};
}
test('outline uses the alpha mask, faction tint, dark edge and a single original sprite',()=>{
 const surfaces=[];const outlines=new SpriteOutlines(2,()=>{const c=surface();surfaces.push(c);return c;});
 const image={naturalWidth:100,naturalHeight:200};
 const sprite=outlines.get(image,'building',40,40,'#ff7700',1,2);
 assert.deepEqual(surfaces[1].getContext().calls.filter(c=>c[0]==='fill'),[['fill','source-in','#07131d'],['fill','source-in','#ff7700']]);
 assert.equal(surfaces[0].getContext().calls.filter(c=>c[0]==='draw'&&c[1]===image).length,1);
 const original=surfaces[0].getContext().calls.at(-1);assert.deepEqual(original.slice(-2),[20,40]);
 assert.equal(outlines.get(image,'building',40,40,'#ff7700',1,2),sprite);assert.equal(surfaces.length,2);
 assert.notEqual(outlines.get(image,'building',40,40,'#00bbff',1,2),sprite,'ownership change gets new contour');
 outlines.get(image,'another',40,40,'#00bbff',1,2);assert.equal(outlines.cache.size,2);
 const next=outlines.get({naturalWidth:100,naturalHeight:200},'another',40,40,'#00bbff',1,2);assert.notEqual(next.image,image,'replaced source invalidates cached outline');
});
test('PNG army and construction sprites replace procedural versions, with fallbacks on failure',()=>{
 const map=Object.create(WorldMap.prototype);map.zoom=1;map.dpr=2;
 let ready=true,formations=0,strokes=0;const urls=[];
 map.assets={drawOutlined(c,url){urls.push(url);return ready;}};map.art={formation(){formations++;}};
 const c={beginPath(){},moveTo(){},lineTo(){},stroke(){strokes++;}};
 map.armyArt(c,{levy:10,archer:30},0,0,'#fff','A',0);assert.equal(formations,0);assert.equal(urls.at(-1),ART.units.archer);
 map.constructionArt(c,.6,'#fff');assert.equal(strokes,0);assert.equal(urls.at(-1),ART.construction[1]);
 ready=false;map.armyArt(c,{levy:10},0,0,'#fff','A',0);map.constructionArt(c,.6,'#fff');assert.equal(formations,1);assert.ok(strokes>0);
});

test('terrain chooses exactly one background renderer',()=>{
 const map=Object.create(WorldMap.prototype);let ready=true,oldGround=0,newGround=0;
 map.assets={draw(){newGround++;return ready;}};map.art={ground(){oldGround++;}};
 map.groundArt({}, {terrain:'plains',q:1,r:2},0,0);assert.equal(newGround,1);assert.equal(oldGround,0);
 ready=false;map.groundArt({}, {terrain:'plains',q:1,r:2},0,0);assert.equal(oldGround,1);
});
