/* Rebuild the hand-inspected sprite rectangles, retaining source PNG alpha. */
'use strict';
const fs = require('node:fs'), path = require('node:path');
const root = path.join(__dirname, '../assets/visual/settlements'), file = path.join(root, 'manifest.json');
const m = JSON.parse(fs.readFileSync(file, 'utf8'));
const rows = [
  ['homes', ['hut','lodge','warLodge'], [0,290,610,971]],
  ['services', ['lookout','workShelter','storage'], [0,360,660,971]],
  ['civic', ['training','nursery','rallyGrove'], [0,336,640,971]],
  ['defense', ['spearBattery','spearBallista','barrier'], [0,368,650,971]],
  ['living', ['garden','orchard','cooking'], [0,324,648,971]]
];
m.atlases = m.atlases.filter(a => !a.id.startsWith('construction-'));
m.construction = {};
for (const [family,names,bounds] of rows) {
  const id = 'construction-' + family, data = fs.readFileSync(path.join(root,id+'.png'));
  if (data.readUInt32BE(16)!==1619 || data.readUInt32BE(20)!==971) throw Error('Unexpected atlas dimensions: '+id);
  m.atlases.push({id,file:'settlements/'+id+'.png',width:1619,height:971});
  for (let row=0;row<3;row++) {
    const h=bounds[row+1]-bounds[row];
    m.construction[names[row]] = Array.from({length:5},(_,stage)=>{
      const left=Math.floor(stage*1619/5),right=Math.floor((stage+1)*1619/5);
      return {atlas:id,rect:[left,bounds[row],right-left,h],anchor:[(right-left)/2,h-48],stage};
    });
  }
}
m.notes.construction = '15 five-frame authored strips (75 frames). Actual project.stage chooses stages0–3; the same strip supplies completed stage4. No time-based fake completion. Anchors remain fixed across each strip.';
// The earlier concept sheets remain reusable source artwork in the package.
// The playable bundle needs only the final construction frame for these names.
for(const [name,frames] of Object.entries(m.construction))m.frames[name]=frames[4];
m.atlases=m.atlases.filter(a=>!['ape-structures','civic-structures'].includes(a.id));
fs.writeFileSync(file,JSON.stringify(m,null,2)+'\n');
console.log('Indexed 75 construction frames across 15 strips.');
