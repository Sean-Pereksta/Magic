const {test}=require('node:test');
const assert=require('node:assert/strict');
const manifest=require('../assets/animation-manifest.json');
const roster=require('../assets/unit-artwork-roster.json');
const artwork=require('../assets/visual-asset-index.json');

test('every troop action and direction uses an audited repair of its registered source sheet',()=>{
  const sheets=new Set();
  for(const type of Object.keys(roster)){
    const asset=manifest.assets[type];assert.ok(asset,type+': missing troop');
    for(const [state,definition]of Object.entries(asset.states)){
      const label=type+'/'+state,repair=manifest.cropRepairs[definition.sheet];
      assert.ok(repair,label+': troop sheet was omitted from the crop audit');sheets.add(definition.sheet);
      assert.deepEqual(definition.directions,manifest.directions,label);
      assert.equal(repair.sourceFrameWidth,definition.frameWidth,label+': original horizontal scale');
      assert.equal(repair.sourceFrameHeight||repair.frameHeight,definition.frameHeight,label+': original vertical scale');
      for(const direction of definition.directions){
        const row=repair.crops[definition.rows[direction]];assert.ok(row,label+'/'+direction);
        assert.ok((definition.firstColumn||0)+definition.frames<=row.length,label+'/'+direction+': missing action frames');
      }
    }
  }
  assert.ok(sheets.size>=Object.keys(roster).length*3,'audit must cover locomotion, combat and demise for the whole roster');
});

test('audited crops retain padding, source provenance and mobile-sized repaired atlases',()=>{
  for(const [url,repair]of Object.entries(manifest.cropRepairs)){
    const source=artwork.images[new URL(url).pathname.slice(1)];assert.ok(source,url);
    assert.equal(repair.sourceSha256,source.sha256,url+': stale source measurements');
    assert.equal(repair.crops.length,manifest.directions.length,url+': all directions');
    assert.equal(repair.height,repair.crops.length*repair.frameHeight,url);
    assert.ok(Math.max(repair.width,repair.height)<=2048,url+': mobile texture limit');
    for(const [row,crops]of repair.crops.entries()){
      assert.equal(repair.width,crops.length*repair.frameWidth,url);
      for(const [col,[x,y,w,h]]of crops.entries()){
        const label=url+'/'+row+'/'+col;
        assert.ok(x>=0&&y>=0&&w>0&&h>0&&x+w<=source.width&&y+h<=source.height,label+': source bounds');
        if(url.endsWith('-demise.webp')&&col<5){
          const deathEnd=repair.deathRegionEnds?.[row]??repair.deathRegionEnd??5*repair.sourceFrameWidth+4;
          assert.ok(x+w<=deathEnd,label+': death must not select a living ability pose');
        }
        const [dx,dy]=repair.destinationOffsets?.[row]?.[col]||[(repair.frameWidth-w)/2,y-row*repair.frameHeight];
        assert.ok(dx>=4&&dx+w<=repair.frameWidth-4,label+': horizontal gutter');
        assert.ok(dy>=0&&dy+h<=repair.frameHeight,label+': destination bounds');
        if(repair.destinationOffsets)assert.ok(dy>=4&&dy+h<=repair.frameHeight-4,label+': vertical gutter');
      }
    }
  }
});
