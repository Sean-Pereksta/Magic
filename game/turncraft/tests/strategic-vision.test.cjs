const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

test('cached visibility keeps ground occlusion, scans, aircraft and dead sources authoritative',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    for(let y=0;y<BOARD_H;y++)tileAt(8,y).blocked=true;
    const unit=addEntity('unit','rifleman',0,5,5),enemy=addEntity('unit','zealot',1,10,5);rebuildVisionCache(true);
    const seen=()=>visionCache.get(0).has('10,5'),occluded=!seen();scanEffects=[{faction:0,x:10,y:5,radius:4,endAt:testClock+500}];testClock+=100;rebuildVisionCache();const scanned=seen();
    testClock+=600;rebuildVisionCache();const expired=!seen();const air=addEntity('unit','wraith',0,5,5);rebuildVisionCache();const aircraft=seen();air.alive=false;testClock+=100;rebuildVisionCache();
    return {occluded,scanned,expired,aircraft,deadHidden:!seen(),dropped:!visionFootprints.has(air.id)};
  })()`);
  assert.deepEqual(result,{occluded:true,scanned:true,expired:true,aircraft:true,deadHidden:true,dropped:true});
});

test('unchanged visibility reuses footprints while movement invalidation updates visibility immediately',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const units=Array.from({length:24},(_,i)=>addEntity('unit','rifleman',i%3,2+i%8,3+Math.floor(i/8)));
    const original=visionLineClear;let checks=0;visionLineClear=(...args)=>{checks++;return original(...args)};rebuildVisionCache(true);const first=checks;
    testClock+=100;rebuildVisionCache();const reused=checks===first;
    for(const unit of units){unit.x+=15;visionCacheAt=0;rebuildVisionCache()}
    const immediate=checks>first,before=checks;testClock+=100;rebuildVisionCache();const refreshed=checks===before;
    return {reused,immediate,refreshed,bounded:visionFootprints.size===units.length,private:!visionCache.get(0).has('8,3')};
  })()`);
  assert.deepEqual(result,{reused:true,immediate:true,refreshed:true,bounded:true,private:true});
});
