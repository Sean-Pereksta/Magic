const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../js/player-models.js'),'utf8');
function harness(){const context=vm.createContext({console});vm.runInContext(source,context);return context;}

test('jersey, shorts and sneaker palettes resolve independently without changing equipped definitions',()=>{
  const context=harness(),jersey={primary:0x102030,secondary:0x405060,trim:0xfafafa,pattern:'stars'},shoes={primary:0x703010,accent:0x81b3c5,sole:0x191919,type:'high'};
  const before=JSON.stringify([jersey,shoes]);
  const appearance=context.resolveBasketballEquipment({},jersey,shoes,{shorts:{color:'#ad321f'},shoes:{color:'#c24a78',accent:'#bac521'}});
  assert.equal(appearance.jerseyStyle.primary,jersey.primary);assert.equal(appearance.jerseyStyle.secondary,jersey.secondary);
  assert.equal(appearance.shortsStyle.secondary,0xad321f);assert.equal(appearance.sneakerStyle.primary,0xc24a78);assert.equal(appearance.sneakerStyle.accent,0xbac521);
  assert.equal(appearance.sneakerStyle.type,'high');assert.equal(JSON.stringify([jersey,shoes]),before);
});

test('equipped overrides take priority over profile defaults and preserve unspecified accessory details',()=>{
  const context=harness(),profile={equipment:{jersey:{color:'#123456'},sleeve:{color:'#aa1122',enabled:true,side:'left'}}};
  const appearance=context.resolveBasketballEquipment(profile,{}, {},{jersey:{color:'#fedcba'},sleeves:{color:'#aabbcc'}});
  assert.equal(appearance.regions.jersey.color,0xfedcba);assert.equal(appearance.regions.sleeve.color,0xaabbcc);assert.equal(appearance.regions.sleeve.enabled,true);assert.equal(appearance.regions.sleeve.side,'left');
  assert.equal(profile.equipment.jersey.color,'#123456');assert.equal(profile.equipment.sleeve.color,'#aa1122');
});

test('legacy numeric items, aliases and invalid stored colors produce bounded region colors',()=>{
  const context=harness(),appearance=context.resolveBasketballEquipment({}, {primary:0x123456,trim:0xfedcba},{primary:0x777777},{jersey:{color:'red'},shorts:0xffcc11,sneaker:'#224488',sleeves:{color:-1,side:'invalid'},accent:{color:'#113355'},accessories:{enabled:true}});
  assert.equal(appearance.regions.jersey.color,0x123456);assert.equal(appearance.regions.shorts.color,0xffcc11);assert.equal(appearance.regions.shoes.color,0x224488);assert.equal(appearance.regions.sleeve.color,0x111820);assert.equal(appearance.regions.sleeve.side,'right');assert.equal(appearance.regions.accessory.enabled,true);assert.equal(appearance.regions.jersey.accent,0x113355);
  assert.ok(Object.values(appearance.regions).every(region=>Number.isInteger(region.color)&&region.color>=0&&region.color<=0xffffff));
});

test('a stale receiving timer cannot pin an off-ball runner in the catch pose',()=>{
  const context=harness(),player={hasBall:false,catchAnimation:.28,onGround:true,velocity:{length:()=>3.8},moveState:'normal'};
  assert.equal(context.chooseBasketballPlayerAnimation(player).name,'run');
  player.hasBall=true;assert.equal(context.chooseBasketballPlayerAnimation(player).mode,'receive');
});
