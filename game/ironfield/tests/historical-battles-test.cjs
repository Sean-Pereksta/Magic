'use strict';
const assert = require('node:assert/strict');
function register(test) {
  const D = global.GameData, Engine = global.BattleEngine;
  test('Historical catalog supplies twenty sourced, explicitly deployed battles across five eras', () => {
    const battles = D.HISTORICAL_BATTLES;
    assert.equal(battles.length, 20);
    assert.equal(new Set(D.PRESETS.map(p => p.id)).size, D.PRESETS.length);
    assert.equal(new Set(battles.map(p => p.historical.era)).size, 5);
    assert(Math.min(...battles.map(p => p.historical.year)) < 0);
    assert.equal(Math.max(...battles.map(p => p.historical.year)), 1815);
    for (const p of battles) {
      assert(D.PRESETS.includes(p)); assert.equal(p.map, 'custom');
      assert(p.historical.location && p.historical.deployment && p.historical.unitNotes);
      assert.match(p.historical.scaleNote, /[Ss]caled/);
      assert(p.historical.sources.length > 0);
      for (const s of p.historical.sources) { assert(s.title); assert.equal(new URL(s.url).protocol, 'https:'); }
      const counts = [{}, {}];
      for (const g of p.formations) {
        assert(D.TYPES[g.type]); assert(D.FORMATIONS[g.formation]); assert(g.name);
        assert(Number.isFinite(g.facing)); assert(g.count > 0);
        counts[g.team][g.type] = (counts[g.team][g.type] || 0) + g.count;
        if (g.offsets) assert.equal(g.offsets.length, g.count);
      }
      for (const team of [0,1]) for (const type of D.TYPE_ORDER) assert.equal(p.armies[team].roster[type], counts[team][type] || 0, p.name + ' has undeployed or duplicate soldiers');
    }
    return { battles: battles.length, eras: [...new Set(battles.map(p => p.historical.era))] };
  });
  test('Every historical deployment starts with all intended soldiers, clean terrain, and separated bodies', () => {
    const measurements = [];
    for (const p of D.HISTORICAL_BATTLES) {
      const e = new Engine(p), cells = new Map(); let minimum = Infinity;
      assert.equal(e.units.length, p.formations.reduce((n,g) => n + g.count, 0), p.name + ': dropped troops');
      assert.equal(e.regiments.length, p.formations.length, p.name + ': changed formation count');
      for (const u of e.units) {
        assert([u.x,u.y,u.angle].every(Number.isFinite), p.name + ': invalid body');
        assert(!e._bodyBlocked(u.x,u.y,u._radius), p.name + ': solid terrain under ' + e.regiments[u.regiment].name + ' at ' + u.x + ',' + u.y);
        assert(u.x >= u._radius && u.y >= u._radius && u.x <= e.width-u._radius && u.y <= e.height-u._radius, p.name + ': body outside map');
        const cx = Math.floor(u.x/80), cy = Math.floor(u.y/80);
        for (let yy=cy-1;yy<=cy+1;yy++) for(let xx=cx-1;xx<=cx+1;xx++) for(const v of cells.get(xx+':'+yy)||[]) {
          const ratio = Math.hypot(u.x-v.x,u.y-v.y)/(u._radius+v._radius); minimum = Math.min(minimum,ratio);
          assert(ratio >= .9, p.name + ': overlapping ' + e.regiments[u.regiment].name + ' / ' + e.regiments[v.regiment].name + ' ratio=' + ratio);
        }
        const key = cx+':'+cy; if(!cells.has(key)) cells.set(key,[]); cells.get(key).push(u);
      }
      measurements.push({battle:p.name,units:e.units.length,formations:e.regiments.length,minimumBodyRatio:Math.round(minimum*1000)/1000});
    }
    return measurements;
  });
  test('Historical layouts preserve their characteristic depth, flanks, and reserves', () => {
    const find = id => D.HISTORICAL_BATTLES.find(p=>p.id===id);
    const named = (p, text) => p.formations.find(g=>g.name.includes(text));
    const depth = g => Math.max(...g.offsets.map(o=>o.x))-Math.min(...g.offsets.map(o=>o.x));
    const marathon=find('historyMarathon'); assert(depth(named(marathon,'Athenian right')) > depth(named(marathon,'Shallow Greek'))*4);
    const leuctra=find('historyLeuctra'); assert(depth(named(leuctra,'Deep Theban')) > depth(named(leuctra,'Boeotian center'))*4);
    const cannae=find('historyCannae'); assert(named(cannae,'Projecting Gallic').x > named(cannae,'African left').x); assert(named(cannae,'Projecting Gallic').x > named(cannae,'African right').x);
    const cowpens=find('historyCowpens'); assert(named(cowpens,'Forward rifle').x > named(cowpens,'Pickens').x); assert(named(cowpens,'Pickens').x > named(cowpens,'Howard').x);
    const waterloo=find('historyWaterloo'); assert(named(waterloo,'Hougoumont').x > named(waterloo,'ridge right').x); assert(named(waterloo,'Guard reserve').x > named(waterloo,'French center infantry').x);
    assert(named(marathon,'Plataean left').y < named(marathon,'Athenian right').y);
    const hastings=find('historyHastings'); assert(named(hastings,'English left').y < named(hastings,'English right').y); assert(named(hastings,'Breton horse').y > named(hastings,'French horse').y);
    const blenheim=find('historyBlenheim'); assert(named(blenheim,'Eugene').y > named(blenheim,'Marlborough').y); assert(named(blenheim,'Lutzingen').y > named(blenheim,'Blenheim defenders').y); assert(blenheim.customMap.regions.some(r=>r.type==='water'&&r.y===0&&r.w===6400));
    assert(named(waterloo,'ridge right').y > named(waterloo,'ridge left').y); assert(named(waterloo,'French right infantry').y < named(waterloo,'French left infantry').y); assert(named(waterloo,'Hougoumont').y > named(waterloo,'Papelotte').y);
    const gaugamela=new Engine(find('historyGaugamela')), companions=gaugamela.regiments.find(r=>r.name==='Companion Cavalry');
    const xs=new Map(); for(const id of companions.unitIds) { const key=Math.round(gaugamela.units[id].x*100)/100; xs.set(key,(xs.get(key)||0)+1); }
    assert.equal(xs.get(Math.max(...xs.keys())),1,'Companions lost their pointed front'); assert(Math.max(...xs.values())>=8,'Companions became a rectangular saved layout');
    return {strongGreekWings:true,deepThebanLeft:true,carthaginianConvexCenter:true,threeAmericanLines:true,waterlooOutposts:true,armyRelativeFlanks:true,companionWedge:true};
  });
}
module.exports = register;
if (require.main === module) {
  global.window = global;
  for (const name of ['data','tactics','impacts','siege','engine']) require('../src/'+name+'.js');
  register((name, fn) => { try { const result=fn(); console.log('PASS '+name+' '+JSON.stringify(result)); } catch(error) { console.error('FAIL '+name+'\n'+error.stack); process.exitCode=1; } });
}
