const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
function load(){const c=vm.createContext({});c.window=c;c.ATSCommandAnimation={orderFor:a=>a.order};vm.runInContext(fs.readFileSync(path.join(__dirname,'../command-ui.js'),'utf8'),c);return c.ATSCommandHUD;}
test('selected status follows current orders, individuals and mixed groups',()=>{
 const hud=load(),g={siege:{selected:['gorilla','gibbon'],unitIds:null},apes:[{id:'a',hp:100,species:'gorilla',state:'hold',order:'defend'},{id:'b',hp:80,species:'gibbon',state:'follow',order:'follow'},{id:'c',hp:0,species:'gorilla',state:'charge',order:'charge'},{id:'d',hp:80,species:'gibbon',state:'settled',order:'hold'}]};
 assert.equal(hud.summarize(g).kind,'mixed');assert.equal(hud.summarize(g).total,2);
 g.siege.unitIds=['a'];assert.equal(hud.summarize(g).kind,'defend');assert.equal(hud.summarize(g).total,1);
 g.apes[0].order='retreat';assert.equal(hud.summarize(g).label,'Retreat');
 g.siege.selected=[];assert.equal(hud.summarize(g),null);
});
