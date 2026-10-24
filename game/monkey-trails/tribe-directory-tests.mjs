import test from 'node:test';
import assert from 'node:assert/strict';
import {newSave} from './core.mjs';
import {snapshotTeam} from './social-combat.mjs';
import {SocialStore,createMemorySocialAdapter,SOCIAL_CONFIG,applicationStatus} from './social-store.mjs';
import {renderSocialView,renderTribeRankings} from './social-ui.mjs';
import {tribeWeek} from './tribe-adventures.mjs';

function fixture(){let now=1800000000000,serial=0;const adapter=createMemorySocialAdapter();return{adapter,now:()=>now,advance:n=>now+=n,async player(uid){const store=new SocialStore(adapter,{uid,now:()=>now,id:()=>`TRIBE${++serial}XXX`});await store.register({saveId:`save-${uid}`,name:`Trail ${uid}`,team:snapshotTeam(newSave(`Trail ${uid}`,'capuchin','gps',22))});return store;}};}
test('approval requests are bounded by three slots, expire and retain terminal statuses',async()=>{
 const f=fixture(),applicant=await f.player('applicant'),leaders=[];
 for(let i=0;i<4;i++){const owner=await f.player(`leader${i}`);leaders.push({owner,tribe:await owner.createTribe({name:`Approval Tribe ${i}`,admissionMode:'approval'})});}
 const requests=await Promise.all(leaders.slice(0,3).map(({tribe})=>applicant.requestTribe(tribe.id)));
 assert.equal((await applicant.getPlayer()).tribeId,null);assert.equal((await applicant.requestTribe(leaders[0].tribe.id)).id,requests[0].id);
 await assert.rejects(applicant.requestTribe(leaders[3].tribe.id),/three pending/);
 await leaders[0].owner.reviewApplication(requests[0].id,false);assert.equal((await f.adapter.get(`applications/${requests[0].id}`)).status,'rejected');
 const replacement=await applicant.requestTribe(leaders[3].tribe.id);assert.equal(replacement.slot,0);
 f.advance(SOCIAL_CONFIG.applicationTTL+1);assert.equal(applicationStatus(requests[1],f.now()),'expired');
 await applicant.expireApplications(requests.slice(1));assert.equal((await f.adapter.get(`applications/${requests[1].id}`)).status,'expired');
 await assert.rejects(leaders[3].owner.reviewApplication(replacement.id,true),/no longer pending/);
 const next=await applicant.requestTribe(leaders[0].tribe.id);assert.equal(next.status,'pending');
});
test('competing leader approvals serialize to one membership and rejection cannot mutate membership',async()=>{
 const f=fixture(),a=await f.player('a'),b=await f.player('b'),c=await f.player('c'),ta=await a.createTribe({name:'Alpha Leaves',admissionMode:'approval'}),tb=await b.createTribe({name:'Beta Leaves',admissionMode:'approval'}),ra=await c.requestTribe(ta.id),rb=await c.requestTribe(tb.id);
 const result=await Promise.allSettled([a.reviewApplication(ra.id,true),b.reviewApplication(rb.id,true)]);assert.equal(result.filter(r=>r.status==='fulfilled').length,1);
 const member=await c.getPlayer();assert.ok([ta.id,tb.id].includes(member.tribeId));
 const other=member.tribeId===ta.id?b:a,pending=member.tribeId===ta.id?rb:ra;await other.reviewApplication(pending.id,false);assert.equal((await c.getPlayer()).tribeId,member.tribeId);
 await assert.rejects(c.reviewApplication(ra.id,true),/leader/);
});
test('simultaneous approvals for the last place cannot exceed ten members',async()=>{
 const f=fixture(),owner=await f.player('owner'),t=await owner.createTribe({name:'Last Branch',admissionMode:'approval'}),a=await f.player('a'),b=await f.player('b'),ra=await a.requestTribe(t.id),rb=await b.requestTribe(t.id);
 await f.adapter.transaction(async tx=>{const tribe=await tx.get(`tribes/${t.id}`);tribe.members.push(...Array.from({length:8},(_,i)=>`fixture${i}`));tx.set(`tribes/${t.id}`,tribe);});
 const result=await Promise.allSettled([owner.reviewApplication(ra.id,true),owner.reviewApplication(rb.id,true)]);assert.equal(result.filter(r=>r.status==='fulfilled').length,1);assert.equal((await f.adapter.get(`tribes/${t.id}`)).members.length,10);
});
test('incoming and outgoing escrow prevent leaving and locks are removed atomically on cancellation',async()=>{
 const f=fixture(),a=await f.player('a'),b=await f.player('b'),t=await a.createTribe({name:'Trade Keepers'});await b.joinTribe(t.id);await a.deposit('fruit',10,'fund');
 const offer=await a.offerTrade('b',{asset:'fruit',amount:2},{asset:'berry',amount:1});await assert.rejects(a.leaveTribe(),/pending trades/);await assert.rejects(b.leaveTribe(),/pending trades/);
 for(const uid of ['a','b'])assert.deepEqual((await f.adapter.get(`tradeLocks/${uid}`)).offers,[offer.id]);
 await b.cancelTrade(offer.id);for(const uid of ['a','b'])assert.deepEqual((await f.adapter.get(`tradeLocks/${uid}`)).offers,[]);
 await b.leaveTribe();assert.equal((await b.getPlayer()).tribeId,null);assert.equal((await a.getPlayer()).wallet.fruit,10);
});
test('directory searches actual tribes, filters capacity, and public profiles omit private fields',async()=>{
 const f=fixture(),a=await f.player('a'),b=await f.player('b'),t=await a.createTribe({name:'CANOPY Gatherers',admissionMode:'approval'});await b.createTribe({name:'Paused Crew',acceptingApplications:false});
 assert.equal((await a.listDirectory({search:'canopy',availableOnly:true}))[0].id,t.id);assert.equal((await a.listDirectory({availableOnly:true})).length,1);
 const profile=await b.getPublicTribe(t.id),text=JSON.stringify(profile);assert.equal(profile.members[0].name,'Trail a');assert.doesNotMatch(text,/saveId|wallet|location|seenAt|route|activeRaid/);
 await a.updateTribe({acceptingApplications:false});await assert.rejects(b.requestTribe(t.id),/Leave/);
 assert.equal((await a.listDirectory({search:'canopy',availableOnly:true})).length,0);
});
test('weekly and power rankings use shared records and equal scores share ranks',async()=>{
 const f=fixture(),a=await f.player('a'),b=await f.player('b'),t=await a.createTribe({name:'Strong Canopy'}),other=await b.createTribe({name:'Weekly Winners'}),week=tribeWeek(f.now()),zero={meters:0,discoveries:0,challenges:0,raids:0,trades:0};
 await a.joinAdventure(zero);await a.contributeAdventure({...zero,meters:500});await b.joinAdventure(zero);await b.contributeAdventure({...zero,meters:1000});
 assert.equal((await a.listTribeRankings('weekly'))[0].id,other.id);const power=await a.listTribeRankings('power');assert.equal(power[0].rank,1);assert.equal(power[1].rank,1);
 assert.equal((await f.adapter.get(`tribePublicActivity/${t.id}`)).weekStart,week);assert.equal((await f.adapter.get(`tribePublicActivity/${t.id}`)).points,10);
});
test('directory and request controls escape remote names and expose readable admission statuses',()=>{
 const now=1800000000000,model={status:'ready',now,me:{uid:'me',wallet:{fruit:0,items:{}}},directoryStatus:'ready',directory:[{id:'ONE',name:'<script>Tribe</script>',memberCount:2,rank:3,bossLevel:4,admissionMode:'approval',acceptingApplications:true}],applications:[{id:'me-0',tribeId:'ONE',tribeName:'<b>Tribe</b>',status:'pending',expiresAt:now+10}],tribeRankings:[]};
 const html=renderSocialView(model);assert.match(html,/Tribe directory/);assert.match(html,/Request pending/);assert.match(html,/Pending/);assert.match(html,/&lt;script&gt;/);assert.doesNotMatch(html,/<script>/);
 model.rankingMetric='weekly';model.tribeRankings=[{id:'ONE',name:'First',rank:1,score:150,memberCount:2,bossesDefeated:3}];assert.match(renderTribeRankings(model),/150/);assert.match(renderTribeRankings(model),/weekly points/);
});
