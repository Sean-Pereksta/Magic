import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {memberActivity, filterMembers, activityTime} from '../lobby-admin-members.mjs';

const core=fs.readFileSync(new URL('../lobby-core.html',import.meta.url),'utf8');
function source(name){
  const start=core.search(new RegExp(`    (?:async )?function ${name}\\(`));
  assert.ok(start>=0,name);
  return core.slice(start,core.indexOf('\n    }',start)+6);
}
function onboarding({username='',seen=false,sessionSeen=false,invite=false,modal=false}={}){
  const saved=new Map(seen?[['seen','1']]:[]),session=new Map(sessionSeen?[['seen','1']]:[]),opened=[];
  const ctx=vm.createContext({username,SK:{profilePromptSeen:'seen'},read:k=>saved.get(k),readSession:k=>session.get(k),save:(k,v)=>saved.set(k,v),saveSession:(k,v)=>session.set(k,v),initialParams:new URLSearchParams(invite?'lobby=abc':''),$:()=>({hidden:!modal}),openAccount:tab=>opened.push(tab)});
  vm.runInContext(source('showFirstVisitProfilePrompt'),ctx);
  return {ctx,saved,session,opened};
}
test('first visit opens creation once and remembers it across visits',()=>{
  const h=onboarding();h.ctx.showFirstVisitProfilePrompt();h.ctx.showFirstVisitProfilePrompt();
  assert.deepEqual(h.opened,['create']);assert.equal(h.saved.get('seen'),'1');
  for(const options of [{seen:true},{sessionSeen:true}]){
    const returning=onboarding(options);returning.ctx.showFirstVisitProfilePrompt();assert.equal(returning.opened.length,0);
  }
});
test('existing players are remembered without prompting; invite previews and active modals take priority',()=>{
  const existing=onboarding({username:'Sean'});existing.ctx.showFirstVisitProfilePrompt();existing.ctx.username='';existing.ctx.showFirstVisitProfilePrompt();assert.equal(existing.opened.length,0);
  for(const options of [{invite:true},{modal:true}]){const h=onboarding(options);h.ctx.showFirstVisitProfilePrompt();assert.equal(h.opened.length,0);assert.equal(h.saved.size,0);}
});
test('activity normalization handles legacy dates, excludes secrets and ignores invalid counts',()=>{
  const row=memberActivity('Example',{password:'NEVER_DISPLAY',chatAuthUid:'uid',lastLogin:{seconds:1000},createdAt:{toMillis:()=>500000},gamePlayCounts:{cat:3,chess:'4',bad:-2,invalid:'no',fraction:1.5},recentlyPlayed:['cat',{key:'chess',playedAt:600000},null]});
  assert.equal(row.totalPlays,7);assert.equal(row.games.length,2);assert.equal(row.lastLogin,1000000);assert.equal(row.createdAt,500000);assert.equal(row.recent.length,2);assert.ok(!JSON.stringify(row).includes('NEVER_DISPLAY'));assert.ok(!('chatAuthUid' in row));
  assert.equal(activityTime({}),null);assert.equal(activityTime(Infinity),null);assert.equal(activityTime(9e15),null);
  assert.equal(memberActivity('Old',{}).hasPlayCounts,false);
});
test('all members can be searched and sorted including missing login dates',()=>{
  const rows=Array.from({length:85},(_,i)=>memberActivity('Player'+i,{displayName:i===84?'Final member':'Player',lastLogin:i?i*1000:null,gamePlayCounts:{cat:i}}));
  assert.equal(filterMembers(rows).length,85);assert.equal(filterMembers(rows,'final')[0].username,'Player84');assert.equal(filterMembers(rows,'','plays')[0].username,'Player84');assert.equal(filterMembers(rows).at(-1).username,'Player0');
});
function adminHarness(){
  const elements=new Map();const $=id=>{if(!elements.has(id))elements.set(id,{hidden:false,value:'',innerHTML:'',textContent:''});return elements.get(id);};
  const ctx=vm.createContext({username:'Sean',seanAdminUnlocked:false,adminMembers:[],adminMembersLoaded:false,adminLoadSeq:0,$,db:{},memberActivity,normalize:v=>v.toLowerCase(),collection:()=>({}),doc:()=>({}),setMessage:(_id,message)=>{$('adminMembersMessage').textContent=message;},setBusy:()=>{},renderAdminMembers:()=>{},getDocs:async()=>({forEach:()=>{}})});
  for(const name of ['canViewAdminMembers','renderAdminMembersAccess','lockAdminMembers','loadAdminMembers','unlockAdminMembers'])vm.runInContext(source(name),ctx);
  return {ctx,$};
}
test('a remembered username or display name cannot unlock the dashboard',()=>{
  const h=adminHarness();assert.equal(h.ctx.canViewAdminMembers(),false);
  h.ctx.seanAdminUnlocked=true;assert.equal(h.ctx.canViewAdminMembers(),true);
  for(const name of ['sean','Other','']){h.ctx.username=name;assert.equal(h.ctx.canViewAdminMembers(),false);h.ctx.renderAdminMembersAccess();assert.equal(h.$('adminMembersPanel').hidden,true);}
});
test('dashboard unlock requires the actual Sean password and clears password input',async()=>{
  const h=adminHarness();h.ctx.getDoc=async()=>({exists:()=>true,data:()=>({password:'unique-secret'})});
  h.$('adminPassword').value='wrong';await h.ctx.unlockAdminMembers();assert.equal(h.ctx.seanAdminUnlocked,false);assert.equal(h.$('adminPassword').value,'');
  h.$('adminPassword').value='unique-secret';await h.ctx.unlockAdminMembers();assert.equal(h.ctx.seanAdminUnlocked,true);assert.equal(h.$('adminPassword').value,'');
});
test('dashboard includes every registered account and handles missing lastLogin',async()=>{
  const h=adminHarness();h.ctx.seanAdminUnlocked=true;
  h.ctx.getDocs=async()=>({forEach:callback=>{for(let i=0;i<85;i++)callback({id:'Player'+i,data:()=>({username:'Player'+i})});}});
  await h.ctx.loadAdminMembers();assert.equal(h.ctx.adminMembers.length,85);assert.equal(h.ctx.adminMembers[0].lastLogin,null);
});
test('locking while a member request is in flight prevents stale results from reappearing',async()=>{
  const h=adminHarness();h.ctx.seanAdminUnlocked=true;
  let resolve;h.ctx.getDocs=()=>new Promise(r=>resolve=r);
  const pending=h.ctx.loadAdminMembers();h.ctx.lockAdminMembers();
  resolve({forEach:callback=>callback({id:'Private',data:()=>({username:'Private'})})});await pending;
  assert.equal(h.ctx.adminMembers.length,0);assert.equal(h.ctx.seanAdminUnlocked,false);assert.equal(h.$('adminMembersContent').hidden,true);
});
test('locking cancels an in-flight password verification',async()=>{
  const h=adminHarness();let resolve;h.ctx.getDoc=()=>new Promise(r=>resolve=r);
  h.$('adminPassword').value='unique-secret';const pending=h.ctx.unlockAdminMembers();h.ctx.lockAdminMembers();
  resolve({exists:()=>true,data:()=>({password:'unique-secret'})});await pending;
  assert.equal(h.ctx.seanAdminUnlocked,false);
});
