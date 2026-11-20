'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const Module = require('node:module');
const { EventEmitter } = require('node:events');
const { test } = require('node:test');
const { managedInvite } = require('../desktop/managed-invite.cjs');
const { configureSaveSession } = require('../desktop/save-sync.cjs');

function windowsHarness() {
  const views = [], sessions = [], external = [], windows = [];
  let nextId = 1;
  class Contents extends EventEmitter {
    constructor() { super(); this.id = nextId++; this.url = ''; }
    async loadURL(url) { this.url = url; }
    getURL() { return this.url; }
    setWindowOpenHandler(callback) { this.popup = callback; }
    isDestroyed() { return false; }
    close() {}
    send() {}
  }
  class BrowserWindow extends EventEmitter {
    constructor() { super(); this.webContents = new Contents(); this.contentView = { addChildView() {} }; windows.push(this); }
    getContentSize() { return [1200,800]; }
    isFullScreen() { return false; }
    isDestroyed() { return false; }
    loadFile(_file,options) { this.toolbar=options.query; return Promise.resolve(); }
    close() { this.emit('closed'); }
    focus() { this.focused=true; }
    show() {}
  }
  class WebContentsView {
    constructor({webPreferences}) { this.webContents = new Contents(); this.prefs = webPreferences; views.push(this); }
    setBounds() {}
  }
  const electron = {
    BrowserWindow, WebContentsView, ipcMain: new EventEmitter(), dialog: {},
    shell: { async openExternal(url) { external.push(url); } },
    session: { fromPartition(partition) {
      const entry = { partition, webRequest:{ onBeforeRequest(callback) { entry.filter=callback; } },
        setPermissionRequestHandler(callback) { entry.permit=callback; }, setPermissionCheckHandler() {} };
      sessions.push(entry);
      return entry;
    } },
  };
  const source = require.resolve('../desktop/windows.cjs');
  const instance = new Module(source,module);
  const original = Module.createRequire(source);
  instance.require = name => name === 'electron' ? electron : original(name);
  instance._compile(fs.readFileSync(source,'utf8'),source);
  const open = instance.exports.createGames({ library:new BrowserWindow(),root:path.dirname(path.dirname(source)),resolveGame() {} });
  return { open, views, sessions, external, windows, ipc:electron.ipcMain };
}

function navigate(wc, target, event='will-navigate') {
  let prevented = false;
  wc.emit(event,{preventDefault(){prevented=true;}},target);
  return !prevented;
}
function allowed(session,url,resourceType='xhr',method='GET') {
  let permitted;
  session.filter({url, resourceType, method},({cancel})=>{permitted=!cancel;});
  return permitted;
}

test('desktop game handoffs open only scoped multiplayer invitations in an isolated guest session', async () => {
  const origin='https://chosen-host.trycloudflare.com', join=origin+'/__catnmice/join';
  for (const id of ['bannerlands','trashtactics','cat','acornrally']) {
    const { open,views,sessions,external,windows,ipc }=windowsHarness();
    const local='http://127.0.0.1:43125/', localSession={persisted:true};
    let hostStarts=0;
    const pin=id==='acornrally'?origin+'/?room=join#join=1&name=Player+1&code=0042':join+'?name=Player+1#code=0042';
    const pub=id==='acornrally'?origin+'/?room=join#join=1':join;
    const legacy=origin+'/#room=abcdef012345';
    await open({url:local+'#desktop=private-local-token',game:{id,title:id},title:id,offline:true,session:localSession,
      localHost:true,requestHost:id==='trashtactics'?async()=>{hostStarts++;}:undefined,
      guestInvite:id==='cat'?(value=>value===legacy?value:null):undefined});
    const wc=views[0].webContents;
    assert.ok(navigate(wc,local+'settings'));
    assert.equal(navigate(wc,pin,'will-redirect'),false);
    assert.equal(navigate(wc,'https://untrusted.example/__catnmice/join'),false);
    assert.equal(navigate(wc,origin+'/arbitrary'),false);
    assert.equal(navigate(wc,'https://user:pass@chosen-host.trycloudflare.com/__catnmice/join'),false);
    assert.equal(views.length,1);
    assert.equal(navigate(wc,pin),false);
    await new Promise(resolve=>setImmediate(resolve));
    assert.equal(views.length,2);
    const remote=views[1].webContents,guest=sessions[0];
    assert.equal(views[1].prefs.session,guest);
    assert.notEqual(guest,localSession);
    assert.ok(!guest.partition.startsWith('persist:'));
    assert.equal(remote.getURL(),pin);
    assert.equal(wc.getURL(),local+'#desktop=private-local-token');
    assert.ok(navigate(remote,origin+'/index.html','will-redirect'));
    assert.equal(navigate(remote,local+'__catnmice/start-host'),false);
    assert.equal(navigate(remote,'https://other-host.trycloudflare.com/','will-redirect'),false);
    assert.equal(navigate(remote,'https://user@chosen-host.trycloudflare.com/'),false);
    assert.equal(allowed(guest,local+'api/desktop/control'),false);
    assert.equal(allowed(guest,origin+'/game.js','script'),true);
    assert.equal(allowed(guest,'wss://chosen-host.trycloudflare.com/socket','webSocket'),true);
    assert.equal(allowed(guest,'wss://evil.example/socket','webSocket'),false);
    assert.equal(allowed(guest,'file:///C:/private','mainFrame'),false);
    assert.equal(hostStarts,0);
    assert.equal(external.length,0);
    assert.deepEqual(wc.popup({url:pub}),{action:'deny'});
    await new Promise(resolve=>setImmediate(resolve));
    assert.equal(views[2].webContents.getURL(),pub);
    assert.notEqual(sessions[1].partition,guest.partition);
    if(id==='cat') {
      assert.equal(navigate(wc,legacy),false);
      assert.deepEqual(external,[legacy]);
    }
    assert.equal(windows[2].toolbar.returnLabel,'Back to '+id);
    ipc.emit('game-control',{sender:windows[2].webContents},'back');
    assert.equal(windows[1].focused,true);
    if(id==='trashtactics') {
      assert.equal(navigate(wc,local+'__catnmice/start-host'),false);
      assert.equal(hostStarts,1);
    }
  }
});

test('invite validation keeps game-specific routes, leading zeroes and bounded names', () => {
  const base='https://one-host.trycloudflare.com/__catnmice/join';
  for(const suffix of ['#code=12','#code=0007&code=7777','?host=1','?name='+('a'.repeat(23)),'?name=a%0Db']) {
    assert.equal(managedInvite('cat',base+suffix),null);
  }
  assert.equal(managedInvite('acornrally',base),null);
  assert.equal(managedInvite('other',base),null);
  const official='https://rally.catnmice.com/?room=join#join=1';
  assert.equal(managedInvite('acornrally',official),official);
  assert.equal(managedInvite('bannerlands',official),null);
});

test('Acorn desktop discovery can read its listing and probe a host while retaining offline save isolation', () => {
  const directory='https://firestore.googleapis.com/v1/projects/bible-game-246c0/databases/(default)/documents';
  const local='http://127.0.0.1:43125', origin='https://one-host.trycloudflare.com';
  const ses={webRequest:{onBeforeRequest(callback){ses.filter=callback;}}};
  const close=configureSaveSession(ses,{gameId:'acornrally',origin:local});
  assert.ok(allowed(ses,directory+':runQuery','xhr','POST'));
  for(const url of [directory+'/acornRallyService/public',directory+'/acornRallyLobbies/0009',origin+'/api/rooms/status']) {
    assert.ok(allowed(ses,url));
    assert.equal(allowed(ses,url,'mainFrame'),false);
    assert.equal(allowed(ses,url,'xhr','PATCH'),false);
  }
  assert.equal(allowed(ses,origin+'/api/desktop/control'),false);
  assert.equal(allowed(ses,directory+'/unrelated/game'),false);
  assert.equal(allowed(ses,directory+':commit','xhr','POST'),false);
  close();
  assert.equal(allowed(ses,origin+'/api/rooms/status'),false);
  configureSaveSession(ses,{gameId:'other',origin:local});
  assert.equal(allowed(ses,directory+':runQuery','xhr','POST'),false);
});
