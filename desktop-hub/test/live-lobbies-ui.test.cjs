'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const { test } = require('node:test');
const vm = require('node:vm');
const filename = require.resolve('../ui/hub.js');
const source = fs.readFileSync(filename, 'utf8');

function createUI(results) {
  let clock = 1000;
  let timerId = 0;
  const timers = new Map(), events = new Map(), commands = [];
  const list = { innerHTML:'', contains:() => false };
  const status = { textContent:'' };
  const select = { id:'lobby-game', value:'', innerHTML:'', dataset:{} };
  const search = { id:'lobby-search', value:'', closest:() => null };
  const selectors = {
    '#view':{}, '#dialog':{}, '#search':{ addEventListener() {} },
    '#public-servers':list, '#public-servers-status':status, '#lobby-game':select, '#lobby-search':search,
    '#status':{ classList:{ toggle() {}, remove() {} } },
  };
  const context = vm.createContext({
    document:{
      querySelector:selector => selectors[selector],
      querySelectorAll:() => [],
      addEventListener:(name,callback) => events.set(name,callback),
    },
    window:{
      addEventListener() {},
      hub:{ invoke(action,payload) {
        if (action === 'state') return new Promise(() => {});
        commands.push([action,payload]);
        if (action === 'publicServers') return typeof results === 'function' ? results() : Promise.resolve(results);
        return Promise.resolve();
      } },
    },
    icon:() => '',
    Date:{ now:() => clock },
    setTimeout(callback,delay) {
      const id = ++timerId;
      timers.set(id,{ callback,delay });
      return id;
    },
    clearTimeout:id => timers.delete(id),
  });
  const api = vm.runInContext(source + '\n;({ lobbies, loadPublicServers, onAction, stopPublicServers });',context,{filename});
  return {
    ...api, list, status, select, timers, commands,
    filter(game) { select.value=game; events.get('change')({target:select}); },
    search(query) { search.value=query; events.get('input')({target:search}); },
    async tick(delay,time) {
      clock=time;
      const timer = [...timers].find(([,entry]) => entry.delay === delay);
      assert.ok(timer,'The timer should be scheduled');
      timers.delete(timer[0]);
      await timer[1].callback();
    },
    close() {
      delete selectors['#lobby-game'];
      delete selectors['#public-servers'];
      api.stopPublicServers();
    },
  };
}

const game = (id,title,rooms) => ({id,title,rooms,failed:false});
const room = (code,name,phase,players,capacity,expires=9000,joinable=true) =>
  ({code,name,phase,players,capacity,expires,joinable});

test('each launcher lobby shows its game, server, players, match status, and join action', async () => {
  const ui = createUI([
    game('acornrally','Acorn Rally',[room('1212','Race Night','racing',3,8)]),
    game('cat','Cat & Mouse',[room('0004','Hearth <Cup>','playing',4,4,9000,false)]),
    game('bannerlands','Bannerlands',[room('5912','Practice','waiting',0,8)]),
    game('lastsignal','Last Signal',[room('4481','Final Arena','finished',2,8)]),
  ]);

  assert.match(ui.lobbies(), /id="invite-form"/);
  await ui.loadPublicServers();
  assert.match(ui.list.innerHTML, /Acorn Rally.*Race Night[\s\S]+3 \/ 8 players[\s\S]+Racing[\s\S]+data-code="1212"/);
  assert.match(ui.list.innerHTML, /Cat &amp; Mouse.*Hearth &lt;Cup&gt;[\s\S]+4 \/ 4 players[\s\S]+In game[\s\S]+Check for seat/);
  assert.match(ui.list.innerHTML, /Bannerlands.*Practice[\s\S]+0 \/ 8 players[\s\S]+In lobby/);
  assert.match(ui.list.innerHTML, /Last Signal.*Final Arena[\s\S]+2 \/ 8 players[\s\S]+Match finished/);
  assert.match(ui.select.innerHTML, /<option value="cat"[^>]*>Cat &amp; Mouse<\/option>/);
  assert.equal(ui.status.textContent,'4 servers');
  ui.onAction({dataset:{ action:'server-list-join', id:'cat', code:'0004', expires:'9000' }});
  assert.equal(ui.commands.at(-1)[0],'joinPublicServer');
  assert.equal(ui.commands.at(-1)[1].id,'cat');
  assert.equal(ui.commands.at(-1)[1].code,'0004');
});

test('game and search controls combine without fetching the directory again', async () => {
  const ui = createUI([
    game('cat','Cat & Mouse',[room('0092','Club Café','waiting',1,4),room('1010','Other','waiting',1,4)]),
    game('lastsignal','Last Signal',[room('3382','Club Cafe','playing',4,8)]),
  ]);
  await ui.loadPublicServers();
  ui.filter('cat');
  ui.search('cafe');
  assert.match(ui.list.innerHTML, /Club Café/);
  assert.doesNotMatch(ui.list.innerHTML, /Last Signal|Other/);
  assert.equal(ui.status.textContent,'1 server');
  ui.search('cat');
  assert.match(ui.list.innerHTML, /Other/);
  assert.equal(ui.status.textContent,'2 servers');
  ui.search('moon');
  assert.equal(ui.status.textContent,'No public servers match these filters.');
  assert.equal(ui.commands.length,1);
});

test('expired entries disappear and polling stops when leaving the launcher view', async () => {
  const ui = createUI([
    game('acornrally','Acorn Rally',[room('0150','Expiring host','waiting',1,8,1200)]),
  ]);
  await ui.loadPublicServers();
  assert.match(ui.list.innerHTML,/Expiring host/);
  await ui.tick(200,1201);
  assert.equal(ui.list.innerHTML,'');
  assert.equal(ui.status.textContent,'No public servers are online.');
  await ui.tick(30000,31000);
  assert.equal(ui.commands.length,2);
  ui.close();
  assert.equal(ui.timers.size,0);
});

test('a late directory response cannot restart polling on another launcher page', async () => {
  let resolve;
  const ui = createUI(() => new Promise(done => { resolve = done; }));
  const pending = ui.loadPublicServers();
  await ui.loadPublicServers();
  assert.equal(ui.commands.length,1);
  ui.close();
  resolve([game('acornrally','Acorn Rally',[])]);
  await pending;
  assert.equal(ui.timers.size,0);
});
