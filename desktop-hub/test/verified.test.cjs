const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {EventEmitter} = require('node:events');
const {createRequire} = require('node:module');

const filename = require.resolve('../desktop/verified.cjs');
const src = fs.readFileSync(filename,'utf8');
const localRequire = createRequire(filename);
const site = 'https://www.catnmice.com';
const api = 'https://hub.catnmice.com/v1/desktop/';
const sessionUrl = site+'/hub/desktop-session.html';
const username = 'QaPlayer';
const token = 'TEST-ONLY-CUSTOM-CREDENTIAL';
const partition = 'persist:isolated-test-profile';

// Load the unmodified native module with an in-memory Electron surface and
// injected fetch. Polling advances a virtual clock and never waits on a network.
async function fixture(options = {}) {
  const {createDesktopAuthorization} = await import('../services/desktop-authorization.mjs');
  const urls=[],requests=[],logs=[],windows=[],completions=[];
  let now=Date.UTC(2030,0,1),ticket,mints=0;
  class Clock extends Date {static now(){return now;}}
  const service=createDesktopAuthorization({
    now:()=>now,
    mint:async identity=>{
      assert.equal(identity.username,username);
      mints++;
      return token;
    }
  });
  class Window extends EventEmitter {
    constructor(prefs) {
      super();this.prefs=prefs;this.closed=false;
      const wc=new EventEmitter();this.webContents=wc;
      wc.setWindowOpenHandler=handler=>{this.openHandler=handler;};
      wc.loadURL=async url=>{urls.push(url);this.loaded=options.changedOrigin?'https://outside.invalid/':url;};
      wc.getURL=()=>this.loaded;
      wc.executeJavaScript=async code=>{
        if(prefs.show!==false) {
          const match=/^document\.getElementById\("code"\)\.textContent=(.+)$/.exec(code);
          assert.ok(match,'Prompt sets only its confirmation code.');
          this.displayCode=JSON.parse(match[1]);
          return;
        }
        if(code==='typeof window.catnmiceCompleteDesktopSignIn==="function"') return true;
        const match=/^window\.catnmiceCompleteDesktopSignIn\((.+)\)$/.exec(code);
        assert.ok(match,'The hidden session calls the official completion function.');
        assert.equal(this.loaded,sessionUrl);
        const payload=JSON.parse(match[1]);
        completions.push(payload);
        if(options.sessionError) throw Error('Private fixture details '+token);
        return {username:options.otherUsername?'DifferentPlayer':payload.username};
      };
      windows.push(this);
    }
    async loadFile(file){this.file=file;}
    isDestroyed(){return this.closed;}
    destroy(){if(this.closed)return;this.closed=true;this.emit('closed');}
  }
  const shell={openExternal:async url=>{
    urls.push(url);
    if(options.closePrompt) windows[0].destroy();
    if(options.deny) service.deny(ticket.id,ticket.code);
  }};
  const fetch=async (url,init)=>{
    const operation=url.slice(api.length);
    assert.equal(url,api+operation);
    assert.equal(init.method,'POST');
    assert.equal(init.redirect,'error');
    assert.equal(init.credentials,'omit');
    assert.deepEqual(Object.keys(init.headers),['Content-Type']);
    urls.push(url);
    const body=JSON.parse(init.body);
    requests.push({operation,body});
    try {
      if(operation==='start'){
        ticket=service.start(body.challenge,'qa-local');
        // No server-provided authorization target can redirect the player.
        return Response.json({...ticket,verificationUrl:'https://outside.invalid/authorize',...(options.malformed?{expiresAt:new Clock(now+1000).toISOString()}: {})},{status:201});
      }
      if(operation==='redeem') {
        if(options.malformed)return Response.json({state:'pending',username,customToken:token},{status:202});
        const count=requests.filter(r=>r.operation==='redeem').length;
        if(count===2) service.approve(ticket.id,windows[0].displayCode,{uid:'fixture-uid',username});
        const response=await service.redeem(body.id,body.verifier);
        return Response.json(response,{status:response.state==='pending'?202:200});
      }
      if(operation==='cancel')return Response.json(service.cancel(body.id,body.verifier));
      throw Error('Unknown request '+operation);
    } catch(error){
      if(!error.status)throw error;
      return Response.json({error:'Authorization refused'},{status:error.status});
    }
  };
  const modules={
    electron:{BrowserWindow:Window,shell},
    'node:timers/promises':{setTimeout:async (ms,value,{signal})=>{signal.throwIfAborted();now+=ms;return value;}}
  };
  const context={exports:{}};
  vm.compileFunction(src,['module','require','fetch','Date','console'],{filename})(
    context,name=>modules[name]||localRequire(name),fetch,Clock,
    Object.fromEntries(['log','info','warn','error','debug'].map(method=>[method,(...args)=>logs.push(args.join(' '))]))
  );
  const invoke=()=>context.exports.authorizeDesktop(path.dirname(path.dirname(filename)),partition);
  return {invoke,windows,urls,requests,completions,logs,get mints(){return mints;},get ticket(){return ticket;}};
}
function checkNoCredentialDisclosure(env,outcome) {
  const verifier=env.requests.find(r=>r.body.verifier)?.body.verifier;
  assert.ok(verifier);
  const visible=[...env.urls,...env.logs,String(outcome)].join('\n');
  for(const secret of [token,verifier,env.ticket.code]) assert.ok(!visible.includes(secret),'Authorization material escaped into a URL, log or return value.');
}
test('native handoff redeems a matching verifier only after explicit browser approval and exact account confirmation',async()=>{
  const env=await fixture();
  const result=await env.invoke();
  assert.equal(result,username);
  assert.deepEqual(env.requests.map(r=>r.operation),['start','redeem','redeem']);
  const [start,first,second]=env.requests.map(r=>r.body);
  assert.match(first.verifier,/^[A-Za-z0-9_-]{43}$/);
  assert.equal(second.verifier,first.verifier);
  assert.equal(start.challenge,crypto.createHash('sha256').update(first.verifier,'ascii').digest('base64url'));
  assert.equal(env.mints,1);
  assert.deepEqual(env.completions,[{username,customToken:token}]);
  const external=env.urls.find(url=>new URL(url).pathname==='/hub/authorize-desktop.html');
  assert.equal(external,site+'/hub/authorize-desktop.html#request='+env.ticket.id);
  assert.ok(env.urls.every(url=>url.startsWith(api)||url===external||url===sessionUrl));
  const session=env.windows[1];
  assert.equal(session.prefs.webPreferences.partition,partition);
  assert.deepEqual(session.openHandler(),{action:'deny'});
  const navigationBlocked=target=>{let blocked=false;session.webContents.emit('will-navigate',{preventDefault:()=>{blocked=true;}},target);return blocked;};
  assert.equal(navigationBlocked(sessionUrl),false);
  assert.equal(navigationBlocked('https://outside.invalid/hub/desktop-session.html'),true);
  assert.equal(navigationBlocked(sessionUrl+'?forward=1'),true);
  let redirected=false;session.webContents.emit('will-redirect',{preventDefault:()=>{redirected=true;}});assert.ok(redirected);
  assert.ok(env.windows.every(w=>w.isDestroyed()));
  checkNoCredentialDisclosure(env,result);
});
for(const [name,options,expected] of [
  ['closing the launcher confirmation cancels the request',{closePrompt:true},/canceled/],
  ['browser denial consumes the request without a session',{deny:true},/expired or was declined/],
  ['pending responses with credentials cannot authorize a session',{malformed:true},/Authorization did not complete/],
  ['the session origin must remain exactly the official session page',{changedOrigin:true},/could not open its account session/],
  ['the confirmed username must match the approved username',{otherUsername:true},/did not confirm the selected profile/],
  ['session errors with private details are redacted',{sessionError:true},/could not verify this device session/]
]) {
  test(name,async()=>{
    const env=await fixture(options);
    let message='';
    await assert.rejects(env.invoke(),error=>{message=error.message;return expected.test(message);});
    assert.ok(env.requests.some(r=>r.operation==='cancel'));
    assert.ok(env.windows.every(w=>w.isDestroyed()));
    if(options.closePrompt||options.deny||options.malformed||options.changedOrigin)assert.equal(env.completions.length,0);
    if(options.closePrompt||options.deny||options.malformed)assert.equal(env.mints,0);
    checkNoCredentialDisclosure(env,message);
  });
}
