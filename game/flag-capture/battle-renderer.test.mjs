import test from 'node:test';
import assert from 'node:assert/strict';
import {patchBattleMarkup} from './battle-renderer.mjs';

// A deliberately small DOM facade. Native template parsing is supplied as a
// prebuilt fragment so these tests exercise identity and movement, not a parser.
class Node {
  constructor(type,name,document,value=null){this.nodeType=type;this.nodeName=name;this.ownerDocument=document;this.nodeValue=value;this.childNodes=[];this.parentNode=null;this.attrs=new Map();this.namespaceURI=type===1?'http://www.w3.org/1999/xhtml':null;}
  get firstChild(){return this.childNodes[0]||null;}
  get nextSibling(){if(!this.parentNode)return null;return this.parentNode.childNodes[this.parentNode.childNodes.indexOf(this)+1]||null;}
  get attributes(){return [...this.attrs].map(([name,value])=>({name,value}));}
  get outerHTML(){if(this.nodeType===3)return this.nodeValue;const attrs=[...this.attrs].map(([name,value])=>` ${name}="${value}"`).join('');return `<${this.nodeName}${attrs}>${this.childNodes.map(node=>node.outerHTML).join('')}</${this.nodeName}>`;}
  get open(){return this.hasAttribute('open');}
  get isConnected(){let node=this;while(node){if(node.nodeType===9)return true;node=node.parentNode;}return false;}
  getAttribute(name){this.ownerDocument.reads++;return this.attrs.has(name)?this.attrs.get(name):null;}
  hasAttribute(name){return this.attrs.has(name);}
  setAttribute(name,value){this.attrs.set(name,String(value));this.ownerDocument.writes++;}
  removeAttribute(name){this.attrs.delete(name);this.ownerDocument.writes++;}
  contains(node){return node===this||this.childNodes.some(child=>child.contains(node));}
  appendChild(node){return this.insertBefore(node,null);}
  insertBefore(node,reference){
    if(node===reference)return node;
    if(reference&&reference.parentNode!==this)throw new Error('Invalid reference');
    if(node.parentNode)node.parentNode.removeChild(node);
    const index=reference?this.childNodes.indexOf(reference):this.childNodes.length;
    this.childNodes.splice(index,0,node);node.parentNode=this;return node;
  }
  moveBefore(node,reference){
    if(node===reference)return;
    if(reference&&reference.parentNode!==this)throw new Error('Invalid reference');
    if(node.parentNode){const source=node.parentNode;source.childNodes.splice(source.childNodes.indexOf(node),1);}
    const index=reference?this.childNodes.indexOf(reference):this.childNodes.length;
    this.childNodes.splice(index,0,node);node.parentNode=this;this.ownerDocument.preservedMoves++;
  }
  removeChild(node){
    const index=this.childNodes.indexOf(node);if(index<0)throw new Error('Not a child');
    if(node.contains(this.ownerDocument.activeElement))this.ownerDocument.activeElement=null;
    this.childNodes.splice(index,1);node.parentNode=null;return node;
  }
  cloneNode(deep=false){const clone=new Node(this.nodeType,this.nodeName,this.ownerDocument,this.nodeValue);clone.attrs=new Map(this.attrs);if(deep)this.childNodes.forEach(child=>clone.appendChild(child.cloneNode(true)));return clone;}
  focus(options){this.ownerDocument.activeElement=this;this.ownerDocument.focusCalls.push(options);}
}
class Document extends Node {
  constructor(){super(9,'#document',null);this.ownerDocument=this;this.writes=0;this.reads=0;this.preservedMoves=0;this.activeElement=null;this.focusCalls=[];this.nextTemplate=null;}
  createElement(name){if(name!=='template')return new Node(1,name.toUpperCase(),this);const template={content:this.nextTemplate};Object.defineProperty(template,'innerHTML',{set:value=>{this.parsedHTML=value;}});return template;}
}
const text=(document,value)=>new Node(3,'#text',document,value);
function el(document,name,attrs={},...contents){const node=new Node(1,name.toUpperCase(),document);for(const [key,value]of Object.entries(attrs))node.setAttribute(key,value);for(const child of contents)node.appendChild(typeof child==='string'?text(document,child):child);return node;}
function fragment(document,...nodes){const node=new Node(11,'#fragment',document);nodes.forEach(child=>node.appendChild(child));return node;}
function setup(...nodes){const document=new Document(),container=el(document,'main',{id:'app'});document.appendChild(container);nodes.forEach(make=>container.appendChild(make(document)));return{document,container};}
function patch(fixture,...nodes){fixture.document.nextTemplate=fragment(fixture.document,...nodes.map(make=>make(fixture.document)));fixture.document.writes=0;return patchBattleMarkup(fixture.container,'<battle>native parser fixture</battle>');}
function find(node,predicate){if(predicate(node))return node;for(const child of node.childNodes){const result=find(child,predicate);if(result)return result;}return null;}
const keyed=(node,name,value)=>find(node,child=>child.getAttribute?.(name)===value);
const token=(document,id,label=id)=>el(document,'span',{'data-unit-token':id,class:'unit-token'},el(document,'span',{class:'sprite'},label));
const cell=(document,id,...nodes)=>el(document,'button',{'data-cell':id,class:'tile'},...nodes);
const board=(document,...nodes)=>el(document,'div',{class:'board'},...nodes);
const command=(document,side,index=0,label='Ready')=>el(document,'button',{'data-mode':'ability','data-side':side,'data-skill-index':index,class:'dock-command'},label);
const dock=(document,side,...nodes)=>el(document,'section',{class:'command-dock','data-side':side},...nodes);

test('unchanged battle markup retains all node identities without attribute writes',()=>{
  const make=document=>el(document,'section',{class:'compact-battle'},el(document,'div',{class:'compact-shell'},board(document,cell(document,'0,0',token(document,'u1'))),dock(document,'player',command(document,'player'))));
  const fixture=setup(make),old=[fixture.container.firstChild,keyed(fixture.container,'data-cell','0,0'),keyed(fixture.container,'data-unit-token','u1'),keyed(fixture.container,'data-mode','ability')];
  const stats=patch(fixture,make);
  assert.deepEqual([fixture.container.firstChild,keyed(fixture.container,'data-cell','0,0'),keyed(fixture.container,'data-unit-token','u1'),keyed(fixture.container,'data-mode','ability')],old);
  assert.equal(stats.created,0);assert.equal(stats.removed,0);assert.ok(stats.reused>8);assert.equal(fixture.document.writes,0);assert.ok(stats.ms>=0);
});

test('health text and attributes update in place while transient animation state survives',()=>{
  const fixture=setup(document=>el(document,'button',{'data-cell':'1,1',class:'tile selected moving flash queued','data-node-id':'measured-8','data-stale':'remove'},'10 HP'));
  const node=fixture.container.firstChild,oldText=node.firstChild;
  patch(fixture,document=>el(document,'button',{'data-cell':'1,1',class:'tile damaged','data-node-id':'new-id','aria-label':'7 health'},'7 HP'));
  assert.equal(fixture.container.firstChild,node);assert.equal(node.firstChild,oldText);assert.equal(oldText.nodeValue,'7 HP');
  assert.equal(node.getAttribute('data-node-id'),'measured-8');assert.equal(node.getAttribute('data-stale'),null);assert.equal(node.getAttribute('aria-label'),'7 health');
  const names=new Set(node.getAttribute('class').split(' '));for(const name of ['moving','flash','queued','damaged'])assert.ok(names.has(name));assert.equal(names.has('selected'),false);
});

test('unit tokens move between globally keyed cells without being discarded by source cleanup',()=>{
  const fixture=setup(document=>board(document,cell(document,'0,0',token(document,'u1')),cell(document,'1,0',token(document,'u2'))));
  const a=keyed(fixture.container,'data-unit-token','u1'),b=keyed(fixture.container,'data-unit-token','u2');a.animationState={progress:.42};
  const stats=patch(fixture,document=>board(document,cell(document,'0,0',token(document,'u2')),cell(document,'1,0',token(document,'u1'))));
  assert.equal(keyed(fixture.container,'data-cell','0,0').firstChild,b);assert.equal(keyed(fixture.container,'data-cell','1,0').firstChild,a);
  assert.equal(a.animationState.progress,.42);assert.ok(a.isConnected);assert.equal(stats.created,0);assert.equal(stats.removed,0);assert.ok(fixture.document.preservedMoves>=2);
});

test('later destinations can claim tokens whose old earlier cell has no unit',()=>{
  const fixture=setup(document=>board(document,cell(document,'0,0',token(document,'u1')),cell(document,'1,0'))),unit=keyed(fixture.container,'data-unit-token','u1');
  patch(fixture,document=>board(document,cell(document,'0,0'),cell(document,'1,0',token(document,'u1'))));
  assert.equal(keyed(fixture.container,'data-cell','1,0').firstChild,unit);assert.equal(keyed(fixture.container,'data-cell','0,0').childNodes.length,0);
});

test('top and bottom docks retain each exact special button and current focus across reordering',()=>{
  const fixture=setup(document=>dock(document,'player',command(document,'player',0),command(document,'player',2)),document=>dock(document,'ai',command(document,'ai',0)));
  const p1=keyed(fixture.container,'data-side','player'),secondary=p1.childNodes[1],p2=keyed(fixture.container,'data-side','ai');secondary.focus({preventScroll:true});
  patch(fixture,document=>dock(document,'ai',command(document,'ai',0)),document=>dock(document,'player',command(document,'player',2),command(document,'player',0)));
  assert.equal(fixture.container.firstChild,p2);assert.equal(fixture.container.childNodes[1],p1);assert.equal(p1.firstChild,secondary);assert.equal(fixture.document.activeElement,secondary);
});

test('standard DOM movement restores focused controls when state-preserving moves are unavailable',()=>{
  const fixture=setup(document=>dock(document,'player',command(document,'player',0),command(document,'player',1)));
  const parent=fixture.container.firstChild,active=parent.childNodes[1];parent.moveBefore=undefined;active.focus({preventScroll:true});
  patch(fixture,document=>dock(document,'player',command(document,'player',1),command(document,'player',0)));
  assert.equal(fixture.document.activeElement,active);assert.deepEqual(fixture.document.focusCalls.at(-1),{preventScroll:true});
});

test('a reused key never reuses a node with a mismatched element type',()=>{
  const fixture=setup(document=>el(document,'button',{'data-command':'menu'},'Menu')),old=fixture.container.firstChild;
  const stats=patch(fixture,document=>el(document,'div',{'data-command':'menu'},'Menu'));
  assert.notEqual(fixture.container.firstChild,old);assert.equal(fixture.container.firstChild.nodeName,'DIV');assert.equal(stats.created,2);assert.equal(stats.removed,2);
});

test('open native dialogs preserve live contents and cannot have their controls stolen by duplicate keys',()=>{
  const fixture=setup(document=>el(document,'dialog',{id:'victory',open:'',class:'result-card'},el(document,'button',{'data-command':'help'},'Live choice')));
  const dialog=fixture.container.firstChild,live=dialog.firstChild;live.focus({preventScroll:true});
  patch(fixture,document=>el(document,'dialog',{id:'victory',class:'result-card refreshed'},el(document,'button',{'data-command':'help'},'Replacement')),document=>el(document,'button',{'data-command':'help'},'Outside help'));
  assert.equal(fixture.container.firstChild,dialog);assert.equal(dialog.open,true);assert.equal(dialog.firstChild,live);assert.equal(live.firstChild.nodeValue,'Live choice');assert.equal(fixture.document.activeElement,live);
  assert.notEqual(fixture.container.childNodes[1],live);assert.equal(fixture.container.childNodes[1].firstChild.nodeValue,'Outside help');
});

test('external body effects stay outside the patch and old removed nodes are counted once',()=>{
  const fixture=setup(document=>board(document,cell(document,'0,0',token(document,'u1')))),outside=el(fixture.document,'svg',{id:'combat-effect'});
  fixture.document.appendChild(outside);const oldNodes=[];const walk=node=>{node.childNodes.forEach(child=>{oldNodes.push(child);walk(child);});};walk(fixture.container);
  const stats=patch(fixture,document=>el(document,'p',{},'Done'));
  assert.equal(outside.parentNode,fixture.document);assert.equal(outside.getAttribute('id'),'combat-effect');assert.equal(stats.removed,oldNodes.length);
});

test('duplicate new keys do not alias the same old control into two places',()=>{
  const fixture=setup(document=>command(document,'player',0)),old=fixture.container.firstChild;
  patch(fixture,document=>command(document,'player',0),document=>command(document,'player',0));
  assert.equal(fixture.container.childNodes.length,2);assert.equal(fixture.container.firstChild,old);assert.notEqual(fixture.container.childNodes[0],fixture.container.childNodes[1]);
});

test('live tile numbers and burst rings finish their own lifecycle across health updates',()=>{
  const fixture=setup(document=>cell(document,'0,0',token(document,'u1'),el(document,'span',{class:'effect heal'},'+3'),el(document,'i',{class:'burst'})));
  const parent=fixture.container.firstChild,effect=parent.childNodes[1],burst=parent.childNodes[2];
  patch(fixture,document=>cell(document,'0,0',token(document,'u1','7 HP')));
  assert.equal(effect.parentNode,parent);assert.equal(burst.parentNode,parent);assert.equal(effect.firstChild.nodeValue,'+3');
  parent.removeChild(effect);parent.removeChild(burst);
  patch(fixture,document=>cell(document,'0,0',token(document,'u1','7 HP')));
  assert.equal(parent.childNodes.length,1);assert.equal(effect.parentNode,null);
});

test('floating effect spans cannot be repurposed as ordinary new UI spans',()=>{
  const fixture=setup(document=>cell(document,'0,0',el(document,'span',{class:'effect damage'},'−2'))),effect=fixture.container.firstChild.firstChild;
  patch(fixture,document=>cell(document,'0,0',el(document,'span',{class:'new-health'},'5 HP')));
  const parent=fixture.container.firstChild;
  assert.equal(parent.childNodes.length,2);assert.notEqual(parent.firstChild,effect);assert.equal(parent.firstChild.getAttribute('class'),'new-health');assert.equal(effect.firstChild.nodeValue,'−2');
});

test('cached tile artwork skips repeat attribute walks while live effects and focus survive',()=>{
  const make=document=>board(document,cell(document,'0,0',token(document,'u1')),cell(document,'1,0',token(document,'u2')));
  const fixture=setup(make);fixture.document.reads=0;patch(fixture,make);const firstReads=fixture.document.reads;
  const parent=keyed(fixture.container,'data-cell','0,0'),effect=el(fixture.document,'span',{class:'effect heal'},'+2');parent.appendChild(effect);parent.focus();
  fixture.document.reads=0;const stats=patch(fixture,make),repeatReads=fixture.document.reads;
  assert.ok(repeatReads<firstReads*.75,`Expected cached artwork to reduce reads: ${repeatReads} / ${firstReads}`);
  assert.equal(effect.parentNode,parent);assert.equal(fixture.document.activeElement,parent);assert.equal(stats.created,0);assert.equal(stats.removed,0);
});

test('a previously cached source cannot hide a token claimed by an earlier destination',()=>{
  const make=document=>board(document,cell(document,'0,0'),cell(document,'1,0',token(document,'u1'))),fixture=setup(make);
  patch(fixture,make);const unit=keyed(fixture.container,'data-unit-token','u1');
  patch(fixture,document=>board(document,cell(document,'0,0',token(document,'u1')),cell(document,'1,0',token(document,'u1'))));
  const first=keyed(fixture.container,'data-cell','0,0'),second=keyed(fixture.container,'data-cell','1,0');
  assert.equal(first.firstChild,unit);assert.ok(second.firstChild);assert.notEqual(first.firstChild,second.firstChild);
});

test('native keyed indexing preserves token moves and excludes live modal descendants',()=>{
  const make=document=>el(document,'section',{class:'compact-battle'},board(document,cell(document,'0,0',token(document,'u1')),cell(document,'1,0')),el(document,'dialog',{id:'victory',open:''},command(document,'player',0,'Live modal')));
  const fixture=setup(make),unit=keyed(fixture.container,'data-unit-token','u1'),dialog=keyed(fixture.container,'id','victory'),live=dialog.firstChild;
  fixture.container.querySelectorAll=selector=>{
    assert.ok(selector.includes('[data-unit-token]'));const result=[];
    const walk=node=>{for(const child of node.childNodes){if(child.nodeType===1){child.closest=function(){for(let ancestor=this;ancestor;ancestor=ancestor.parentNode)if(ancestor.nodeName==='DIALOG'&&ancestor.open)return ancestor;return null;};result.push(child);}walk(child);}};walk(fixture.container);
    // A native query returns only matching candidates; returning extra elements
    // here also checks that key() rejects unrelated terrain/sprite detail nodes.
    return result;
  };
  patch(fixture,document=>el(document,'section',{class:'compact-battle'},board(document,cell(document,'0,0'),cell(document,'1,0',token(document,'u1'))),el(document,'dialog',{id:'victory'},command(document,'player',0,'Fresh modal')),command(document,'player',0,'Outside')));
  assert.equal(keyed(fixture.container,'data-cell','1,0').firstChild,unit);assert.equal(dialog.firstChild,live);assert.equal(live.firstChild.nodeValue,'Live modal');
  const outside=fixture.container.firstChild.childNodes[2];assert.notEqual(outside,live);assert.equal(outside.firstChild.nodeValue,'Outside');
});
