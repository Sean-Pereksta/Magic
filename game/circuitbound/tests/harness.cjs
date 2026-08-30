const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const E = require('../engineering.js');
const source = fs.readFileSync(path.join(__dirname,'../app.js'),'utf8');
function fixture(width=40,height=24) {
  const context=vm.createContext({E});
  vm.runInContext(source.slice(source.indexOf('  const T='),source.indexOf('  const palette='))+'\nglobalThis.catalog={T,defs,items,recipes}',context);
  const {T,defs,items,recipes}=context.catalog;
  const world=new Uint8Array(width*height),meta=new Map();
  const get=(x,y)=>x>=0&&y>=0&&x<width&&y<height?world[x+y*width]:T.STONE;
  const sim=new E.Simulation({width,height,get,meta:()=>meta,defs,T});
  const put=(x,y,id,m={})=>{world[x+y*width]=id;if(id)meta.set(x+y*width,{rot:0,...m});else meta.delete(x+y*width);sim.changed(x,y);return meta.get(x+y*width)};
  return {T,defs,items,recipes,world,meta,get,sim,put,key:(x,y)=>x+y*width};
}
function game(saved) {
  class Element {
    constructor(){this.style={};this.children=[];this.dataset={};this.listeners={};this._classes=new Set();this.classList={add:c=>this._classes.add(c),remove:c=>this._classes.delete(c),contains:c=>this._classes.has(c),toggle:(c,on)=>on??!this._classes.has(c)?this._classes.add(c):this._classes.delete(c)}}
    append(...els){this.children.push(...els)} appendChild(e){this.append(e)} prepend(e){this.children.unshift(e)} replaceChildren(...els){this.children=els}
    setAttribute(){} addEventListener(k,f){this.listeners[k]=f} matches(){return false}
    getBoundingClientRect(){return {left:0,top:0,width:1280,height:800}}
  }
  const html=fs.readFileSync(path.join(__dirname,'../../circuitbound.html'),'utf8');
  const ids=new Map([...html.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],new Element()]));
  const selectors=new Map(['.tag','.hint','.top-actions','.manual-grid'].map(k=>[k,new Element()]));
  const ctx=new Proxy({}, {get:(o,k)=>k in o?o[k]:k==='createLinearGradient'||k==='createRadialGradient'?()=>({addColorStop(){}}):()=>{},set:(o,k,v)=>(o[k]=v,true)});
  ids.get('game').getContext=()=>ctx;
  const storage=new Map(saved?[['circuitbound-save-v1',JSON.stringify(saved)]]:[]),listeners={};
  const env={CircuitEngineering:E,console,Uint8Array,Map,Set,Math,Date,JSON,innerWidth:1280,innerHeight:800,devicePixelRatio:1,
    document:{getElementById:id=>ids.get(id)||[...selectors.values()].flatMap(e=>e.children).find(e=>e.id===id),querySelector:s=>selectors.get(s),querySelectorAll:s=>s==='.modal'?['craftPanel','manual','pausePanel'].map(k=>ids.get(k)):[],createElement:()=>new Element()},
    localStorage:{getItem:k=>storage.get(k)||null,setItem:(k,v)=>storage.set(k,v)},
    addEventListener:(name,fn)=>listeners[name]=fn,requestAnimationFrame:()=>{},setInterval:()=>0,setTimeout:()=>0,clearTimeout(){},confirm:()=>true};
  const context=vm.createContext(env);
  const instrumented=source.replace(/\}\)\(\);\s*$/,`globalThis.api={T,defs,items,recipes,engine,player,inventory,stats,keys,defaultMeta,set,get,idx,simulateNetworks,updateMachines,moveGroup,rotateAssembly,useOrPlace,finishLink,saveGame,loadGame,generate,render,tick,toggleModal,mineStep,interactItems,dropHeldItem,collectDrops,configureComponent,
    get meta(){return meta},get world(){return world},get pointer(){return pointer},get selected(){return selected},set selected(v){selected=v},get linkDrag(){return linkDrag},set linkDrag(v){linkDrag=v},set linkMode(v){linkMode=v},set selectedAssembly(v){selectedAssembly=v},get paused(){return paused}};})();`);
  vm.runInContext(instrumented,context);
  return {...context.api,api:context.api,ids,storage,listeners,context,run:code=>vm.runInContext(code,context)};
}
module.exports={fixture,game,E};
