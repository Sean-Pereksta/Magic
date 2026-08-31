// Headless integration harness: executes the same core and ordered extensions
// as the shipping loader. No HTML preview or browser is generated.
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {webcrypto} from 'node:crypto';

export function createSimulation({seed=937145}={}){
  const elements=new Map();
  const paint=new Proxy({measureText:s=>({width:String(s).length*7}),createRadialGradient:()=>({addColorStop(){}}),createLinearGradient:()=>({addColorStop(){}})}, {get:(t,k)=>k in t?t[k]:(()=>{}),set:(t,k,v)=>(t[k]=v,true)});
  const make=(tag='div',id='')=>{
    const node={tagName:tag.toUpperCase(),id,hidden:true,innerHTML:'',textContent:'',value:'',children:[],dataset:{},attributes:{},clientWidth:1200,clientHeight:800,width:1200,height:800,scrollTop:0,scrollHeight:0,offsetHeight:0,isConnected:true,
      classList:{add(){},remove(){},toggle(){},contains:()=>false},style:{setProperty(){},removeProperty(){}},addEventListener(){},removeEventListener(){},setAttribute(k,v){this.attributes[k]=v},getAttribute(k){return this.attributes[k]},removeAttribute(){},getContext:()=>paint,getBoundingClientRect:()=>({left:0,top:0,right:1200,bottom:800,width:1200,height:800}),
      appendChild(child){child.parentElement=this;this.children.push(child);if(child.id)elements.set(child.id,child);return child},append(...children){children.forEach(c=>this.appendChild(c))},after(child){if(child.id)elements.set(child.id,child)},before(child){this.after(child)},prepend(child){this.children.unshift(child)},insertBefore(child){return this.appendChild(child)},replaceChildren(...children){this.children=[];this.append(...children)},remove(){elements.delete(this.id)},contains:()=>false,matches:()=>false,closest:()=>null,querySelector:()=>null,querySelectorAll:()=>[],focus(){},blur(){},setPointerCapture(){},releasePointerCapture(){},
      insertAdjacentHTML(_where,html){this.innerHTML+=html;for(const [,id] of html.matchAll(/id="([^"]+)"/g))if(!elements.has(id))elements.set(id,make('div',id))}
    };
    let html='';Object.defineProperty(node,'innerHTML',{get:()=>html,set:value=>{html=String(value);node.children=[];for(const [,id] of html.matchAll(/id="([^"]+)"/g))if(id!==node.id&&!elements.has(id))elements.set(id,make('div',id))}});
    return node;
  };
  const core=readFileSync(new URL('../space-tyrants-core.html',import.meta.url),'utf8');
  for(const [,id] of core.matchAll(/id="([^"]+)"/g))elements.set(id,make('div',id));
  const doc={getElementById:id=>elements.get(id)||null,createElement:tag=>make(tag),head:make('head'),body:make('body'),documentElement:make('html'),querySelector:()=>null,querySelectorAll:()=>[],addEventListener(){}};
  const storage=new Map(),sandbox={console,document:doc,innerWidth:1200,innerHeight:800,devicePixelRatio:1,performance:{now:()=>0},localStorage:{getItem:k=>storage.get(k)||null,setItem:(k,v)=>storage.set(k,v),removeItem:k=>storage.delete(k)},requestAnimationFrame(){},cancelAnimationFrame(){},setTimeout(){},clearTimeout(){},setInterval(){},clearInterval(){},addEventListener(){},removeEventListener(){},matchMedia:()=>({matches:false,addEventListener(){}}),getComputedStyle:()=>({getPropertyValue:()=>'',overflowY:'auto'}),ResizeObserver:class{observe(){}},MutationObserver:class{observe(){}},navigator:{maxTouchPoints:0},location:{search:''},URLSearchParams};
  Object.assign(sandbox,{TextEncoder,TextDecoder,crypto:webcrypto,Uint8Array,ArrayBuffer,btoa,atob});
  const seededMath=Object.create(Math);seededMath.random=()=>.5;
  Object.assign(sandbox,{Math:seededMath,Date:class extends Date{static now(){return seed}}});
  sandbox.window=sandbox;sandbox.globalThis=sandbox;
  const loader=readFileSync(new URL('../space-tyrants.html',import.meta.url),'utf8'),paths=[...loader.matchAll(/'\.\/space-tyrants\/([^']+\.js)'/g)].map(x=>x[1]);
  const patches=paths.map(p=>readFileSync(new URL(p,import.meta.url),'utf8')).join('\n');
  let script=core.match(/<script>([\s\S]*)<\/script>/)[1];
  const at=script.lastIndexOf('})();');
  script=script.slice(0,at)+patches+'\nglobalThis.run=source=>eval(source);\n'+script.slice(at);
  vm.createContext(sandbox);vm.runInContext(script,sandbox,{filename:'space-tyrants-runtime.js'});
  sandbox.run('generateGalaxy();state.running=true;state.nextCommand=1e9;state.speed=1;');
  return {run:sandbox.run,context:sandbox,elements,storage};
}
