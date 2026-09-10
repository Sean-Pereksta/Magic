import {JSDOM} from 'jsdom';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
const read=name=>readFileSync(new URL(name,import.meta.url),'utf8');
export function runtime(touch=false){
 const html=read('../arcane-wilds.html'),dom=new JSDOM(html,{url:'https://arcane.test/game/arcane-wilds.html',runScripts:'outside-only',pretendToBeVisual:true});
 const w=dom.window;let now=1000,raf=new Map(),nextId=0;const timers=new Map(),errors=[];let draws=0;
 w.matchMedia=()=>({matches:touch,addEventListener(){},removeEventListener(){}});
 Object.defineProperty(w,'innerWidth',{value:touch?390:1280,writable:true});Object.defineProperty(w,'innerHeight',{value:touch?844:800,writable:true});
 w.performance.now=()=>now;w.requestAnimationFrame=fn=>{const id=++nextId;raf.set(id,fn);return id;};w.cancelAnimationFrame=id=>raf.delete(id);
 w.setTimeout=(fn,delay=0)=>{const id=++nextId;timers.set(id,{fn,at:now+delay});return id;};w.clearTimeout=id=>timers.delete(id);
 w.console.error=(...args)=>errors.push(args.map(String).join(' '));w.alert=()=>{};w.confirm=()=>true;
 w.HTMLCanvasElement.prototype.getContext=function(){return new Proxy({canvas:this,measureText:text=>({width:String(text).length*6}),createLinearGradient:()=>({addColorStop(){}}),createRadialGradient:()=>({addColorStop(){}}),getTransform:()=>({a:1,b:0,c:0,d:1,e:0,f:0})},{get(obj,key){if(key in obj)return obj[key];return (...args)=>{if(['arc','ellipse','moveTo','lineTo','translate','scale','fillRect','strokeRect'].includes(key)&&args.some(v=>typeof v==='number'&&!Number.isFinite(v)))throw new Error(key+' nonfinite coordinates');draws++;};},set(obj,key,value){obj[key]=value;return true;}});};
 w.HTMLElement.prototype.animate=()=>({cancel(){},finished:Promise.resolve()});
 const context=dom.getInternalVMContext(),run=source=>vm.runInContext(source,context);
 for(const m of html.matchAll(/<script src="arcane-wilds\/([^"]+)"/g))vm.runInContext(read(m[1]),context,{filename:m[1]});
 const step=(count=1,ms=16.67)=>{for(let i=0;i<count;i++){now+=ms;const callbacks=raf;raf=new Map();for(const fn of callbacks.values())fn(now);for(const [id,timer] of timers)if(timer.at<=now){timers.delete(id);if(typeof timer.fn==='function')timer.fn();}}};
 return {w,run,step,errors,draws:()=>draws,close:()=>dom.window.close()};
}
