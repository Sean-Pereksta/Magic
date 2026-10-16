export const TOUCH_LAYOUT_KEY='jesusRunnerTouchV1';
export const TOUCH_CONTROLS={
  move:{id:'joystick',label:'Move pad',min:64,max:160},
  jump:{id:'jumpBtn',label:'Jump',min:44,max:120},
  dash:{id:'dashBtn',label:'Dash',min:44,max:120},
  ability:{id:'abilityBtn',label:'Blessing',min:44,max:120},
};
const clamp=(n,min,max)=>Math.max(min,Math.min(max,n));
const copy=value=>JSON.parse(JSON.stringify(value));
export const layoutMode=view=>view.width>view.height?'landscape':'portrait';
export function cleanLayouts(value){
  const result={portrait:{},landscape:{}};
  for(const mode of Object.keys(result))for(const [key,spec]of Object.entries(TOUCH_CONTROLS)){
    const p=value?.[mode]?.[key];
    if(p&&[p.x,p.y,p.size].every(Number.isFinite))result[mode][key]={x:clamp(p.x,0,1),y:clamp(p.y,0,1),size:clamp(p.size,spec.min,spec.max)};
  }
  return result;
}
export function defaultControls(view){
  const short=view.height<500,pad=short?86:98,jump=short?60:66;
  const left=(view.left||0)+25,right=view.width-(view.right||0)-24,bottom=view.height-(view.bottom||0)-(short?16:22);
  return{move:{x:left+pad/2,y:bottom-pad/2,size:pad},jump:{x:right-jump/2,y:bottom-jump/2,size:jump},
    dash:{x:right-jump-16-24,y:bottom-24,size:48},ability:{x:right-jump-3,y:bottom-jump-22-10,size:44}};
}
export function controlsOverlap(a,b,gap=8){return Math.abs(a.x-b.x)<(a.size+b.size)/2+gap&&Math.abs(a.y-b.y)<(a.size+b.size)/2+gap;}
function boundedControl(p,spec,view){
  const top=Math.max(76,view.top||0,view.topLimit||0);
  const size=clamp(p.size,spec.min,Math.max(spec.min,Math.min(spec.max,view.width-(view.left||0)-(view.right||0)-24,view.height-(view.bottom||0)-12-top)));
  const half=size/2,minX=(view.left||0)+12+half,maxX=view.width-(view.right||0)-12-half;
  const minY=top+half,maxY=view.height-(view.bottom||0)-12-half;
  return{x:clamp(p.x,minX,maxX),y:clamp(p.y,minY,maxY),size};
}
// Keep actual hit rectangles separate, not just their circular artwork.
export function placeControl(desired,others,spec,view){
  for(let size=Math.max(spec.min,desired.size);;size=Math.max(spec.min,size-4)){
    const p=boundedControl({...desired,size},spec,view),free=c=>!others.some(other=>controlsOverlap(c,other));
    if(free(p))return p;
    const low=boundedControl({x:0,y:0,size:p.size},spec,view),high=boundedControl({x:view.width,y:view.height,size:p.size},spec,view);
    let nearest=null,distance=Infinity;
    for(let x=low.x;x<=high.x;x+=8)for(let y=low.y;y<=high.y;y+=8){
      const next={x,y,size:p.size},d=(x-p.x)**2+(y-p.y)**2;
      if(d<distance&&free(next)){nearest=next;distance=d;}
    }
    if(nearest)return nearest;
    if(size===spec.min)break;
  }
  return boundedControl({...desired,size:spec.min},spec,view);
}
export function resolveControls(profile,view){
  const defaults=defaultControls(view),result={};
  for(const [key,spec]of Object.entries(TOUCH_CONTROLS)){
    const saved=profile?.[key],valid=saved&&[saved.x,saved.y,saved.size].every(Number.isFinite);
    const desired=valid?{x:saved.x*view.width,y:saved.y*view.height,size:clamp(saved.size,spec.min,spec.max)}:defaults[key];
    result[key]=placeControl(desired,Object.values(result),spec,view);
  }
  return result;
}
export class TouchLayoutEditor{
  constructor(game){
    this.game=game;this.active=false;this.selected='jump';this.drags=new Map();this.settings=cleanLayouts(null);
    try{this.settings=cleanLayouts(JSON.parse(localStorage.getItem(TOUCH_LAYOUT_KEY)||'null'));}catch{}
    this.elements=Object.fromEntries(Object.entries(TOUCH_CONTROLS).map(([key,spec])=>[key,document.getElementById(spec.id)]));
    this.shell=document.getElementById('shell');this.panel=document.getElementById('touchLayoutPanel');
    this.select=document.getElementById('touchControlSelect');this.slider=document.getElementById('touchControlSize');this.output=document.getElementById('touchSizeValue');
    this.select.addEventListener('change',()=>this.choose(this.select.value));
    this.slider.addEventListener('input',()=>this.change(this.selected,{size:Number(this.slider.value)}));
    document.getElementById('touchLayoutDone').addEventListener('click',()=>this.close(true));
    document.getElementById('touchLayoutCancel').addEventListener('click',()=>this.close(false));
    document.getElementById('touchLayoutReset').addEventListener('click',()=>{this.draft[layoutMode(this.view())]={};this.apply();this.choose(this.selected);});
    for(const [key,element]of Object.entries(this.elements)){
      element.addEventListener('pointerdown',e=>{
        if(!this.active)return;e.preventDefault();this.choose(key);
        if([...this.drags.values()].some(d=>d.key===key))return;
        const p=this.positions[key];this.drags.set(e.pointerId,{key,dx:e.clientX-p.x,dy:e.clientY-p.y});element.setPointerCapture(e.pointerId);
      });
      element.addEventListener('pointermove',e=>{const d=this.drags.get(e.pointerId);if(!this.active||!d)return;e.preventDefault();this.change(d.key,{x:e.clientX-d.dx,y:e.clientY-d.dy});});
      for(const name of ['pointerup','pointercancel','lostpointercapture'])element.addEventListener(name,e=>this.drags.delete(e.pointerId));
      element.addEventListener('keydown',e=>{if(!this.active||!['ArrowLeft','ArrowRight','ArrowUp','ArrowDown'].includes(e.key))return;e.preventDefault();e.stopPropagation();this.choose(key);const p=this.positions[key],n=e.shiftKey?24:8;this.change(key,{x:p.x+(e.key==='ArrowRight'?n:e.key==='ArrowLeft'?-n:0),y:p.y+(e.key==='ArrowDown'?n:e.key==='ArrowUp'?-n:0)});});
    }
    document.getElementById('mobileControls').addEventListener('contextmenu',e=>e.preventDefault());
  }
  view(){
    const rect=this.shell.getBoundingClientRect(),style=getComputedStyle(this.shell),inset=key=>parseFloat(style.getPropertyValue('--safe-'+key))||0;
    return{width:rect.width,height:rect.height,left:inset('left'),right:inset('right'),top:inset('top'),bottom:inset('bottom'),topLimit:this.active?this.panel.getBoundingClientRect().bottom+12:0};
  }
  apply(){
    const view=this.view(),mode=layoutMode(view),settings=this.active?this.draft:this.settings;
    this.positions=resolveControls(settings[mode],view);
    for(const [key,p]of Object.entries(this.positions)){
      Object.assign(this.elements[key].style,{left:(p.x-p.size/2)+'px',top:(p.y-p.size/2)+'px',right:'auto',bottom:'auto',width:p.size+'px',height:p.size+'px'});
      this.elements[key].classList.toggle('layout-selected',this.active&&this.selected===key);
    }
    if(this.active){document.getElementById('touchLayoutMode').textContent=mode==='portrait'?'Portrait layout':'Landscape layout';this.choose(this.selected);}
  }
  choose(key){
    if(!TOUCH_CONTROLS[key])return;this.selected=key;this.select.value=key;
    const spec=TOUCH_CONTROLS[key];this.slider.min=spec.min;this.slider.max=spec.max;this.slider.value=Math.round(this.positions[key].size);this.output.value=Math.round(this.positions[key].size/defaultControls(this.view())[key].size*100)+'%';
    for(const [id,element]of Object.entries(this.elements))element.classList.toggle('layout-selected',this.active&&id===key);
  }
  change(key,patch){
    const view=this.view(),mode=layoutMode(view),p=placeControl({...this.positions[key],...patch},Object.entries(this.positions).filter(([id])=>id!==key).map(([,value])=>value),TOUCH_CONTROLS[key],view);
    // Save all resolved positions so resizing one control cannot move another.
    this.draft[mode]=Object.fromEntries(Object.entries({...this.positions,[key]:p}).map(([id,value])=>[id,{x:value.x/view.width,y:value.y/view.height,size:value.size}]));
    this.apply();
  }
  open(){
    this.game.input.cancelRebind();this.game.input.reset();this.draft=copy(this.settings);this.active=true;this.drags.clear();
    this.game.setScreen('touchLayout');this.shell.classList.add('editing-touch');this.apply();this.select.focus();
  }
  close(save){
    if(save){this.settings=cleanLayouts(this.draft);try{localStorage.setItem(TOUCH_LAYOUT_KEY,JSON.stringify(this.settings));}catch{this.game.touchLayoutNotice='Layout kept for this visit; browser storage is unavailable.';}}
    this.active=false;this.drags.clear();this.shell.classList.remove('editing-touch');this.game.input.reset();this.apply();this.game.showControls(this.game.infoReturn||'menu');
  }
}
