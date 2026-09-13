// Dependency-free simulation regression checks against the real inline game script.
// DOM/canvas are inert fixtures. No browser rendering or Firebase calls are performed.
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const noop=()=>{};
const canvas=new Proxy({measureText:()=>({width:10}),createLinearGradient:()=>({addColorStop:noop}),createRadialGradient:()=>({addColorStop:noop})},{get:(o,k)=>o[k]||noop});
const elements=new Map();
function element(){return {style:{setProperty:noop},dataset:{},classList:{add:noop,remove:noop,toggle:noop,contains:()=>false},children:[],appendChild:noop,append:noop,remove:noop,setAttribute:noop,addEventListener:noop,querySelector:()=>null,querySelectorAll:()=>[],getContext:()=>canvas,getBoundingClientRect:()=>({left:0,top:700,width:1280,height:100,right:1280,bottom:800}),insertAdjacentHTML:noop,textContent:'',innerHTML:'',scrollTop:0,scrollLeft:0};}
const document={getElementById:id=>{if(!elements.has(id))elements.set(id,element());return elements.get(id)},createElement:element,querySelector:()=>null,querySelectorAll:()=>[],addEventListener:noop,body:element(),documentElement:element()};
const context={console,document,window:{addEventListener:noop},location:{search:'',href:'http://turncraft.test/game/turncraft.html'},URL,URLSearchParams,localStorage:{getItem:()=>null,setItem:noop},innerWidth:1280,innerHeight:900,devicePixelRatio:1,performance:{now:()=>0},setTimeout:()=>0,clearTimeout:noop,setInterval:()=>0,requestAnimationFrame:noop,matchMedia:()=>({matches:false,addEventListener:noop}),HTMLInputElement:class{},HTMLSelectElement:class{},Image:class{},initializeApp:()=>({}),getAuth:()=>({}),getFirestore:()=>({}),navigator:{},screen:{}};
context.window=context;context.addEventListener=noop;
const html=fs.readFileSync(path.join(__dirname,'../game/turncraft.html'),'utf8');
let code=html.match(/<script type="module">([\s\S]*?)<\/script>/)[1].replace(/^import .*;$/gm,'');
let scenarios=fs.readFileSync(path.join(__dirname,'turncraft-warfare-scenarios.js'),'utf8');
scenarios=scenarios.slice(0,scenarios.indexOf("reset();const caster="))+`check(!ABILITY_KEYS.some(k=>['w','a','s','d','x','b','f'].includes(k)),'Ability keys do not conflict with movement or existing commands');\nreturn {checks:results.length,results};`;
code=code.replace('\nboot();',`\nrefreshUI=()=>{};showCommandMarker=()=>{};pushBattleAlert=()=>{};globalThis.report=(function(){${scenarios}})();`);
try{vm.runInNewContext(code,context,{timeout:30000,filename:'turncraft.html'});console.log(JSON.stringify(context.report,null,2))}catch(e){console.error(e);process.exit(1)}
