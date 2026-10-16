/*
 * Original Basketball 2 athlete assets. No downloaded meshes or textures.
 * Run: node game/basketball2/tools/build-models.cjs
 * Produces standard glTF 2.0 binary assets with embedded geometry, an articulated
 * skin, inverse bind matrices, PBR materials, and twelve authored motion clips.
 */
'use strict';
const fs = require('node:fs');
const path = require('node:path');

const OUT = path.join(__dirname, '..', 'assets', 'models');
const PI = Math.PI;
const add = (a,b) => a.map((n,i)=>n+b[i]);
const sub = (a,b) => a.map((n,i)=>n-b[i]);
const mul = (a,s) => a.map(n=>n*s);
const mix = (a,b,t) => a.map((n,i)=>n+(b[i]-n)*t);
const dot = (a,b) => a.reduce((n,v,i)=>n+v*b[i],0);
const cross = (a,b) => [a[1]*b[2]-a[2]*b[1],a[2]*b[0]-a[0]*b[2],a[0]*b[1]-a[1]*b[0]];
const unit = v => mul(v,1/(Math.hypot(...v)||1));
const clamp = (v,a,b) => Math.max(a,Math.min(b,v));
function quat(x=0,y=0,z=0) {
  const a=Math.sin(x/2),b=Math.cos(x/2),c=Math.sin(y/2),d=Math.cos(y/2),e=Math.sin(z/2),f=Math.cos(z/2);
  return [a*d*f+b*c*e,b*c*f-a*d*e,b*d*e+a*c*f,b*d*f-a*c*e];
}

const MATERIALS = [
  ['jersey',[.045,.19,.36,1],.88], ['shorts',[.035,.12,.25,1],.9],
  ['trim',[.98,.69,.20,1],.75], ['skin',[.55,.31,.19,1],.85],
  ['hair',[.025,.016,.013,1],.95], ['sneaker',[.88,.9,.92,1],.68],
  ['sole',[.10,.13,.17,1],.92], ['sock',[.93,.94,.95,1],.96],
  ['eyes',[.018,.012,.01,1],.45], ['mouth',[.21,.075,.055,1],.9],
  ['number',[.98,.96,.86,1],.85], ['wristband',[.9,.91,.94,1],.96]
];

function buildAthlete(profile) {
  const bodyScale=profile.width, heightScale=profile.height;
  const shape = p => [p[0]*bodyScale,p[1]*heightScale,p[2]*bodyScale];
  const nodes=[], joints={}, absolute=[];
  function bone(name,parent,position) {
    const index=nodes.length, world=shape(position), parentWorld=parent==null?[0,0,0]:absolute[joints[parent]];
    nodes.push({name,translation:sub(world,parentWorld)});
    if(parent!=null) (nodes[joints[parent]].children ||= []).push(index);
    joints[name]=index; absolute.push(world); return index;
  }
  bone('root',null,[0,0,0]); bone('hips','root',[0,1.055,0]);
  bone('spine','hips',[0,1.27,0]); bone('chest','spine',[0,1.56,0]);
  bone('neck','chest',[0,1.805,0]); bone('head','neck',[0,1.91,0]);
  for(const side of ['L','R']) {
    const sign=side==='L'?1:-1;
    bone('clavicle_'+side,'chest',[sign*.16,1.69,0]);
    bone('upper_arm_'+side,'clavicle_'+side,[sign*.303,1.685,0]);
    bone('forearm_'+side,'upper_arm_'+side,[sign*.419,1.315,.018]);
    bone('hand_'+side,'forearm_'+side,[sign*.452,.997,.04]);
    bone('thumb_'+side,'hand_'+side,[sign*.416,.945,.07]);
    bone('thigh_'+side,'hips',[sign*.142,1.035,0]);
    bone('shin_'+side,'thigh_'+side,[sign*.143,.59,.026]);
    bone('foot_'+side,'shin_'+side,[sign*.143,.148,.012]);
    bone('toes_'+side,'foot_'+side,[sign*.143,.075,.17]);
  }
  const pos=[],norm=[],uv=[],weights=[],skinJoints=[], groups=MATERIALS.map(()=>[]);
  const mat = Object.fromEntries(MATERIALS.map((m,i)=>[m[0],i]));
  function influence(named) {
    const sorted=Object.entries(named).filter(([,w])=>w>0).sort((a,b)=>b[1]-a[1]).slice(0,4);
    const total=sorted.reduce((n,[,w])=>n+w,0)||1;
    const js=sorted.map(([name])=>joints[name]), ws=sorted.map(([,w])=>w/total);
    while(js.length<4) {js.push(0);ws.push(0);} return [js,ws];
  }
  function vert(p,n,t,w) {
    const index=pos.length/3;
    pos.push(...shape(p)); norm.push(...unit([n[0]/bodyScale,n[1]/heightScale,n[2]/bodyScale])); uv.push(...t);
    const [js,ws]=influence(w); skinJoints.push(...js);weights.push(...ws); return index;
  }
  function tri(material,a,b,c) {groups[mat[material]].push(a,b,c);}
  function ellipsoid(material,center,radius,weight,rings=10,segments=18,minPhi=0,maxPhi=PI) {
    const first=pos.length/3;
    for(let y=0;y<=rings;y++) {
      const phi=minPhi+(maxPhi-minPhi)*y/rings;
      for(let x=0;x<=segments;x++) {
        const theta=2*PI*x/segments;
        const v=[Math.sin(phi)*Math.cos(theta),Math.cos(phi),Math.sin(phi)*Math.sin(theta)];
        vert(add(center,v.map((n,i)=>n*radius[i])),v.map((n,i)=>n/radius[i]),[x/segments,y/rings],weight);
      }
    }
    for(let y=0;y<rings;y++) for(let x=0;x<segments;x++) {
      const a=first+y*(segments+1)+x,b=a+segments+1;
      tri(material,a,a+1,b);tri(material,a+1,b+1,b);
    }
  }
  // Continuous ring surfaces keep elbows/knees connected and blend weights on
  // either side of each anatomical joint instead of attaching rigid primitives.
  function tube(material,rings,segments=24) {
    const first=pos.length/3;
    for(let r=0;r<rings.length;r++) {
      const ring=rings[r],axis=unit(sub(rings[Math.min(r+1,rings.length-1)].p,rings[Math.max(0,r-1)].p));
      let axisX=unit(cross(axis,[0,0,1])); if(Math.hypot(...axisX)<.5) axisX=[1,0,0];
      const axisZ=unit(cross(axisX,axis));
      for(let s=0;s<=segments;s++) {
        const angle=s/segments*2*PI,normal=add(mul(axisX,Math.cos(angle)),mul(axisZ,Math.sin(angle)));
        const offset=add(mul(axisX,Math.cos(angle)*ring.rx),mul(axisZ,Math.sin(angle)*ring.rz));
        vert(add(ring.p,offset),normal,[s/segments,r/(rings.length-1)],ring.w);
      }
    }
    for(let r=0;r<rings.length-1;r++)for(let s=0;s<segments;s++) {
      const a=first+r*(segments+1)+s,b=a+segments+1;
      tri(material,a,b,a+1);tri(material,a+1,b,b+1);
    }
  }
  function patch(material,points,w,normal=[0,0,1]) {
    const ids=points.map((p,i)=>vert(p,normal,[i%2,Math.floor(i/2)],w));
    for(let i=1;i<ids.length-1;i++) {
      const outward=dot(cross(sub(points[i],points[0]),sub(points[i+1],points[0])),normal)>=0;
      tri(material,ids[0],outward?ids[i]:ids[i+1],outward?ids[i+1]:ids[i]);
    }
  }
  const torso=(y,rx,rz,w) => ({p:[0,y,0],rx,rz,w});
  tube('skin',[
    torso(1.0,.19,.105,{hips:1}),torso(1.12,.205,.115,{hips:.7,spine:.3}),
    torso(1.28,.21,.128,{spine:1}),torso(1.49,.275,.145,{spine:.3,chest:.7}),
    torso(1.64,.305,.138,{chest:1}),torso(1.735,.235,.115,{chest:.8,neck:.2}),
    torso(1.795,.075,.067,{neck:1})
  ],32);
  tube('jersey',[
    torso(1.11,.215,.124,{hips:.6,spine:.4}),torso(1.18,.219,.128,{hips:.3,spine:.7}),
    torso(1.32,.225,.137,{spine:.85,chest:.15}),torso(1.49,.284,.151,{spine:.3,chest:.7}),
    torso(1.64,.309,.145,{chest:1}),torso(1.716,.26,.125,{chest:.9,neck:.1}),
    torso(1.765,.105,.078,{chest:.7,neck:.3})
  ],32);
  // Collar piping, hem, side stripes, chest crest, and a modeled jersey numeral.
  tube('trim',[torso(1.755,.113,.084,{chest:.7,neck:.3}),torso(1.774,.107,.079,{chest:.65,neck:.35})],32);
  tube('trim',[torso(1.113,.218,.127,{hips:.6,spine:.4}),torso(1.134,.22,.129,{hips:.5,spine:.5})],32);
  for(const s of [-1,1]) patch('trim',[[s*.213,1.16,.06],[s*.233,1.34,.07],[s*.286,1.58,.07],[s*.302,1.63,.025],[s*.262,1.35,.025],[s*.22,1.16,.022]],{spine:.6,chest:.4},[s,0,0]);
  patch('trim',[[-.145,1.625,.138],[-.107,1.625,.144],[-.107,1.588,.146],[-.145,1.588,.14]],{chest:1});
  function number(digit,y=1.49,z=.153,back=false) {
    const segments={0:'abcdef',1:'bc',2:'abdeg',3:'abcdg',4:'bcfg',5:'acdfg',6:'acdefg',7:'abc',8:'abcdefg',9:'abcdfg'};
    const all={a:[-.04,.054,.04,.067],b:[.028,.006,.042,.054],c:[.028,-.054,.042,-.006],d:[-.04,-.067,.04,-.054],e:[-.042,-.054,-.028,-.006],f:[-.042,.006,-.028,.054],g:[-.04,-.007,.04,.007]};
    for(const key of segments[digit]) {
      const [x1,y1,x2,y2]=all[key],sign=back?-1:1;
      patch('number',[[x1,y+y1,z*sign],[x2,y+y1,z*sign],[x2,y+y2,z*sign],[x1,y+y2,z*sign]],{chest:.8,spine:.2},[0,0,sign]);
    }
  }
  number(profile.number);number(profile.number,1.49,.154,true);
  ellipsoid('skin',[0,1.813,0],[.074,.09,.068],{neck:.65,head:.35},8,20);
  ellipsoid('skin',[0,1.985,.006],[.151,.205,.137],{head:1},18,28);
  // Defined jaw/chin, cheekbones, nose, ears, brows and eyes remain part of the
  // same deforming mesh; no detached facial sprites or remote texture assets.
  ellipsoid('skin',[0,1.875,.025],[.1,.07,.09],{head:1},8,18);
  ellipsoid('skin',[0,1.972,.132],[.024,.035,.031],{head:1},8,12);
  for(const s of [-1,1]) {
    ellipsoid('skin',[s*.148,1.982,0],[.026,.052,.032],{head:1},8,12);
    ellipsoid('skin',[s*.071,1.959,.108],[.046,.045,.015],{head:1},8,12);
    ellipsoid('eyes',[s*.059,2.012,.132],[.019,.011,.009],{head:1},6,12);
    ellipsoid('hair',[s*.061,2.037,.131],[.031,.007,.006],{head:1},6,12);
  }
  ellipsoid('mouth',[0,1.916,.132],[.037,.007,.009],{head:1},6,16);
  ellipsoid('skin',[0,1.904,.133],[.031,.006,.007],{head:1},6,14);
  ellipsoid('hair',[0,1.989,.006],[.155,.211,.141],{head:1},12,28,0,1.37);
  if(profile.hair==='curls') {
    for(let row=0;row<4;row++) for(let col=0;col<9;col++) {
      const a=col/9*PI*2+row*.25,p=(row+1)/5*PI*.47;
      ellipsoid('hair',[Math.cos(a)*Math.sin(p)*.14,2.046+Math.cos(p)*.153,Math.sin(a)*Math.sin(p)*.13],[.029,.026,.029],{head:1},5,8);
    }
  }
  if(profile.hair==='fade') {
    ellipsoid('hair',[0,2.147,-.006],[.112,.055,.108],{head:1},7,20);
  }
  for(const side of ['L','R']) {
    const s=side==='L'?1:-1, U='upper_arm_'+side,F='forearm_'+side,H='hand_'+side,T='thumb_'+side;
    ellipsoid('skin',[s*.294,1.674,0],[.106,.104,.101],{chest:.3,[U]:.7},10,20);
    tube('skin',[
      {p:[s*.275,1.69,0],rx:.093,rz:.097,w:{chest:.35,[U]:.65}},
      {p:[s*.318,1.632,.002],rx:.109,rz:.107,w:{[U]:1}},
      {p:[s*.363,1.49,.008],rx:.096,rz:.091,w:{[U]:1}},
      {p:[s*.401,1.37,.015],rx:.076,rz:.071,w:{[U]:.8,[F]:.2}},
      {p:[s*.419,1.315,.018],rx:.073,rz:.068,w:{[U]:.5,[F]:.5}},
      {p:[s*.427,1.258,.023],rx:.082,rz:.074,w:{[U]:.16,[F]:.84}},
      {p:[s*.44,1.13,.031],rx:.068,rz:.059,w:{[F]:1}},
      {p:[s*.451,1.025,.039],rx:.048,rz:.042,w:{[F]:.75,[H]:.25}},
      {p:[s*.454,.99,.041],rx:.047,rz:.039,w:{[F]:.3,[H]:.7}},
      {p:[s*.457,.944,.043],rx:.052,rz:.032,w:{[H]:1}},
      {p:[s*.458,.895,.044],rx:.042,rz:.027,w:{[H]:1}},
      {p:[s*.458,.881,.044],rx:.014,rz:.016,w:{[H]:1}}
    ],20);
    // Palm contours and separately modeled fingers/thumbs read at replay range.
    for(let finger=0;finger<4;finger++) {
      const x=s*(.423+finger*.022),length=[.058,.071,.067,.05][finger];
      ellipsoid('skin',[x,.89-length/2,.047],[.011,length/2,.021],{[H]:1},7,10);
    }
    ellipsoid('skin',[s*.411,.943,.071],[.023,.041,.024],{[H]:.5,[T]:.5},8,12);
    if(side==='L') tube('wristband',[
      {p:[s*.45,1.038,.038],rx:.052,rz:.045,w:{[F]:1}},
      {p:[s*.453,1.001,.041],rx:.052,rz:.045,w:{[F]:.7,[H]:.3}}
    ],20);
    const Th='thigh_'+side,Sh='shin_'+side,Ft='foot_'+side,To='toes_'+side;
    tube('skin',[
      {p:[s*.14,1.059,0],rx:.126,rz:.129,w:{hips:.35,[Th]:.65}},
      {p:[s*.143,.96,.001],rx:.133,rz:.133,w:{[Th]:1}},
      {p:[s*.143,.82,.008],rx:.119,rz:.116,w:{[Th]:1}},
      {p:[s*.143,.666,.019],rx:.089,rz:.087,w:{[Th]:.88,[Sh]:.12}},
      {p:[s*.143,.59,.026],rx:.079,rz:.08,w:{[Th]:.5,[Sh]:.5}},
      {p:[s*.143,.532,.024],rx:.085,rz:.085,w:{[Th]:.14,[Sh]:.86}},
      {p:[s*.143,.425,.019],rx:.087,rz:.082,w:{[Sh]:1}},
      {p:[s*.143,.279,.015],rx:.061,rz:.061,w:{[Sh]:1}},
      {p:[s*.143,.165,.012],rx:.047,rz:.047,w:{[Sh]:.7,[Ft]:.3}}
    ],24);
    // Shorts are separate continuous leg openings so knees never pull a single
    // unbroken shorts tube into a skirt when the athlete runs or plants a foot.
    tube('shorts',[
      {p:[s*.12,1.127,0],rx:.13,rz:.136,w:{hips:.8,[Th]:.2}},
      {p:[s*.139,1.035,0],rx:.143,rz:.146,w:{hips:.35,[Th]:.65}},
      {p:[s*.143,.895,.006],rx:.142,rz:.142,w:{[Th]:1}},
      {p:[s*.143,.815,.008],rx:.137,rz:.135,w:{[Th]:1}}
    ],24);
    tube('trim',[
      {p:[s*.143,.83,.008],rx:.14,rz:.138,w:{[Th]:1}},
      {p:[s*.143,.808,.009],rx:.139,rz:.137,w:{[Th]:1}}
    ],24);
    tube('sock',[
      {p:[s*.143,.326,.016],rx:.066,rz:.066,w:{[Sh]:1}},
      {p:[s*.143,.282,.015],rx:.065,rz:.064,w:{[Sh]:1}},
      {p:[s*.143,.166,.012],rx:.052,rz:.052,w:{[Sh]:.65,[Ft]:.35}},
      {p:[s*.143,.119,.014],rx:.049,rz:.05,w:{[Ft]:1}}
    ],20);
    tube('trim',[
      {p:[s*.143,.309,.016],rx:.067,rz:.067,w:{[Sh]:1}},
      {p:[s*.143,.297,.016],rx:.067,rz:.067,w:{[Sh]:1}}
    ],20);
    // Shaped basketball sneakers: heel, raised collar, toe box, outsole and laces.
    ellipsoid('sole',[s*.143,.042,.094],[.088,.04,.18],{[Ft]:.8,[To]:.2},8,24);
    ellipsoid('sneaker',[s*.143,.095,.092],[.081,.062,.168],{[Ft]:.85,[To]:.15},10,24);
    ellipsoid('sneaker',[s*.143,.143,-.008],[.063,.075,.073],{[Ft]:1},8,20);
    ellipsoid('trim',[s*.218,.096,.082],[.006,.024,.075],{[Ft]:.85,[To]:.15},6,14);
    for(let lace=0;lace<4;lace++) patch('sole',[
      [s*.103,.155-lace*.003,.033+lace*.024],
      [s*.181,.155-lace*.003,.041+lace*.024],
      [s*.181,.158-lace*.003,.047+lace*.024],
      [s*.103,.158-lace*.003,.039+lace*.024]
    ],{[Ft]:.9,[To]:.1},[0,1,0]);
  }
  // Waistband closes both shorts legs into a single tailored silhouette.
  const crotchWeights={hips:.8,thigh_L:.1,thigh_R:.1};
  patch('shorts',[[-.18,1.125,.142],[.18,1.125,.142],[.125,.925,.142],[-.125,.925,.142]],crotchWeights);
  patch('shorts',[[-.18,1.125,-.142],[.18,1.125,-.142],[.125,.925,-.142],[-.125,.925,-.142]],crotchWeights,[0,0,-1]);
  patch('shorts',[[-.125,.925,.142],[.125,.925,.142],[.125,.925,-.142],[-.125,.925,-.142]],crotchWeights,[0,-1,0]);
  tube('shorts',[torso(1.084,.218,.137,{hips:1}),torso(1.133,.216,.135,{hips:.85,spine:.15})],32);
  tube('trim',[torso(1.125,.218,.137,{hips:.85,spine:.15}),torso(1.138,.218,.137,{hips:.85,spine:.15})],32);

  const animations=[];
  function clip(name,duration,frameCount,poseAt) {
    const time=[], poses=[];
    for(let i=0;i<frameCount;i++) {const t=i/(frameCount-1);time.push(t*duration);poses.push(poseAt(t));}
    const tracks=[];
    const used=new Set(poses.flatMap(p=>Object.keys(p)));
    for(const key of used) {
      const translation=key==='hips_position', node=translation?joints.hips:joints[key];
      const values=[];
      for(const pose of poses) values.push(...(translation?shape(add([0,1.055,0],pose[key]||[0,0,0])):quat(...(pose[key]||[0,0,0]))));
      tracks.push({node,path:translation?'translation':'rotation',values,type:translation?'VEC3':'VEC4'});
    }
    animations.push({name,duration,time,tracks});
  }
  const resting=(p={})=>Object.assign({upper_arm_L:[0,0,-.055],upper_arm_R:[0,0,.055],forearm_L:[-.12,0,0],forearm_R:[-.12,0,0]},p);
  clip('idle',2,25,t=>resting({chest:[.018*Math.sin(t*PI*2),0,.008*Math.sin(t*PI*2)],hips_position:[0,.008*Math.sin(t*PI*2),0],head:[0,.035*Math.sin(t*PI*2),0]}));
  for(const [name,duration,power] of [['run',.72,.67],['sprint',.54,.98]]) clip(name,duration,25,t=>{
    const phase=t*PI*2,a=Math.sin(phase),b=Math.sin(phase+PI);
    return resting({hips_position:[0,.027*(1-Math.cos(phase*2)),0],hips:[.025,0,.045*a],chest:[-.09*power,-.08*a,.018*a],head:[.05*power,.025*a,0],
      thigh_L:[power*a,0,.018],thigh_R:[power*b,0,-.018],shin_L:[Math.max(0,-a)*1.18*power+.1,0,0],shin_R:[Math.max(0,-b)*1.18*power+.1,0,0],
      foot_L:[-.18*Math.max(0,a),0,0],foot_R:[-.18*Math.max(0,b),0,0],toes_L:[.15*Math.max(0,-a),0,0],toes_R:[.15*Math.max(0,-b),0,0],
      upper_arm_L:[-.58*a*power,0,-.1],upper_arm_R:[-.58*b*power,0,.1],forearm_L:[-.9-.2*a,0,0],forearm_R:[-.9-.2*b,0,0]});
  });
  clip('defensive_shuffle',.8,25,t=>{
    const a=Math.sin(t*PI*2),step=Math.max(0,a),other=Math.max(0,-a);
    return resting({hips_position:[0,-.13+.02*Math.cos(t*PI*4),0],hips:[-.12,0,.035*a],chest:[.15,0,-.06*a],
      thigh_L:[-.28,0,-.18-.1*a],thigh_R:[-.28,0,.18-.1*a],shin_L:[.52+.18*step,0,0],shin_R:[.52+.18*other,0,0],
      foot_L:[-.18,0,.1*a],foot_R:[-.18,0,.1*a],upper_arm_L:[-.15,-.12,-.88-.06*a],upper_arm_R:[-.15,.12,.88+.06*a],
      forearm_L:[-.38,0,-.1],forearm_R:[-.38,0,.1],hand_L:[.08,0,-.15],hand_R:[.08,0,.15]});
  });
  clip('pivot',.6,21,t=>{
    const a=Math.sin(t*PI),turn=.55*Math.sin(t*PI*2);
    return resting({hips_position:[0,-.05*a,0],hips:[0,turn*.4,0],chest:[0,turn,0],head:[0,turn*.3,0],thigh_L:[-.12*a,turn*.3,0],thigh_R:[-.12*a,-turn*.3,0],shin_L:[.22*a,0,0],shin_R:[.22*a,0,0],upper_arm_L:[-.5*a,0,-.15],upper_arm_R:[-.5*a,0,.15],forearm_L:[-.85*a,0,0],forearm_R:[-.85*a,0,0]});
  });
  function smooth(t) {return t*t*(3-2*t);}
  function sequence(t,keys) {
    let a=keys[0],b=keys[keys.length-1];
    for(let i=1;i<keys.length;i++) if(t<=keys[i][0]){a=keys[i-1];b=keys[i];break;}
    const f=smooth(clamp((t-a[0])/(b[0]-a[0]||1),0,1)),result={};
    for(const name of new Set([...Object.keys(a[1]),...Object.keys(b[1])]))result[name]=mix(a[1][name]||[0,0,0],b[1][name]||[0,0,0],f);
    return resting(result);
  }
  const shotLoad={hips_position:[0,-.09,0],hips:[-.08,0,0],chest:[.1,0,0],thigh_L:[-.28,0,-.035],thigh_R:[-.28,0,.035],shin_L:[.54,0,0],shin_R:[.54,0,0],upper_arm_L:[-.95,0,-.16],upper_arm_R:[-1.02,0,.14],forearm_L:[-1.28,0,.08],forearm_R:[-1.4,0,-.04]};
  const shotRelease={hips_position:[0,.015,0],chest:[-.04,0,0],upper_arm_L:[-2.46,0,-.16],upper_arm_R:[-2.63,0,.08],forearm_L:[-.38,0,.1],forearm_R:[-.18,0,0],hand_L:[.5,0,-.1],hand_R:[.75,0,0],foot_L:[.12,0,0],foot_R:[.12,0,0]};
  clip('jump_shot',.95,33,t=>sequence(t,[[0,shotLoad],[.22,shotLoad],[.52,shotRelease],[.75,shotRelease],[1,{}]]));
  clip('layup',.9,33,t=>sequence(t,[[0,{upper_arm_R:[-.6,0,.1],forearm_R:[-1.15,0,0],upper_arm_L:[-.5,0,-.4]}],[.28,{hips_position:[0,-.07,0],thigh_L:[-.22,0,0],shin_L:[.45,0,0],thigh_R:[-.72,0,.03],shin_R:[.45,0,0],upper_arm_R:[-1.35,0,.12],forearm_R:[-.85,0,0]}],[.6,{chest:[-.08,0,-.04],thigh_R:[-1.0,0,0],shin_R:[1.18,0,0],thigh_L:[.13,0,0],shin_L:[.28,0,0],upper_arm_R:[-2.72,0,.1],forearm_R:[-.12,0,0],hand_R:[.48,0,0],upper_arm_L:[-.45,0,-.65],forearm_L:[-.55,0,0]}],[.77,{upper_arm_R:[-2.62,0,.15],forearm_R:[-.06,0,0],hand_R:[.65,0,0],thigh_R:[-.55,0,0],shin_R:[.8,0,0]}],[1,{}]]));
  clip('dunk',1,33,t=>sequence(t,[[0,shotLoad],[.3,{thigh_R:[-.65,0,0],shin_R:[.8,0,0],upper_arm_L:[-1.4,0,-.1],upper_arm_R:[-1.4,0,.1],forearm_L:[-.8,0,0],forearm_R:[-.8,0,0]}],[.57,{chest:[-.1,0,0],upper_arm_L:[-2.85,0,-.12],upper_arm_R:[-2.85,0,.12],forearm_L:[-.04,0,0],forearm_R:[-.04,0,0],hand_L:[.3,0,0],hand_R:[.3,0,0],shin_L:[.6,0,0],shin_R:[.9,0,0]}],[.72,{chest:[.12,0,0],upper_arm_L:[-2.4,0,-.14],upper_arm_R:[-2.4,0,.14],forearm_L:[-.1,0,0],forearm_R:[-.1,0,0],hand_L:[1,0,0],hand_R:[1,0,0],shin_L:[.7,0,0],shin_R:[.7,0,0]}],[1,{}]]));
  clip('pass',.55,25,t=>sequence(t,[[0,{}],[.25,{chest:[0,-.08,0],upper_arm_L:[-.7,0,-.25],upper_arm_R:[-.7,0,.25],forearm_L:[-1.35,0,.15],forearm_R:[-1.35,0,-.15]}],[.57,{chest:[.08,.08,0],upper_arm_L:[-1.57,0,-.2],upper_arm_R:[-1.57,0,.2],forearm_L:[-.04,0,.1],forearm_R:[-.04,0,-.1],hand_L:[.35,-.3,0],hand_R:[.35,.3,0]}],[.76,{upper_arm_L:[-1.4,0,-.25],upper_arm_R:[-1.4,0,.25]}],[1,{}]]));
  clip('catch',.45,21,t=>sequence(t,[[0,{}],[.3,{upper_arm_L:[-1.3,0,-.18],upper_arm_R:[-1.3,0,.18],forearm_L:[-.22,0,.15],forearm_R:[-.22,0,-.15],hand_L:[-.12,-.35,0],hand_R:[-.12,.35,0]}],[.65,{chest:[-.04,0,0],upper_arm_L:[-.75,0,-.15],upper_arm_R:[-.75,0,.15],forearm_L:[-1.12,0,.12],forearm_R:[-1.12,0,-.12]}],[1,{}]]));
  clip('block',.8,29,t=>sequence(t,[[0,{}],[.2,{hips_position:[0,-.08,0],thigh_L:[-.22,0,-.08],thigh_R:[-.22,0,.08],shin_L:[.45,0,0],shin_R:[.45,0,0],upper_arm_L:[-.4,0,-.7],upper_arm_R:[-.4,0,.7]}],[.52,{chest:[-.03,0,0],upper_arm_L:[-2.85,0,-.3],upper_arm_R:[-2.9,0,.25],forearm_L:[-.05,0,0],forearm_R:[-.05,0,0],hand_L:[-.1,0,0],hand_R:[-.1,0,0],foot_L:[.12,0,0],foot_R:[.12,0,0]}],[.7,{upper_arm_L:[-2.8,0,-.3],upper_arm_R:[-2.8,0,.3],hand_L:[.3,0,0],hand_R:[.3,0,0]}],[1,{}]]));
  clip('crossover',.65,25,t=>{
    const a=Math.sin(t*PI*2),dip=Math.sin(t*PI)**2;
    return resting({hips_position:[0,-.12*dip,0],hips:[-.08*dip,-.1*a,.09*a],chest:[.14*dip,-.16*a,-.1*a],thigh_L:[-.22*dip,0,-.08*dip],thigh_R:[-.22*dip,0,.08*dip],shin_L:[.42*dip,0,0],shin_R:[.42*dip,0,0],upper_arm_R:[-.35-.25*dip,-.35*a,.1+.35*a],forearm_R:[-.4-.45*dip,0,0],hand_R:[.45*dip,0,0],upper_arm_L:[-.3-.25*dip,-.2*a,-.1+.35*a],forearm_L:[-.35-.4*dip,0,0],hand_L:[.4*dip,0,0]});
  });

  const authoredHeight=pos.reduce((max,value,i)=>i%3===1?Math.max(max,value):max,0);
  const metadata={authoredHeight,authoredWidth:.62*bodyScale,profileHeight:heightScale,profileWidth:bodyScale,front:'+Z',floor:0,originalAsset:true};
  const json={asset:{version:'2.0',generator:'Basketball 2 original athlete authoring pipeline',copyright:'Original Basketball 2 mesh, rig and animation assets',extras:metadata},scene:0,scenes:[{name:profile.name, nodes:[nodes.length+1]}],nodes,materials:MATERIALS.map(([name,color,roughness])=>({name,pbrMetallicRoughness:{baseColorFactor:color,metallicFactor:0,roughnessFactor:roughness},doubleSided:false})),meshes:[],skins:[],animations:[],buffers:[{byteLength:0}],bufferViews:[],accessors:[]};
  const chunks=[];let byteOffset=0;
  function accessor(array,componentType,type,target,withRange=false) {
    const buffer=Buffer.from(array.buffer,array.byteOffset,array.byteLength),padding=(4-byteOffset%4)%4;
    if(padding){chunks.push(Buffer.alloc(padding));byteOffset+=padding;}
    const view=json.bufferViews.length;json.bufferViews.push({buffer:0,byteOffset,byteLength:buffer.length,...(target?{target}:{})});chunks.push(buffer);byteOffset+=buffer.length;
    const sizes={SCALAR:1,VEC2:2,VEC3:3,VEC4:4,MAT4:16},size=sizes[type];
    const acc={bufferView:view,componentType,count:array.length/size,type};
    if(withRange){acc.min=Array(size).fill(Infinity);acc.max=Array(size).fill(-Infinity);for(let i=0;i<array.length;i++){acc.min[i%size]=Math.min(acc.min[i%size],array[i]);acc.max[i%size]=Math.max(acc.max[i%size],array[i]);}}
    const index=json.accessors.length;json.accessors.push(acc);return index;
  }
  const attributes={POSITION:accessor(new Float32Array(pos),5126,'VEC3',34962,true),NORMAL:accessor(new Float32Array(norm),5126,'VEC3',34962),TEXCOORD_0:accessor(new Float32Array(uv),5126,'VEC2',34962),JOINTS_0:accessor(new Uint16Array(skinJoints),5123,'VEC4',34962),WEIGHTS_0:accessor(new Float32Array(weights),5126,'VEC4',34962)};
  const primitives=groups.map((indices,material)=>({attributes,indices:accessor(new Uint16Array(indices),5123,'SCALAR',34963),material,mode:4})).filter(p=>json.accessors[p.indices].count);
  json.meshes.push({name:'Athlete',primitives});
  const inverseBind=[];
  for(const world of absolute)inverseBind.push(1,0,0,0,0,1,0,0,0,0,1,0,-world[0],-world[1],-world[2],1);
  json.skins.push({name:'AthleteRig',inverseBindMatrices:accessor(new Float32Array(inverseBind),5126,'MAT4'),skeleton:joints.root,joints:Object.values(joints)});
  const athleteNode=nodes.length;nodes.push({name:'Athlete',mesh:0,skin:0});
  nodes.push({name:'AthleteRoot',children:[joints.root,athleteNode],extras:metadata});
  for(const animation of animations) {
    const input=accessor(new Float32Array(animation.time),5126,'SCALAR',undefined,true),samplers=[],channels=[];
    for(const track of animation.tracks) {
      const output=accessor(new Float32Array(track.values),5126,track.type);
      channels.push({sampler:samplers.length,target:{node:track.node,path:track.path}});samplers.push({input,output,interpolation:'LINEAR'});
    }
    json.animations.push({name:animation.name,samplers,channels,extras:{duration:animation.duration,inPlace:true}});
  }
  if(pos.length/3>65535)throw new Error('Mesh exceeded unsigned-short vertex range');
  json.buffers[0].byteLength=byteOffset;
  const binary=Buffer.concat(chunks),binPadding=Buffer.alloc((4-binary.length%4)%4),rawJSON=Buffer.from(JSON.stringify(json)),jsonPadding=Buffer.alloc((4-rawJSON.length%4)%4,0x20),jsonChunk=Buffer.concat([rawJSON,jsonPadding]),binChunk=Buffer.concat([binary,binPadding]);
  const header=Buffer.alloc(12);header.writeUInt32LE(0x46546c67,0);header.writeUInt32LE(2,4);header.writeUInt32LE(12+8+jsonChunk.length+8+binChunk.length,8);
  const jh=Buffer.alloc(8);jh.writeUInt32LE(jsonChunk.length,0);jh.writeUInt32LE(0x4e4f534a,4);const bh=Buffer.alloc(8);bh.writeUInt32LE(binChunk.length,0);bh.writeUInt32LE(0x004e4942,4);
  const file=path.join(OUT,'player-'+profile.id+'.glb');fs.writeFileSync(file,Buffer.concat([header,jh,jsonChunk,bh,binChunk]));
  return {file:path.basename(file),vertices:pos.length/3,triangles:groups.reduce((n,g)=>n+g.length/3,0),joints:absolute.length,clips:animations.length,bytes:fs.statSync(file).size,height:json.nodes.at(-1).extras.authoredHeight};
}

fs.mkdirSync(OUT,{recursive:true});
const results=[
  {id:'guard',name:'Guard — agile build',width:.94,height:.96,number:3,hair:'fade'},
  {id:'wing',name:'Wing — balanced build',width:1,height:1,number:7,hair:'curls'},
  {id:'center',name:'Center — strong build',width:1.09,height:1.055,number:9,hair:'crop'}
].map(buildAthlete);
fs.writeFileSync(path.join(OUT,'manifest.json'),JSON.stringify({version:1,format:'glTF 2.0 binary',license:'Original assets authored for Basketball 2',coordinateSystem:{up:'+Y',front:'+Z',floor:0},models:results,clips:['idle','run','sprint','defensive_shuffle','pivot','jump_shot','layup','dunk','pass','catch','block','crossover']},null,2)+'\n');
for(const result of results)console.log(`${result.file}: ${result.vertices} vertices, ${result.triangles} triangles, ${result.joints} joints, ${result.clips} clips, ${(result.bytes/1024).toFixed(0)} KiB`);
