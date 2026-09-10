'use strict';
/* Small vector bodies retain family colors, existing poses, hit flashes and boss telegraphs. */
(() => {
  function family(e){
    const name=`${e.type||''} ${e.name||''} ${e.ai||''}`.toLowerCase();
    if(/slime|ooze/.test(name))return 'slime';
    if(/serpent|snake/.test(name))return 'serpent';
    if(/beetle|burrow|scarab|spider/.test(name))return 'insect';
    if(/moth|harpy|drake|phoenix|cinderwing|diver/.test(name))return 'winged';
    if(/wolf|hound|charger|ram|boar|beast/.test(name))return 'beast';
    if(/golem|sentinel|behemoth|mimic|turret/.test(name))return 'golem';
    if(/eye|wisp|orbiter|lantern|wheel/.test(name))return 'spirit';
    return 'humanoid';
  }
  function line(points,color,width=2){ctx.strokeStyle=color;ctx.lineWidth=width;ctx.beginPath();points.forEach(([x,y],i)=>i?ctx.lineTo(x,y):ctx.moveTo(x,y));ctx.stroke();}
  function shape(points,color){ctx.fillStyle=color;ctx.strokeStyle='#20212b';ctx.lineWidth=1.2;ctx.beginPath();points.forEach(([x,y],i)=>i?ctx.lineTo(x,y):ctx.moveTo(x,y));ctx.closePath();ctx.fill();ctx.stroke();}
  function oval(x,y,rx,ry,color){ctx.fillStyle=color;ctx.strokeStyle='#26252c';ctx.lineWidth=1;ctx.beginPath();ctx.ellipse(x,y,rx,ry,0,0,TAU);ctx.fill();ctx.stroke();}
  function eyes(x,y,side,color){oval(x+side*3,y,1.3,1.5,color);}
  const baseBody=drawEnemyBody;
  drawEnemyBody=function(e){
    if(e.hidden)return baseBody(e);
    const f=family(e),c=e.flash>0?'#fff':e.color||'#90748f',trim=e.proj||'#d8c9a9';
    const side=(e.facing?.x??1)-(e.facing?.y??0)>=0?1:-1;
    const pose=window.AWPresentation?.pose(e),stride=Math.sin(pose?.stride||0)*(pose?.speed||0),step=stride*4;
    const name=`${e.type||''} ${e.name||''} ${e.ai||''}`.toLowerCase();
    ctx.save();ctx.lineJoin='round';ctx.lineCap='round';ctx.shadowBlur=0;
    if(f==='humanoid'){
      const skeleton=/skeleton|crypt blade|bone/.test(name),caster=/mage|shaman|seer|witch|necro|caller|priest|cultist|oracle|puppeteer|choir|bomber|spread|beam/.test(name);
      const archer=/archer|bandit|sniper|ranged/.test(name),armored=/knight|warden|executioner|guard|shield/.test(name);
      // Two jointed legs with boots; the pelvis sits below the shoulders.
      for(const n of [-1,1]){line([[n*5,0],[n*6+n*step,8],[n*6+n*step*.6,15]],skeleton?trim:'#343342',4);line([[n*6+n*step*.6,15],[n*6+n*step*.6+side*3,15]],'#23232d',4);}
      if(caster)shape([[-9,-20],[9,-20],[14,9],[3,5],[-12,10]],c);
      else shape([[-10,-20],[10,-20],[8,-1],[0,4],[-8,-1]],c);
      line([[-8,-17],[0,-13],[8,-17]],trim,1.4);line([[-8,-1],[8,-1]],'#493932',3);oval(0,-1,2,2,trim);
      const lift=e.telegraph?7:0;
      for(const n of [-1,1]){line([[n*10,-17],[n*14,-9-lift],[n*16,-5-lift]],c,5);oval(n*16,-5-lift,2.5,3,skeleton?trim:'#d2ad94');}
      oval(side,-27,6.5,8,skeleton?'#dcd5bd':'#d2ad94');
      if(skeleton){oval(-2,-28,2,2,'#252530');oval(3,-28,2,2,'#252530');line([[-2,-23],[4,-23]],'#57515a',1);for(let j=0;j<3;j++)line([[-5,-15+j*4],[5,-15+j*4]],trim,1.5);}
      else{shape([[-6,-29],[-5,-35],[4,-35],[8,-30],[side*2,-29]],armored?'#84919e':'#3d303a');eyes(side,-28,side,'#231e2b');line([[side*3,-23],[side*5,-23]],'#79534f',1);}
      if(armored){oval(-10,-18,5,4,trim);oval(10,-18,5,4,trim);line([[-4,-16],[0,-6],[5,-15]],'#c8d6df',1);}
      if(caster){line([[16,4],[19,-31]],'#6b5145',3);shape([[19,-39],[24,-32],[19,-25],[14,-32]],trim);line([[-5,-19],[-3,5]],trim,1);}
      else if(archer){ctx.strokeStyle='#dfbe88';ctx.lineWidth=2;ctx.beginPath();ctx.arc(15,-12,12,-Math.PI/2,Math.PI/2);ctx.stroke();line([[15,-24],[15,0]],'#e4d8b7',1);line([[8,-11],[29,-11]],'#e4d8b7',1.5);}
      else{line([[16,1],[19,-19]],'#b6bdc9',3);shape([[19,-25],[22,-17],[18,-14]],trim);line([[13,-4],[21,-4]],'#d6ae74',2);}
      if(/shield|warden|mirror.*knight/.test(name))shape([[-20,-20],[-9,-22],[-7,-6],[-14,0],[-22,-8]],'#8996a3');
    }else if(f==='beast'){
      ctx.scale(side,1);
      for(let i=0;i<4;i++){const x=-11+i*7,s=(i%2?step:-step);line([[x,-4],[x+s,4],[x+s+2,10]],'#45413e',4);}
      oval(-2,-10,18,10,c);oval(14,-16,9,9,c);oval(22,-13,7,4,c);
      shape([[10,-22],[10,-32],[17,-24]],c);shape([[19,-22],[24,-29],[24,-19]],c);
      line([[-17,-12],[-25,-18],[-29,-13]],c,5);oval(28,-14,2,2,'#23222c');eyes(16,-18,1,'#fff1ba');
      line([[-11,-15],[-5,-10],[-2,-16],[4,-11]],'#c6b995',1.3);
      if(/ram|horn|charger/.test(name)){ctx.strokeStyle=trim;ctx.lineWidth=3;ctx.beginPath();ctx.arc(12,-24,7,0,Math.PI*1.8);ctx.stroke();}
    }else if(f==='insect'){
      for(const n of [-1,1])for(let i=0;i<3;i++)line([[n*8,-16+i*7],[n*19,-19+i*9+step],[n*23,-10+i*8]],'#655443',2.5);
      oval(0,-8,13,16,c);line([[0,-22],[0,5]],trim,1.5);for(let i=0;i<3;i++)line([[-10,-15+i*6],[0,-12+i*6],[10,-15+i*6]],'#b9b099',1);
      oval(0,-24,7,5,c);eyes(-5,-25,1,'#fbe7a5');eyes(3,-25,1,'#fbe7a5');
      line([[-4,-28],[-10,-34]],trim,2);line([[4,-28],[10,-34]],trim,2);
    }else if(f==='serpent'){
      const bend=Math.sin(elapsed*5+(e.phase||0))*3;
      line([[-21,4],[-9,1],[5,-5],[-3,-14],[side*8+bend,-24]],'#263335',9);
      line([[-21,4],[-9,1],[5,-5],[-3,-14],[side*8+bend,-24]],c,6);
      oval(side*8+bend,-25,7,5,c);eyes(side*8+bend,-26,side,'#fff4c3');
      line([[side*13+bend,-24],[side*21+bend,-23]],'#e59aac',1);
    }else if(f==='winged'){
      const flap=Math.sin(elapsed*7+(e.phase||0))*5;
      for(const n of [-1,1]){shape([[n*5,-18],[n*23,-31+flap],[n*32,-13+flap],[n*21,-17],[n*14,-6],[n*6,-3]],c);line([[n*6,-17],[n*23,-26+flap],[n*20,-12]],trim,1);}
      oval(0,-11,7,14,c);oval(side*3,-28,6,7,c);eyes(side*3,-29,side,'#fff1a5');
      shape([[side*7,-29],[side*16,-25],[side*7,-23]],trim);line([[-3,0],[-5,8],[-9,9]],trim,2);line([[3,0],[5,8],[9,9]],trim,2);
    }else if(f==='golem'){
      for(const n of [-1,1]){shape([[n*3,-1],[n*11,-1],[n*13,12],[n*3,12]],c);oval(n*17,-13,7,10,c);line([[n*18,-5],[n*20,3]],trim,5);}
      shape([[-13,-24],[11,-25],[15,-6],[7,2],[-10,0],[-16,-12]],c);
      shape([[-7,-36],[7,-36],[10,-26],[-8,-23]],c);line([[-5,-29],[5,-29]],trim,2);
      shape([[0,-21],[5,-13],[0,-7],[-5,-13]],trim);line([[-11,-20],[-6,-13],[-12,-5]],'#b3a391',1);
    }else if(f==='slime'){
      oval(0,-5,17,12,c);oval(-7,-11,5,3,'#99daa5');oval(-5,-7,2,3,'#28372f');oval(6,-7,2,3,'#28372f');line([[-2,-1],[3,0],[6,-2]],'#344b3a',1.5);oval(10,0,3,2,'#89cf91');
    }else{
      for(let i=0;i<4;i++){const x=(i-1.5)*7;line([[x,-5],[x+Math.sin(elapsed*3+i)*4,5],[x-2,12]],c,3);}
      oval(0,-15,13,15,c);oval(0,-18,9,6,'#292133');oval(side*2,-18,4,5,trim);oval(side*3,-18,1.5,3,'#181527');
      line([[-10,-29],[0,-35],[10,-29]],trim,1.5);
    }
    ctx.restore();
  };
  window.AWEnemyAnatomy={family};
})();
