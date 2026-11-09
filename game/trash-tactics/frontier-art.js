// Original code-native tabletop scenery. Both layers are painted only when
// the ground cache is rebuilt; collider artwork follows the same rectangles
// the navigation graph uses, so painted passages remain real passages.
const TAU=Math.PI*2,INK='#354b4b';
function line(c,points,color,width=2){c.beginPath();points.forEach((p,i)=>i?c.lineTo(...p):c.moveTo(...p));c.strokeStyle=color;c.lineWidth=width;c.lineJoin='round';c.lineCap='round';c.stroke();}
function rect(c,x,y,w,h,fill,stroke=null,r=6,width=1.5){c.beginPath();c.roundRect(x,y,w,h,r);if(fill){c.fillStyle=fill;c.fill();}if(stroke){c.strokeStyle=stroke;c.lineWidth=width;c.stroke();}}
function oval(c,x,y,rx,ry,fill,stroke=null,width=1.5){c.beginPath();c.ellipse(x,y,rx,ry,0,0,TAU);if(fill){c.fillStyle=fill;c.fill();}if(stroke){c.strokeStyle=stroke;c.lineWidth=width;c.stroke();}}
const palettes={inkdelta:{base:'#c7d8d039',road:'#dce4cb',edge:'#6b9e98',ink:'#528c88'},corkcross:{base:'#d5bb7d39',road:'#ead7ab',edge:'#a68b59',ink:'#937443'},serverstack:{base:'#9bbeb439',road:'#cedbd0',edge:'#678f87',ink:'#518c81'}};
const routePoints=lane=>Array.isArray(lane)?lane:lane.points||[];

export function paintFrontierMap(renderer,c,layer='details'){
 const map=renderer.game.map||{},w=renderer.game.width||map.width,h=renderer.game.height||map.height;if(!map.frontier)return;
 const palette=palettes[map.id]||{base:'#bed0a216',road:map.id==='breakroom'?'#dce3ca':'#ede0bb',edge:'#9eaa7b',ink:'#7b9264'};
 if(layer==='base'){
  c.save();c.fillStyle=palette.base;c.fillRect(36,37,w-82,h-84);
  if(map.id==='serverstack'){
   for(let y=150;y<h-100;y+=220)for(let x=140;x<w-100;x+=240){rect(c,x,y,216,194,(Math.round(x/240)+Math.round(y/220))%2?'#90b1a914':'#c9d8bd18','#668f8530',4,1);oval(c,x+10,y+10,2,2,'#739b8977');oval(c,x+206,y+184,2,2,'#739b8977');}
   for(const x of [2300,4800,7300])for(const side of [-1,1])for(let j=0;j<16;j++){const y=230+j*370,xx=x+side*340;line(c,[[xx,y],[xx,y+180],[xx+side*40,y+220],[xx+side*95,y+220]],'#78a28c50',3);oval(c,xx+side*95,y+220,6,6,'#d6d3a766','#88a38b77',1);}
  }else if(map.id==='corkcross'){
   for(let i=0;i<950;i++){const x=75+(i*577.139)%(w-150),y=75+(i*353.73)%(h-150);oval(c,x,y,1+i%4,1+i%2,i%2?'#a48b5122':'#e5cc9638');}
   for(const [x,y,a]of [[1500,1650,-.05],[7400,1650,.07],[1400,4550,.04],[7450,4800,-.05]]){c.save();c.translate(x,y);c.rotate(a);rect(c,-320,-210,640,420,'#efe1b123','#b6a16d35',2,1);for(let j=0;j<6;j++)line(c,[[-275,-150+j*55],[265,-150+j*55]],'#ae9b6c20',2);rect(c,-80,-230,160,45,'#e4c37a2d',null,2);c.restore();}
  }else if(map.id==='inkdelta'){
   for(const x of [2800,6800])for(let i=0;i<8;i++){const y=420+i*790;oval(c,x+(i%2?100:-100),y,440,180,'#90b3a623');c.beginPath();c.ellipse(x,y,360,125,-.1,.1,Math.PI*1.6);c.strokeStyle='#769e8d2a';c.lineWidth=3;c.stroke();}
  }
  // Route widths come from the tactical metadata. These beds are flat surface
  // materials; solid terrain and exact ramp silhouettes are painted above.
  for(const [i,lane]of (map.lanes||[]).entries()){
   const points=routePoints(lane);if(points.length<2)continue;const info=map.laneInfo?.[i],width=info?.width||400;
   line(c,points,palette.edge+'20',width+24);line(c,points,palette.road+'66',width);line(c,points,'#fff7df25',Math.max(80,width-85));
  }
  c.restore();return;
 }
 c.save();
 for(const [i,lane]of (map.lanes||[]).entries()){
  const points=routePoints(lane),info=map.laneInfo?.[i];if(points.length<2||!info||info.name?.startsWith('Camp '))continue;
  const middle=Math.max(1,Math.floor(points.length/2)),a=points[middle-1],b=points[middle],x=a[0]*.4+b[0]*.6,y=a[1]*.4+b[1]*.6;
  if((map.expansions||[]).some(p=>Math.hypot(p.x-x,p.y-y)<250))continue;
  const angle=Math.atan2(b[1]-a[1],b[0]-a[0]);c.save();c.translate(x,y);c.rotate(Math.cos(angle)<0?angle+Math.PI:angle);
  rect(c,-94,-38,188,22,'#f6edcf80',palette.edge+'55',3,.8);c.fillStyle=palette.ink;c.textAlign='center';c.font='bold 12px "Trebuchet MS",sans-serif';c.fillText(info.role==='main'?'MAIN ROUTE':info.role==='highground'?'RAMP ROUTE':info.role==='crossing'?'CROSSING':'FLANK ROUTE',0,-23);
  for(const side of [-1,1])line(c,[[side*112-7,-8],[side*112,0],[side*112-7,8]],palette.edge+'88',2);c.restore();
 }
 for(const p of map.expansions||[]){
  const rich=p.richness>=1.5,col=p.role==='natural'?'#6f9c70':rich?'#af8c49':palette.edge;
  c.save();c.translate(p.x,p.y);c.setLineDash([7,12]);oval(c,0,0,225,225,null,col+'44',1.5);c.setLineDash([]);
  for(const sx of [-1,1])for(const sy of [-1,1])line(c,[[sx*134,sy*109],[sx*134,sy*134],[sx*109,sy*134]],col+'b0',2.6);
  const label=p.role==='natural'?'SHELTERED NATURAL':rich?(p.role==='outer'?'RICH OUTER SALVAGE':'RICH CENTER CACHE'):p.role==='highground'?'ELEVATED SCRAP':'CONTESTED SCRAP';
  rect(c,-115,231,230,40,'#f8efccad',col+'77',3,1);c.fillStyle=col;c.font='bold 12px "Trebuchet MS",sans-serif';c.textAlign='center';c.fillText(label,0,247);c.font='10px "Trebuchet MS",sans-serif';c.fillText(`${Math.round((p.amountMultiplier||1)*100)}% scrap · ${p.risk==='safe'?'sheltered':p.risk==='exposed'?'exposed approaches':'shared approaches'}`,0,262);
  if(rich)for(let j=0;j<3;j++)rect(c,-8+j*7,-10-j*3,12,7,'#e5c070a0',col+'b0',1,.8);
  c.restore();
 }
 c.restore();
}

export function drawFrontierObstacle(renderer,c,o){
 if(!['inkpool','serverrack','corkbundle','pinbox'].includes(o.type))return false;
 const {x,y,w,h}=o;c.save();c.translate(x,y);
 rect(c,6,12,w,h,'#344b3b30',null,9);
 if(o.type==='inkpool'){
  rect(c,0,0,w,h,'#557d7d','#3f676a',7,3);rect(c,7,7,w-14,h-14,'#426870','#335d64',5,1.5);
  c.save();c.beginPath();c.rect(9,9,w-18,h-18);c.clip();
  for(let j=0;j<9;j++){const xx=25+j*(w-50)/8;c.beginPath();c.moveTo(xx,-15);for(let yy=0;yy<h+60;yy+=45)c.lineTo(xx+Math.sin(yy*.012+j)*14,yy);c.strokeStyle=j%2?'#6d9d982b':'#2f596438';c.lineWidth=16+j%3*4;c.stroke();}
  for(let i=0;i<Math.ceil(w*h/38000);i++){const xx=24+(i*137.3)%(w-48),yy=24+(i*223.7)%(h-48);oval(c,xx,yy,5+i%3*2,2+i%2,'#a2c5b029');}
  c.restore();line(c,[[9,h-7],[w-9,h-7]],'#9bbba288',2);line(c,[[9,8],[w-9,8]],'#c6d8bd99',2);
  for(const xx of [0,w-8])for(let yy=10;yy<h-10;yy+=36)rect(c,xx,yy,8,19,'#dbc88b66',null,1);
 }else if(o.type==='serverrack'){
  rect(c,0,0,w,h,'#5b7a76','#3b585a',8,3);rect(c,9,9,w-18,h-18,'#82998a','#4e6b66',4,1);
  const columns=Math.max(2,Math.floor(w/165)),cw=(w-34)/columns;
  for(let row=0;row<Math.floor((h-22)/103);row++)for(let col=0;col<columns;col++){
   const xx=17+col*cw,yy=18+row*103;rect(c,xx,yy,cw-7,92,'#506b68','#3c5859',3,1.2);rect(c,xx+6,yy+5,cw-19,16,'#9bad91',null,2);
   for(let j=0;j<4;j++)line(c,[[xx+10,yy+35+j*9],[xx+cw-31,yy+35+j*9]],'#8da595',2);oval(c,xx+cw-19,yy+39,3,3,row%3?'#abc589':'#dfba6c');oval(c,xx+cw-19,yy+52,2,2,'#d7d8ab');
  }
  for(const xx of [8,w-8])for(let yy=24;yy<h-5;yy+=130){oval(c,xx,yy,2.5,2.5,'#c0c5a2',INK,1);line(c,[[xx-2,yy],[xx+2,yy]],'#58726b',.8);}
  line(c,[[4,6],[w-4,6]],'#d6d8b5',3);line(c,[[4,h-5],[w-4,h-5]],'#355956',4);
 }else if(o.type==='corkbundle'){
  rect(c,0,0,w,h,'#a78c58','#7f754f',7,2.5);rect(c,3,-5,w-6,h-9,'#d1b67a','#a08b57',5,2);
  for(let i=0;i<Math.ceil(w*h/450);i++){const xx=9+(i*43.71)%(w-18),yy=7+(i*79.17)%(h-25);oval(c,xx,yy,1+i%3,1+i%2,i%2?'#937a493a':'#efd39f55');}
  for(let xx=22;xx<w;xx+=110)line(c,[[xx,3],[xx-7,h-20]],'#a28a5350',1.5);
  rect(c,w*.43,-5,w*.13,h-9,'#e5d1a18a','#b7a16c44',1,.8);line(c,[[8,h-12],[w-8,h-12]],'#efd6a077',2);
 }else{
  rect(c,0,0,w,h,'#ae9b72','#7f805f',9,2.5);rect(c,8,7,w-16,h-20,'#d3c394','#a89870',5,1.3);
  for(let row=0;row<4;row++)for(let col=0;col<6;col++){const xx=25+col*(w-50)/5,yy=23+row*(h-55)/3;line(c,[[xx,yy+7],[xx+7,yy+20]],'#627970',2);oval(c,xx,yy,9,8,['#b87666','#75a58f','#cfaf66','#9a87ad'][(row+col)%4],'#64756a',1.2);oval(c,xx-2,yy-2,3,2,'#f3e3b591');}
  line(c,[[10,h-12],[w-10,h-12]],'#ece0b0',3);
 }
 c.restore();return true;
}
