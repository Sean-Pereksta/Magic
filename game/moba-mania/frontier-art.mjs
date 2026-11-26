// Pixel sprites and terrain dressing share the game's canvas palette.
export function drawFrontierHero(c,id,{frame,stage,back,palette},art) {
  const {box,line,diamond,ring}=art;
  const [dark,mid,light]=palette, ink='#182333';
  const step=[0,2,0,-2][frame%4], thrust=[0,-4,7,4,1][stage]||0;
  const rect=(fill,x,y,w,h)=>box(c,fill,x,y,w,h);
  if(id==='mirelle'){
    rect(ink,10,21,21,20);rect(dark,11,21,19,17);rect(mid,13,22,7,14);
    rect(ink,10,36,8,6+step);rect(ink,24,36,7,6-step);
    rect('#684953',7,8,26,9);rect('#9d6c64',11,5,19,10);
    rect('#c89f75',11,7,6,3);rect(light,9,13,23,3);rect('#5a4649',13,16,15,8);
    if(!back){rect('#dca684',15,17,11,6);rect(ink,21,19,3,2);}
    rect('#8e533d',32+thrust,14,3,24);rect(light,29+thrust,12,9,9);
    rect('#6aa078',30+thrust,17,7,4);rect('#fbefe1',32+thrust,13,3,2);
    rect(light,7,26,7,9);rect('#935840',8,31,4,3);
  }else if(id==='knell'){
    rect(ink,5,21,10,12);rect(ink,27,21,10,12);rect(mid,5,22,8,8);rect(mid,29,22,8,8);
    rect(ink,11,31,7,11+step);rect(ink,22,31,7,11-step);
    rect(dark,11,3,18,8);rect(light,16,3,8,3);
    rect(ink,7,10,26,20);rect(mid,9,9,22,22);rect(light,12,10,5,14);
    rect(dark,7,25,27,10);rect(light,7,25,27,3);rect(mid,11,30,18,4);
    if(!back){rect(ink,14,17,14,5);rect('#fff3bb',17,18,3,2);rect('#fff3bb',24,18,3,2);}
    rect(dark,33+thrust,17,4,19);rect(light,29+thrust,13,13,7);rect(mid,31+thrust,19,10,3);
  }else if(id==='zephyr'){
    for(const side of [-1,1]){
      const x=side<0?12:23;
      for(let i=0;i<4;i++){
        rect(ink,x+side*i*4,18+i*2+step,7,15-i*2);
        rect(i%2===0?mid:dark,x+side*i*4+1,19+i*2+step,5,11-i);
      }
    }
    rect(ink,13,19,15,19);rect(mid,15,20,11,16);rect(light,19,24,3,10);
    rect(ink,15,35,5,7+step);rect(ink,24,34,5,7-step);
    rect(dark,13,8,17,14);rect('#edf3e8',16,7,14,10);rect('#adcacd',15,5,9,4);
    if(!back){rect(dark,22,11,6,3);rect(light,28,13,7,4);rect('#2e252d',30,16,3,2);}
    line(c,light,29+thrust,27,39+thrust,20,3);line(c,'#eaffdc',39+thrust,20,42+thrust,26,2);
  }else{
    rect(ink,13,20,15,16);rect(mid,14,22,12,10);rect(dark,16,33,8,5);
    diamond(c,20,14,17,24,dark,light);diamond(c,20,13,9,16,mid,'#fbf1ff');
    if(!back)rect('#4d3a72',19,11,4,4);
    for(const [x,y,size] of [[7,23+step,6],[34+thrust,21-step,9],[25,37,5]]){
      diamond(c,x,y,size,size*2,dark,light);
      rect('#fff1f9',x,y-2,2,3);
    }
    if(stage===2)ring(c,light,33+thrust,21,11,2);
  }
}

export const LANE_PALETTES=[
  ['#293d36','#54704d','#848b63','#a7a477','#b5af80','#8c936d'],
  ['#273d47','#536666','#788687','#9baba6','#adb7ad','#839695'],
  ['#253f40','#49695b','#78846a','#9c9374','#b6a482','#827c61']
];

export function drawLaneEdges(c,lane,art) {
  const {box,line,hash}=art;
  for(let j=2;j<lane.points.length-1;j++){
    const a=lane.points[j-1],b=lane.points[j],dx=b.x-a.x,dy=b.y-a.y,len=Math.hypot(dx,dy);
    for(let d=42;d<len-28;d+=54){
      const h=hash(d,j),x=a.x+dx*d/len-dy/len*(70+h*14),y=a.y+dy*d/len+dx/len*(70+h*14);
      if(Math.abs(x-1600)<140)continue;
      if(lane.id===0){
        box(c,'#424c45',x-10,y-14,24,15);
        box(c,'#747a67',x-9,y-15,21,7);
        box(c,'#909782',x-6,y-20,13,6);
        box(c,'#416849',x+2,y-17,5,12);
        box(c,'#6d8a56',x+6,y-12,8,4);
        if(h>.8){box(c,'#586252',x+15,y-32,10,32);box(c,'#858e76',x+13,y-33,14,5);}
      }else if(lane.id===1){
        box(c,'#506a69',x-12,y-5,25,8);
        box(c,'#748984',x-11,y-8,21,5);
        line(c,'#6f9f99',x-9,y+5,x+8,y+8,2);
      }else{
        box(c,'#385d56',x-15,y-10,28,18);
        box(c,'#517469',x-12,y-8,20,4);
        for(let k=0;k<3;k++){
          const xx=x+k*5-7;
          line(c,'#94a56c',xx,y,xx+3,y-11-k*2,2);
          box(c,'#6f674e',xx+1,y-13-k*2,4,5);
        }
      }
    }
  }
}

export function drawLaneCrossings(c,art) {
  const {box}=art;
  const bridgeX=1604;
  // Stone floodgates bracket the middle bridge. Wooden decking crosses the marsh.
  for(const [y,wood] of [[330,false],[1100,false],[1870,true]]){
    box(c,wood?'#4a4b43':'#425757',bridgeX-112,y-42,224,84);
    box(c,wood?'#a58c65':'#9da9a1',bridgeX-109,y-34,218,68);
    for(let x=bridgeX-105;x<bridgeX+108;x+=wood?12:28){
      box(c,wood?'#b39a70':'#aeb8ae',x,y-30,wood?9:24,60);
      if(!wood)box(c,'#808f85',x,y+((x>>1)%3-1)*9,24,2);
    }
    for(const edge of [-1,1]){
      const yy=y+edge*35;
      box(c,wood?'#796951':'#748680',bridgeX-112,yy,224,5);
      for(const x of [bridgeX-96,bridgeX+91]){
        box(c,wood?'#655044':'#546a67',x,yy-7,9,14);
        box(c,wood?'#c0a585':'#a8b5a9',x-2,yy-8,13,4);
        if(y===1100)box(c,'#6dc0b3',x+2,yy-3,5,4);
      }
    }
  }
}
