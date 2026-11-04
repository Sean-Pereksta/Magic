// Trail scenery and actors are painted in-game on a low-resolution pixel canvas.
(() => {
  'use strict';
  const LOOP = 864;
  const floor = Math.floor, R = Math.round, sin = Math.sin, cos = Math.cos;
  const elements = {leaf:'#8cdf69',fire:'#ff8c47',frost:'#89e2f6',storm:'#f8d968',stone:'#efc176',spirit:'#d4a6ff'};
  // Sky, haze, light, distant ridge, mid ridge, mid shadow, upper foliage, leaf, leaf shade, trunk, grass, road sun, road, earth, accent.
  const climates = [
    ['#74b6b4','#f6d991','#fff0ad','#abc8a0','#81b098','#5d9786','#b7c969','#54925d','#235b4e','#344f43','#a5ba61','#e8bd79','#cfa46f','#544f3a','#ffd081'],
    ['#9a6875','#ffc58b','#ffe8a0','#d7957a','#b77569','#985963','#d98151','#a84e43','#633b4a','#51343b','#c0834e','#dcad77','#bc8768','#633e43','#ffd060'],
    ['#81abc3','#e5eae0','#fff3bf','#adcad1','#89adbd','#678b9e','#cbe6e4','#7eb1c0','#41677f','#3c5469','#c7dce2','#d8dfdb','#acbec7','#526178','#a5f5ff'],
    ['#7d809e','#e5bf9e','#ffe6ad','#a4a4b1','#858a9f','#656e84','#bdab79','#758780','#444f66','#3e4459','#a5aa82','#d5c4a0','#b0a594','#504b55','#ffc674'],
    ['#cc9c79','#ffe1a3','#fff4c4','#dcb487','#c19777','#a6755e','#c4ad66','#88954a','#576440','#674c3d','#c6a65c','#eccb8b','#ceab72','#735744','#f39759'],
    ['#555580','#c9a6a7','#ffe8c5','#99869b','#766f91','#605a7b','#99b5a0','#617f79','#3c515c','#424458','#a3ac81','#b7a992','#9a8c84','#474453','#a4efe1'],
    ['#507d91','#bad4c2','#f9e9b5','#91b8b4','#749d9b','#5b8085','#9bc7a1','#639d8f','#355d64','#364e59','#9aba98','#ccc8a2','#aaa98c','#4c5856','#8de2e3'],
    ['#494b78','#c6aac2','#ffedbe','#9c94af','#777b9b','#626582','#b1accb','#8c83a9','#49496d','#43425a','#b6abc5','#d5c0ae','#ae9faa','#554b60','#ffe4a9'],
    ...[
    // The outer circuit: coast, forge, hollow, dunes, monastery, fossils, reef, court, fjord, rookery, quarry, summit.
    '68b9c9 fbe6ae fff0c2 9accc4 69b5b5 4d8d93 f39783 ba6965 784e57 805b60 b8d8af f4daa1 d9bd8c 6d7967 ffc3a4',
    '6b6b77 f0aa72 ffe294 b68b7e 867176 6e5963 d09753 b07c49 604a44 493f49 bc9562 cab990 ac987f 5d5351 ffb858',
    '252540 776985 dbedc9 555470 4a4663 3c3853 c2dfaf 6bb3a5 4e5e68 493b50 9a9e88 b4a897 958b8a 514554 b7f8cd',
    'c2a5c9 ffe7c1 fff8cf ddc2b5 bc9999 a78086 e9d9e6 bdaac1 877c95 797181 e4cfc0 e7d6c3 ccb7b1 857477 b3e8e0',
    '81a9ca f5d6aa fff4c8 b3c7d4 8faab5 748b9e d2a167 a0785c 5c5d68 61515c c6b998 e5cea8 c3ad96 74685e ffcd7c',
    '9daba1 e3cfac f6e8b7 c0b697 a69c83 867e70 c1ac7e 99866a 655e55 5c5148 bba477 d4bb93 b9a17f 726254 e8d5a5',
    '244e6a 77afa6 d5f8c9 598c9d 467d8b 386771 9be2c6 57a7a7 33657b 354f66 8cb9a3 b4c8aa 98ad9a 566e71 f5bbdf',
    'a488aa f3ccb7 ffe9bb cbb0b7 a294a6 887b8e da94a6 9c767f 535d62 675366 aba18e dcc4b0 c5a797 77615f f5bfd2',
    '323d66 92a9b4 e8edc7 7c94b0 627d9a 505d80 b9dce2 7e9fb3 445b79 425269 aebeca cad7d6 a3b7c2 586777 9ae2bf',
    '8a87aa e7c7b1 ffedb9 b6a3b1 968b9c 76748a ceb480 a88d65 635968 5a4e5c baac82 d7c3a0 b6a58d 6f615f f7ca72',
    '4c426e cd9d9a ffddaf 9e839e 806a89 665870 b6a7c5 8c839a 534b63 4d4657 ad9fa5 c5b5ad a7999c 61555f ffcc85',
    'bd9b76 ffe7a2 fff7c2 d8bf8b b3aa7c 92906c d8cb68 9baa64 627852 6b6045 b9b774 ebd3a0 cbb581 76694c ffe093'
    ].map(row=>row.split(' ').map(hex=>'#'+hex))
  ];
  const colorKeys = ['sky','haze','light','far','mid','shade','leafTop','leaf','dark','wood','grass','roadHi','road','earth','accent'];
  const palettes = climates.map(row => Object.fromEntries(colorKeys.map((s,i)=>[s,row[i]])));
  function rgb(c) {
    if (typeof c !== 'string' || !/^#[a-f0-9]{6}$/i.test(c)) return [137,170,115];
    return [1,3,5].map(i=>parseInt(c.slice(i,i+2),16));
  }
  function mix(a,b,k) {
    const A=rgb(a),B=rgb(b);
    return '#'+A.map((n,i)=>R(n+(B[i]-n)*k).toString(16).padStart(2,'0')).join('');
  }
  function rng(s) { return () => {s=(Math.imul(s,1664525)+1013904223)>>>0;return s/4294967296;}; }
  function surface(w,h){const c=document.createElement('canvas');c.width=w;c.height=h;return c;}
  function rect(g,c,x,y,w,h){g.fillStyle=c;g.fillRect(R(x),R(y),Math.max(1,R(w)),Math.max(1,R(h)));}
  // Row fills keep every skyline and character silhouette aligned to the pixel grid.
  function poly(g,c,pts){
    let min=1e5,max=-1e5;
    for(let i=1;i<pts.length;i+=2){min=Math.min(min,pts[i]);max=Math.max(max,pts[i]);}
    g.fillStyle=c;
    for(let y=Math.ceil(min-.5);y<max-.5;y++){
      let hits=[];
      for(let i=0,j=pts.length-2;i<pts.length;j=i,i+=2){
        let a=pts[j+1],b=pts[i+1],v=y+.5;
        if((a<=v&&b>v)||(b<=v&&a>v))hits.push(pts[j]+(v-a)*(pts[i]-pts[j])/(b-a));
      }
      hits.sort((a,b)=>a-b);
      for(let i=0;i+1<hits.length;i+=2){const left=R(hits[i]);g.fillRect(left,y,R(hits[i+1])-left,1);}
    }
  }
  function oval(g,c,x,y,rx,ry){
    g.fillStyle=c;rx=Math.max(1,rx);ry=Math.max(1,ry);
    for(let i=-Math.floor(ry);i<=ry;i++){
      let n=R(rx*Math.sqrt(Math.max(0,1-((i)/(ry+.35))**2)));
      g.fillRect(R(x)-n,R(y)+i,n*2+1,1);
    }
  }
  function line(g,c,x,y,x2,y2,w=1){
    x=R(x);y=R(y);x2=R(x2);y2=R(y2);
    let dx=Math.abs(x2-x),sx=x<x2?1:-1,dy=-Math.abs(y2-y),sy=y<y2?1:-1,err=dx+dy;
    g.fillStyle=c;const k=w>>1;
    for(;;){
      g.fillRect(x-k,y-k,w,w);if(x===x2&&y===y2)break;
      const e=2*err;if(e>=dy){err+=dy;x+=sx;}if(e<=dx){err+=dx;y+=sy;}
    }
  }
  function outerSky(g,h,p,r) {
    const w=g.canvas.width,random=rng(489+r),sx=R(w*.69),sy=R(h*.21);
    if(r===10||r===14){
      if(r===10){
        // Basalt ceiling cut by luminous cracks.
        for(let x=-8;x<w;x+=7){const len=8+random()*13+(x%3===0?random()*20:0);poly(g,p.shade,[x-6,0,x+11,0,x+5,len,x+3,len+4,x,len*.7]);}
      }else{
        g.save();g.globalAlpha=.12;
        for(let i=0;i<9;i++){const x=i*w/8;poly(g,p.light,[x-5,0,x+3,0,x+27,h*.69,x+7,h*.69]);}g.restore();
        for(let i=0;i<32;i++){let x=random()*w,y=random()*11;line(g,p.far,x,y,x+9+random()*18,y);}
      }
      for(let i=0;i<62;i++){const x=random()*w,y=10+random()*h*.54;if(i%6===0&&r===14)orbit(g,p.far,x,y,2,1,0);else rect(g,i%4?p.far:p.accent,x,y,1,i%5?1:2);}
      return;
    }
    const night=r===16||r===18;
    if(night){
      for(let i=0;i<75;i++)rect(g,i%4?p.far:p.light,random()*w,random()*h*.49,1,1);
      if(r===16){
        // Two long curtain-like bands rather than the usual mountain sky.
        for(let i=0;i<w;i+=2)for(let b=0;b<2;b++){
          const y=h*(.14+b*.1)+sin(i*.018+b*2)*9+sin(i*.044)*2,depth=8+7*sin(i*.032+b);
          rect(g,mix(p.sky,b?p.leafTop:p.accent,.29),i,y-depth,2,depth+1);rect(g,mix(p.sky,p.accent,.58),i,y,2,2);
        }
      }else for(let i=0;i<5;i++){const x=w*(.12+i*.21),y=h*(.08+random()*.24);line(g,p.far,x-13,y-6,x,y);rect(g,p.light,x,y,2,2);}
    }
    if(r!==9){
      const radius=r===19?27:night?9:16;
      oval(g,mix(p.light,p.haze,.45),sx,sy,radius+3,radius+3);oval(g,p.light,sx,sy,radius,radius);
      if(r===18)oval(g,mix(p.sky,p.haze,.2),sx+4,sy-3,radius-1,radius-1);
    }
    if(!night)for(let i=0;i<12;i++){
      const x=random()*w,y=6+random()*h*.32,n=11+random()*30;
      if(r===12){oval(g,p.haze,x,y,n,3);oval(g,p.light,x-3,y-2,n*.6,3);}
      else{rect(g,mix(p.haze,p.light,.27),x,y,n,1);rect(g,p.haze,x+n*.25,y-2,n*.45,2);}
    }
  }
  // All landmark pixels are stamped onto scrolling scenery only when the biome or viewport changes.
  function waymark(g,x,y,scale,r,p,small,seed){
    const random=rng(seed),D=p.dark,M=p.leaf,H=p.leafTop,A=p.accent,T=p.wood,F=p.light;
    g.save();g.translate(R(x),R(y));g.scale(scale,scale);
    const B=(c,x,y,w,h)=>rect(g,c,x,y,w,h),P=(c,...pts)=>poly(g,c,pts),O=(c,x,y,a,b)=>oval(g,c,x,y,a,b);
    const L=(c,x,y,xx,yy,n=1)=>line(g,c,x,y,xx,yy,n);
    const gear=(x,y,rad)=>{O(D,x,y,rad,rad);O(M,x,y,rad-2,rad-2);for(let i=0;i<8;i++){const a=i*Math.PI/4;L(D,x+cos(a)*rad,y+sin(a)*rad,x+cos(a)*(rad+3),y+sin(a)*(rad+3),3);}O(D,x,y,rad*.65,rad*.65);for(let i=0;i<4;i++){const a=i*Math.PI/2+.4;L(H,x,y,x+cos(a)*(rad-2),y+sin(a)*(rad-2),2);}O(T,x,y,2,2);};
    const branch=(x,y,xx,yy,n)=>{L(D,x+1,y,xx+1,yy,n+1);L(M,x,y,xx,yy,n);};
    if(r===8){
      if(!small){ // Weathered coral gate with an open channel.
        P(D,-41,0,-36,-12,-35,-22,-28,-31,-18,-36,5,-38,20,-34,33,-24,37,-7,42,0,20,0,17,-17,11,-23,-2,-25,-12,-22,-18,-15,-19,-6,-23,0);
        P(M,-39,-1,-34,-19,-30,-27,-19,-34,4,-36,19,-32,29,-25,21,-25,16,-27,4,-29,-9,-28,-20,-24,-27,-13,-29,-3);
        for(let i=0;i<27;i++){const x=random()*39-19;B(i%3?H:A,x,-31-Math.cos(x/19)*3+random()*3,2,1);}
      }else{
        for(let i=0;i<5;i++){const dx=(i-2)*6,tip=-13-random()*16;branch(0,0,dx,-7,3);branch(dx,-7,dx+2,tip,3);L(A,dx+1,tip+7,dx+5,tip+4,2);B(H,dx+1,tip,2,2);}
      }
    }else if(r===9){
      if(!small){
        for(let i=0;i<3;i++){let sx=-19+i*17,ht=31+i%2*21;
          for(let j=0;j<4;j++)O(p.shade,sx+j*4,-ht-3-j*5,4+j*2,3+j);
          B(T,sx-3,-ht,8,ht);B(M,sx-2,-ht,2,ht);B(D,sx-4,-ht,10,3);
        }
        P(D,-32,0,-32,-22,-20,-28,-20,-23,-8,-30,-8,-23,5,-29,5,-23,29,-23,29,0);B(M,-27,-18,25,18);B(D,-21,-11,13,12);B(A,-19,-9,9,8);for(let j=0;j<3;j++)B(D,-19+j*3,-10,1,9);
        gear(16,-15,8);
      }else{gear(2,-11,10);B(T,-13,-2,28,3);}
    }else if(r===10){
      const n=small?12:28;
      P(T,-n*.25,0,-n*.08,-n*.8,-n*.21,-n*1.42,n*.17,-n*1.48,n*.2,-n*.8,n*.12,-n*.2,n*.4,0);
      P(M,-n*.12,0,0,-n*.76,-n*.12,-n*1.42,n*.08,-n*1.42,n*.1,-n*.8,n*.03,0);
      O(H,0,-n*1.33,n,n*.18);for(let j=0;j<7;j++)L(M,0,-n*1.46,(j-3)*n*.29,-n*1.33);
      P(M,-n,-n*1.42,-n*.79,-n*1.7,-n*.3,-n*2.01,0,-n*2.08,n*.3,-n*2,n*.73,-n*1.72,n,-n*1.4,n*.55,-n*1.46,0,-n*1.45);
      for(let j=0;j<7;j++){let xx=(j-3)*n*.21;O(j%2?A:H,xx,-n*(1.58+.2*(1-Math.abs(xx)/n)),small?1:2,1);}
      if(!small)for(let j=-2;j<3;j++){let xx=j*9,dy=5+random()*8;L(H,xx,-n*1.29,xx,-n*1.29+dy);O(A,xx,-n*1.29+dy,1,2);}
    }else if(r===11){
      if(!small){
        P(D,-37,0,-2,-49,33,0);P(H,-36,-1,-2,-49,2,-1);P(M,-2,-49,32,-1,2,-1);
        for(let j=1;j<6;j++){const yy=-49+j*8;L(p.haze,-2-j*5.6,yy,-2+j*.65,yy);L(D,-2+j*.65,yy,-2+j*5.6,yy);}
        L(F,-34,-2,-2,-48);L(A,-2,-48,2,-1);
      }else for(const [x,ht] of [[-6,15],[0,25],[6,10]]){P(D,x-4,0,x-4,-ht+5,x,-ht,x+3,-ht+3,x+3,0);P(A,x-3,-1,x-3,-ht+4,x,-ht+1,x,-1);B(H,x+1,-ht+5,1,ht-6);}
    }else if(r===12){
      if(!small){
        P(p.shade,-36,-2,-26,8,-18,9,-9,20,0,7,13,12,22,3,31,0);B(H,-34,-5,67,4);
        for(let level=0;level<3;level++){
          const yy=-5-level*15,w=25-level*5;B(T,-w*.6,yy-12,w*1.2,13);B(M,-w*.4,yy-10,w*.8,8);B(D,-2,yy-10,4,9);
          P(D,-w-3,yy-12,-w,yy-16,-w*.8,yy-15,-4,yy-21,4,yy-21,w*.8,yy-15,w,yy-16,w+3,yy-12);L(H,-w+1,yy-13,w-1,yy-13);
        }B(A,-1,-63,2,8);O(F,0,-63,2,2);
      }else{B(T,-10,-21,3,22);B(T,8,-21,3,22);B(H,-12,-23,25,3);L(T,0,-20,0,-15);P(M,-6,-6,-4,-8,-3,-15,2,-16,4,-13,4,-8,7,-6);B(A,-6,-6,13,2);B(D,-1,-4,2,2);}
    }else if(r===13){
      if(!small){ // Massive rib cage and vertebrae above the worn trail.
        for(let j=0;j<7;j++){let xx=-29+j*9,ht=13+11*sin((j+1)*.39);
          O(T,xx,-3,5,3);L(D,xx,-3,xx-4,-ht*.5,5);L(M,xx-4,-ht*.5,xx-3,-ht+3,4);L(H,xx-3,-ht+3,xx+2,-ht,3);L(A,xx+2,-ht,xx+5,-ht+1);}
        P(H,24,-5,25,-13,32,-14,38,-10,42,-8,41,-4,31,-3);B(D,30,-11,3,3);B(T,37,-7,4,1);
      }else{P(D,-9,0,-7,-7,-8,-23,-5,-29,3,-30,7,-27,6,-8,11,0);P(M,-7,0,-5,-12,-6,-25,0,-27,1,-3);O(H,-1,-28,6,3);O(M,-1,-28,4,2);B(A,-2,-29,3,1);L(T,5,-16,8,-20,3);}
    }else if(r===14){
      if(!small){
        const yy=-29;for(let j=0;j<7;j++){let xx=(j-3)*4;for(let k=0;k<4;k++)L(j%3?M:H,xx+sin(j+k)*2,yy+k*6,xx+sin(j+k+1)*2,yy+(k+1)*6,j%3===0?2:1);}
        O(D,0,yy,17,3);P(M,-18,yy,-16,yy-8,-10,yy-13,-4,yy-16,5,yy-16,13,yy-12,18,yy-5,18,yy);O(H,-3,yy-11,8,3);L(A,-16,yy-1,16,yy-1);for(let j=-12;j<15;j+=6)B(A,j,yy+1,3,2);
      }else for(let j=-2;j<=2;j++){let xx=j*4,ht=14+random()*22;for(let k=0;k<4;k++){const yy=-ht*k/4;L(D,xx+sin(k+j)*3,yy,xx+sin(k+j+1)*3,yy-ht/4,2);P(M,xx,yy-3,xx-4+(j%2)*2,yy-8,xx+1,yy-5);}O(H,xx+sin(4+j)*3,-ht,2,2);}
    }else if(r===15){
      if(!small){
        B(D,-29,-2,59,3);for(let i=-2;i<=2;i++){let xx=i*10;B(T,xx-1,-26,3,24);B(H,xx-2,-26,5,2);}
        P(D,-29,-26,-23,-37,-10,-44,0,-47,11,-43,22,-37,29,-26);
        for(let i=0;i<5;i++){let xx=-25+i*10;P(i%2?A:H,xx,-28,xx+5,-35-(2-Math.abs(i-2))*4,xx+9,-28);}
        B(M,-29,-26,59,3);B(F,-1,-50,2,4);for(let i=-2;i<=2;i++)P(H,i*10-3,-7,i*10,-11,i*10+3,-7,i*10,-3);
      }else{B(T,-4,-12,8,13);O(D,0,-16,12,12);O(p.grass,-2,-18,9,8);for(let j=0;j<8;j++){const xx=(random()-.5)*16,yy=-16+(random()-.5)*16;O(M,xx,yy,2,2);B(A,xx-1,yy-1,2,1);}}
    }else if(r===16){
      if(!small){
        P(D,-34,-13,-28,-8,-21,-6,18,-6,29,-11,34,-18,32,-9,24,-2,16,0,-16,0,-27,-4);L(H,-24,-5,23,-5);B(T,0,-46,2,39);L(M,0,-44,-24,-8);L(M,1,-43,22,-8);
        P(H,-14,-41,15,-41,19,-33,15,-18,-11,-18,-8,-31);for(let j=0;j<3;j++)P(M,-12+j*8,-40,-8+j*8,-40,-5+j*8,-31,-9+j*8,-19,-12+j*8,-19,-9+j*8,-31);
        for(let i=-3;i<4;i++)O(i%2?M:A,i*6,-6,2,2);
      }else{P(D,-12,0,-9,-6,-3,-8,3,-18,6,-17,9,-8,13,-3);P(H,-11,-1,-8,-5,-2,-7,3,-16,3,-8,-4,-3);L(A,4,-15,6,-8);}
    }else if(r===17){
      if(!small){
        B(T,-12,-49,25,49);B(M,-10,-47,10,46);P(D,-19,-48,-11,-55,-5,-67,4,-67,13,-54,20,-48);B(H,-18,-49,36,2);B(A,-1,-73,2,7);
        O(D,0,-38,9,9);O(H,0,-38,7,7);L(T,0,-38,-2,-43,2);L(T,0,-38,4,-39);B(D,-5,-24,11,21);L(H,0,-23,3,-10);O(A,3,-9,3,3);for(let i=0;i<3;i++)L(H,-18+i*7,-27,-13+i*7,-27,2);
      }else{B(T,-1,-26,3,27);B(M,-10,-27,22,3);O(D,-1,-26,10,3);for(let i=-3;i<=3;i++)L(H,i*2,-26,i*2+3,-29);gear(0,-12,5);}
    }else if(r===18){
      const stone=(x,y,n)=>{P(D,x-n,y-2,x-n*.72,y-n*.7,x,y-n,x+n*.8,y-n*.6,x+n,y+3,x+n*.25,y+n*.6,x-n*.65,y+n*.35);P(M,x-n*.86,y-2,x-n*.63,y-n*.62,x,y-n*.84,x+n*.64,y-n*.5,x+n*.1,y-1,x-n*.2,y+n*.34);L(A,x-n*.63,y-n*.57,x-n*.2,y-n*.23,2);L(H,x-n*.2,y-n*.23,x+n*.3,y-n*.39);};
      if(!small){
        B(T,-17,-40,4,40);B(T,-4,-40,4,40);for(let j=0;j<3;j++){L(M,-16,-j*12-1,-3,-j*12-11,2);L(M,-4,-j*12-1,-16,-j*12-11);}
        L(D,-23,-41,22,-50,5);L(H,-20,-43,21,-51);L(T,20,-49,20,-37);stone(20,-26,12);
      }else{O(D,0,-1,12,2);stone(0,-8,8);B(A,-1,-15,2,1);}
    }else{
      if(!small){
        for(let j=0;j<3;j++){let w=56-j*12;B(D,-w/2,-j*4-3,w,4);B(H,-w/2,-j*4-3,w,1);}
        P(T,-16,-9,-6,-16,-7,-35,-19,-47,-33,-49,-34,-53,-17,-51,-3,-42,0,-65,5,-69,4,-50,17,-58,29,-61,23,-55,11,-48,5,-37,6,-17,17,-10);
        L(A,-7,-11,1,-22);L(H,1,-22,1,-35,2);
        for(const [xx,yy,ww] of [[-28,-53,17],[-18,-61,18],[0,-68,21],[17,-65,16],[29,-60,14],[4,-76,14]]){O(D,xx,yy+2,ww,5);O(M,xx,yy,ww,6);O(H,xx-2,yy-3,ww*.78,3);B(A,xx-4,yy-5,5,1);}
      }else{B(T,-2,-20,4,21);for(let j=0;j<6;j++){let q=j*Math.PI/3;O(H,cos(q)*5,-22+sin(q)*5,3,3);}O(A,0,-22,3,3);}
    }
    g.restore();
  }
  function outpost(g,x,y,r,p,rnd){
    g.save();g.translate(R(x),R(y));
    const D=p.dark,M=p.leaf,H=p.leafTop,T=p.wood,A=p.accent;
    const B=(c,x,y,w,h)=>rect(g,c,x,y,w,h),P=(c,...a)=>poly(g,c,a),O=(c,x,y,w,h)=>oval(g,c,x,y,w,h),L=(c,x,y,xx,yy,s=1)=>line(g,c,x,y,xx,yy,s);
    if(r===8){
      P(T,-23,0,-29,-15,-16,-9,12,-10,27,-16,20,-5,12,0);L(H,-18,-7,18,-8);L(D,-2,-8,9,-42,2);L(T,7,-36,-18,-28,2);
      P(p.grass,-13,-28,6,-34,3,-20,-1,-23,-7,-18,-7,-22);L(D,9,-40,24,-14);
      for(let i=-2;i<3;i++)B(A,8*i,-3,3,2);
    }else if(r===9){
      O(T,-5,-23,14,5);B(T,-19,-23,29,19);O(M,-5,-25,14,5);B(M,-16,-22,5,17);B(H,-19,-14,29,2);
      L(D,7,-7,21,-7,4);L(D,21,-7,21,-30,4);L(H,21,-30,29,-30,4);B(A,26,-33,5,6);B(T,-16,-4,4,5);B(T,5,-4,4,5);
    }else if(r===10){
      for(let j=-2;j<3;j++){const xx=j*7,yy=-9-rnd()*18;L(T,xx,0,xx+3,yy,3);
        for(let i=0;i<3;i++){const h=yy*(.35+i*.27);O(i%2?M:D,xx+(i%2?4:0),h,5,2);B(A,xx+(i%2?2:-4),h-1,5,1);}}
    }else if(r===11){
      O(D,0,-1,28,4);O(p.shade,0,-2,25,3);O(A,-2,-2,19,1);
      for(let i=0;i<4;i++){let xx=-23+i*5,hh=10+i%3*7;P(M,xx-3,-2,xx-3,-hh+3,xx,-hh,xx+3,-hh+4,xx+3,-2);L(H,xx,-hh+2,xx,-3);}
    }else if(r===12){
      for(const dx of [-28,29]){P(D,dx-10,-10,dx+11,-10,dx+8,-3,dx+2,4,dx-2,1,dx-5,3);B(H,dx-9,-12,19,3);B(T,dx,-30,2,18);}
      for(let j=0;j<=12;j++){const xx=-28+j*4.75,rr=j/12,yy=sin(rr*Math.PI)*5;
        B(T,xx,-14+yy,4,3);L(M,xx,-24+yy,xx+4.75,-24+sin((j+1)/12*Math.PI)*5);if(j%2===0)L(D,xx,-23+yy,xx,-14+yy);}
    }else if(r===13){
      P(T,-29,0,-27,-8,19,-23,25,-20,28,-12,-18,1);P(M,-27,-6,19,-21,23,-19,-22,-3);O(D,-23,-3,6,6);O(H,-24,-4,5,4);O(M,-24,-4,3,3);B(A,-25,-5,2,2);
      L(H,-4,-14,-9,-23,3);L(T,-7,-20,-5,-28,2);L(D,11,-19,14,-29,3);
    }else if(r===14){
      for(let j=0;j<12;j++){const xx=(rnd()-.5)*44,yy=-9-rnd()*25;
        L(D,xx,0,xx+3,yy*.55,3);L(M,xx+3,yy*.55,xx,yy,3);O(j%2?H:A,xx,yy,3,2);B(D,xx-1,yy,3,1);}
    }else if(r===15){
      for(const xx of [-20,20]){B(D,xx-3,-29,7,30);B(H,xx-2,-28,2,27);O(M,xx,-32,5,5);B(A,xx-2,-34,3,2);}
      for(let i=-3;i<=3;i++){let xx=i*5,yy=-21-4*Math.abs(i)/3;B(D,xx,yy,2,-yy);P(H,xx-2,yy+1,xx+1,yy-4,xx+3,yy+1);}
      L(M,-17,-15,17,-15,2);L(M,-17,-7,17,-7);
    }else if(r===16){
      for(let j=0;j<5;j++){let xx=-18+j*9,yy=-8-rnd()*22,dx=j%2?8:-4;P(D,xx-9,0,xx+dx-4,yy+3,xx+dx,yy,xx+8,-3);P(H,xx-8,-1,xx+dx-3,yy+4,xx+dx,yy+2,xx+1,-1);}
      L(p.far,-24,0,24,0,2);
    }else if(r===17){
      B(T,-2,-43,5,43);L(M,-15,-4,1,-33,2);B(H,-15,-32,32,2);L(T,-17,-30,2,-17,2);
      for(const xx of [-11,10]){O(D,xx,-33,7,2);P(M,xx-5,-34,xx-2,-38,xx+1,-37,xx+3,-40,xx+5,-38,xx+2,-35);B(A,xx+3,-38,3,1);}
      L(D,0,-45,0,-54);P(H,-10,-49,-4,-53,-3,-50,9,-50,12,-54,12,-45,9,-48,-3,-48,-4,-45);
    }else if(r===18){
      for(let j=0;j<3;j++){const xx=-25+j*17;P(T,xx-3,-11,xx+12,-11,xx+10,-3,xx,-3);P(M,xx-3,-11,xx+1,-16,xx+5,-14,xx+8,-19,xx+13,-11);B(A,xx+6,-15,4,2);O(D,xx+1,-2,2,2);O(D,xx+9,-2,2,2);}B(H,-30,0,70,1);
    }else{
      for(let j=0;j<3;j++){let w=44-j*9;B(D,-w/2,-j*3-2,w,3);B(H,-w/2,-j*3-2,w,1);}
      B(T,-13,-31,4,23);B(T,10,-31,4,23);P(M,-20,-30,-14,-34,-8,-33,0,-38,8,-33,14,-34,20,-30);
      B(A,-17,-31,35,2);for(let i=-1;i<=1;i++)P(i?H:A,i*7-2,-29,i*7+2,-29,i*7+2,-19,i*7,-16,i*7-2,-19);
    }
    g.restore();
  }
  function outerScenery(mi,ne,h,p,region,horizon,ridgeAt,rnd){
    const coast=[8,14,16].includes(region),cloud=region===12;
    // Shifting landforms at two different scales give each major landmark space to breathe.
    const mist={...p,wood:p.shade,dark:mix(p.mid,p.shade,.63),leaf:mix(p.mid,p.shade,.2),leafTop:mix(p.far,p.mid,.46),light:p.far,accent:mix(p.far,p.mid,.3)};
    for(let j=0;j<9;j++){
      let x=23+j*99+rnd()*19,y=ridgeAt(x)+3;
      waymark(mi,x,y,.42+rnd()*.19,region,mist,j%3===0,531+j*13+region);
    }
    if(coast||cloud){
      rect(ne,cloud?p.haze:mix(p.mid,p.leaf,.46),0,horizon-11,LOOP,16);
      for(let i=0;i<125;i++){let x=rnd()*LOOP,y=horizon-10+rnd()*13;rect(ne,i%4?p.far:p.light,x,y,2+rnd()*10,1);}
    }
    for(let j=0;j<6;j++){
      const x=51+j*142+rnd()*16,base=horizon+4+(j%2)*2;
      if(j%2===0)waymark(ne,x,base,.91+rnd()*.21,region,p,false,731+j*23+region);
      else outpost(ne,x,base,region,p,rnd);
      waymark(ne,x+57,base+2,.65+rnd()*.24,region,p,true,331+j*7);
      if(j%2===0)waymark(ne,x-31,base+3,.5,region,p,true,j*3+109);
    }
  }
  function outerRoad(g,h,p,r,top,bottom,random){
    for(let j=0;j<22;j++){
      const x=j*41+random()*7,y=j%2?top+1:bottom-1,d=mix(p.road,p.earth,.22);
      if(r===8||r===14){oval(g,p.road,x,y,9,2);oval(g,p.mid,x,y,7,1);rect(g,p.light,x-4,y,4,1);}
      else if(r===9||r===17){rect(g,d,x-3,y-1,9,3);rect(g,p.roadHi,x-1,y,1,1);rect(g,p.wood,x+3,y,1,1);}
      else if(r===10||r===19){line(g,d,x-5,y,x,y-3);line(g,d,x,y-3,x+6,y+1);rect(g,p.accent,x-1,y-3,2,1);}
      else if(r===11||r===16){line(g,p.roadHi,x-6,y,x-3,y-3);line(g,d,x-3,y-3,x+1,y-1);line(g,p.roadHi,x+1,y-1,x+4,y-2);}
      else if(r===13){for(let i=0;i<5;i++)rect(g,i%2?p.roadHi:d,x+i*2,y-i%2,2,1);}
      else if(r===18){oval(g,d,x,y,7,2);rect(g,p.accent,x-3,y,3,1);}
      else{poly(g,d,[x-3,y,x,y-2,x+3,y,x,y+2]);rect(g,p.roadHi,x,y-1,1,1);}
    }
  }
  function layers(g,h,p,index){
    const W=g.canvas.width;
    for(let y=0;y<h;y+=3)rect(g,mix(p.sky,p.haze,Math.min(1,y/(h*.71))),0,y,W,3);
    if(index>=8){outerSky(g,h,p,index);return;}
    const sunY=R(h*(index===5||index===7?.21:.295)),sx=R(W*.689),rad=index===5?15:20;
    oval(g,mix(p.light,p.haze,.47),sx,sunY,rad+4,rad+4);
    oval(g,p.light,sx,sunY,rad,rad);
    if(index===5)oval(g,mix(p.sky,p.haze,.19),sx+6,sunY-4,rad-2,rad-2);
    const rnd=rng(1222+index);
    if(index===5||index===7){
      for(let i=0;i<72;i++){let x=rnd()*W,y=rnd()*h*.5;rect(g,p.light,x,y,1,rnd()>.88?3:1);}
      if(index===7){for(const [x,y] of [[66,26],[74,16],[83,22],[93,12]])oval(g,p.light,x,y,1,1);line(g,p.far,67,25,74,17);line(g,p.far,75,17,82,21);line(g,p.far,84,21,92,13);}
    }
    // Narrow, stepped ribbons of cloud.
    for(let i=0;i<16;i++){
      const x=rnd()*W,y=12+rnd()*h*.42,len=12+rnd()*56,c=mix(p.haze,p.light,.2+rnd()*.25);
      rect(g,c,x,y,len,1);rect(g,c,x+len*.20,y-2,len*.61,2);rect(g,c,x+len*.37,y-4,len*.27,2);
    }
  }
  function ridge(g,p,h,rnd,far,type){
    const base=h*(far?.58:.68),amp=h*(far?.19:.21),step=far?11:8,pts=[],heights=[];
    let noise=0,phase=rnd()*3.14;
    for(let x=0;x<=LOOP+step;x+=step){
      let t=Math.min(1,x/LOOP)*Math.PI*2;
      let lift=.66+.33*sin(t*3+phase)+.23*sin(t*5+1)+.11*sin(t*12);
      noise+=((rnd()-.5)*5-noise)*.52;
      let y=R(base-lift*amp+noise);if(type===0||type===4)y=R(y/3)*3;
      heights.push(y);pts.push(x,y);
    }
    heights[heights.length-1]=heights[0];pts[pts.length-1]=pts[1];
    const c=far?p.far:p.mid;
    poly(g,c,[0,h,...pts,LOOP+step,h]);
    // Facets follow local ridgelines and fracture the silhouette into rock spurs.
    const shaded=far?mix(p.far,p.mid,.33):p.shade;
    for(let i=2;i<heights.length-3;i++){
      if(heights[i]<=heights[i-1]&&heights[i]<heights[i+1]){
        let x=i*step,y=heights[i],depth=11+rnd()*30;
        poly(g,shaded,[x,y,x+step,heights[i+1],x+step*2,heights[i+2],x+step+3,y+depth*.64,x-9,y+depth+7,x-2,y+9]);
        if(type===2){poly(g,far?mix(p.far,p.light,.47):p.leafTop,[x,y,x+step,heights[i+1],x+step+5,heights[i+1]+4,x+3,y+3,x-3,y+7,x-2,y+3,x-9,heights[i-1]+3]);}
      }
    }
    return x => heights[Math.min(heights.length-1,Math.max(0,R(x/step)))];
  }
  function monument(g,x,y,s,kind,a,b,hi){
    g.save();g.translate(R(x),R(y));g.scale(s,s);
    const P=(c,pts)=>poly(g,c,pts),B=(c,x,y,w,h)=>rect(g,c,x,y,w,h);
    if(kind===0||kind===5){ // Abandoned stepped shrines.
      for(let i=0;i<4;i++){let w=46-i*9;B(a,-w/2,-i*5-5,w,7);B(hi,-w/2,-i*5-5,w,1);B(b,w/2-7,-i*5-4,7,5);}
      B(a,-7,-30,14,11);B(b,-2,-27,5,7);B(a,-10,-31,20,3);B(hi,-10,-32,20,1);P(a,[-8,-32,0,-38,8,-32]);
    }else if(kind===2||kind===7){ // Crystals.
      [[-13,16,6],[0,34,9],[11,24,7]].forEach(([xx,ht,w])=>{
        P(a,[xx-w,0,xx-w,-ht+6,xx,-ht,xx+w,-ht+7,xx+w,0]);P(hi,[xx-w,-ht+6,xx,-ht,xx-1,-2,xx-w,0]);P(b,[xx,-ht,xx+w,-ht+7,xx+w,0,xx+3,-2]);
      });
    }else if(kind===4){
      P(a,[-28,0,-18,-20,-12,-20,-8,-36,3,-38,10,-21,14,-21,25,0]);P(b,[0,-37,6,-31,10,-21,14,-21,25,0,8,0]);B(hi,-8,-36,11,2);
    }else{ // Pillared ruins and collapsed aqueducts.
      for(let i=0;i<3;i++){const xx=i*12-17;B(a,xx,-22,6,22);B(b,xx+4,-19,2,19);B(hi,xx-1,-22,8,2);}
      B(a,-19,-27,33,5);B(hi,-19,-28,27,1);B(b,6,-27,9,3);P(a,[-20,-28,-16,-31,-6,-31,3,-28]);
    }
    g.restore();
  }
  function boulder(g,x,y,k,p){
    let w=12*k,h=8*k;
    poly(g,p.wood,[x-w,y,x-w+2,y-h*.65,x-w*.38,y-h,x+w*.4,y-h+1,x+w,y-3,x+w,y]);
    poly(g,p.shade,[x-w+2,y-h*.65,x-w*.38,y-h,x+w*.4,y-h+1,x+2,y-h*.52,x-w*.3,y-h*.45,x-w*.7,y-2]);
    rect(g,p.leafTop,x-w*.38,y-h+1,w*.52,1);
  }
  function frond(g,x,y,s,dir,tone,glint){
    const a=Math.abs(s),sign=dir<0?-1:1;
    let prev=[x,y];
    for(let i=1;i<=9;i++){
      let t=i/9,xx=x+sign*a*t,yy=y-a*.31*Math.sin(Math.PI*t)+a*.14*t;
      line(g,tone,...prev,xx,yy,i<5?2:1);prev=[xx,yy];
      if(i>=2&&i<9){
        let l=(1-t*.66)*a*.35;
        poly(g,tone,[xx-sign*2,yy,xx+sign*5,yy-1,xx+sign*(l*.66),yy+l,xx+sign*(l*.27),yy+l*.63]);
        if(i<6)line(g,glint,xx+sign*1,yy,xx+sign*(l*.36),yy+l*.48);
      }
    }
  }
  function palm(g,x,y,size,p,rnd){
    let hh=27*size,w=5*size,bend=(rnd()-.5)*15*size,tx=x+bend,ty=y-hh;
    poly(g,p.wood,[x-w,y,x-w/2+bend*.3,y-hh*.48,tx-2,ty,tx+3,ty,x+w/2+bend*.3,y-hh*.48,x+w,y]);
    for(let i=0;i<4;i++)line(g,p.leaf,x+bend*i/5-1,y-hh*i/5,x+bend*i/5+2,y-hh*i/5-1);
    for(const d of [-1,1]){frond(g,tx,ty,23*size,d,p.dark,p.leaf);frond(g,tx,ty-2,15*size,d,p.leaf,p.leafTop);}
    poly(g,p.leafTop,[tx-1,ty+1,tx-4,ty-9*size,tx,ty-6*size,tx+2,ty,tx+7*size,ty-10*size,tx+5,ty]);
  }
  function pine(g,x,y,k,p){
    rect(g,p.wood,x-2*k,y-25*k,4*k,25*k);
    for(let i=0;i<3;i++){
      let w=(13-i*3)*k,base=y-(4+i*7)*k,top=base-(15-i*2)*k;
      poly(g,p.dark,[x-w,base,x-2,top,x+1,top-2,x+w,base-1,x+4,base-2,x+2,base,x-5,base-1]);
      poly(g,p.leaf,[x-w,base,x-2,top,x+1,top-2,x+2,top+3,x-1,top+4,x-3,top+7,x-4,top+7,x-7,base-3]);
      poly(g,p.leafTop,[x-6*k,top+7*k,x+1,top-2,x+6*k,top+7*k,x+2,top+4,x,top+6,x-2,top+5]);
    }
  }
  function crown(g,x,y,k,p,rnd,willow){
    const hi=p.leafTop,mid=p.leaf,sh=p.dark;
    poly(g,p.wood,[x-5*k,y,x-3*k,y-19*k,x-13*k,y-29*k,x-10*k,y-30*k,x-1*k,y-23*k,x+2*k,y-35*k,x+5*k,y-34*k,x+4*k,y-24*k,x+13*k,y-31*k,x+15*k,y-30*k,x+4*k,y-17*k,x+5*k,y]);
    [[-12,-28,11,6],[13,-31,12,6],[-2,-37,14,8],[-14,-34,8,5],[6,-37,10,7]].forEach(([a,b,w,h])=>{
      let xx=x+a*k,yy=y+b*k;oval(g,sh,xx,yy+2*k,w*k,h*k);oval(g,mid,xx,yy,w*k,h*k);
      oval(g,hi,xx-3*k,yy-3*k,w*k*.64,h*k*.47);
      for(let i=0;i<7;i++)rect(g,rnd()<.65?mid:hi,xx+(rnd()-.5)*w*k*1.4,yy+(rnd()-.5)*h*k*1.3,1+rnd()*3*k,k);
      if(willow)for(let i=0;i<7;i++){let dx=(rnd()-.5)*w*k*1.6;rect(g,rnd()>.55?mid:sh,xx+dx,yy+2*k,1+rnd()*2*k,5+rnd()*13*k);}
    });
  }
  function grass(g,x,y,s,a,b){
    poly(g,a,[x-8*s,y,x-12*s,y-9*s,x-5*s,y-5*s,x-6*s,y-12*s,x-1*s,y-5*s,x+2*s,y-15*s,x+3*s,y-5*s,x+9*s,y-11*s,x+6*s,y-2*s,x+13*s,y-5*s,x+10*s,y]);
    line(g,b,x-2*s,y-1,x-5*s,y-8*s);line(g,b,x+3*s,y-2*s,x+7*s,y-8*s);
  }
  function ancientTree(g,x,base,p,willow,rnd){
    // A forked trunk, broken bark, and deep leaf layers frame the near forest.
    const lean=x%2===0?1:-1, dark=p.wood, back=mix(p.dark,p.wood,.25), bark=mix(p.wood,p.leafTop,.21);
    const P=(c,pts)=>poly(g,c,pts.flatMap(([xx,yy])=>[x+xx*lean,base+yy]));
    P(dark,[[-13,0],[-6,-4],[-4,-22],[-6,-39],[-6,-60],[-2,-79],[-10,-91],[-19,-102],[-27,-105],[-30,-110],[-18,-107],[-6,-98],[0,-91],[4,-97],[2,-111],[4,-125],[10,-139],[6,-115],[10,-97],[18,-109],[28,-113],[35,-127],[34,-115],[29,-107],[19,-101],[10,-88],[6,-72],[4,-52],[6,-28],[9,-10],[17,-1],[10,-2],[5,-4],[3,-1]]);
    P(bark,[[-9,-1],[-5,-8],[-2,-24],[-4,-49],[-3,-63],[0,-76],[1,-73],[-1,-56],[0,-34],[2,-20],[0,-9]]);
    for(let j=0;j<6;j++){let yy=base-11-j*8;line(g,p.dark,x-3*lean,yy,x+2*lean,yy-3,1);}
    for(let i=0;i<40;i++){
      let off=(rnd()-.5)*102, yy=-108-(1-Math.abs(off)/60)*20-rnd()*13,w=7+rnd()*14,h=3+rnd()*7;
      let xx=x+off*lean,cy=base+yy;
      oval(g,back,xx,cy+3,w,h);oval(g,p.dark,xx,cy,w,h);
      if(rnd()>.22)oval(g,p.leaf,xx-2,cy-2,w*.69,h*.5);
      if(rnd()>.50){rect(g,p.leafTop,xx-3,cy-h*.64,3+rnd()*4,1);rect(g,p.leafTop,xx+2,cy-h*.53,2,1);}
      if((i%6===0||willow)&&rnd()>.34){
        const len=5+rnd()*(willow?33:17);
        line(g,p.dark,xx,cy+2,xx-2,cy+len,2);rect(g,p.leaf,xx-3,cy+len*.71,3,2);
      }
    }
    if(!willow){frond(g,x,base-5,19,-lean,p.dark,p.leaf);frond(g,x+4,base-3,15,lean,p.leaf,p.leafTop);}
  }
  function groundPlants(g,x,y,s,p,region,rnd){
    if(region===2||region===7){monument(g,x,y,s*.5,2,p.leaf,p.dark,p.leafTop);return;}
    if(region===4){rect(g,p.dark,x-s*2,y-s*16,4*s,16*s);rect(g,p.leaf,x-2*s,y-17*s,2*s,15*s);line(g,p.dark,x-2*s,y-8*s,x-7*s,y-10*s,2);rect(g,p.dark,x-8*s,y-13*s,2*s,4*s);return;}
    grass(g,x,y,s,p.dark,p.leaf);if(rnd()>.57){for(let i=0;i<3;i++){let xx=x+(rnd()-.5)*13*s,yy=y-(3+rnd()*7)*s;rect(g,p.accent,xx,yy,2*s,s);}}
  }
  let cache=null,previousScene=null;
  function buildScene(W,h,region){
    const p=palettes[region], horizon=R(h*.657),roadY=R(h*.704),bottom=R(h*.926);
    const make=width=>{let c=surface(width,h);return [c,c.getContext('2d')];};
    const [sky,sk]=make(W),[rays,ra]=make(W),[far,fa]=make(LOOP),[mid,mi]=make(LOOP),[near,ne]=make(LOOP),[road,ro]=make(LOOP),[front,fr]=make(LOOP);
    layers(sk,h,p,region);ridge(fa,p,h,rng(453+region),true,region);const ridgeAt=ridge(mi,p,h,rng(231+region),false,region);
    let rnd=rng(region+170);
    if(region>=8)outerScenery(mi,ne,h,p,region,horizon,ridgeAt,rnd);
    else{
    for(let j=0;j<13;j++){
      const x=j*69+17+rnd()*20,y=ridgeAt(x)+2,s=.45+rnd()*.43;
      if(j%3===1 || region===6 || region===4)monument(mi,x,y,s,region,p.shade,mix(p.shade,p.mid,.3),mix(p.haze,p.mid,.78));
      else if(region===2)pine(mi,x,y,s,{...p,leafTop:p.far,leaf:p.mid,dark:p.shade,wood:p.shade});
      else if(region===0||region===5||region===1){
        let muted={...p,leafTop:mix(p.mid,p.haze,.15),leaf:p.mid,dark:p.shade,wood:p.shade};
        if(region===0)palm(mi,x,y,s*.8,muted,rnd); else crown(mi,x,y,s*.7,muted,rnd,region===5);
      }
    }
    // Descending water from the far escarpment, then horizontal bands of reflected light.
    if([0,2,5,6].includes(region)){
      for(let x of [98,314,602,768]){
        let y=ridgeAt(x)+9,hh=Math.max(2,horizon-y),ww=region===5?3:5;
        rect(mi,mix(p.haze,p.mid,.53),x,y,ww,hh);
        rect(mi,mix(p.light,p.mid,.48),x+1,y+2,1,hh*.7);
        rect(mi,mix(p.light,p.mid,.48),x-2,y+hh-2,ww+5,2);
      }
      rect(ne,mix(p.mid,p.haze,.47),0,horizon-8,LOOP,11);
      for(let i=0;i<160;i++)rect(ne,rnd()>.4?p.mid:mix(p.light,p.haze,.6),rnd()*LOOP,horizon-7+rnd()*10,2+rnd()*12,1);
    }
    // Individual tree groups leave open views to the distant ruins.
    for(let j=0;j<16;j++){
      let x=j*55+3+rnd()*15, y=horizon+3+rnd()*6;
      let size=j%5===0?1.44:j%5===1?.86:.36+rnd()*.47;
      if(region===0){palm(ne,x,y,size*1.18,p,rnd);if(j%5===0)palm(ne,x+16,y+2,size*.83,p,rnd);}
      else if(region===3){pine(ne,x,y,size*.92,p);if(j%4===1){line(ne,p.wood,x+10,y,x+10,y-19,2);poly(ne,p.accent,[x+10,y-19,x+21,y-17,x+15,y-14,x+10,y-14]);}}
      else if(region===2)pine(ne,x,y,size*1.18,p);
      else if(region===1||region===5)crown(ne,x,y,size,p,rnd,region===5);
      else if(region===6){if(j%3===0)monument(ne,x,y,size,6,p.leaf,p.dark,p.leafTop);else crown(ne,x,y,size*.8,p,rnd,true);}
      else if(region===7){monument(ne,x,y,size,7,p.leaf,p.dark,p.leafTop);}
      else {if(j%5===0)monument(ne,x,y,size,4,p.leaf,p.dark,p.grass);else boulder(ne,x,y,size,p);}

    }
    if(region===0||region===5){
      for(const x of [8,503,820])ancientTree(ne,x,horizon+4,p,region===5,rnd);
    }
    }
    // Repeating ground sentinels on the horizon.
    for(let x=0;x<LOOP;x+=3+rnd()*5){
      let y=horizon+rnd()*4;
      oval(ne,rnd()<.5?p.leaf:p.dark,x,y,4+rnd()*7,2+rnd()*5);
    }
    rect(ro,p.earth,0,roadY-5,LOOP,h-roadY+5);
    rect(ro,p.grass,0,roadY-6,LOOP,5);rect(ro,p.roadHi,0,roadY-1,LOOP,bottom-roadY+1);
    rect(ro,mix(p.road,p.roadHi,.38),0,roadY+2,LOOP,bottom-roadY-3);
    rect(ro,p.road,0,bottom-3,LOOP,6);rect(ro,p.wood,0,bottom+3,LOOP,2);
    rect(ro,mix(p.earth,p.leaf,.22),0,bottom+6,LOOP,h-bottom);
    for(let x=0;x<LOOP;x+=2+rnd()*9){
      rect(ro,p.leafTop,x,roadY-6-rnd()*2,3+rnd()*7,2);
      if(rnd()>.45)grass(ro,x,roadY-4,.22+rnd()*.19,p.leaf,p.leafTop);
      rect(ro,mix(p.earth,p.grass,.75),x,bottom+1,2+rnd()*5,2);
    }
    for(let i=0;i<180;i++){
      let x=rnd()*LOOP,y=roadY+2+rnd()*(bottom-roadY-5),w=2+rnd()*9;
      rect(ro,rnd()>.48?p.roadHi:p.road,x,y,w,1);
      if(rnd()>.70){rect(ro,p.road,x+2,y+1,w*.65,1);}
    }
    // Inlaid courses of broken flagstones define a continuous horizontal road.
    for(let row=0;row<3;row++){
      let y=roadY+row*(bottom-roadY)/3+1;
      for(let x=-40+row*24;x<LOOP;x+=23+floor(rnd()*27)){
        let w=12+floor(rnd()*20);
        if(rnd()>.33){rect(ro,mix(p.road,p.earth,.17),x,y,w,1);rect(ro,p.roadHi,x+2,y+1,w*.76,1);if(rnd()>.74)line(ro,p.road,x+w,y,x+w+3,y-4);}
      }
    }
    if(region>=8)outerRoad(ro,h,p,region,roadY,bottom,rnd);
    for(let j=0;j<65;j++){
      let x=rnd()*LOOP, y=bottom+7+rnd()*Math.max(3,h-bottom-8);
      if(region>=8&&j%5===0)waymark(fr,x,y,.22+rnd()*.18,region,p,true,j*31);
      else if(j%7===0)boulder(fr,x,y,.4+rnd()*.55,p);
      else groundPlants(fr,x,y,.33+rnd()*.72,p,region,rnd);
    }
    if(region===0||region===5){
      for(let j=0;j<20;j++){
        let x=j*45+rnd()*16,y=h+2+rnd()*5,size=.6+rnd()*.65;
        // Close broad-leaved understory along the path.
        for(let i=0;i<5;i++){
          let dx=(i-2)*(4+rnd()*2)*size,dy=(7+rnd()*10)*size,xx=x+dx,yy=y-dy;
          poly(fr,i%2===0?p.dark:p.leaf,[x,y,xx-4,yy+3,xx,yy,xx+3,yy+3,xx+4,yy+8]);
          line(fr,i%2===0?p.leaf:p.leafTop,x,y-1,xx,yy+3);
        }
        if(j%4===1){for(let i=0;i<3;i++){oval(fr,p.accent,x+i*3,y-4-i%2*3,2,2);rect(fr,p.light,x+i*3-1,y-5-i%2*3,1,1);}}
      }
      // Ferns creeping into the top edge frame the sky.
      [[4,2],[31,-3],[487,-4],[782,1]].forEach(([x,y])=>{
        line(fr,p.dark,x-8,y-3,x+12,y+2,3);
        frond(fr,x,y+2,24,1,p.dark,p.leaf);frond(fr,x+9,y+1,18,-1,p.dark,p.leaf);
        let xx=x+4;for(let i=0;i<14;i++){line(fr,p.dark,xx,y+i,xx+(i%5===0?1:0),y+i+1);if(i%5===0)rect(fr,p.leaf,xx+1,y+i,2,2);}
      });
    }
    if([0,1,6].includes(region)){
      ra.globalAlpha=.065;
      for(let j=0;j<3;j++){let xx=R(W*.56)+j*30;poly(ra,p.light,[xx,1,xx+11+j,1,xx-48+j,roadY+4,xx-64+j,roadY+4]);}
    }
    return {w:W,h,region,sky,far,mid,near,road,front,rays,roadY,bottom};
  }
  function scroll(g,layer,d){
    const x=((floor(d)%LOOP)+LOOP)%LOOP;
    g.drawImage(layer,-x,0);if(x+g.canvas.width>LOOP)g.drawImage(layer,LOOP-x,0);
  }
  function landscape(g,scene,d){
    g.drawImage(scene.sky,0,0);scroll(g,scene.far,d*.09);scroll(g,scene.mid,d*.22);
    if(scene.region===0||scene.region===1||scene.region===6)g.drawImage(scene.rays,0,0);
    scroll(g,scene.near,d*.58);scroll(g,scene.road,d);
  }
  const actorCache=new Map();
  function monkey(g,t,stance){
    const dark='#312e33',shadow='#533b36',fur='#a25d37',light='#cf8650',face='#f4ba7b',cream='#ffe0a4',teal='#287a77',mint='#73d3af';
    const P=(c,...a)=>poly(g,c,a), O=(c,x,y,a,b)=>oval(g,c,x,y,a,b),B=(c,x,y,w,h)=>rect(g,c,x,y,w,h);
    const attack=stance==='cast',walk=stance==='walk'||attack,stride=walk?R(sin(t)*4):2,bob=walk?Math.abs(R(sin(t)*1.65)):0;
    // Curl is drawn behind the pack and remains visible while running.
    const sway=R(sin(t)*(walk?2:1));
    let tail=[[-6,-11],[-12,-9],[-16,-9],[-20,-12],[-22,-16+sway],[-22,-20+sway],[-19,-23+sway],[-16,-23+sway],[-14,-21+sway],[-14,-18+sway],[-16,-17+sway]];
    for(let pass=0;pass<2;pass++)for(let i=1;i<tail.length;i++)line(g,pass?light:dark,...tail[i-1],...tail[i],pass?2:4);
    // Rear foot, forward foot, boots.
    P(dark,-3,-14,4,-11,2+stride,-6,5+stride,-3,5+stride,-1,-2+stride,-1,-5,-6);
    B(shadow,-1+stride,-5,4,3);
    P(dark,-5,-12,1,-10,1-stride,-6,-2-stride,-4,2-stride,-3,3-stride,-1,1-stride,0,-7-stride,0,-7-stride,-3,-5,-7);
    B(fur,-6-stride,-4,4,3);B(face,-4-stride,-2,5,1);
    g.save();g.translate(0,-bob);
    // Torso, canvas satchel, bronze buckle, and scarf tails.
    O(dark,-1,-15,7,9);O(fur,0,-15,5,7);O(light,2,-17,3,5);
    P(dark,-6,-24,-10,-23,-12,-17,-12,-12,-8,-9,-4,-10,-3,-21);
    P('#a76e37',-8,-22,-10,-18,-10,-13,-7,-11,-4,-12,-5,-21);
    B('#ffce76',-10,-17,5,2);B(shadow,-8,-16,2,4);B(cream,-8,-16,2,2);
    let swing=attack?4:walk?stride:0;
    line(g,dark,3,-20,7+swing,-15,5);line(g,fur,3,-20,7+swing,-15,3);
    O(dark,8+swing,-15,3,3);O(face,8+swing,-15,2,2);
    P(teal,-4,-23,-9,-21,-16,-21+sway,-13,-24+sway,-17,-25+sway,-10,-25,-5,-26);
    B(mint,-14,-24+sway,5,1);
    // Large warm ears, cream mask and expressive eye.
    O(dark,1,-30,10,9);O(fur,1,-30,9,8);
    P(dark,-6,-35,-4,-39,-1,-38,2,-41,5,-37,8,-37,9,-33);
    P(light,-5,-35,-3,-38,0,-37,2,-39,5,-35,8,-34);
    O(dark,-7,-29,5,5);O(light,-7,-29,4,4);O('#b4684f',-7,-29,2,2);B(face,-9,-32,3,1);
    O(light,3,-30,7,7);O(face,5,-30,6,6);O(cream,6,-32,4,3);
    O(dark,9,-27,5,4);O(face,9,-27,4,3);B(cream,7,-29,5,1);
    B(cream,4,-34,5,4);B(dark,5,-34,3,4);B('#fff8d6',6,-34,2,1);B(light,4,-35,4,1);B(dark,11,-29,2,1);B(shadow,8,-25,3,1);
    // Headband and scarf separate silhouette from jungle greens.
    P(teal,-8,-35,-4,-37,4,-37,8,-36,9,-34,3,-35,-4,-35,-7,-33);
    B(mint,-5,-36,9,1);B('#fff8d6',6,-33,1,1);
    P(dark,-5,-24,0,-23,6,-24,5,-20,2,-17,-1,-20,-6,-21);
    P(teal,-5,-24,0,-23,6,-24,4,-21,2,-18,-1,-21,-5,-22);
    B(mint,-4,-23,6,1);g.restore();
  }
  function enemySprite(g,kind,base,frame,boss){
    const d='#322f3b',sh=mix(base,'#29353e',.57),mid=mix(base,'#46384a',.25),hi=mix(base,'#fff0bc',.46),pale='#ffdf9f',eye='#fff6c6';
    const P=(c,...a)=>poly(g,c,a),O=(c,x,y,a,b)=>oval(g,c,x,y,a,b),B=(c,x,y,w,h)=>rect(g,c,x,y,w,h);
    const L=(c,x,y,xx,yy,w=1)=>line(g,c,x,y,xx,yy,w);
    let n=R(sin(frame)*1.6), f=R(cos(frame)*2);
    const joint=(a,b,c,w,color=mid)=>{L(d,...a,...b,w+2);L(d,...b,...c,w+2);L(color,...a,...b,w);L(color,...b,...c,w);};
    const glint=(x,y)=>{B(d,x-1,y-1,4,4);B(pale,x,y,2,2);B(eye,x,y,1,1);};
    if(kind==='beetle'){
      for(let i=0;i<3;i++)joint([-5+i*8,-11],[-8+i*8+f,-5],[-10+i*8+f,-1],2,sh);
      O(d,3,-15,14,11);O(mid,3,-16,13,9);
      P(base,-9,-19,-4,-25,3,-27,12,-24,16,-19,14,-14,4,-12,-6,-14);
      P(hi,-7,-20,-2,-24,4,-25,8,-24,2,-22,0,-19);
      L(sh,-2,-21,9,-14);L(mid,-1,-23,11,-17);L(hi,-1,-24,10,-19);
      L(mid,3,-23,7,-24);L(sh,4,-19,3,-16);B(hi,9,-22,2,1);P(d,-18,-12,-24,-17,-25,-21,-21,-18,-16,-18);
      O(d,-11,-11,8,6);O(sh,-12,-12,6,4);
      P(base,-15,-15,-17,-22,-14,-26,-12,-24,-13,-20,-9,-17);glint(-16,-13);
      [[3,-25],[5,-21],[13,-18]].forEach(([x,y])=>B(hi,x,y,2,1));
    }else if(kind==='boar'||kind==='wolf'){
      const wolf=kind==='wolf';
      joint([8,-14],[12,-8],[11+f,-2],3,sh);joint([-8,-14],[-6,-7],[-7-f,-2],3,sh);
      if(wolf)P(d,10,-20,16,-19,23,-26,23,-21,18,-14,12,-13);
      else {L(d,12,-19,17,-22);L(d,17,-22,17,-25);B(hi,16,-25,3,1);}
      O(d,1,-17,wolf?14:16,wolf?8:11);O(mid,2,-17,wolf?12:14,wolf?7:9);
      P(base,-13,-20,-11,-25,-7,-24,-5,-29,-1,-26,3,-29,6,-25,11,-23,14,-18,6,-18,2,-16,-3,-18,-8,-16);
      P(d,-10,-24,-12,-31,-16,-29,-17,-24,-22,-20,-24,-13,-20,-10,-13,-10,-8,-15);
      P(base,-12,-23,-13,-28,-15,-26,-15,-23,-19,-20,-22,-16,-19,-13,-13,-13,-9,-18);
      P(hi,-17,-19,-22,-15,-20,-12,-14,-12,-12,-15);B(d,-22,-15,3,2);glint(-18,-21);
      if(!wolf)P(pale,-19,-12,-23,-12,-24,-18,-21,-16);
      else P(pale,-19,-12,-17,-12,-18,-9);
      for(let i=0;i<3;i++)L(hi,-6+i*5,-23,-3+i*5,-21);
      joint([6,-12],[5,-7],[6-f,-2],3,base);joint([-6,-14],[-10,-8],[-10+f,-2],3,base);
      B(d,4-f,-2,5,2);B(d,-12+f,-2,5,2);
    }else if(kind==='frog'){
      O(d,3,-9,13,8);O(base,3,-10,11,7);O(sh,7,-6,8,5);
      O(d,-7,-14,11,7);O(base,-7,-15,10,5);O(d,-11,-19,5,5);O(hi,-11,-19,4,4);
      glint(-13,-21);P(hi,-18,-15,-11,-14,-5,-14,-3,-12,-8,-11,-16,-12);
      L(d,-17,-13,-7,-12);B(mid,1,-14,2,2);B(hi,4,-11,3,2);B(hi,8,-10,2,1);
      joint([-6,-10],[-9,-4],[-14,-2+n],2,base);joint([11,-7],[13,-4],[7,-2],2,base);
      B(hi,-15,-2+n,5,1);B(hi,5,-2,4,1);
    }else if(kind==='moth'||kind==='bat'||kind==='bird'){
      const y=-21+n*2, flap=R(cos(frame)*6);
      g.save();g.translate(0,y);
      if(kind==='moth'){
        P(d,0,0,-9,-18-flap,-20,-22-flap,-22,-17-flap,-19,-7,-10,1,-18,3,-17,9,-12,12,-6,9,0,4);
        P(base,-2,-2,-10,-16-flap,-19,-19-flap,-19,-14-flap,-17,-7,-9,-1);
        P(mid,-2,2,-15,3,-15,8,-11,10,-5,7);
        P(d,1,1,5,-17-flap,16,-24-flap,20,-24-flap,21,-16-flap,17,-6,10,0,15,0,18,4,13,10,8,11,2,6);
        P(base,2,-1,7,-16-flap,17,-22-flap,19,-21-flap,18,-14-flap,14,-6,8,-1);
        P(hi,4,2,13,1,16,4,12,8,8,9);
        O(sh,-14,-10-flap*.6,3,4);O(hi,-14,-10-flap*.6,1,2);O(sh,13,-12-flap*.6,3,4);O(hi,13,-12-flap*.6,1,2);
        O(d,0,1,3,8);O(mid,0,2,2,6);O(d,-1,-6,4,4);glint(-3,-7);
        L(d,-3,-8,-6,-13);L(d,0,-8,2,-14);
      }else if(kind==='bat'){
        P(d,-1,-3,-9,-13-flap,-20,-18-flap,-26,-10,-22,-12,-18,-6,-16,-7,-10,-2,-7,3,0,5);
        P(base,-3,-3,-11,-11-flap,-20,-15-flap,-24,-11,-20,-11,-16,-7,-13,-6,-9,-3);
        P(d,2,-3,9,-13-flap,20,-18-flap,26,-10,22,-12,18,-6,16,-7,10,-2,7,3,0,5);
        P(mid,4,-3,11,-11-flap,20,-15-flap,24,-11,20,-11,16,-7,13,-6,9,-3);
        L(hi,-8,-6,-19,-14-flap);L(hi,8,-6,19,-14-flap);
        O(d,0,2,6,6);O(base,0,2,4,5);P(d,-5,0,-7,-11,-2,-7,2,-7,6,-11,5,1);
        O(base,-1,-3,5,4);B(pale,-5,-4,2,1);B(pale,-1,-4,2,1);B(d,-4,-1,3,1);B(eye,-4,0,1,2);
      }else{
        P(d,2,1,13,-3,22,-7,17,0,21,0,11,5,6,7,-1,7);
        P(mid,1,3,15,-1,11,3,6,5);
        O(d,-3,0,11,7);O(base,-3,-1,9,5);
        P(d,-4,-2,0,-10,7,-19-flap,17,-24-flap,15,-18-flap,19,-20-flap,14,-12,6,-5,2,1);
        P(base,-2,-2,3,-11,9,-18-flap,15,-21-flap,12,-15,7,-9);
        L(hi,4,-8,12,-16-flap);L(sh,2,-5,9,-12-flap);
        O(d,-10,-4,6,6);O(hi,-11,-5,4,4);P(pale,-15,-5,-21,-4,-17,-1,-14,-2);glint(-13,-6);
        L(sh,-5,5,-6,8);L(pale,-6,8,-9,8);
      }g.restore();
    }else if(kind==='serpent'){
      O(d,4,-5,14,5);O(base,3,-6,12,3);L(hi,0,-7,8,-7);
      let body=[[13,-5],[18,-7],[19,-11],[17,-13]];
      for(let i=1;i<body.length;i++)L(d,...body[i-1],...body[i],3);
      body=[[5,-5],[1,-9],[2,-13+n],[-1,-18+n],[-7,-20+n]];
      for(let k=0;k<2;k++)for(let i=1;i<body.length;i++)L(k?base:d,...body[i-1],...body[i],k?6:8);
      O(d,-8,-21+n,7,4);O(base,-9,-21+n,5,3);glint(-12,-23+n);
      B(hi,-10,-19+n,5,1);L('#dd8b72',-14,-20+n,-19-f,-20+n);B('#dd8b72',-20-f,-21+n,2,1);
      [[0,-16],[-1,-18],[1,-10]].forEach(([x,y])=>L(hi,x,y+n,x+3,y+n+1));
    }else if(kind==='golem'){
      P(d,-12,-16,-4,-16,-3,-2,-5,0,-14,0,-13,-6);P(sh,-10,-14,-5,-14,-5,-3,-11,-2);
      P(d,2,-15,10,-15,13,-4,12,0,2,0,1,-2);P(base,3,-12,9,-13,10,-3,3,-2);
      P(d,-13,-27,-8,-32,6,-31,13,-27,12,-17,6,-12,-6,-12,-13,-18);
      P(base,-11,-26,-7,-30,4,-29,11,-26,9,-19,4,-16,-6,-16,-11,-20);
      P(hi,-10,-26,-5,-29,3,-28,-1,-25,-7,-24);P(sh,5,-27,10,-26,9,-20,4,-17,1,-19);
      P(pale,-5,-23,-2,-25,0,-22,-3,-19);
      P(d,-10,-33,-8,-38,1,-39,6,-36,5,-30,-5,-29);P(mid,-8,-34,-7,-36,1,-37,4,-35,3,-32,-5,-31);B(pale,-8,-34,6,1);
      for(const [x,dy] of [[-15,n],[12,-n]]){
        P(d,x-4,-27+dy,x+1,-29+dy,x+6,-25+dy,x+4,-17+dy,x+5,-11+dy,x-1,-8+dy,x-6,-11+dy,x-6,-18+dy);
        P(base,x-4,-25+dy,x,-27+dy,x+3,-24+dy,x+2,-18+dy,x-4,-17+dy);
        B(mid,x-4,-16+dy,7,5);B(hi,x-4,-25+dy,3,2);
      }
    }else if(kind==='wisp'){
      const y=-16+n*2;
      g.save();g.translate(0,y);
      P(sh,-10,-1,-12,-9,-7,-14,-4,-24,-1,-19,0,-14,5,-18,5,-13,11,-9,13,-4,9,3,5,6,1,11,-1,6,-7,4);
      P(base,-9,-3,-8,-9,-3,-13,-2,-19,1,-12,5,-15,5,-10,10,-7,9,0,5,4,1,7,-1,4,-7,3);
      P(hi,-6,-3,-5,-7,-1,-10,1,-7,4,-9,6,-5,5,0,1,4,-4,1);
      B(d,-6,-5,2,3);B(d,0,-5,2,3);
      [[-11,-17],[12,-15],[7,7]].forEach(([x,yy])=>B(hi,x+f,yy-n,2,2));g.restore();
    }else if(kind==='crab'){
      for(let i=0;i<3;i++){joint([i*4,-10],[6+i*4,-8],[8+i*4+f,-2],2,sh);joint([-i*3,-10],[-5-i*4,-7],[-7-i*4-f,-2],2,mid);}
      O(d,1,-12,12,8);O(base,1,-13,10,6);P(hi,-8,-14,-5,-18,2,-19,6,-17,0,-17,-2,-15);
      for(let x of [-7,-2]){L(d,x,-17,x-1,-22,2);glint(x-2,-24);}
      for(let [x,y,dir] of [[-13,-14-n,-1],[13,-12+n,1]]){
        joint([x*.6,-12],[x,-8],[x+dir*2,y],2,mid);
        P(d,x,y,x+dir*4,y-5,x+dir*10,y-8,x+dir*8,y-2,x+dir*12,y-3,x+dir*8,y+3,x+dir*4,y+4);
        P(base,x+dir,y,x+dir*5,y-4,x+dir*8,y-6,x+dir*6,y-1,x+dir*10,y-2,x+dir*7,y+2,x+dir*4,y+2);
      }
    }else if(kind==='mantis'){
      // Slender runner with folded scythes and translucent, veined wing covers.
      joint([5,-15],[16,-17],[23+f,-2],1,sh);joint([2,-16],[11,-18],[13-f,-2],1,mid);
      joint([1,-13],[5,-16],[4+f,-2],2,base);joint([-3,-14],[-5,-11],[-7-f,-2],1,hi);
      P(d,-7,-18,-8,-27,-5,-31,-1,-27,-1,-21,6,-18,23,-22,20,-17,8,-12,0,-12);
      P(base,-6,-21,-6,-27,-3,-29,-2,-26,-3,-21,5,-16,0,-15);
      P(mid,0,-18,9,-19,23,-23,18,-17,5,-14);L(hi,4,-17,18,-21);
      P(d,-13,-30,-9,-34,-3,-34,1,-31,-5,-25,-9,-26);P(hi,-11,-30,-8,-32,-2,-32,-5,-28,-8,-27);
      glint(-11,-31);L(d,-9,-33,-15,-39+n);L(mid,-4,-33,-4,-40+n);
      joint([-6,-22],[-13,-15+n],[-17,-22+n],2,sh);
      joint([-5,-21],[-11,-12-n],[-19,-18-n],2,base);
      P(hi,-20,-17-n,-20,-26-n,-15,-32-n,-17,-25-n,-16,-21-n);
    }else if(kind==='turtle'){
      joint([-6,-10],[-8,-6],[-10-f,-2],4,sh);joint([8,-10],[11,-6],[11+f,-2],4,sh);
      P(d,12,-11,22,-11,26,-14,24,-8,16,-6);
      O(d,-13,-11,9,6);O(base,-15,-12,7,4);glint(-19,-14);L(sh,-21,-10,-18,-9);
      O(d,3,-14,15,12);O(mid,3,-15,13,10);P(base,-9,-15,-7,-20,-2,-24,7,-24,13,-20,16,-14,12,-11,-7,-11);
      for(const [xx,yy] of [[0,-19],[8,-18],[4,-12],[-6,-14]]){
        P(sh,xx-4,yy,xx-2,yy-3,xx+2,yy-3,xx+4,yy,xx+2,yy+3,xx-2,yy+3);
        P(base,xx-3,yy,xx-1,yy-2,xx+2,yy-2,xx+3,yy,xx+1,yy+2,xx-1,yy+2);B(hi,xx-1,yy-2,2,1);
      }
      L(d,-11,-9,16,-9,3);L(hi,-10,-10,13,-10);
      joint([-5,-8],[-6,-5],[-7+f,-2],3,base);joint([11,-8],[13,-6],[11-f,-2],3,base);
    }else if(kind==='scorpion'){
      for(let i=0;i<4;i++)joint([i*3-5,-9],[i*4-6,-6],[i*4-9+(i%2?f:-f),-2],1,mid);
      const tail=[[8,-11],[17,-13],[21,-20],[20,-26+n],[15,-30+n],[9,-28+n]];
      for(let j=0;j<tail.length;j++){const [x,y]=tail[j];O(d,x,y,j===0?5:4,4);O(mid,x,y-1,3,2);B(hi,x-1,y-2,2,1);}
      P(d,10,-29+n,5,-28+n,2,-23+n,3,-19+n,6,-24+n,11,-25+n);P(pale,9,-27+n,5,-26+n,3,-22+n,6,-24+n);
      O(d,0,-11,10,6);O(base,0,-12,8,4);L(sh,0,-15,2,-10);L(sh,4,-14,6,-11);
      O(d,-9,-11,5,4);O(mid,-10,-12,4,3);glint(-12,-13);
      for(let j=0;j<2;j++){let x=-14-j*5,y=-8-j*4+(j?f:-f);joint([-5,-11],[x+1,y+3],[x-3,y],2,mid);P(d,x-1,y+2,x-7,y+2,x-13,y-2,x-13,y-6,x-7,y-3,x-10,y-8,x-6,y-7,x-2,y-4);P(base,x-3,y,x-7,y,x-11,y-3,x-7,y-2,x-8,y-5,x-5,y-4);}
    }else if(kind==='stag'){
      for(let j=0;j<4;j++){let x=[-6,8,-4,11][j],u=(j===1||j===2?f:-f)*2;joint([x,-17],[x+u,-10],[x+u*1.5,-2],j<2?1:2,j<2?sh:mid);B(d,x+u*1.5-2,-2,3,2);}
      O(d,2,-21,13,7);O(base,2,-22,12,5);P(hi,10,-24,18,-29,16,-21,12,-19);
      P(d,-7,-17,-11,-21,-13,-30,-21,-32,-22,-36,-18,-38,-12,-36,-7,-36,-5,-28,-1,-24);
      P(base,-9,-21,-12,-31,-18,-33,-20,-35,-17,-36,-11,-34,-8,-34,-7,-27,-3,-23);
      P(hi,-11,-35,-10,-41,-4,-42,-7,-37);glint(-17,-35);B(sh,-22,-35,3,2);
      for(const [x,y] of [[-13,-36],[-10,-36]]){L(d,x,y,x-3,y-8,2);L(mid,x-3,y-8,x-8,y-12+n,2);L(hi,x-7,y-11+n,x-5,y-16+n);L(hi,x-4,y-8+n,x-1,y-15+n);}
      for(let j=0;j<5;j++)B(hi,j*3-4,-25+j%2*2,2,1);
    }else if(kind==='snail'){
      O(d,1,-5,18,5);O(mid,0,-5,17,3);P(d,-17,-4,-23,-8,-23,-15,-20,-18,-16,-17,-13,-12,-11,-10,-7,-6);
      P(base,-16,-5,-21,-9,-21,-15,-19,-16,-17,-15,-15,-11,-11,-8);
      for(const [x,y] of [[-21,-16],[-17,-15]]){L(sh,x,y,x-2-f,y-6,2);O(d,x-2-f,y-7,2,2);B(pale,x-3-f,y-8,2,1);}
      O(d,4,-18,13,13);O(base,4,-19,11,11);O(mid,4,-18,9,9);
      const coil=[[-4,-12],[-6,-19],[-2,-26],[5,-28],[11,-25],[14,-19],[11,-13],[4,-11],[-1,-14],[-2,-20],[2,-23],[7,-22],[9,-18],[7,-16],[4,-16],[3,-18]];
      for(let i=1;i<coil.length;i++)L(i%3?sh:hi,...coil[i-1],...coil[i],2);B(hi,0,-27,4,2);
      for(let i=0;i<6;i++)B(hi,-13+i*5+f,-4,2,1);
    }else if(kind==='fox'){
      P(d,5,-14,9,-21,15,-19,20,-21,24,-29,27,-31,28,-22,24,-13,17,-8,10,-8);
      P(base,8,-14,11,-19,16,-17,22,-21,25,-28,26,-22,22,-15,16,-11);P(hi,20,-18,24,-26,26,-29,26,-22,23,-17);
      for(let j=0;j<4;j++){let x=[-6,6,-4,9][j],u=(j===1||j===2?f:-f)*2;joint([x,-13],[x+u,-7],[x+u*1.5,-2],2,j<2?sh:base);B(d,x+u*1.5-2,-2,4,2);}
      O(d,1,-16,12,6);O(base,1,-17,11,4);
      P(d,-9,-13,-13,-15,-22,-16,-25,-19,-18,-21,-17,-31,-13,-28,-11,-24,-6,-29,-4,-28,-5,-19);
      P(base,-9,-16,-14,-17,-22,-18,-17,-21,-16,-28,-12,-23,-7,-26,-7,-20);P(hi,-23,-19,-16,-18,-13,-20,-10,-20,-12,-16,-18,-16);glint(-16,-22);B(d,-24,-20,3,2);
    }else if(kind==='spider'){
      for(let i=0;i<8;i++){const x=(i%4)*3-5,dir=i<4?-1:1,k=(i%2?f:-f);joint([x,-12],[x+dir*(9+i%4*2),-16-i%4],[x+dir*(14+i%4*2)+k,-2],2,i<4?sh:mid);}
      O(d,1,-19,11,10);O(mid,2,-20,9,8);O(base,2,-22,7,5);P(hi,-2,-22,2,-26,7,-22,4,-23,2,-20);
      O(d,-8,-12,8,6);O(base,-9,-13,6,4);glint(-13,-14);glint(-8,-14);B(hi,-12,-16,1,1);B(hi,-9,-16,1,1);
      P(pale,-14,-9,-11,-9,-12,-5);P(pale,-7,-9,-5,-9,-6,-5);
    }else if(kind==='axolotl'){
      // Gills fan out like branching coral; a translucent dorsal fin spans the tail.
      P(d,5,-11,10,-17,14,-13,21,-17,27,-23+n,27,-18+n,23,-10,14,-7,5,-7);
      P(hi,7,-11,11,-15,14,-12,22,-15,25,-20+n,24,-14+n,20,-11,13,-9);
      O(d,0,-11,12,6);O(base,0,-12,10,4);
      for(let j=0;j<4;j++){let x=[-4,6,-7,9][j],u=(j%2?f:-f);joint([x,-10],[x+u,-5],[x+u-3,-3],2,j<2?sh:mid);}
      const pink=mix(base,'#ff8f99',.57);
      for(const [xx,yy] of [[-6,-27],[-1,-24],[1,-19]]){L(d,-10,-14,xx+f,yy,3);L(pink,-10,-14,xx+f,yy,2);L(pink,xx+f-1,yy+4,xx+f+3,yy+1);L(hi,xx+f,yy+2,xx+f-2,yy-1);}
      O(d,-13,-13,9,6);O(base,-14,-14,7,4);glint(-19,-15);glint(-12,-16);L(sh,-18,-11,-14,-10);B(hi,-18,-12,2,1);
    }else if(kind==='octopus'){
      for(let j=0;j<8;j++){
        const dx=(j-3.5)*4,off=R(sin(frame+j)*2);
        const curve=[[dx*.42,-14],[dx*.67,-9],[dx,-4+off],[dx+(j<4?-3:3),-3+off],[dx+(j<4?-5:5),-6+off]];
        for(let i=1;i<curve.length;i++)L(d,...curve[i-1],...curve[i],i<3?5:3);
        for(let i=1;i<curve.length;i++)L(j%2?base:mid,...curve[i-1],...curve[i],i<3?3:1);
        B(hi,dx-1,-5+off,2,1);
      }
      O(d,0,-22+n,12,14);O(mid,0,-23+n,10,12);O(base,-2,-26+n,8,8);O(hi,-4,-29+n,3,2);
      O(d,-9,-18+n,4,4);O(hi,-10,-19+n,3,2);B(d,-12,-20+n,2,3);glint(-2,-19+n);
      for(const [xx,yy] of [[4,-29],[6,-23],[3,-14]])orbit(g,hi,xx,yy+n,2,.8,0);
    }else if(kind==='ray'){
      const flap=R(sin(frame)*4), yy=-19+n;
      const tail=[[3,yy],[14,yy-2],[22,yy+3],[28,yy+1+f],[31,yy-3+f]];
      for(let i=1;i<tail.length;i++)L(i<3?d:mid,...tail[i-1],...tail[i],i<3?3:1);
      P(d,-22,yy,-12,yy-6,-2,yy-11,10,yy-19-flap,10,yy-10,5,yy-2,12,yy+7,9,yy+17+flap,-1,yy+11,-14,yy+5);
      P(base,-20,yy,-10,yy-5,0,yy-10,8,yy-16-flap,7,yy-9,2,yy-2,9,yy+7,8,yy+13+flap,-2,yy+7,-11,yy+3);
      P(hi,-15,yy,-4,yy-3,0,yy-2,-2,yy+1,-11,yy+2);glint(-12,yy-4);
      L(sh,-3,yy-4,6,yy-12-flap);L(sh,-3,yy+3,6,yy+9+flap);B(hi,1,yy-8,2,1);
    }else if(kind==='ram'){
      for(let j=0;j<4;j++){const xx=[-7,8,-5,11][j],step=j%2?f:-f;joint([xx,-15],[xx+step,-8],[xx+step*1.3,-2],3,j<2?sh:mid);B(d,xx+step-2,-2,4,2);}
      O(d,2,-18,14,9);for(let i=0;i<5;i++)O(i%2?mid:base,-7+i*4,-20-(i%2),5,6);
      P(hi,10,-22,18,-26,17,-22,13,-18);O(d,-11,-22,9,8);P(mid,-15,-27,-9,-27,-7,-22,-12,-16,-18,-15,-23,-18,-23,-21);
      O(d,-10,-26,8,8);O(hi,-10,-26,7,7);O(mid,-10,-25,5,5);
      const horn=[[-14,-21],[-17,-25],[-15,-30],[-11,-32],[-6,-29],[-6,-25],[-9,-23],[-12,-25],[-11,-27]];
      for(let i=1;i<horn.length;i++)L(pale,...horn[i-1],...horn[i],2);
      glint(-18,-23);B(d,-22,-19,3,2);
    }else if(kind==='owl'){
      const flap=R(sin(frame)*3);
      P(d,3,-12,13,-15,20,-19,17,-11,18,-11,13,-6,5,-5);
      O(d,0,-17,12,13);O(mid,-1,-17,10,11);O(hi,-5,-15,6,8);
      P(d,0,-20,4,-29-flap,16,-38-flap,22,-41-flap,20,-34-flap,24,-36-flap,20,-28,12,-21,5,-16);
      P(base,2,-20,6,-28-flap,17,-36-flap,20,-38-flap,16,-31,10,-25);L(hi,5,-23,16,-33-flap);
      P(d,-16,-24,-17,-37,-11,-33,-5,-35,1,-34,6,-37,4,-24,-4,-19);
      O(base,-6,-28,10,7);O(hi,-11,-28,5,4);O(hi,-3,-28,5,4);glint(-13,-29);glint(-5,-29);P(pale,-10,-25,-7,-26,-8,-21);
      for(let i=0;i<4;i++)B(mid,-9+i*3,-18+i%2*3,2,2);
      L(pale,-5,-6,-6,-2);L(pale,1,-6,0,-2);
    }else if(kind==='pangolin'){
      joint([2,-12],[6,-6],[4+f,-2],2,sh);joint([-8,-12],[-11,-7],[-12-f,-2],2,sh);
      P(d,-13,-9,-19,-8,-25,-10,-23,-14,-15,-20,-12,-24,-4,-28,8,-26,15,-19,18,-14,28,-9,31,-6,24,-6,12,-8,4,-6,-5,-7);
      P(mid,-22,-11,-21,-14,-14,-19,-9,-24,0,-26,8,-24,14,-18,18,-12,26,-8,16,-9,7,-11,-1,-9,-11,-11);
      for(const [xx,yy,w] of [[-11,-21,6],[-6,-24,7],[0,-24,7],[6,-21,7],[-9,-17,6],[-3,-19,7],[3,-18,7],[9,-16,7],[14,-13,6],[19,-10,5]]){
        P(sh,xx-3,yy,xx,yy-3,xx+w,yy,xx+w-1,yy+5,xx+1,yy+3);P(base,xx-2,yy,xx,yy-2,xx+w-1,yy,xx+w-2,yy+3,xx+1,yy+2);B(hi,xx,yy-2,3,1);
      }glint(-18,-16);B(d,-24,-12,2,2);joint([-5,-11],[-7,-7],[-9+f,-2],2,base);joint([8,-11],[10,-6],[9-f,-2],2,mid);B(pale,-12+f,-2,4,1);
    }else if(kind==='hare'){
      const hop=Math.max(0,n);g.save();g.translate(0,-hop);
      O(d,12,-13,4,4);O(hi,12,-14,3,3);O(d,2,-12,12,10);O(base,2,-13,10,8);O(mid,5,-9,6,6);
      P(d,-14,-21,-18,-39-f,-17,-43-f,-13,-42-f,-9,-24,-5,-24,-5,-42+f,-2,-47+f,0,-45+f,-1,-25,-4,-19);
      P(hi,-14,-23,-16,-40-f,-14,-39-f,-10,-24);P(base,-4,-24,-4,-40+f,-2,-44+f,-2,-28);
      O(d,-10,-19,8,7);O(base,-10,-20,7,5);O(hi,-15,-17,4,3);glint(-15,-22);B(sh,-19,-18,2,1);
      L(d,-5,-10,-9-f,-3,3);B(hi,-12-f,-3,4,2);L(d,7,-5,2+f,-2,4);B(hi,-3+f,-3,6,2);g.restore();
    }else if(kind==='jellyfish'){
      const y=-26+n;
      for(let j=-3;j<=3;j++){let xx=j*3;for(let k=0;k<4;k++){const a=sin(frame+j+k)*2,b=sin(frame+j+k+1)*2;L(j%2?mid:hi,xx+a,y+k*5,xx+b,y+(k+1)*5,j%2?1:2);}}
      O(d,0,y,14,4);P(d,-15,y,-14,y-6,-9,y-13,-4,y-16,4,y-16,10,y-12,14,y-7,15,y);
      P(base,-13,y,-12,y-6,-8,y-11,-3,y-14,4,y-14,9,y-10,12,y-6,13,y);O(hi,-3,y-10,5,2);
      L(pale,-12,y+1,12,y+1);glint(-8,y-3);glint(-2,y-3);for(let i=0;i<3;i++)B(hi,4+i*3,y-4-i%2*2,1,2);
    }else if(kind==='dragonfly'){
      const y=-18+n,flap=R(cos(frame)*6);
      for(const [dx,dy] of [[-12,-17-flap],[11,-22-flap],[-7,11+flap],[14,10+flap]]){
        const px=dx*.6,py=y+dy*.6;P(d,-4,y,px-3,py-2,dx,y+dy,dx+4,y+dy+1,px+4,py,0,y+2);
        P(hi,-2,y,px-1,py-1,dx+1,y+dy+1,dx+3,y+dy+2,px+2,py);L(mid,-2,y,dx+2,y+dy+1);
      }
      for(let j=0;j<7;j++){const xx=2+j*3,yy=y+sin(frame*.7+j*.3)*2;O(d,xx,yy,j<4?3:2,2);B(base,xx-1,yy-1,3,2);}
      O(d,-4,y,6,4);O(base,-5,y-1,4,3);O(d,-11,y-2,5,5);O(hi,-12,y-3,4,3);B(sh,-14,y-4,2,2);
      L(d,-14,y-6,-17,y-10);L(mid,-9,y+2,-13,y+6);
    }else if(kind==='elk'){
      for(let i=0;i<4;i++){let x=[-8,8,-5,11][i],stride=(i%2?f:-f);joint([x,-16],[x+stride,-9],[x+stride*2,-2],2,i<2?sh:mid);B(d,x+stride*2-2,-2,4,2);}
      O(d,2,-21,15,9);O(base,3,-22,13,7);O(mid,-8,-24,8,10);P(hi,10,-23,18,-27,17,-22,13,-20);
      P(d,-8,-18,-15,-22,-15,-31,-23,-32,-26,-36,-23,-38,-16,-37,-13,-39,-6,-34,-5,-25);
      P(base,-10,-22,-13,-26,-13,-32,-20,-34,-22,-36,-16,-35,-13,-37,-8,-33,-7,-27);glint(-20,-36);B(sh,-25,-36,3,2);
      for(const [x,y,dir] of [[-14,-37,-1],[-10,-36,1]]){
        L(d,x,y,x+dir*5,y-9,3);P(hi,x+dir*2,y-5,x+dir*7,y-5,x+dir*12,y-12,x+dir*11,y-15,x+dir*7,y-10,x+dir*7,y-17,x+dir*5,y-15,x+dir*4,y-10,x+dir,y-13,x+dir,y-8);
      }for(let i=0;i<3;i++)L(hi,-3+i*4,-25,1+i*4,-24);
    }else if(kind==='griffin'){
      joint([9,-14],[13,-8],[11+f,-2],3,sh);joint([-6,-14],[-6,-7],[-9-f,-2],2,sh);
      L(d,9,-16,20,-15,3);L(d,20,-15,24,-22+f,2);O(hi,24,-23+f,2,3);
      O(d,1,-17,13,8);O(base,2,-17,11,6);
      P(d,-2,-18,2,-28,13,-42-n,20,-46-n,18,-39,24,-42,18,-31,21,-32,14,-23,5,-17);
      P(base,0,-20,5,-29,15,-40-n,18,-42-n,14,-34,18,-36,12,-28,15,-29,9,-22);L(hi,3,-23,14,-38-n);L(mid,5,-21,15,-31);
      P(d,-12,-16,-17,-22,-17,-31,-11,-35,-5,-35,-3,-37,-3,-32,-1,-32,-3,-27,-3,-19);
      P(hi,-12,-18,-15,-24,-15,-29,-10,-33,-5,-33,-4,-29,-6,-26,-3,-25,-6,-23,-6,-20);glint(-15,-29);
      P(pale,-17,-28,-22,-27,-24,-23,-21,-24,-20,-21,-19,-25,-15,-25);
      joint([5,-12],[6,-7],[6-f,-2],3,mid);joint([-9,-13],[-10,-8],[-13+f,-2],2,hi);B(pale,-15+f,-2,4,1);
    }else if(kind==='mole'){
      O(d,2,-13,13,10);O(mid,3,-14,11,8);O(base,-2,-17,8,5);O(d,-12,-13,8,6);
      P(base,-19,-13,-17,-17,-10,-18,-6,-15,-9,-12,-17,-11);P(hi,-19,-14,-23,-12,-21,-10,-17,-11);B('#f1ad9c',-23,-13,3,2);B(d,-16,-16,2,2);
      for(const [x,y] of [[-9,-7+n],[5,-6-n]]){O(d,x,y,6,4);O(hi,x,y-1,4,3);for(let j=0;j<3;j++)P(pale,x-4+j*3,y,x-3+j*3,y+4,x-1+j*3,y);}
      L(hi,-18,-12,-24,-14);L(mid,-18,-10,-24,-9);B(hi,1,-21,4,1);
    }else if(kind==='dragon'){
      joint([10,-15],[14,-8],[12+f,-2],3,sh);joint([-6,-15],[-7,-8],[-8-f,-2],3,sh);
      P(d,5,-17,16,-18,23,-23,27,-31+n,29,-32+n,28,-22,24,-15,17,-11,9,-10);
      P(base,9,-15,17,-17,24,-22,27,-28+n,26,-22,22,-17,15,-13);O(d,1,-18,14,8);O(base,1,-18,12,6);
      P(d,-4,-19,1,-32,13,-47-n,19,-49-n,17,-43-n,23,-41,26,-31,20,-34,16,-27,11,-29,4,-21);
      P(mid,-2,-20,3,-32,14,-45-n,16,-46-n,14,-40-n,21,-39,23,-34,18,-36,15,-30,10,-31);L(hi,0,-22,13,-43-n);L(base,13,-43-n,15,-31);
      P(d,-9,-16,-14,-21,-14,-29,-20,-28,-25,-29,-26,-33,-22,-35,-18,-36,-16,-40,-10,-37,-7,-40,-7,-34,-4,-30,-3,-23);
      P(base,-10,-19,-12,-24,-12,-31,-18,-31,-23,-30,-24,-32,-19,-34,-16,-34,-15,-37,-12,-35,-9,-36,-10,-31,-6,-30,-6,-24);glint(-17,-35);B(hi,-24,-33,3,1);P(pale,-21,-30,-18,-30,-20,-27);
      L(hi,-10,-37,-7,-43,2);B(hi,-1,-23,3,2);joint([5,-12],[5,-6],[3-f,-2],3,base);joint([-7,-14],[-9,-7],[-12+f,-2],2,base);
    }else{
      // Lantern mushroom.
      B(d,-7,-8,6,8);B(sh,-6,-8,4,6);B(d,2,-8,6,8);B(mid,3,-8,4,6);
      O(d,0,-13,8,9);O(hi,0,-13,6,7);glint(-5,-15);glint(1,-15);
      P(d,-16,-19,-13,-25,-7,-30+n,-1,-32+n,5,-30+n,12,-25,16,-20,16,-17,9,-16,-10,-16,-16,-17);
      P(base,-14,-20,-11,-24,-6,-29+n,0,-30+n,6,-28+n,11,-24,14,-20,9,-19,-11,-18);
      O(hi,-6,-25+n,3,2);O(hi,5,-24+n,2,2);B(hi,0,-28+n,2,1);L(sh,-13,-18,10,-17);
    }
    if(boss){
      // The hovering three-point crown is a readable boss marker even on pale backgrounds.
      const yy=-Math.min(55,Math.max(43,(stature[kind]||34)+3))+n;
      P('#5d4555',-7,yy-3,-3,yy,0,yy-6,3,yy,7,yy-3,5,yy+3,-5,yy+3);
      P(pale,-6,yy-2,-3,yy+1,0,yy-4,3,yy+1,6,yy-2,4,yy+2,-4,yy+2);
      B('#fff4c9',-1,yy,2,2);
    }
  }
  function actor(kind,base,t,stance,boss=false){
    const frame=((floor(t*(stance==='walk'||stance==='cast'?9:6))%8)+8)%8,key=[kind,base,frame,stance,boss].join('/');
    if(actorCache.has(key))return actorCache.get(key);
    const c=surface(72,68),g=c.getContext('2d');g.translate(35,61);
    if(kind==='monkey')monkey(g,frame*Math.PI/4,stance);
    else enemySprite(g,kind,base||'#a2bc75',frame*Math.PI/4,boss);
    if(actorCache.size>=210)actorCache.delete(actorCache.keys().next().value);
    actorCache.set(key,c);return c;
  }
  function place(g,kind,color,t,stance,boss,x,y,k){
    const a=actor(kind,color,t,stance,boss);
    g.drawImage(a,R(x-35*k),R(y-61*k),R(72*k),R(68*k));
  }
  function orbit(g,c,x,y,r,ratio,offset,width=1){
    for(let i=0;i<25;i++){
      const a=offset+i*Math.PI/12,b=a+Math.PI/12;
      line(g,c,x+cos(a)*r,y+sin(a)*r*ratio,x+cos(b)*r,y+sin(b)*r*ratio,width);
    }
  }
  function burst(g,c,x,y,r,count,age){
    for(let i=0;i<count;i++){let a=i*Math.PI*2/count-2.1,rr=r*(.85+.15*sin(i*71+1));line(g,c,x+cos(a)*(rr-3),y+sin(a)*(rr-3),x+cos(a)*rr,y+sin(a)*rr,age<.36?2:1);}
  }
  // Spell artwork is drawn at the actors' current positions so entry and recoil never detach a projectile.
  function abilityArt(g,pattern,a,hx,hy,tx,ty,ground){
    if(a>.86)return;
    const u=Math.min(1,a/.60),wave=sin(Math.PI*u);
    const leaf=(x,y,w,h,c)=>poly(g,c,[x-w,y+2,x-w*.4,y-h*.6,x+w*.5,y-h,x+w,y-h,x+w*.5,y-1,x-w*.25,y+3]);
    g.save();g.globalAlpha=Math.min(1,(.88-a)*4);
    if(pattern==='razor_bloom'){
      for(let i=0;i<3;i++){
        const p=Math.min(1,Math.max(0,(a-i*.07)/.38));if(!p||p>=1)continue;
        const x=hx+(tx-hx)*p,y=hy+(ty-hy)*p-(9+i*5)*sin(p*Math.PI);
        leaf(x,y,5,5,'#526c3d');leaf(x+1,y-1,4,4,'#b6f674');line(g,'#f4ffb4',x-3,y+1,x+3,y-3);
      }
      if(a>.35)burst(g,'#d3ff88',tx,ty,6+(a-.35)*27,6,a);
    }else if(pattern==='seismic_bell'){
      const d=6+u*21;orbit(g,'#4e4145',tx,ground,d,.23,0,2);orbit(g,'#ffd586',tx,ground-1,d,.23,0);
      for(let i=-1;i<=1;i++){const x=tx+i*10,h=(7+5*(1-Math.abs(i)))*wave;
        poly(g,'#6c5948',[x-4,ground,x-2,ground-h*.8,x+2,ground-h,x+4,ground]);line(g,'#f3c484',x-2,ground-1,x+1,ground-h+1);}
    }else if(pattern==='cinder_swarm'){
      for(let j=0;j<5;j++){
        const p=Math.max(0,Math.min(1,(a-j*.053)/.35));if(p===0||p>=1)continue;
        const xx=tx+(j-2)*4+8*(1-p),yy=ty-(1-p)*(29+j*3);
        line(g,'#e3543f',xx+3,yy-6,xx,yy,3);rect(g,'#ffb449',xx-2,yy-1,3,3);rect(g,'#fff3a2',xx-1,yy+1,2,1);
      }if(a>.33)burst(g,'#ff9c44',tx,ty+3,7+(a-.33)*20,8,a);
    }else if(pattern==='whiteout'){
      for(let j=0;j<3;j++){const w=7+14*wave;for(let b=0;b<5;b++){let q=(b+j*.7)*.55+a*6;line(g,b%2?'#e5ffff':'#84d8ec',tx+cos(q)*w,ty-13+j*10+sin(q)*3,tx+cos(q+.45)*w,ty-13+j*10+sin(q+.45)*3,2);}}
      for(let j=0;j<3;j++){let x=tx+(j-1)*9;poly(g,'#d7fcff',[x-3,ground,x-2,ground-8-5*wave,x+1,ground-11-5*wave,x+3,ground]);}
    }else if(pattern==='forked_sky'){
      if(a<.56){let px=hx,py=hy;
        for(let j=1;j<=7;j++){let r=j/7,x=hx+(tx-hx)*r,y=hy+(ty-hy)*r+(j===7?0:sin(j*43+floor(a*17))*7);
          line(g,'#a16fbd',px,py,x,y,3);line(g,'#ffef91',px,py,x,y);if(j%3===0)line(g,'#fbdaa3',x,y,x-4,y-6);px=x;py=y;}}
      burst(g,'#fdeb83',tx,ty,6+u*11,6,a);
    }else if(pattern==='jade_shelter'){
      orbit(g,'#4f9472',hx,hy,14+wave*7,1.2,0,2);orbit(g,'#abefb3',hx,hy,15+wave*7,1.2,0);
      for(let j=0;j<4;j++){let q=j*Math.PI/2+a*2;leaf(hx+cos(q)*(15+wave*6),hy+sin(q)*(18+wave*6),4,5,'#b9f98b');}
      rect(g,'#fff4b9',hx-3,hy,7,1);rect(g,'#fff4b9',hx,hy-3,1,7);
    }else if(pattern==='moon_siphon'){
      for(let j=0;j<13;j++){
        let p=j/12,x=tx+(hx-tx)*p,y=ty+(hy-ty)*p+sin(p*Math.PI*3-a*8)*4;
        if(j){const q=(j-1)/12;line(g,j%4?'#d19ceb':'#ffe2ff',tx+(hx-tx)*q,ty+(hy-ty)*q+sin(q*Math.PI*3-a*8)*4,x,y,j%4?1:2);}
      }orbit(g,'#f5c7ff',hx,hy,4+u*8,.65,0);
    }else if(pattern==='clockwork_lunge'){
      const x=hx+(tx-hx)*u,y=hy+(ty-hy)*u-10*wave,rad=5+wave*2;
      orbit(g,'#825c45',x,y,rad,1,0,3);orbit(g,'#ffe1a1',x,y,rad,1,0);
      for(let j=0;j<8;j++){let q=j*Math.PI/4+a*7;line(g,'#f3ba72',x+cos(q)*rad,y+sin(q)*rad,x+cos(q)*(rad+3),y+sin(q)*(rad+3),2);}
      line(g,'#fff4c5',x,y,x+cos(-a*14)*4,y+sin(-a*14)*4);
    }
    g.restore();
  }
  function relicGlow(g,rarity,x,y,size,t){
    if(rarity!=='rare'&&rarity!=='mythic')return;
    const mythic=rarity==='mythic',tone=window.Catalog.rarities[rarity].color,r=size*.50+1;
    g.save();g.globalAlpha*=mythic?.28:.19;oval(g,tone,x,y,r,r*.81);g.restore();
    if(mythic){
      orbit(g,'#62435c',x,y,r+1,.72,0,2);orbit(g,tone,x,y,r+1,.72,0);
      for(let j=0;j<3;j++){const a=t*2.8+j*2.095;let xx=x+cos(a)*(r+1),yy=y+sin(a)*(r+1)*.72;
        poly(g,'#60405a',[xx,yy-3,xx+3,yy,xx,yy+3,xx-3,yy]);rect(g,j===1?'#ff9bc2':'#fff1ac',xx-1,yy-1,3,2);}
    }else{
      for(let j=0;j<2;j++){let xx=x+(j?1:-1)*(r*.64+1),yy=y+(j?-1:1)*(r*.54)+sin(t*3+j)*1.2;
        line(g,'#46607c',xx-2,yy,xx+2,yy,2);line(g,'#46607c',xx,yy-2,xx,yy+2,2);
        line(g,tone,xx-2,yy,xx+2,yy);line(g,tone,xx,yy-2,xx,yy+2);rect(g,'#fff2c7',xx,yy,1,1);}
    }
  }
  function cast(g,event,hx,ground,foe,i,showLabel,chain,reducedMotion){
    const a=event.age||0,k=event.kind,c=elements[event.element]||'#ffdb92',pat=event.pattern||event.abilityId||'',cream='#fff6c9';
    if(a>1.1)return;
    const fx=foe.x,fy=foe.center,feet=foe.y,hand=ground-24,incoming=k==='enemy';
    const sx=incoming?fx-5:hx+11,sy=incoming?fy:hand,ex=incoming?hx+4:fx,ey=incoming?hand:fy;
    if(k==='ability'){
      const ox=chain?chain.x:hx,oy=chain?chain.center:hand;
      abilityArt(g,pat,a,ox,oy,fx,fy,feet);
    }else if(k==='loot'){
      const sprite=window.ItemArt?.sprite(event.id);if(!sprite)return;
      const lane=event.lane??0,full=event.outcome==='full',delay=.09+(lane%3)*.045;
      const u=Math.max(0,Math.min(1,(a-delay)/.70)),travel=u*u*(3-2*u),arrival=delay+.70;
      const x=full?fx-a*20:fx+(hx-10-fx)*travel;
      const yy=full?feet-7-7*Math.max(0,sin(Math.min(1,a/.4)*Math.PI)):feet-12+(ground-24-(feet-12))*travel-(29+(lane%3)*7)*sin(u*Math.PI);
      const size=full?15:R(18-9*travel);
      g.save();g.globalAlpha=full?Math.min(1,(1.1-a)*2):Math.max(0,1-Math.max(0,a-arrival+.05)*9);
      relicGlow(g,window.Catalog.items[event.id]?.rarity,x,yy,size,reducedMotion?0:a*2);
      g.drawImage(sprite,R(x-size/2),R(yy-size/2),size,size);
      if(!full&&u<1){for(let j=0;j<3;j++){const z=a*6+j*2.1;rect(g,j===1?cream:c,x+cos(z)*9,yy+sin(z)*6,2,1);}}
      g.restore();if(!full&&a>=arrival&&a<arrival+.2)burst(g,cream,hx-10,ground-24,3+(a-arrival)*25,5,a);
    }else if(k==='attack'||incoming){
      const t=Math.min(1,a/.21),arch=pat==='pierce'||event.element==='storm'?0:13;
      const at=u=>[sx+(ex-sx)*u,sy+(ey-sy)*u-arch*4*u*(1-u)];
      if(a<.21){
        const [x,y]=at(t);
        if(event.element==='storm'){
          let ox=sx,oy=sy;for(let j=1;j<=7;j++){const [xx,yy]=at(t*j/7),zz=yy+(j===7?0:sin(j*23+floor(a*14))*4);line(g,c,ox,oy,xx,zz,2);ox=xx;oy=zz;}
        }else{
          for(let j=4;j>=0;j--){const [xx,yy]=at(Math.max(0,t-j*.038));rect(g,j?c:cream,xx,yy,Math.max(1,5-j),j%2===0?2:1);}
          if(event.element==='leaf')poly(g,c,[x-4,y,x+1,y-4,x+7,y-5,x+3,y,x-2,y+2]);
          if(event.element==='frost')poly(g,c,[x-3,y,x+1,y-3,x+8,y,x+1,y+2]);
          if(event.element==='stone')oval(g,c,x,y,3,3);
          if(event.element==='spirit')orbit(g,c,x,y,4,.8,0);
        }
      }
      if(a>=.18&&a<.56)burst(g,c,ex,ey,4+(a-.18)*27,7,a);
    }else if(k==='hit'||k==='kill'){
      if(a<.64)burst(g,k==='kill'?cream:c,fx,fy,3+a*(k==='kill'?36:21),k==='kill'?10:5,a);
    }else if(k==='heal'&&(pat==='drain'||pat==='lifesteal')){
      // Each pulse leaves the struck body, then turns mint as it reaches Pip.
      for(let j=0;j<3;j++){
        const u=(a-j*.09)/.54;if(u<0||u>1)continue;
        const q=Math.max(0,u-.065),bend=10+j*4;
        const x=fx+(hx-fx)*u,y=fy+(hand-fy)*u-bend*sin(u*Math.PI);
        line(g,u<.5?'#ac76b6':'#80c9a9',fx+(hx-fx)*q,fy+(hand-fy)*q-bend*sin(q*Math.PI),x,y,2);
        oval(g,u<.5?'#e0a5df':'#b2f8ca',x,y,2,2);rect(g,'#fff1d8',x-1,y-1,2,1);
      }
      if(a>.53&&a<.86){
        const y=hand-(a-.53)*27;rect(g,'#b2f8ca',hx-2,y,5,1);rect(g,'#b2f8ca',hx,y-2,1,5);
      }
    }else if(k==='heal'||pat==='guard'){
      for(let j=0;j<3;j++){let x=hx-15+j*14+sin(j+a*4)*2,y=ground-10-a*22-j%2*6;rect(g,c,x-2,y,5,1);rect(g,c,x,y-2,1,5);}
    }else if(k==='status'){
      if(pat==='chill')for(let j=0;j<3;j++)poly(g,c,[fx-10+j*8,feet-2,fx-9+j*8,feet-10-j*2,fx-5+j*8,feet-2]);
      else if(pat==='burn')for(let j=0;j<4;j++){let x=fx-12+j*8;poly(g,c,[x,feet-3,x+1,feet-9,x+4,feet-13-sin(a*12+j)*3,x+6,feet-5,x+4,feet-3]);}
      else if(pat==='shock')line(g,c,fx-8,fy-10,fx+5,fy-7,2);
      else for(let j=0;j<3;j++)orbit(g,c,fx-8+j*7,feet-7-a*15-(j%2)*6,2,.9,0);
    }else if(k==='milestone'){
      if(pat==='echo'){
        for(let j=0;j<3;j++){let u=Math.max(0,Math.min(1,(a-.08*j)/.45));if(u>0&&u<1){const x=hx+(fx-hx)*u,y=hand+(fy-hand)*u-sin(u*Math.PI)*7+j*3;poly(g,c,[x-6,y-2,x+1,y-2,x+5,y,x+1,y+2,x-6,y+2,x-2,y]);}}
      }else if(pat==='ring'){orbit(g,c,hx,hand,13+sin(a*Math.PI)*9,1.22,a);orbit(g,cream,hx,hand,12+sin(a*Math.PI)*8,.47,a);}
      else{const r=5+Math.min(1,a/.65)*28;orbit(g,c,fx,fy,r,.78,a);if(a<.8)burst(g,cream,fx,fy,r+4,10,a);}
    }
    if(showLabel&&event.label&&a>.16&&['enemy','attack','hit','heal'].includes(k)){
      const hero=incoming||k==='heal',txt=String(event.label),y=R((hero?ground-52:foe.barY-4)-a*10-(i%2)*6);
      g.save();g.globalAlpha=Math.max(0,Math.min(1,(1.12-a)*4));g.textAlign='center';g.font='bold 8px monospace';g.lineWidth=3;g.lineJoin='round';g.strokeStyle='#332e37';
      const x=R(Math.min(g.canvas.width-txt.length*2.5-2,Math.max(txt.length*2.5+2,hero?hx:fx)));
      g.strokeText(txt,x,y);g.fillStyle=incoming?'#ffb6a2':k==='heal'?'#caffac':cream;g.fillText(txt,x,y);g.restore();
    }
  }
  // Lanes stay fixed for a whole wave, including the gaps left by defeated enemies.
  const formations=[
    null,[[.69,.855,1.35]],
    [[.59,.877,1.22],[.83,.77,1.1]],
    [[.53,.878,1.14],[.715,.746,1.02],[.885,.883,1.16]],
    [[.50,.885,1.08],[.62,.746,.99],[.755,.89,1.1],[.89,.738,.99]],
    [[.45,.896,1.02],[.53,.731,.94],[.672,.906,1.10],[.77,.731,.95],[.906,.877,1.03]]
  ];
  const stature={octopus:38,ray:42,ram:34,owl:47,pangolin:30,hare:50,jellyfish:44,dragonfly:50,elk:54,griffin:49,mole:25,dragon:51,mantis:41,turtle:28,scorpion:36,stag:53,snail:32,fox:32,spider:31,axolotl:29,
    beetle:27,boar:32,wolf:33,frog:26,moth:50,bat:43,bird:47,serpent:28,golem:40,wisp:41,crab:25,shroom:33};
  const torsos={octopus:22,ray:20,ram:20,owl:23,pangolin:16,hare:17,jellyfish:29,dragonfly:20,elk:24,griffin:23,mole:15,dragon:23,mantis:22,turtle:13,scorpion:13,stag:22,snail:16,fox:18,spider:16,axolotl:13,
    beetle:15,boar:18,wolf:18,frog:13,moth:21,bat:24,bird:24,serpent:16,golem:24,wisp:21,crab:14,shroom:21};
  function location(e,count,w,h,t,entry,hit,recoil){
    const lane=Math.max(0,Math.min(count-1,e.lane||0)),[ax,ay,scale]=formations[count][lane];
    const compact=count>1?Math.min(1,Math.max(.65,(w+25)/260)):1;
    const size=scale*compact*(e.boss?1.18:1),phase=t+lane*.71,near=e.attackProgress>.77;
    let x=w*ax+(sin(phase*1.5)*2.5+sin(phase*.65)*2)*(count===1?1.7:.65);
    if(t>0&&entry<1){const u=Math.max(0,Math.min(1,1-(entry-.055*lane)/.71));x+=(w*(1-ax)+28)*u*u;}
    if(near)x+=R((e.attackProgress-.77)*13);
    if(hit)x+=sin((hit.age-.16)/.29*Math.PI)*3;
    if(recoil)x-=Math.max(0,1-recoil.age/.3)*6;
    const y=R(h*ay),airborne=['bat','bird','moth','wisp','ray','owl','dragonfly','jellyfish'].includes(e.species);
    const hop=airborne?R(sin(phase*3)*2):e.species==='frog'?R(Math.max(0,sin(phase*5))*4):R(Math.abs(sin(phase*5)));
    const bodyY=y-hop,head=e.boss?Math.min(63,Math.max(51,(stature[e.species]||34)+11)):(stature[e.species]||32);
    return {x:R(x),y,size,bodyY,center:R(bodyY-(torsos[e.species]||18)*size),barY:R(bodyY-head*size-9),e};
  }
  const affinitySigils={
    leaf:['00111','01110','11110','11100','10000'],fire:['00100','00110','10110','11111','01110'],
    frost:['10101','01110','11111','01110','10101'],storm:['00111','01110','11111','00110','01100'],
    stone:['01110','11111','11111','11111','01110'],spirit:['00100','01110','11011','01110','00100']
  };
  function foeGauge(g,z,compact){
    const e=z.e;if(!e.maxHp)return;
    const w=compact?21:27,y=z.barY,x=R(z.x-w/2),hp=Math.max(0,Math.min(1,e.hp/e.maxHp)),charge=Math.max(0,Math.min(1,e.attackProgress||0)),urgent=charge>=.79&&!(e.stun>0);
    rect(g,'#282b2e',x-1,y-1,w+2,8);rect(g,'#51654e',x+6,y,w-7,2);rect(g,'#594744',x+6,y+4,w-7,1);
    if(hp>0)rect(g,hp<.3?'#f29579':'#aaed9c',x+6,y,Math.max(1,(w-7)*hp),2);
    if(charge>.01)rect(g,e.stun>0||e.chill>0?'#93e6ff':urgent?'#ff8d70':'#edc875',x+6,y+4,Math.max(1,(w-7)*charge),1);
    const sigil=affinitySigils[e.weak],c=elements[e.weak]||'#eed58c';
    if(sigil)sigil.forEach((row,j)=>{for(let i=0;i<row.length;i++)if(row[i]==='1')rect(g,c,x+i,y+j,1,1);});
    if(urgent){
      rect(g,'#4e393c',x+w-2,y-7,5,7);rect(g,'#ffd099',x+w,y-6,1,3);rect(g,'#ffd099',x+w,y-2,1,1);
    }else if(e.stun>0){
      line(g,'#fff1a8',x+w-2,y-3,x+w+2,y-3);line(g,'#fff1a8',x+w,y-5,x+w,y-1);
    }
  }

  const cameras=new WeakMap();
  function regionIndex(index){
    const n=Math.min(palettes.length,window.Catalog?.regions?.length||palettes.length);
    return ((floor(index||0)%n)+n)%n;
  }
  function draw(canvas,view){
    const v=view||{},idx=regionIndex(v.region);
    const width=canvas.clientWidth||544,height=canvas.clientHeight||width/2.5,ratio=width/height;
    // Keep the pixel size and actors proportional, with a tighter camera on square or portrait displays.
    const W=Math.min(690,Math.max(165,R(ratio*236))),h=Math.max(1,R(W/ratio));
    const t=v.time||0,d=v.distance||0,p=palettes[idx];
    if(canvas.width!==W||canvas.height!==h){canvas.width=W;canvas.height=h;}
    if(!cache||cache.w!==W||cache.h!==h||cache.region!==idx){
      previousScene=cache&&cache.w===W&&cache.h===h&&cache.region!==idx&&t>0?{art:cache,since:t}:null;
      cache=buildScene(W,h,idx);
    }
    const g=canvas.getContext('2d');g.imageSmoothingEnabled=false;
    landscape(g,cache,d);
    let blend=0;
    if(previousScene){
      const age=t-previousScene.since;
      if(age>=0&&age<.62){blend=1-age/.62;g.save();g.globalAlpha=blend;landscape(g,previousScene.art,d);g.restore();}
      else previousScene=null;
    }
    for(let i=0;i<15;i++){
      let xx=((i*137+sin(i*9+t*.45)*5+t*(idx===2?5:1.75)-d*.4)%(W+14)+W+14)%(W+14)-7;
      let yy;
      if(idx===2) yy=(9+i*23+t*9)%(cache.roadY-5);
      else yy=h*.27+((i*19)%R(h*.47))+sin(t*.7+i)*4;
      if(sin(i*9+t*.73)>.10)rect(g,p.light,xx,yy,i%4===0?2:1,1);
    }
    const effects=v.effects||[];
    const foes=(v.phase==='fight'||v.phase==='dead')?(v.enemies||[v.enemy].filter(Boolean)):[];
    const ex=R(W*(v.enemyX??.69)),ground=R(h*.85);
    let camera=cameras.get(canvas);
    if(!camera||t<camera.clock||v.round<camera.round){
      camera={clock:t,round:v.round,count:1,entry:-10,hadFoes:foes.length>0,slots:new Map(),loot:new WeakMap(),
        hero:(v.heroX??.3)-(foes.length>1?.085:0),last:null};
      cameras.set(canvas,camera);
    }
    if(camera.round!==v.round){camera.round=v.round;camera.count=1;camera.hadFoes=false;}
    if(!camera.hadFoes&&foes.length)camera.entry=t;
    camera.hadFoes=foes.length>0;
    camera.count=Math.max(camera.count,Math.min(5,v.waveSize||0),Math.min(5,foes.length),...foes.map(e=>Math.min(5,1+(e.lane||0))));
    const count=camera.count,entry=foes.length?(v.encounterAge??(t-camera.entry)):1;
    const targetFraction=(v.heroX??.30)-(foes.length>0&&count>1?.085:0);
    camera.hero+=(targetFraction-camera.hero)*(t===0?1:Math.min(1,Math.max(0,t-camera.clock)*6));
    camera.clock=t;
    const attack=effects.find(a=>a.kind==='attack'&&a.age<.21),impact=effects.find(a=>a.kind==='enemy'&&a.age>.15&&a.age<.43);
    const heroX=R(W*camera.hero)+(attack?R(sin(attack.age/.21*Math.PI)*3):0)-(impact?2:0)+R(sin(t*.96)*2+sin(t*.37)*3);
    const k=count>2?1.25:1.38;
    const live=foes.map((e,index)=>{
      const uid=e.uid??(v.round+'-'+index),actor=e.lane===undefined?{...e,lane:index}:e;
      const hits=effects.find(a=>a.kind==='attack'&&a.age>.16&&a.age<.45&&(a.targetUid===uid||(foes.length===1&&!a.targetUid)));
      const swing=effects.find(a=>a.kind==='enemy'&&a.age<.30&&(a.sourceUid===uid||(foes.length===1&&!a.sourceUid)));
      const z=location(actor,count,W,h,t,entry,hits,swing);
      camera.slots.set(uid,z);camera.last=z;return z;
    });
    while(camera.slots.size>36)camera.slots.delete(camera.slots.keys().next().value);
    const running=v.phase==='walk'||v.phase==='fight',stride=attack&&running?'cast':running?'walk':v.phase;
    if(running&&t>0){
      for(let i=0;i<3;i++){const a=(t*1.9+i/3)%1;if(a>.76)continue;
        rect(g,a<.35?p.road:p.roadHi,heroX-8-a*20,ground+1-sin(a*Math.PI)*5,2,1);
      }
    }
    oval(g,mix(p.road,p.earth,.42),heroX-1,ground+1,15,3);oval(g,mix(p.road,p.earth,.26),heroX-1,ground+2,19,2);
    if(v.phase==='dead'){
      g.save();g.translate(heroX,ground-5);g.rotate(-Math.PI/2);place(g,'monkey',null,t,'dead',false,-17,3,k);g.restore();
    }else place(g,'monkey',null,t,stride||'walk',false,heroX,ground,k);
    if(v.heldId&&v.phase!=='dead'&&window.ItemArt?.sprite){
      const sprite=window.ItemArt.sprite(v.heldId),item=window.Catalog?.items[v.heldId];
      if(sprite&&item){
        const long=['staff','torch','dart','needle','pick','sling','feather','fang'].includes(item.icon);
        const frame=(floor(t*(running?9:6))%8)*Math.PI/4;
        const swing=stride==='cast'?4:running?R(sin(frame)*4):0;
        const bob=running?Math.abs(R(sin(frame)*1.65)):0;
        const handX=R(heroX+(8+swing)*k),handY=R(ground-(15+bob)*k);
        const size=long?25:16;
        const left=handX-(long?5:3),top=handY-(long?20:10);
        relicGlow(g,item.rarity,left+size*.5,top+size*.5,size,v.reducedMotion?0:t);
        g.drawImage(sprite,left,top,size,size);
        rect(g,'#533b36',handX-1,handY+1,3,2);rect(g,'#f4ba7b',handX-2,handY-1,4,2);
      }
    }
    const killed=effects.filter(ev=>ev.kind==='kill'&&ev.age<.6),ghosts=[];
    const fallenWaveSize=Math.min(5,Math.max(count,...killed.map(ev=>(ev.lane??ev.enemy?.lane??0)+1)));
    for(const ev of killed){
      const e=ev.enemy||v.defeated||camera.last?.e;if(!e)continue;
      const uid=ev.targetUid??e.uid;
      const saved=camera.slots.get(uid);
      const z=saved||location(e,fallenWaveSize,W,h,t,1);
      if(!saved&&uid!==undefined)camera.slots.set(uid,z);
      ghosts.push({...z,e,age:ev.age});camera.last=z;
    }
    // Depth sort keeps rear ranks behind the nearer bodies; gauges are drawn in a separate pass.
    const bodies=[...ghosts,...live].sort((a,b)=>a.y-b.y);
    for(const z of bodies){
      const e=z.e,x=z.x,y=z.y,phase=t+(e.lane||0)*.71;
      if(z.age!==undefined){
        const u=z.age/.6;g.save();g.globalAlpha=1-u;g.translate(R(x+u*12),y);g.rotate(u*.65);
        place(g,e.species,e.color,phase,'enemy',e.boss,0,R(u*2),z.size);g.restore();
        if(u<.85)burst(g,p.light,x,z.center,5+u*14,7,u);
        continue;
      }
      oval(g,mix(p.road,p.earth,.38),x,y+1,Math.max(8,13*z.size),2);
      if(e.attackProgress>.79&&!(e.stun>0))orbit(g,'#c36a50',x,y+1,12*z.size+(e.attackProgress-.79)*12,.18,0);
      place(g,e.species,e.color,phase,'enemy',e.boss,x,z.bodyY,z.size);
    }
    scroll(g,cache.front,d*1.13);
    if(blend){g.save();g.globalAlpha=blend;scroll(g,previousScene.art.front,d*1.13);g.restore();}
    const fallback=live[0]||camera.last||{x:ex,y:ground,center:ground-21,barY:ground-46};
    // Preserve every individual drop in a crowded frame, even when later attacks have filled the FX budget.
    const visibleEffects=effects.length>62?effects.filter((ev,i)=>ev.kind==='loot'||i>=effects.length-62):effects;
    const labelCounts=new Map(),labels=new Set();
    for(let i=visibleEffects.length-1;i>=0;i--){
      const ev=visibleEffects[i];if(!(ev.age>.16&&ev.label&&['attack','hit','enemy','heal'].includes(ev.kind)))continue;
      const key=ev.kind==='enemy'||ev.kind==='heal'?'pip':ev.targetUid??'foe',n=labelCounts.get(key)||0;
      if(n<(count>2?1:2)){labels.add(i);labelCounts.set(key,n+1);}
    }
    let firstBolt=null;
    visibleEffects.forEach((ev,i)=>{
      const uid=ev.kind==='enemy'||ev.kind==='heal'?(ev.sourceUid??ev.targetUid):ev.targetUid;
      let z=camera.slots.get(uid);
      if(!z&&(ev.lane!==undefined||ev.enemyId!==undefined)){
        const match=f=>(ev.lane===undefined||ev.lane===f.e.lane)&&(ev.enemyId===undefined||ev.enemyId===f.e.id);
        z=ghosts.find(match)||live.find(match);
      }
      if(ev.kind==='loot'){
        z=camera.loot.get(ev)||z||camera.last||fallback;
        camera.loot.set(ev,z);
      }else z=z||fallback;
      const fork=ev.kind==='ability'&&ev.pattern==='forked_sky';
      const chain=fork&&firstBolt&&Math.abs(firstBolt.age-ev.age)<.002&&firstBolt.z!==z?firstBolt.z:null;
      cast(g,ev,heroX,ground,z,i,labels.has(i),chain,v.reducedMotion);
      if(fork)firstBolt={age:ev.age,z};
    });
    for(const z of live)foeGauge(g,z,count>2||W<210);

  }
  function portrait(canvas,species,color,boss=false){
    canvas.width=64;canvas.height=64;
    const g=canvas.getContext('2d');g.clearRect(0,0,64,64);g.imageSmoothingEnabled=false;
    place(g,species,color,0,'enemy',boss,33,boss?59:54,boss?.93:1);
  }
  function regionPortrait(canvas,index){
    canvas.width=368;canvas.height=136;
    const scene=buildScene(canvas.width,canvas.height,regionIndex(index)),g=canvas.getContext('2d');
    g.imageSmoothingEnabled=false;landscape(g,scene,12);scroll(g,scene.front,14);
  }
  window.WorldArt={draw,portrait,regionPortrait};
})();
