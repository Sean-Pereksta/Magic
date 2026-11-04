// Courtyard landmarks are built once from the same footprints used by navigation.
const stone=[.61,.60,.53], coping=[.66,.64,.56], shadow=[.16,.18,.18], wood=[.41,.30,.19], plaster=[.70,.65,.52], slate=[.33,.36,.39], tile=[.49,.29,.21];

export function drawCastleItems(castle,place,height,heraldry){
  if(!castle)return;
  const items=castle.items||[];
  const list=castle.keep&&!items.some(i=>i.kind===castle.keep.kind)?[...items,castle.keep]:items;
  for(const item of list){
    const w=item.width||1,d=item.depth||1,h=item.height||2,y=height(item.x,item.z);
    const part=(mesh,x,up,z,s,tint,r=[0,0,0])=>place(mesh,[item.x+x,y+up,item.z+z],s,tint,r);
    const box=(x,up,z,s,tint,mesh='box')=>part(mesh,x,up,z,s,tint);
    const roof=(x,up,z,rw,rh,rd,tint)=>part('roof',x,up,z,[rw/2,rh,rd/2],tint);
    function tower(x,z,tw,td,th,crowned=false){
      const cap=crowned?th*.27:Math.min(1.1,th*.14),shaft=th-cap,low=0;
      box(x,low,z,[tw*.92,shaft,td*.92],stone,'masonry');
      box(x,low+shaft-.58,z,[tw,.58,td],coping,'masonry');
      if(crowned){
        part('spire',x,shaft,z,[tw*.56,cap,td*.56],slate);
        box(x,shaft*.71,z-td*.467,[tw*.22,shaft*.23,.12],heraldry);
      }else for(const a of [-1,0,1])for(const side of [-1,1]){
        box(x+a*tw*.39,low+shaft,z+side*td*.43,[tw*.19,cap,td*.14],coping);
        if(!a)box(x+side*tw*.43,low+shaft,z,[tw*.14,cap,td*.22],coping);
      }
      for(const a of [-1,1])box(x+a*tw*.23,low+shaft*.46,z-td*.465,[.14,Math.min(1.1,th*.18),.08],shadow);
    }
    switch(item.kind){
      case 'tower': tower(0,0,w,d,h);break;
      case 'keep':{
        const hall=h*.65;
        box(0,0,0,[w*.92,hall,d*.92],stone,'masonry');
        box(0,hall-.55,0,[w,.55,d],coping,'masonry');
        roof(0,hall,0,w*.75,h*.23,d*.85,slate);
        for(const side of [-1,1]){
          tower(side*w*.36,-d*.35,w*.28,d*.29,h);
          tower(side*w*.36,d*.35,w*.28,d*.29,h,castle.tier===4);
        }
        box(0,0,-d*.465,[w*.21,hall*.50,.1],wood);
        for(const side of [-1,1])box(side*w*.20,hall*.57,-d*.467,[w*.09,h*.12,.12],heraldry);
        break;
      }
      case 'banner':
        part('shaft',0,0,0,[2.3,h,2.3],wood);
        part('banner',0,h,0,[1.65,2.3,1],heraldry);break;
      case 'well':
        part('shaft',0,0,0,[w*19,h*.46,d*19],coping);
        for(const s of [-1,1])box(s*w*.34,h*.45,0,[.16,h*.44,.16],wood);
        roof(0,h*.87,0,w,h*.14,d,tile);break;
      case 'cart':
        for(const s of [-1,1])part('wheel',s*w*.40,h*.26,0,[h*.62,1,h*.62],wood,[0,0,Math.PI/2]);
        box(0,h*.26,0,[w*.75,h*.22,d],wood);
        for(const s of [-1,1])box(s*w*.34,h*.48,0,[.12,h*.45,d],wood);
        break;
      case 'market':
        for(const s of [-1,1])for(const t of [-1,1])box(s*w*.42,0,t*d*.42,[.15,h*.81,.15],wood);
        box(0,h*.32,0,[w*.9,h*.13,d*.76],wood);
        for(let i=0;i<5;i++)box((i-2)*w/5,h*.8,0,[w/5,h*.14,d],i%2?plaster:heraldry);
        break;
      default:{
        const stable=item.kind==='stable',chapel=item.kind==='chapel',hall=h*(chapel?.55:.66);
        box(0,0,0,[w*.93,hall,d*.93],chapel?stone:stable?wood:plaster,chapel?'masonry':'box');
        roof(0,hall,0,w,h*(chapel?.24:.38),d,chapel?slate:tile);
        for(const s of [-1,1]){
          box(s*w*.42,0,-d*.47,[.17,hall,.12],wood);
          box(s*w*.28,hall*.39,-d*.47,[w*.12,hall*.31,.09],shadow);
        }
        box(0,0,-d*.47,[w*(stable?.42:.17),hall*.72,.1],wood);
        if(chapel){
          box(0,hall,-d*.25,[w*.28,h*.28,d*.25],coping,'masonry');
          box(0,h*.70,-d*.378,[w*.13,h*.10,.10],shadow);
          roof(0,h*.83,-d*.25,w*.33,h*.17,d*.31,slate);
        }
      }
    }
  }
}
