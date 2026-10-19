/* Original athlete headshots. Stable identity matching never depends on progression or roster status. */
(function(root,factory){
  'use strict';
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  if(root){root.RisePortraits=api;if(root.document)api.bindFallbacks(root.document);}
})(typeof globalThis!=='undefined'?globalThis:this,function(){
  'use strict';
  const families=[
    [0x3d2418],[0x5d3425],[0x704327],[0x8f5737],[0xc8845b],[0xe1aa7a,0xd49a6a]
  ];
  const definitions=[
    [0,0x3d2418,['braids'],'lean'],[0,0x3d2418,['buzz'],'broad'],
    [1,0x5d3425,['shortFade'],'athletic'],[1,0x5d3425,['afro'],'lean'],
    [2,0x704327,['curlyTop'],'athletic'],[2,0x704327,['braids'],'broad'],
    [3,0x8f5737,['shortFade'],'lean'],[3,0x8f5737,['buzz'],'broad'],
    [4,0xc8845b,['curlyTop'],'athletic'],[4,0xc8845b,['shortFade'],'broad'],
    [5,0xe1aa7a,['spiky'],'lean'],[5,0xd49a6a,['shortFade','straight'],'athletic'],
    [0,0x3d2418,['afro'],'athletic'],[0,0x3d2418,['shortFade'],'athletic'],
    [1,0x5d3425,['braids'],'broad'],[2,0x704327,['buzz'],'lean'],
    [2,0x704327,['afro'],'lean'],[3,0x8f5737,['braids'],'athletic'],
    [4,0xc8845b,['buzz'],'lean'],[4,0xc8845b,['afro'],'broad'],
    [5,0xe1aa7a,['buzz'],'broad'],[5,0xd49a6a,['curlyTop'],'athletic']
  ];
  const catalog=Object.freeze(definitions.map(([family,skin,hairStyles,build],i)=>{
    const id='player-'+String(i+1).padStart(2,'0');
    return Object.freeze({id,src:'assets/campaign/portraits/'+id+'.webp',family,skin,hairStyles:Object.freeze(hairStyles),build,ageRange:Object.freeze([20,30])});
  }));
  function hash(value){
    let n=2166136261;for(const character of String(value)){n^=character.charCodeAt(0);n=Math.imul(n,16777619);}
    return n>>>0;
  }
  function skinValue(value){
    if(typeof value==='number'&&Number.isFinite(value))return Math.max(0,Math.min(0xffffff,Math.round(value)));
    if(typeof value==='string'&&/^#[0-9a-f]{6}$/i.test(value))return parseInt(value.slice(1),16);
    return null;
  }
  function skinDistance(a,b){
    const red=(a>>16)-(b>>16),green=(a>>8&255)-(b>>8&255),blue=(a&255)-(b&255);
    return red*red*.3+green*green*.59+blue*blue*.11;
  }
  function hairDistance(wanted,styles){
    if(!wanted)return .5;
    if(styles.includes(wanted))return 0;
    if(wanted==='headband')return styles.includes('buzz')?.2:styles.includes('shortFade')?.3:.8;
    const curls=['afro','curlyTop'],short=['shortFade','buzz','spiky','straight'];
    if(curls.includes(wanted)&&styles.some(style=>curls.includes(style)))return .25;
    if(short.includes(wanted)&&styles.some(style=>short.includes(style)))return .4;
    return 1;
  }
  function buildValue(player){
    const height=Number(player.physical?.heightCm),weight=Number(player.physical?.weightKg);
    if(!Number.isFinite(height)||!Number.isFinite(weight)||height<140||weight<40)return 'athletic';
    const mass=weight/Math.pow(height/100,2);return mass<23?'lean':mass>27?'broad':'athletic';
  }
  function signature(player={}){
    player=player&&typeof player==='object'?player:{};
    return [skinValue(player.appearance?.skin)??'',player.appearance?.hairStyle||'',buildValue(player)].join('|');
  }
  function get(id){return catalog.find(portrait=>portrait.id===id)||null;}
  function select(player={},options={}){
    player=player&&typeof player==='object'?player:{};
    const assigned=!options.legacy&&get(player.appearance?.portraitId);
    const identity=String(player.id??'unknown-athlete'),skin=skinValue(player.appearance?.skin),hair=player.appearance?.hairStyle,build=buildValue(player);
    let family=hash(identity)%families.length;
    if(skin!==null){
      let closest=Infinity;families.forEach((tones,i)=>{const distance=Math.min(...tones.map(tone=>skinDistance(skin,tone)));if(distance<closest){closest=distance;family=i;}});
    }
    if(assigned&&assigned.family===family&&player.appearance.portraitSignature===signature(player))return assigned;
    // Every available portrait depicts an adult. Age stays out of the tie-breaker so season birthdays keep the same face.
    const score=portrait=>hairDistance(hair,portrait.hairStyles)*4+(portrait.build===build?0:.4)+hash(identity+'|'+portrait.id)/4294967295*.3;
    const pool=options.legacy?catalog.slice(0,12):catalog;
    return pool.filter(portrait=>portrait.family===family).sort((a,b)=>score(a)-score(b))[0];
  }
  function assignment(player,options={}){
    return {portraitId:select(player,options).id,portraitSignature:signature(player)};
  }
  const boundDocuments=new WeakSet();
  function bindFallbacks(document){
    if(!document||typeof document.addEventListener!=='function'||boundDocuments.has(document))return;
    boundDocuments.add(document);
    const handle=event=>{
      const image=event.target;
      if(!image?.classList?.contains('rise-headshot-image'))return;
      const wrapper=image.closest?.('.rise-headshot');if(!wrapper)return;
      const loaded=event.type==='load'&&image.naturalWidth>0;
      image.hidden=!loaded;wrapper.classList.toggle('is-loaded',loaded);wrapper.classList.toggle('is-fallback',!loaded);
    };
    document.addEventListener('load',handle,true);document.addEventListener('error',handle,true);
  }
  return Object.freeze({catalog,get,select,signature,assignment,bindFallbacks});
});
