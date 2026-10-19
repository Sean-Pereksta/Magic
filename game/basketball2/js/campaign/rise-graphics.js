/* Original Rise to Glory vector marks. No external fonts, requests, or SVG IDs. */
(function(root,factory){
  'use strict';
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  if(root)root.RiseGraphics=api;
})(typeof globalThis!=='undefined'?globalThis:this,function(){
  'use strict';
  const accent=content=>`<g class="rise-graphic-accent" style="stroke:var(--rise-graphic-accent,#f2b19a)">${content}</g>`;
  const secondary=content=>`<g class="rise-graphic-secondary" style="stroke:var(--rise-graphic-secondary,#6aa8e8)">${content}</g>`;
  const ball=(x,y,r=5)=>`<circle cx="${x}" cy="${y}" r="${r}"/><path d="M${x-r} ${y}h${r*2}M${x} ${y-r}v${r*2}" stroke-width="1.2"/>`;
  const hoop='<path d="M30 10h11v12M28 22h16m-14 0 2 10h7l3-10m-11 4h10m-8-4 2 10m4-10-2 10"/>';
  const star='<path d="m24 8 4.5 9.5 10.5 1.4-7.6 7.4 1.8 10.5L24 32l-9.2 4.8 1.8-10.5L9 18.9l10.5-1.4Z"/>';
  const icons={
    hub:'<path d="m7 22 17-14 17 14M11 20v21h26V20M20 41V29h8v12"/>'+accent(ball(24,19,4)),
    roster:'<path d="M5 41v-5c0-5 5-8 11-8s11 3 11 8v5M31 29c7 0 12 3 12 8v4"/><circle cx="16" cy="17" r="7"/>'+accent('<path d="M29 10a7 7 0 1 1 0 14M10 41h12M34 41h5"/>'),
    develop:'<path d="M9 39h30M12 34V23h7v11m5 0V18h7v16m5 0V10"/>'+accent('<path d="m10 17 12-8 8 3 9-7m-7 0h7v7"/>'),
    market:'<circle cx="21" cy="21" r="13"/><path d="m31 31 11 11"/>'+accent('<path d="m18 13-5 3v11l8 5 8-5V16l-5-3M18 14l3 5 3-5M21 24v4"/>'),
    schedule:'<path d="M9 6v8m30-8v8M6 19h36M10 10h28a4 4 0 0 1 4 4v25a4 4 0 0 1-4 4H10a4 4 0 0 1-4-4V14a4 4 0 0 1 4-4"/>'+accent('<path d="m14 32 6 6 14-13"/>'),
    strategy:'<rect x="5" y="7" width="38" height="34" rx="4"/><path d="M20 41V29h8v12M19 15l7 7m0-7-7 7"/>'+accent('<circle cx="12" cy="32" r="3"/><circle cx="36" cy="16" r="3"/><path d="M15 30 32 19m-7 1 7-1-3 7"/>'),
    training:'<path d="M9 15v18m6-23v28M33 10v28m6-23v18M4 20v8m40-8v8M15 24h18"/>'+accent('<path d="m20 14 4-5 4 5m-8 20 4 5 4-5"/>'),
    stats:'<path d="M7 8v33h35M13 33V23h6v10m5 0V15h6v18m5 0V9h6v24"/>'+accent('<path d="m13 16 8-7 7 1 10-6"/>'),
    basketball:'<circle cx="24" cy="24" r="18"/><path d="M6 24h36M24 6v36M11 11c15 6 15 20 0 26m26-26c-15 6-15 20 0 26"/>',
    trophy:'<path d="M14 8h20v13a10 10 0 0 1-20 0V8ZM14 12H6v6c0 7 6 10 11 10m17-16h8v6c0 7-6 10-11 10M24 31v8m-9 3h18"/>'+accent('<path d="m20 17 4 5 4-5"/>'),
    credits:'<ellipse cx="24" cy="12" rx="16" ry="6"/><path d="M8 12v8c0 3 7 6 16 6s16-3 16-6v-8M8 23v8c0 3 7 6 16 6s16-3 16-6v-8M8 34v3c0 3 7 6 16 6s16-3 16-6v-3"/>'+accent('<path d="M20 10h8m-4-2v8"/>'),
    xp:'<path d="m24 4 5.5 12 13 1.5L33 27l2.5 14L24 34l-11.5 7L15 27l-9.5-9.5L18.5 16Z"/>'+accent('<path d="M24 16v13m-6-6h12"/>'),
    chemistry:'<circle cx="24" cy="10" r="5"/><circle cx="10" cy="35" r="5"/><circle cx="38" cy="35" r="5"/><path d="m20 14-7 16m15-16 7 16M15 35h18"/>'+accent('<path d="m21 26 3 3 5-7"/>'),
    whistle:'<path d="M31 9h11v16H29a12 12 0 1 1-6-10h8V9Z"/><circle cx="16" cy="27" r="5"/>'+accent('<path d="m6 8 5 5m7-9v7m9-6-3 6"/>'),
    lock:'<rect x="10" y="22" width="28" height="21" rx="4"/><path d="M16 22V13a8 8 0 0 1 16 0v9"/>'+accent('<circle cx="24" cy="31" r="2"/><path d="M24 33v4"/>'),
    victory:accent('<path d="m13 25 8 8 17-19"/>')+'<circle cx="24" cy="24" r="19"/>',
    arrow:'<path d="M6 24h34m-13-13 13 13-13 13"/>'
  };
  const abilities={
    powerFinisher:'<path d="M30 8h12v12M28 20h16m-13 0 2 8h6l3-8M9 31l8-10 7 5 8-7M17 21l-4-8m9 13 1 15m-8-12L7 41"/>'+accent(ball(24,13,4)),
    highFlyer:'<path d="M24 25 11 17 4 19l10 11 10 5 10-5 10-11-7-2-13 8M24 35v8"/>'+accent('<path d="m19 14 5-8 5 8m-5-8v17"/>'),
    contactSpecialist:'<path d="M24 5 40 11v13c0 10-16 19-16 19S8 34 8 24V11Z"/>'+accent('<path d="m13 19 7 5-7 5m22-10-7 5 7 5M24 16v16"/>'),
    aerialAcrobat:'<path d="M8 35c-6-13 5-26 18-25m10 4c11 9 7 25-5 29M8 35l-1-8m1 8 8-2M31 43l8-1m-8 1 3-8"/><circle cx="21" cy="18" r="3"/>'+accent('<path d="m23 22 7 6-7 7m7-7 6-6m-13 0-8 5"/>'),
    posterizer:hoop+'<circle cx="16" cy="18" r="3"/><path d="m16 23 5 8-4 11m4-11 7 8M18 25l9-10m-5 16-10-2"/>'+accent(ball(30,12,4)+'<path d="m8 8-3-3m11 3V3M6 15H2"/>'),
    midrangeAssassin:'<path d="M8 41V10h32v31M17 41V29h14v12M16 24a10 10 0 0 1 16 0"/>'+accent('<circle cx="24" cy="20" r="4"/><path d="M24 11v3m-9 6h3m12 0h3M24 26v3"/>'),
    deadeye:'<path d="M4 24s7-13 20-13 20 13 20 13-7 13-20 13S4 24 4 24Z"/><circle cx="24" cy="24" r="8"/>'+accent('<path d="M24 5v10m0 18v10M5 24h10m18 0h10"/><circle cx="24" cy="24" r="2"/>'),
    catchAndShoot:'<path d="M5 31h12l6-8M5 37h16l8-10"/>'+secondary('<path d="m10 14 8-6 8 6M18 8v15"/>')+accent(ball(32,16,6)+'<path d="m30 6 2-3 2 3M38 23l4 4m-4 0 4-4"/>'),
    pullUpSpecialist:'<path d="M6 37h18m-6-6 6 6-6 6M26 30V16m-5 6 5-6 5 6M33 6h9v13M31 19h13"/>'+accent(ball(17,18,5)+'<path d="M8 29h4m-7-6h4"/>'),
    stepBackCreator:'<circle cx="25" cy="11" r="4"/><path d="m23 17-4 10 10 7 8 1m-18-8-4 13m8-23 7 6"/>'+accent(ball(35,23,5)+'<path d="M18 18H5m6-6-6 6 6 6"/>'),
    cornerSpecialist:'<path d="M8 7v34h34M8 14h10v27M18 17c11 2 18 11 18 24"/>'+accent(ball(33,14,6)+'<path d="m25 29 6-6m-7 0h7v7"/>'),
    deepRange:'<path d="M8 37a16 16 0 0 1 32 0M14 37a10 10 0 0 1 20 0M20 37a4 4 0 0 1 8 0M21 41h6"/>'+accent(ball(24,10,5)+'<path d="M24 18v7m-6-4 6 6 6-6"/>'),
    clutchShooter:'<circle cx="24" cy="26" r="17"/><path d="M19 4h10m-5 0v5m14 1 4 4M24 26V15"/>'+accent('<path d="m17 28 6 5 9-12M24 38v2"/>'),
    quickRelease:'<path d="M8 13h12M4 23h12M8 33h12"/>'+accent('<path d="m31 5-12 21h10l-4 17 18-25H31l5-13Z"/>'),
    floorGeneral:'<circle cx="9" cy="37" r="4"/><circle cx="39" cy="37" r="4"/><path d="M13 35 24 24l11 11M24 24v-7m-5 5 5-5 5 5"/>'+accent('<path d="m14 7 6 5 4-7 4 7 6-5-2 12H16Z"/>'),
    rimGuardian:hoop+'<path d="M16 19V9m-5 9V9m-5 12V12M6 21l3 10 9 8 7-4V22c0-3-5-3-5 0v4l-4-7"/>'+accent('<path d="m4 4 2-2m6 1V1m8 3 2-2"/>'),
    pickpocket:'<path d="M5 35h11l10-9 8 1c4 0 5 5 1 6l-9 3-8 7H5M7 28l13-9c4-2 7 2 4 5"/>'+accent(ball(34,12,7)+'<path d="m17 11 3 2m-4-9 4 1"/>'),
    screenSetter:'<circle cx="24" cy="11" r="4"/><path d="M24 16v13m-9-10h18M24 29l-8 13m8-13 8 13"/>'+accent('<path d="M7 29V13h7m-4-4 4 4-4 4M41 18v16h-7m4-4-4 4 4 4"/>')
  };
  const training={
    midrange:'<path d="M7 42V9h34v33M16 42V29h16v13M15 24a11 11 0 0 1 18 0"/>'+accent(ball(24,18,4)+'<path d="m6 24 4 1m28 0 4-1"/>'),
    threePoint:'<path d="M5 40h38M5 12v28m38-28v28M9 40a15 20 0 0 1 30 0M17 40V29h14v11"/>'+accent('<circle cx="7" cy="25" r="2"/><circle cx="14" cy="12" r="2"/><circle cx="24" cy="8" r="2"/><circle cx="34" cy="12" r="2"/><circle cx="41" cy="25" r="2"/>'),
    dunking:hoop+'<path d="M8 41c0-12 6-23 16-29m-8 1 8-1-1 8M15 42c0-6 3-12 7-16"/>'+accent(ball(21,7,4)),
    passing:'<circle cx="9" cy="31" r="5"/><circle cx="38" cy="13" r="5"/><path d="M4 43v-4c0-5 10-5 10 0v4m19-18v-4m10 4v-4"/>'+accent('<path d="m15 26 16-10m-7 0 7 0-2 7M15 32l17-11"/>'),
    dribbling:'<path d="M8 9c7 0 7 25 15 25s7-25 17-25M7 18h5m5 18h5m6-18h5"/>'+accent(ball(24,17,5)+'<path d="m6 38-3 6h8Zm31 0-3 6h8Z"/>'),
    defense:'<path d="M8 13h32M11 18l-6 7 8 10m24-17 6 7-8 10M10 43h28"/><circle cx="24" cy="10" r="4"/><path d="M24 16v13m-11-9 11-4 11 4m-11 9-9 11m9-11 9 11"/>'+accent('<path d="m5 7 5-4m28 0 5 4M18 43l-4-3m16 3 4-3"/>'),
    team:'<circle cx="24" cy="8" r="4"/><circle cx="7" cy="22" r="4"/><circle cx="41" cy="22" r="4"/><circle cx="14" cy="41" r="4"/><circle cx="34" cy="41" r="4"/>'+accent('<path d="m20 10-9 8m0 8 2 11m5 4h12m6-4 3-11m-2-8-9-8M16 23h15m-6-5 6 5-6 5"/>')
  };
  const categories={
    Shooting:'<path d="M29 7h12v12M27 19h17m-15 0 3 11h7l3-11M6 38c0-15 7-24 19-24m-5-4 5 4-5 4"/>'+accent(ball(15,31,6)),
    Finishing:abilities.powerFinisher,
    Athleticism:'<circle cx="29" cy="9" r="4"/><path d="m26 15-8 10 10 7-4 11m-6-18-9 8m13-14 10 6 8-5M11 12h9M5 20h10"/>'+accent('<path d="m7 40 5-1m-8-7h5"/>'),
    Playmaking:abilities.floorGeneral,
    Defense:abilities.contactSpecialist,
    Mental:'<path d="M17 37c-9-3-11-13-7-20-2-9 11-16 17-10 10-4 18 5 14 12 6 10-3 20-12 18M24 8v34m-8-27 8 6 9-7M13 29l11-3 10 6"/>'+accent('<circle cx="16" cy="15" r="2"/><circle cx="34" cy="14" r="2"/><circle cx="34" cy="32" r="2"/>')
  };
  const archetypes={
    playmaker:abilities.floorGeneral,
    sharpshooter:abilities.deepRange,
    midrange:abilities.midrangeAssassin,
    slasher:abilities.highFlyer,
    interior:abilities.powerFinisher,
    defender:abilities.pickpocket,
    rimProtector:abilities.rimGuardian,
    stretchBig:abilities.cornerSpecialist,
    allRounder:star+accent('<path d="m18 24 4 4 8-9"/>')
  };
  const divisionNames=['Street League','City League','Regional League','National League','Elite Championship'];
  const divisionIds=['street','city','regional','national','elite'];
  const divisionColors=['#6aa8e8','#71bec8','#b3abdf','#e8b897','#e6c976'];
  const divisionArt=[
    '<path d="M25 27h30v20M29 28v18m-4 0h34m-29 0 3 14h18l4-14m-23 6h21m-17-6 3 14m9-14-3 14M20 67h40M24 72h32"/>'+accent(ball(42,34,5)),
    '<path d="M20 65V37h11V25h18v14h12v26M20 65h42M36 32h3m4 0h2M36 41h3m4 0h2M25 44v3m0 8v3m29-12v3m0 7v3M36 63V51h10v12"/>'+accent('<path d="M40 17v8m-6-4h12M16 69h48"/>'),
    '<path d="m17 60 17-29 11 19 7-11 13 21ZM28 42l6 5 6-5M22 66h36"/>'+accent('<path d="m48 21 4-6 4 6-4 6Zm-31 8 4-3 4 3-4 3Z"/>'),
    '<path d="m40 36-10-7-14 1 10 14 14 15 14-15 10-14-14-1ZM40 59v9m-6 0h12M24 36l8 8m24-8-8 8"/>'+accent('<path d="m40 16 3 7 8 1-6 5 2 8-7-4-7 4 2-8-6-5 8-1Z"/>'),
    '<path d="M29 31h22v16c0 8-6 13-11 13s-11-5-11-13V31ZM29 35h-9v7c0 8 8 11 11 11m20-18h9v7c0 8-8 11-11 11M40 60v9m-10 1h20"/>'+accent('<path d="m27 18 8 5 5-10 5 10 8-5-3 10H30Z"/>')
  ];
  const escape=value=>String(value??'').replace(/[&<>"']/g,char=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
  const own=(map,key)=>Object.prototype.hasOwnProperty.call(map,key);
  const human=value=>String(value).replace(/([a-z])([A-Z])/g,'$1 $2').replace(/\b\w/g,char=>char.toUpperCase());
  function svg(kind,key,content,options={},viewBox='0 0 48 48',defaultLabel=human(key)){
    const opts=options||{},named=opts.label!==undefined&&opts.label!==null;
    const accessible=named||opts.decorative===false,label=named?String(opts.label):defaultLabel;
    const extra=String(opts.className||'').split(/\s+/).filter(name=>/^[a-zA-Z_][\w-]*$/.test(name)).join(' ');
    const size=Number(opts.size),dimensions=Number.isFinite(size)&&size>=8&&size<=320?` width="${size}" height="${size}"`:'';
    return `<svg xmlns="http://www.w3.org/2000/svg" class="rise-graphic rise-graphic-${kind} rise-graphic-${key}${extra?' '+extra:''}" viewBox="${viewBox}" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" focusable="false"${dimensions}${accessible?` role="img" aria-label="${escape(label)}"`:' aria-hidden="true"'}>${accessible?`<title>${escape(label)}</title>`:''}${content}</svg>`;
  }
  function mark(kind,map,key,options,fallback){
    const safe=own(map,key)?key:fallback;
    return svg(kind,safe,map[safe],options);
  }
  function division(value,options){
    const index=typeof value==='number'&&Number.isInteger(value)&&value>=0&&value<5?value:Math.max(0,divisionIds.indexOf(value));
    const shield='<path d="M40 4 73 15v31c0 19-22 32-33 38C29 78 7 65 7 46V15Z" fill="#132238" stroke-width="2"/>';
    const inner=`<path d="m40 10 27 9v27c0 15-17 26-27 32-10-6-27-17-27-32V19Z" stroke="${divisionColors[index]}" stroke-width="1" opacity=".65"/>`;
    const rank=Array.from({length:index+1},(_,i)=>`<path d="M${40-(index*3.5)+i*7} 74v-3"/>`).join('');
    return svg('division',divisionIds[index],shield+inner+`<g style="color:${divisionColors[index]}">${divisionArt[index]}${rank}</g>`,options,'0 0 80 88',divisionNames[index]+' crest');
  }
  function court(options){
    const lines='<rect x="6" y="6" width="148" height="114" rx="6" fill="#14273c"/><path d="M6 24h148M55 24v39h50V24M55 63a25 25 0 0 0 50 0M34 24v11a46 46 0 0 0 92 0V24M70 16h20m-10 0v6" opacity=".55"/><circle cx="80" cy="26" r="5"/><path d="M60 120a20 20 0 0 1 40 0" opacity=".55"/>';
    const routes=accent('<path d="M68 100 30 74m9 0-9 0 3 9M39 60l26-24m-8 0h8v8" stroke-dasharray="4 5"/>')+secondary('<path d="M92 100c27-1 44-17 40-38m-5 5 5-5 6 5" stroke-dasharray="4 5"/>');
    const spots=[[80,103,1],[27,65,2],[133,52,3]].map(([x,y,number])=>`<circle cx="${x}" cy="${y}" r="10" fill="#254764"/><text x="${x}" y="${y+3.5}" stroke="none" fill="#eef5ff" font-family="Arial,sans-serif" font-size="10" text-anchor="middle" font-weight="700">${number}</text>`).join('');
    return svg('court','halfcourt',lines+routes+spots,options,'0 0 160 128','Three-player halfcourt spacing and movement diagram');
  }
  const keys=Object.freeze({icons:Object.freeze(Object.keys(icons)),abilities:Object.freeze(Object.keys(abilities)),training:Object.freeze(Object.keys(training)),categories:Object.freeze(Object.keys(categories)),divisions:Object.freeze(divisionIds.slice()),archetypes:Object.freeze(Object.keys(archetypes))});
  return Object.freeze({
    icon:(key,opts)=>mark('icon',icons,key,opts,'basketball'),
    ability:(key,opts)=>own(abilities,key)?mark('ability',abilities,key,opts,'powerFinisher'):svg('ability','basketball',icons.basketball,opts),
    training:(key,opts)=>own(training,key)?mark('training',training,key,opts,'midrange'):svg('training','whistle',icons.whistle,opts),
    category:(key,opts)=>own(categories,key)?mark('category',categories,key,opts,'Shooting'):svg('category','basketball',icons.basketball,opts),
    archetype:(key,opts)=>mark('archetype',archetypes,key,opts,'allRounder'),
    division,court,keys
  });
});
