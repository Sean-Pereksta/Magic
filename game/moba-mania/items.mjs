// Permanent match upgrades. Buying a recipe never consumes its prerequisites:
// every purchased item remains active, and its listed cost is an additional cost.
export const ITEM_PASSIVES=Object.freeze({
  'sovereign-aegis':Object.freeze({name:'Shared Aegis',description:'Granting a meaningful shield also wards another nearby allied hero. Recharges in 8s.'}),
  'worldsplitter':Object.freeze({name:'Third Strike',description:'Every third basic hit cleaves other enemies in front of you for 45% attack damage.'}),
  'starfire-scepter':Object.freeze({name:'Falling Star',description:'A direct ability hit calls a small delayed blast at its victim. Recharges in 6s.'}),
  'eternity-hourglass':Object.freeze({name:'Time Sequence',description:'Cast three different abilities within 6s to remove 1.25s from other active cooldowns. Recharges in 12s.'}),
  'celestial-font':Object.freeze({name:'Overflow',description:'Every third cast restores 5.5% maximum mana and releases a small nearby healing pulse. Recharges in 5s.'}),
  'tempest-wings':Object.freeze({name:'Arrival Strike',description:'Movement abilities grant brief haste and empower your next basic hit within 4s.'}),
  'thunderbolt-crossbow':Object.freeze({name:'Chain Spark',description:'Every fourth basic hit arcs lightning to up to two other nearby enemies.'}),
  'phoenix-crown':Object.freeze({name:'Healing Ward',description:'A meaningful heal also grants its hero recipient a brief protective ward. Recharges in 5s.'}),
  'elderwood-staff':Object.freeze({name:'Elder Guardian',description:'The first summon of each cast gains 20% health, 15% damage, a ward and a larger guardian form.'}),
  'crimson-relic':Object.freeze({name:'Lasting Duel',description:'A basic hit on a hero while below half health restores 5.5% health and grants a brief ward. Recharges in 8s.'})
});
const item = (id,name,tier,cost,requires,bonuses,description,color,icon) => Object.freeze({
  id,name,tier,cost,requires:Object.freeze(requires),bonuses:Object.freeze(bonuses),description,color,icon,passive:ITEM_PASSIVES[id]||null,
});

export const ITEMS = Object.freeze([
  item('ruby-heart','Ruby Heart',1,100,[],{hp:.10},'A little more health for the next fight.','#ed7185','heart'),
  item('iron-edge','Iron Edge',1,100,[],{damage:.08},'Sharpen your weapon and damage-scaled skills.','#aabfd5','dagger'),
  item('spark-crystal','Spark Crystal',1,100,[],{spellDamage:.10},'Add force to your damaging abilities.','#a58cf0','crystal'),
  item('sand-clock','Sand Clock',1,120,[],{cooldown:.04},'Bring your abilities back sooner.','#e7bd71','hourglass'),
  item('blue-flask','Blue Flask',1,90,[],{mana:.12,manaRegen:.4},'Carry more mana and recover it faster.','#6bc7ee','flask'),
  item('swift-boots','Swift Boots',1,120,[],{speed:.04},'Move between fights a little faster.','#7bdcc4','boot'),
  item('feather-bow','Feather Bow',1,100,[],{attackSpeed:.08},'Release more basic attacks.','#efc378','bow'),
  item('sun-charm','Sun Charm',1,90,[],{healing:.10,shield:.06},'Strengthen your healing and shielding skills.','#f3d589','sun'),
  item('oak-idol','Oak Idol',1,90,[],{summonPower:.10},'Give your summoned allies more health and damage.','#8cce83','seed'),
  item('blood-fang','Blood Fang',1,130,[],{lifesteal:.025},'Basic-attack damage restores a little health.','#eb879f','fang'),

  item('guardian-plate','Guardian Plate',2,240,['ruby-heart'],{hp:.16,shield:.08},'A heavy breastplate for heroes who hold the line.','#b397e2','plate'),
  item('tempered-blade','Tempered Blade',2,240,['iron-edge'],{damage:.14},'A broad blade that makes every strike count.','#e4d5ad','blade'),
  item('storm-tome','Storm Tome',2,260,['spark-crystal'],{spellDamage:.17,area:.05},'Stronger spells with a wider reach around their impact.','#93aff4','tome'),
  item('chrono-dial','Chrono Dial',2,280,['sand-clock'],{cooldown:.07,manaRegen:.5},'Keep time, recover mana, and cast again sooner.','#81d7d8','dial'),
  item('moon-well','Moon Well',2,220,['blue-flask'],{mana:.18,manaRegen:1},'A deep reserve for sustained spellcasting.','#91c2f1','well'),
  item('wind-striders','Wind Striders',2,260,['swift-boots'],{speed:.06,attackSpeed:.06},'Light greaves for pursuit and quick attacks.','#b4e6ce','greaves'),
  item('falcon-gauntlet','Falcon Gauntlet',2,250,['feather-bow'],{attackSpeed:.14,damage:.04},'A swift grip that speeds and strengthens your blows.','#e4b87d','gauntlet'),
  item('dawn-censer','Dawn Censer',2,240,['sun-charm'],{healing:.18,shield:.12},'Carry a brighter blessing into the frontline.','#f0bfaa','censer'),
  item('grove-totem','Grove Totem',2,260,['oak-idol'],{summonPower:.18,summonHaste:.10},'Your summons grow stronger and strike faster.','#9ad28d','totem'),
  item('reaper-sickle','Reaper Sickle',2,300,['blood-fang'],{lifesteal:.04,damage:.08},'Turn sharper basic attacks into stolen health.','#e191b8','sickle'),

  item('sovereign-aegis','Sovereign Aegis',3,560,['guardian-plate','sun-charm'],{hp:.24,shield:.22},'A royal shield for a lasting frontline.','#d1b1f3','aegis'),
  item('worldsplitter','Worldsplitter',3,600,['tempered-blade','feather-bow'],{damage:.23,attackSpeed:.12},'A greatsword with the speed to match its weight.','#f6d18b','greatsword'),
  item('starfire-scepter','Starfire Scepter',3,610,['storm-tome','sand-clock'],{spellDamage:.28,cooldown:.05},'Channel a star into powerful, frequent spells.','#c3adff','scepter'),
  item('eternity-hourglass','Eternity Hourglass',3,620,['chrono-dial','blue-flask'],{cooldown:.11,spellDamage:.08},'Bend the rhythm of battle in your favor.','#8fe3e8','eternity'),
  item('celestial-font','Celestial Font',3,500,['moon-well','spark-crystal'],{mana:.25,manaRegen:1.6,spellDamage:.12},'A celestial fountain fuels your spell damage.','#99d5f9','font'),
  item('tempest-wings','Tempest Wings',3,580,['wind-striders','iron-edge'],{speed:.07,damage:.10,attackSpeed:.10},'Close the distance and unleash a flurry.','#a1efda','wings'),
  item('thunderbolt-crossbow','Thunderbolt Crossbow',3,600,['falcon-gauntlet','swift-boots'],{attackSpeed:.24,spellDamage:.06},'A crackling mechanism for relentless attacks.','#ffd585','crossbow'),
  item('phoenix-crown','Phoenix Crown',3,560,['dawn-censer','ruby-heart'],{healing:.26,shield:.24,hp:.12},'Empower your protection and survive to use it.','#ffba91','crown'),
  item('elderwood-staff','Elderwood Staff',3,610,['grove-totem','spark-crystal'],{summonPower:.28,summonHaste:.18,hp:.08},'Awaken a stronger, faster summoned host.','#b8e69d','staff'),
  item('crimson-relic','Crimson Relic',3,620,['reaper-sickle','ruby-heart'],{lifesteal:.06,damage:.16,hp:.12},'A durable duelist feeds on every basic strike.','#f59eb4','relic'),
]);

export const ITEM_BY_ID = Object.freeze(Object.assign(Object.create(null),Object.fromEntries(ITEMS.map(value=>[value.id,value]))));

/** Item bonuses share the engine's existing talent keys and add to those talents. */
export function itemBonus(hero,key) {
  if(!Array.isArray(hero?.items))return 0;
  let total=0;
  for(const id of new Set(hero.items)){
    const value=ITEM_BY_ID[id]?.bonuses[key];
    if(Number.isFinite(value))total+=value;
  }
  return total;
}

/** Read-only purchase validation shared by the engine and the shop. */
export function purchaseState(hero,itemId) {
  const selected=ITEM_BY_ID[itemId]||null;
  const owned=new Set(Array.isArray(hero?.items)?hero.items:[]);
  const missing=selected?selected.requires.filter(id=>!owned.has(id)):[];
  const validGold=Number.isFinite(hero?.gold)&&hero.gold>=0;
  const goldNeeded=selected?Math.max(0,selected.cost-(validGold?hero.gold:0)):0;
  let reason='Ready to buy';
  if(!selected)reason='Unknown item';
  else if(hero?.kind!=='hero')reason='Choose a hero first';
  else if(owned.has(itemId))reason='Already active';
  else if(missing.length)reason='Requires '+missing.map(id=>ITEM_BY_ID[id].name).join(' + ');
  else if(!validGold)reason='Gold unavailable';
  else if(goldNeeded>0)reason=`Need ${Math.ceil(goldNeeded)} more gold`;
  return {ok:reason==='Ready to buy',reason,item:selected,missing,goldNeeded};
}

const BONUS_LABELS=Object.freeze({
  hp:'health',damage:'damage',spellDamage:'ability damage',cooldown:'cooldown reduction',mana:'mana',
  manaRegen:'mana/sec',speed:'move speed',attackSpeed:'attack speed',healing:'healing',shield:'shields',
  summonPower:'summon health & damage',summonHaste:'summon attack speed',area:'ability area',lifesteal:'lifesteal',
});
export function describeBonuses(value) {
  const selected=typeof value==='string'?ITEM_BY_ID[value]:value;
  return Object.entries(selected?.bonuses||{}).map(([key,amount])=>{
    const number=key==='manaRegen'?Number(amount.toFixed(2)):Number((amount*100).toFixed(1));
    return `+${number}${key==='manaRegen'?'':'%'} ${BONUS_LABELS[key]||key}`;
  }).join(' · ');
}

/** Small code-drawn pixel art; no DOM, spritesheets, or network assets needed. */
export function drawItemIcon(ctx,value,x=0,y=0,size=32) {
  const selected=typeof value==='string'?ITEM_BY_ID[value]:value;
  if(!selected||!Number.isFinite(size)||size<=0)return;
  const main=selected.color,light='#fff0c8',shade='#35445b',dark='#132331',white='#eaf6ff';
  // Round each grid edge independently so a 20-pixel design remains crisp at any size.
  const p=(color,px,py,w=1,h=1)=>{
    const left=Math.round(x+px*size/20),top=Math.round(y+py*size/20);
    ctx.fillStyle=color;ctx.fillRect(left,top,Math.max(1,Math.round(x+(px+w)*size/20)-left),Math.max(1,Math.round(y+(py+h)*size/20)-top));
  };
  const line=(color,x0,y0,x1,y1,width=1)=>{
    const dx=Math.abs(x1-x0),dy=-Math.abs(y1-y0),sx=x0<x1?1:-1,sy=y0<y1?1:-1;let err=dx+dy;
    for(;;){p(color,x0,y0,width,width);if(x0===x1&&y0===y1)break;const twice=err*2;if(twice>=dy){err+=dy;x0+=sx;}if(twice<=dx){err+=dx;y0+=sy;}}
  };
  const diamond=(color,cx,cy,r)=>{for(let row=-r;row<=r;row++)p(color,cx-r+Math.abs(row),cy+row,2*(r-Math.abs(row))+1);};
  const blade=(cx,top,bottom,width=3)=>{p(dark,cx-1,top-1,width+2,bottom-top+2);p(main,cx,top,width,bottom-top);p(white,cx,top,1,bottom-top);p(light,cx-2,bottom,width+4,2);p('#916846',cx+1,bottom+2,1,3);};
  const boot=(bx,by)=>{p(dark,bx-1,by-1,5,8);p(main,bx,by,3,6);p(main,bx,by+5,6,2);p(white,bx,by,2,1);p(shade,bx,by+7,6,1);};
  const star=(cx,cy)=>{diamond(light,cx,cy,3);p(white,cx,cy-1,1,3);p(white,cx-1,cy,3,1);};
  ctx.save();
  ctx.imageSmoothingEnabled=false;
  p(dark,0,0,20,20);p('#243348',1,1,18,18);p('#1b2a3d',2,2,16,16);
  p(main,1,1,18,1);p(shade,1,18,18,1);
  switch(selected.icon){
    case 'heart':
      p(dark,4,5,12,6);p(main,5,5,4,7);p(main,11,5,4,7);p(main,7,8,6,6);p(main,9,14,2,2);p(white,6,6,2,2);p('#9f4469',12,9,2,3);break;
    case 'dagger':
      line(dark,6,13,14,5,3);line(main,6,12,13,5,2);line(white,6,12,13,5);line(light,5,10,9,14);line('#a07758',5,14,7,12,2);p(light,4,15,2,2);break;
    case 'crystal':
      diamond(dark,10,10,6);diamond(main,10,10,5);line(white,10,5,7,10);line(light,7,10,10,14);line('#7154b7',10,6,12,10);p(white,11,8,1,2);break;
    case 'hourglass':
      p(light,5,4,10,2);p(light,5,15,10,2);line(main,6,6,12,14);line(main,13,6,7,14);p(white,8,6,4,1);p('#d39150',8,13,4,2);p(light,9,9,2,3);break;
    case 'flask':
      p(light,8,3,4,2);p(dark,7,5,6,3);p(main,8,5,4,4);p(dark,5,9,10,8);p(main,6,9,8,7);p('#326b9e',7,12,6,3);p(white,7,9,2,3);p(white,9,6,1,2);break;
    case 'boot':boot(7,6);p(light,7,10,3,1);p(light,10,13,2,1);line(main,4,6,5,5);break;
    case 'bow':
      line(main,6,4,12,7,2);p(main,12,8,2,4);line(main,12,12,6,16,2);line(white,6,5,6,16);p(light,4,10,12,1);p(white,14,9,2,3);p('#bf834f',3,9,2,3);break;
    case 'sun':
      diamond(shade,10,10,5);diamond(main,10,10,4);p(white,8,8,3,2);p(light,9,3,2,2);p(light,9,16,2,2);p(light,3,9,2,2);p(light,16,9,2,2);p(main,5,5,1,1);p(main,14,14,1,1);break;
    case 'seed':
      p('#835c41',8,11,4,5);p('#a57d4f',9,10,2,7);p(main,5,6,4,5);p(main,10,5,5,5);p(white,11,6,2,1);line('#c2e89c',6,7,10,12);line('#c2e89c',13,7,10,11);break;
    case 'fang':
      p(main,5,4,10,4);p(white,6,5,2,2);p(main,7,8,7,3);p(main,9,11,4,3);p(main,9,14,2,2);p('#ac526f',12,8,2,3);p(white,8,8,2,2);p(light,7,16,2,1);break;
    case 'plate':
      p(dark,4,5,12,11);p(main,4,5,4,4);p(main,12,5,4,4);p(main,6,7,8,9);p(shade,8,5,4,2);p(light,8,8,4,2);p(white,7,11,1,3);p(shade,9,11,2,4);p(light,6,15,8,1);break;
    case 'blade':blade(9,4,12,3);p(main,7,6,2,6);p(white,7,6,1,4);p(light,9,17,3,1);break;
    case 'tome':
      p(dark,4,5,13,12);p(main,5,5,11,10);p(white,6,15,10,2);p(shade,5,5,2,10);p(light,8,6,6,1);line(light,11,8,9,11);line(light,9,11,13,10);line(light,13,10,11,13);break;
    case 'dial':
      diamond(dark,10,10,7);diamond(main,10,10,6);diamond('#284f62',10,10,4);p(white,9,5,2,2);p(light,9,9,2,2);line(white,10,10,10,6);line(light,10,10,13,11);p(main,8,2,4,1);break;
    case 'well':
      p(main,4,10,12,2);p(shade,5,12,10,5);p(main,5,12,3,4);p(main,9,12,3,4);p(main,13,12,2,4);p(white,6,9,8,1);p(main,9,4,3,4);p('#1b2a3d',11,3,2,3);p(white,8,7,2,1);break;
    case 'greaves':boot(4,7);boot(11,5);line(white,3,5,7,5);line(white,12,3,16,3);p(light,5,9,2,1);p(light,12,7,2,1);break;
    case 'gauntlet':
      p(main,5,7,3,5);p(main,9,4,2,7);p(main,12,5,2,6);p(main,15,7,2,5);p(main,7,10,9,5);p(light,8,14,7,3);p(shade,9,15,5,1);p(white,9,7,1,3);p(white,12,7,1,3);break;
    case 'censer':
      line(light,6,5,6,11);line(light,14,5,14,11);line(main,6,5,10,3);line(main,14,5,10,3);p(main,5,10,11,2);p(main,7,12,7,4);p(light,9,16,3,1);p(white,8,13,2,2);p('#c886a0',9,6,2,3);break;
    case 'totem':
      p('#856846',8,5,4,12);p(main,4,6,12,5);p(white,6,7,2,2);p(white,12,7,2,2);p(shade,9,9,2,1);p(main,6,12,8,2);p(light,9,13,2,2);p(main,7,3,6,2);break;
    case 'sickle':
      line('#ac7c5c',7,16,11,7,2);p(main,8,4,7,2);p(main,13,6,4,2);p(main,15,8,2,4);p(main,13,12,3,2);p(light,9,4,5,1);p(white,15,8,1,3);p(light,6,16,3,1);break;
    case 'aegis':
      p(light,5,4,10,2);p(main,4,6,12,6);p(main,6,12,8,2);p(main,8,14,4,2);p(main,9,16,2,1);p(white,6,6,1,5);p(shade,9,6,2,9);p(light,7,8,6,2);p(light,9,6,2,7);p(main,3,3,2,2);p(main,15,3,2,2);break;
    case 'greatsword':blade(8,3,12,4);p(main,6,4,2,8);p(main,12,4,2,8);p(white,6,5,1,5);p(light,4,12,12,2);p(main,3,11,2,2);p(main,15,11,2,2);diamond(main,10,8,2);break;
    case 'scepter':
      line('#b58b55',7,17,11,7,2);diamond(main,12,6,4);star(12,6);p(main,5,13,2,2);p(main,15,10,2,2);p(light,6,17,3,1);break;
    case 'eternity':
      p(light,4,3,12,2);p(light,4,16,12,2);p(main,4,5,2,11);p(main,14,5,2,11);line(white,7,5,12,14);line(white,12,5,7,14);diamond(main,10,10,2);p(light,8,13,4,2);p(main,2,8,1,4);p(main,17,8,1,4);break;
    case 'font':
      p(main,4,11,12,2);p(main,7,13,6,2);p(main,9,15,2,2);p(light,5,17,10,1);p(white,7,10,6,1);line(main,6,10,8,6);line(main,14,10,12,6);p(white,9,5,2,5);star(10,5);break;
    case 'wings':
      p(main,3,4,2,7);p(main,5,7,2,7);p(main,7,10,2,6);p(main,15,4,2,7);p(main,13,7,2,7);p(main,11,10,2,6);p(white,4,5,1,5);p(white,15,5,1,5);p(light,9,11,2,6);p(main,2,3,2,2);p(main,16,3,2,2);break;
    case 'crossbow':
      line(main,4,6,9,9,2);line(main,10,9,15,6,2);line(main,4,6,3,10,2);line(main,15,6,16,10,2);line(white,4,10,16,10);p('#b48c5b',9,6,3,12);p(light,10,4,1,12);p(white,9,3,3,2);p(main,6,13,9,2);break;
    case 'crown':
      p(main,4,8,12,7);p(main,3,5,3,4);p(main,9,3,3,6);p(main,14,5,3,4);p(light,5,14,10,2);diamond('#ff8a73',10,10,2);p(white,5,9,1,3);p(light,4,4,1,2);p(light,15,4,1,2);p(white,10,3,1,2);break;
    case 'staff':
      line('#c09b66',9,17,10,7,2);line(main,10,7,5,5,2);line(main,10,7,14,4,2);p(main,4,4,4,3);p(main,12,3,5,3);p(white,13,3,2,1);p(main,12,8,3,2);p(light,9,11,3,1);p(main,6,13,3,2);p(light,8,17,4,1);break;
    case 'relic':
      p(light,5,4,10,2);p(main,4,6,12,2);p(main,5,8,10,4);p(main,7,12,6,2);p(light,9,14,2,2);p(light,6,16,8,2);diamond('#a1375a',10,9,3);p(white,8,8,2,2);p(main,3,7,1,4);p(main,16,7,1,4);break;
  }
  // Tier pips remain visible even where differently colored equipment is similar.
  for(let i=0;i<selected.tier;i++)p(light,2+i*3,17,2,1);
  ctx.restore();
}

