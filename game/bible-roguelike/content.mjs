// Small data tables shared by the solo and cooperative rules engine.
export const CATEGORY_KEYS = ['faith','hope','love','wisdom','courage','prayer','forgiveness','grace','truth','humility','repentance','endurance','temptation','fear','healing','justice','worship','warfare','providence','obedience'];
export const VARIANTS = {
  normal: {name:'', color:'#cbd5e1', hp:1, damage:1, effect:'physical', hint:'Attacks after an incorrect verse. Correct answers block all damage.'},
  enraged: {name:'Enraged', color:'#fb6666', hp:1.05, damage:1.25, effect:'fire', hint:'Stronger attacks; a critical hit every third turn.'},
  deceiver: {name:'Deceiver', color:'#c084fc', hp:1.05, damage:1, effect:'fear', hint:'Weaknesses concealed until discovered. Discernment reveals them.'},
  corrupted: {name:'Corrupted', color:'#86efac', hp:1.1, damage:.85, effect:'poison', hint:'Every third attack poisons its target for two turns.'},
  frozen: {name:'Despairing', color:'#7dd3fc', hp:1.1, damage:.9, effect:'frost', hint:'Every third attack chills its target: healing halved for two verse turns.'},
  exalted: {name:'Exalted', color:'#fcd34d', hp:1.15, damage:.9, effect:'holy', hint:'Armor reduces hits by 25%. Truth or Justice breaks it.'},
  shadow: {name:'Shadow', color:'#94a3b8', hp:.95, damage:1.05, effect:'fear', hint:'Alternating feints and stronger attacks. Stand Firm helps.'}
};
export const ARCHETYPES = {
  fear:{weak:['courage','faith','hope'],resist:'fear',effect:'fear'},
  pride:{weak:['humility','repentance','grace'],resist:'giving',effect:'holy'},
  temptation:{weak:['obedience','warfare','truth'],resist:'temptation',effect:'poison'},
  deception:{weak:['truth','wisdom','word'],resist:'fear',effect:'fear'},
  anger:{weak:['love','peace','forgiveness'],resist:'justice',effect:'fire'},
  despair:{weak:['hope','healing','endurance'],resist:'fear',effect:'frost'},
  greed:{weak:['giving','providence','love'],resist:'kingdom',effect:'physical'},
  doubt:{weak:['faith','wisdom','hope'],resist:'fear',effect:'fear'},
  persecution:{weak:['endurance','prayer','justice'],resist:'law',effect:'physical'},
  chaos:{weak:['peace','wisdom','obedience'],resist:'fear',effect:'physical'},
  corruption:{weak:['repentance','holiness','healing'],resist:'temptation',effect:'poison'}
};
export const ENEMY_ARCHETYPES = {
  'little-imp':'chaos','ash-imp':'anger','idol-priest':'deception',doubter:'doubt',tempter:'temptation',
  'dread-wraith':'fear',accuser:'despair','greed-beast':'greed','pride-giant':'pride','merciless-judge':'persecution',
  captor:'persecution','division-spirit':'chaos','false-prophet':'deception','grave-warden':'despair',
  'death-knight':'fear','church-devourer':'persecution','scarlet-dragon':'anger','beast-of-night':'pride',
  'last-accuser':'despair','breaker-of-brethren':'chaos','rebel-warden':'temptation','unrepentant-shade':'corruption',
  'fear-monger':'fear','crooked-magistrate':'corruption','furnace-of-haste':'anger','thankless-devourer':'greed',
  housebreaker:'chaos','creation-mocker':'doubt','false-disciple':'deception','throne-of-self':'pride'
};
export const ABILITIES = {
  discernment:{name:'Discernment',icon:'◈',desc:'Reveal all weaknesses. Once per encounter.',cooldown:0,cost:26},
  'second-wind':{name:'Second Wind',icon:'♥',desc:'Heal yourself or revive a downed teammate for 18% HP. Recharge: 4 successful verses.',cooldown:4,cost:34},
  silence:{name:'Silence',icon:'✦',desc:'Cancel the next enemy special. Recharge: 4 verses.',cooldown:4,cost:32},
  'stand-firm':{name:'Stand Firm',icon:'⛨',desc:'Reduce your next incoming hit by 65%. Recharge: 3 verses.',cooldown:3,cost:28},
  'double-strike':{name:'Double Strike',icon:'⚔',desc:'Your next verse deals a second 45% hit. Recharge: 5 verses.',cooldown:5,cost:42},
  recall:{name:'Recall',icon:'↻',desc:'Release one used verse for the whole team. Each verse may be recalled only once per encounter.',cooldown:0,cost:40},
  inspiration:{name:'Inspiration',icon:'☀',desc:'A revealed random category gains 30% damage for your next 3 verses. Recharge: 5 verses.',cooldown:5,cost:35},
  challenge:{name:'Challenge',icon:'⚑',desc:'Before attacking: win without receiving any healing for +40% gold. Once per encounter.',cooldown:0,cost:24}
};
export const RELICS = {
  'faith-spark':{name:'Faith Spark',icon:'☀',desc:'Your first Faith verse each encounter gains 20% damage.',cost:32},
  'healing-hands':{name:'Healing Hands',icon:'♥',desc:'All healing you receive is increased by 20%.',cost:34},
  'first-light':{name:'First Light',icon:'✧',desc:'Your first verse each encounter gains 15% damage.',cost:34},
  'unbroken-purse':{name:'Unbroken Purse',icon:'◉',desc:'Gain 8 extra gold after a battle in which you took no damage.',cost:36},
  'wisdom-lens':{name:'Wisdom Lens',icon:'◈',desc:'Wisdom verses reveal all enemy weaknesses.',cost:28},
  'many-books':{name:'Many Books',icon:'▤',desc:'Consecutive verses from different books build damage, up to +20%.',cost:42},
  'ancient-scroll':{name:'Ancient Scroll',icon:'📜',desc:'Old Testament verses gain 10% damage.',cost:36},
  'gospel-guard':{name:'Gospel Guard',icon:'⛨',desc:'Gospel verses grant 3 shield, up to 30.',cost:40},
  'proverb-eye':{name:'Proverb Eye',icon:'◈',desc:'Proverbs verses have an extra 15% critical chance.',cost:36},
  'epistle-seal':{name:'Epistle Seal',icon:'✉',desc:'Epistle verses grant 8% bonus damage on your next verse.',cost:36},
  'martyrs-path':{name:"Martyr’s Path",icon:'⚑',desc:'You take 20% more damage and earn 25% more gold.',cost:26,risk:true},
  'narrow-gate':{name:'Narrow Gate',icon:'▥',desc:'Your shops offer 4 choices instead of 6, with more relics and abilities.',cost:24,risk:true},
  'testing-faith':{name:'Testing of Faith',icon:'?',desc:'Weaknesses start hidden for you; weakness hits gain another 20%.',cost:28,risk:true},
  pilgrim:{name:'Pilgrim',icon:'♟',desc:'Half healing between battles; earn 25% more gold.',cost:24,risk:true}
};
export const CONSUMABLES = {
  cleanse:{name:'Cleansing Water',icon:'💧',kind:'cleanse',desc:'Remove poison and chill.',rarity:'common',cost:14},
  revive:{name:'Restoration Oil',icon:'🕊',kind:'revive',desc:'Revive a downed teammate with 30% health.',rarity:'rare',cost:32},
  recharge:{name:'Renewal',icon:'✦',kind:'recharge',desc:'Ready one equipped ability with a verse cooldown.',rarity:'uncommon',cost:20},
  'shop-token':{name:'Merchant Token',icon:'↻',kind:'reroll',desc:'Reroll your current shop for free.',rarity:'uncommon',cost:18}
};
export const ROOM_NAMES = {battle:'Battle',elite:'Elite Trial',boss:'Boss',shop:'Merchant',rest:'Quiet Place',treasure:'Treasure',scripture:'Scripture Challenge',risk:'A Costly Gift'};
export const ROOM_HINTS = {battle:'The next encounter.',elite:'Stronger enemy; a guaranteed relic.',boss:'A great trial and richer rewards.',shop:'Spend your gold on six rotating offers.',rest:'Recover 28% health or train your next attack.',treasure:'Choose a personal gift.',scripture:'A knowledge challenge with gold, healing and a discount.',risk:'Accept a difficult relic and bonus gold, or pass.'};
export const MERCHANTS = ['Traveling merchant','Relic keeper','Healer','Verse specialist','Trial merchant'];
