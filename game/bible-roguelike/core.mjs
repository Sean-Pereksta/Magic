import {CATEGORY_KEYS, VARIANTS, ARCHETYPES, ENEMY_ARCHETYPES, ABILITIES, RELICS, CONSUMABLES, MERCHANTS} from './content.mjs';

export const VERSION = 3;
export const clone = value => JSON.parse(JSON.stringify(value));
export const normalizeName = value => String(value || '').trim().replace(/\s+/g, ' ').toLowerCase();
export function normalizeBook(value) {
  const name = normalizeName(value).replace(/\./g, '').replace(/^first |^i /, '1 ').replace(/^second |^ii /, '2 ').replace(/^third |^iii /, '3 ');
  return ({psalm:'psalms','song of songs':'song of solomon',revelations:'revelation'})[name] || name;
}
export const refKey = verse => `${normalizeBook(verse.book)} ${Number(verse.chapter)}:${Number(verse.verse)}`;
export const displayRef = verse => `${verse.book} ${verse.chapter}:${verse.verse}`;
export function partyScale(count, boss = false) {
  return (boss ? [1, 1.85, 2.7, 3.5, 4.25] : [1, 1.8, 2.6, 3.35, 4])[Math.max(0, Math.min(4, count - 1))];
}
function demand(condition, message) { if (!condition) throw new Error(message); }
function random(state) {
  let x = state.seed | 0; x ^= x << 13; x ^= x >>> 17; x ^= x << 5;
  state.seed = x >>> 0;
  return state.seed / 4294967296;
}
const pick = (state, list) => list[Math.floor(random(state) * list.length)];
function shuffled(state, list) {
  const result = [...list];
  for (let i = result.length - 1; i > 0; i--) { const j = Math.floor(random(state) * (i + 1)); [result[i], result[j]] = [result[j], result[i]]; }
  return result;
}
const has = (player, id) => player.relics.includes(id);
const isGospel = verse => ['matthew','mark','luke','john'].includes(normalizeBook(verse.book));
export function createRules({verses, enemies, books, classes, legacyItems = {}, legacyRelics = {}, conceptKeys, matchVerseConcept, classifyVerseKeys}) {
  // Catalogs and Bible text never travel with every Firestore snapshot.
  const verseMap = new Map(verses.map(v => [refKey(v), v]));
  const oldBooks = new Set(books.slice(0, 39).map(normalizeBook));
  const tagsCache = new Map();
  const tags = verse => {
    const key = refKey(verse);
    if (!tagsCache.has(key)) tagsCache.set(key, classifyVerseKeys ? classifyVerseKeys(verse.text,conceptKeys) : conceptKeys.filter(c => !!matchVerseConcept(verse.text, c)));
    return tagsCache.get(key);
  };
  const matches = (verse, key) => tagsCache.has(refKey(verse)) ? tagsCache.get(refKey(verse)).includes(key) : !!matchVerseConcept(verse.text,key);
  const items = {...legacyItems, ...CONSUMABLES};
  for (const [id, item] of Object.entries(items)) items[id] = {...item, cost:item.cost || ({common:12,uncommon:20,rare:32,epic:46})[item.rarity] || 20};
  items['armor-of-faith'] = {...items['armor-of-faith'], desc:'Absorb the next 18 damage.'};
  items['scroll-of-recall'] = {...items['scroll-of-recall'], desc:'Release one used verse for the whole team. A verse can be recalled once per encounter.'};
  items['great-recall'] = {...items['great-recall'], desc:'Release one used verse, restore 35% HP and gain 18 shield.'};
  const relics = {...Object.fromEntries(Object.entries(legacyRelics).map(([id, def]) => [id, {...def, cost:Math.round(def.cost * 2)}])), ...RELICS};
  // Keep all existing knowledge relics, with explicit, deterministic triggers.
  Object.assign(relics, {
    'scroll-of-context':{...relics['scroll-of-context'],desc:'Reveal a matching reference and its text at the start of every encounter.'},
    'book-hunter':{...relics['book-hunter'],desc:'One book challenge each encounter; correct answers deal 15 bonus damage.'},
    'testament-seal':{...relics['testament-seal'],desc:'One Testament challenge each encounter; correct answers deal 6 damage and give 8 shield.'},
    'verse-completion':{...relics['verse-completion'],desc:'One missing-word challenge each encounter; correct answers deal 12 damage and heal 6%.'},
    'verse-insight':{...relics['verse-insight'],desc:'Inspect a matching verse and its neighbors once per encounter.'},
    'second-chance':{...relics['second-chance'],desc:'Block your first enemy hit each encounter.'},
    'psalm-mastery':{...relics['psalm-mastery'],desc:'Psalms deal +5 damage and restore 3 HP.'},
    'wisdom-shield':{...relics['wisdom-shield'],desc:'Correct Scripture challenges grant 8 shield.'}
  });
  function parseReference(input) {
    const m = String(input || '').trim().match(/^(.+?)\s+(\d+)\s*:\s*(\d+)$/);
    return m ? verseMap.get(`${normalizeBook(m[1])} ${+m[2]}:${+m[3]}`) || null : null;
  }
  function log(s, text, extra = {}) {
    s.eventSeq++;
    s.events.push({seq:s.eventSeq, text, ...extra});
    s.events = s.events.slice(-40);
  }
  function heal(s, p, fraction, revive = false) {
    if (p.hp <= 0 && !revive) return 0;
    const amount = Math.max(1, Math.round(p.maxHp * fraction * (has(p,'healing-hands') ? 1.2 : 1) * (p.chill ? .5 : 1) * (s.phase !== 'battle' && has(p,'pilgrim') ? .5 : 1)));
    const before = p.hp;
    p.hp = Math.min(p.maxHp, p.hp + amount);
    if (p.hp > before) {
      if (p.challenge) p.challengeBroken = true;
      log(s, `${p.name} +${p.hp - before} HP`, {target:p.id,effect:'healing',amount:p.hp - before});
    }
    return p.hp - before;
  }
  function awardGold(p, base) {
    const amount = Math.round(base * (has(p,'martyrs-path') ? 1.25 : 1) * (has(p,'pilgrim') ? 1.25 : 1));
    p.gold += amount;
    return amount;
  }
  function addRelic(s, p, id) {
    if (!id) { awardGold(p, 22); return; }
    if (!p.relics.includes(id)) p.relics.push(id);
    log(s, `${p.name} found ${relics[id].name}.`);
  }
  function rollRelic(s, p, risk = false) {
    return pick(s, Object.keys(relics).filter(id => !p.relics.includes(id) && (risk ? !!relics[id].risk : !relics[id].risk))) || null;
  }
  function rollItem(s, rare = false) {
    return pick(s, Object.keys(items).filter(id => !rare || ['rare','epic','uncommon'].includes(items[id].rarity)));
  }
  function classOptions(p) {
    return Object.keys(classes).filter(id => classes[id].parent === p.classId && p.level >= classes[id].tier);
  }
  function makePlayer(name, index) {
    return {id:`p${index}`,name:String(name).slice(0,40),uid:'',hp:100,maxHp:100,gold:25,xp:0,level:1,baseDamage:16,
      classId:'word-warden',classHistory:['word-warden'],inventory:['healing-draught'],relics:[],
      abilities:['discernment','second-wind','stand-firm'],equipped:['discernment','second-wind','stand-firm'],
      cooldowns:{},upgrades:{},turn:0,shield:0,poison:0,chill:0,guard:false,double:false,inspiration:null,
      previousBook:'',bookCombo:0,epistleBuff:0,streak:0,mastered:[],battle:{},hint:'',quiz:null,
      challenge:false,challengeBroken:false,discount:0,shop:null,roomClaimed:false,roomDone:false};
  }
  function createChallenge(s, kind = null, now = 0) {
    const verse = pick(s, verses), keys = tags(verse);
    kind ||= pick(s, ['book','testament','completion','theme','find','timed']);
    if (kind === 'theme' && !keys.length) kind = 'book';
    const q = {kind,verse:refKey(verse),text:verse.text,prompt:'',options:[],answer:'',category:'',deadline:0};
    if (kind === 'book') {
      q.prompt = 'Which book contains this verse?'; q.answer = verse.book;
      q.options = shuffled(s, [verse.book, ...shuffled(s, books.filter(b => normalizeBook(b) !== normalizeBook(verse.book))).slice(0,3)]);
    } else if (kind === 'testament') {
      q.prompt = 'Old Testament or New Testament?'; q.answer = oldBooks.has(normalizeBook(verse.book)) ? 'Old Testament' : 'New Testament';
      q.options = ['Old Testament','New Testament'];
    } else if (kind === 'completion') {
      const words = verse.text.split(/\s+/), candidate = words.map((w,i) => ({word:w.replace(/[^a-z']/gi,''),i})).filter(w => w.word.length >= 5);
      if (!candidate.length) return createChallenge(s, 'book', now);
      const word = pick(s, candidate); q.answer = word.word; words[word.i] = '_____'; q.text = words.join(' ');
      const distractors = [...new Set(verses.slice(0,500).flatMap(v => v.text.match(/[a-z']{5,}/gi) || []).map(w => w.toLowerCase()))].filter(w => w !== word.word.toLowerCase());
      q.options = shuffled(s, [word.word, ...shuffled(s, distractors).slice(0,3)]); q.prompt = 'Complete the missing word.';
    } else if (kind === 'theme') {
      q.answer = pick(s, keys); q.category = q.answer;
      q.prompt = 'Which theme is present in this verse?';
      q.options = shuffled(s, [q.answer, ...shuffled(s, conceptKeys.filter(k => !keys.includes(k))).slice(0,3)]);
    } else {
      const viable = CATEGORY_KEYS.filter(key => verses.some(v => matches(v,key)));
      q.category = pick(s, viable) || 'faith'; q.text = ''; q.prompt = `Find a verse about ${q.category}.`;
      if (kind === 'timed') q.deadline = now + 45000;
    }
    return q;
  }
  function enterBattle(s, type) {
    const boss = type === 'boss', elite = type === 'elite';
    let pool = enemies.filter(e => !!e.boss === boss && s.floor >= e.minFloor && (!e.maxFloor || s.floor <= e.maxFloor));
    if (!pool.length) pool = enemies.filter(e => !!e.boss === boss && s.floor >= e.minFloor);
    if (!pool.length) pool = enemies.filter(e => !e.boss);
    const base = pick(s, pool), variant = s.floor <= 2 ? 'normal' : pick(s, Object.keys(VARIANTS)), meta = VARIANTS[variant];
    const archetype = ENEMY_ARCHETYPES[base.id] || 'chaos', category = ARCHETYPES[archetype];
    const depth = s.floor - 1, scale = (1 + depth * .085 + Math.pow(depth,1.4) * .01) * partyScale(s.players.length, boss);
    const maxHp = Math.round(base.hp * scale * meta.hp * (elite ? 1.45 : 1) * (boss ? 1.12 : 1));
    const weak = new Map(base.weaknesses.map(w => [w.concept, Math.max(.7,w.multiplier)]));
    for (const key of category.weak) weak.set(key, Math.max(1.25, weak.get(key) || 0));
    s.enemy = {id:base.id,name:base.name,art:base.art,desc:base.desc,variant,archetype,boss,elite,maxHp,hp:maxHp,
      damage:Math.max(3,Math.round(base.damage * .62 * (1 + depth * .028) * meta.damage * (elite ? 1.2 : 1) * (1 + .025 * (s.players.length - 1)))),
      xp:base.xp,weaknesses:[...weak].map(([concept,multiplier]) => ({concept,multiplier})),turn:0,armor:variant === 'exalted',silenced:false,revealed:false};
    s.phase = 'battle'; s.used = {}; s.recalled = []; s.room = {id:`${s.id}:${s.floor}`,type};
    for (const p of s.players) {
      p.battle = {attacks:0,faithUsed:false,recallUsed:false,discernmentUsed:false,secondChance:true,insightUsed:false,damageTaken:0};
      p.challenge = false; p.challengeBroken = false; p.roomDone = false; p.roomClaimed = false;
      p.poison = 0; p.chill = 0; p.guard = false; p.double = false; p.inspiration = null; p.hint = ''; p.quiz = null;
      for (const [relic, kind] of [['book-hunter','book'],['testament-seal','testament'],['verse-completion','completion']]) {
        if (has(p,relic)) { p.quiz = {...createChallenge(s,kind),relic}; break; }
      }
      if (has(p,'scroll-of-context')) {
        const hint = hintVerse(s); if (hint) p.hint = `${displayRef(hint)} — ${hint.text}`;
      }
    }
    log(s, `${elite ? 'Elite ' : ''}${meta.name ? meta.name + ' ' : ''}${base.name} — Floor ${s.floor}.`);
  }
  function setPaths(s) {
    s.phase = 'path';
    if ((s.floor + 1) % 5 === 0) s.paths = ['boss'];
    else if (s.floor % 5 === 0) s.paths = ['shop','rest','elite'];
    else s.paths = ['battle',pick(s,['shop','rest','treasure','scripture','risk']), 'elite'];
  }
  function finishBattle(s) {
    if (s.phase !== 'battle' || s.enemy.hp > 0) return;
    // Rewarding and leaving battle are one state transition, never separate writes.
    const e = s.enemy;
    for (const p of s.players) {
      const bonus = p.challenge && !p.challengeBroken ? 1.4 : 1;
      const gold = awardGold(p, (12 + s.floor * 3 + (e.elite ? 12 : 0) + (e.boss ? 28 : 0)) * bonus);
      if (has(p,'unbroken-purse') && !p.battle.damageTaken) p.gold += 8;
      p.xp += Math.round(e.xp * (e.elite ? 1.5 : 1));
      while (p.xp >= 24 + (p.level - 1) * 13) {
        p.xp -= 24 + (p.level - 1) * 13; p.level++; p.maxHp += 10; p.baseDamage += 3;
        heal(s,p,.12);
      }
      if (e.elite || e.boss) addRelic(s,p,rollRelic(s,p));
      if (random(s) < .5 || e.boss) p.inventory.push(rollItem(s, e.elite || e.boss));
      if (p.hp <= 0) { heal(s,p,.25,true); log(s,`${p.name} returns to the party.`); }
      p.poison = 0; p.chill = 0; p.inventory = p.inventory.slice(-60);
      log(s, `${p.name} earned ${gold} gold${bonus > 1 ? ' • Challenge complete!' : ''}.`);
    }
    s.score += 40 + s.floor * 12 + (e.boss ? 120 : 0); s.bosses += e.boss ? 1 : 0;
    log(s, `${e.name} defeated. Choose the next room.`, {effect:'victory',target:'enemy'});
    setPaths(s);
  }
  function damagePlayer(s, p, amount, effect, critical = false) {
    if (p.hp <= 0) return;
    if (has(p,'second-chance') && p.battle.secondChance) { p.battle.secondChance = false; log(s,`${p.name} — Second Chance blocked the hit.`); return; }
    let value = Math.max(1,Math.round(amount * (has(p,'martyrs-path') ? 1.2 : 1) * (p.guard ? .35 : 1)));
    p.guard = false;
    const absorbed = Math.min(p.shield, value); p.shield -= absorbed; value -= absorbed;
    p.hp = Math.max(0, p.hp - value); p.battle.damageTaken += value;
    log(s, `${p.name} −${value} HP${absorbed ? ` (${absorbed} shield)` : ''}${p.hp <= 0 ? ' • Downed' : ''}`, {target:p.id,effect,amount:-value,critical});
  }
  function retaliate(s, p) {
    const e = s.enemy; e.turn++;
    if (p.poison > 0) { p.poison--; damagePlayer(s,p,3,'poison'); }
    if (p.chill > 0) p.chill--;
    const special = e.turn % 3 === 0;
    const silenced = special && e.silenced;
    if (silenced) { e.silenced = false; log(s,'Silence cancelled the enemy special.'); }
    const critical = !silenced && ((e.variant === 'enraged' && special) || (e.variant === 'shadow' && e.turn % 2 === 0));
    const effect = e.variant === 'normal' ? ARCHETYPES[e.archetype].effect : VARIANTS[e.variant].effect;
    damagePlayer(s,p,e.damage * (critical ? 1.4 : e.variant === 'shadow' ? .85 : 1),effect,critical);
    if (special && !silenced) {
      if (e.variant === 'corrupted' && p.hp > 0) p.poison = 2;
      if (e.variant === 'frozen' && p.hp > 0) p.chill = 2;
      if (e.boss) {
        log(s,'Boss pulse — the party braces.');
        for (const ally of s.players) if (ally.id !== p.id) damagePlayer(s,ally,Math.round(e.damage * .35),effect);
      }
    }
    if (s.players.every(ally => ally.hp <= 0)) { s.phase = 'ended'; log(s,'The party has fallen. The run is complete.'); }
  }
  function weaknessInfo(s, p, verse) {
    const categories = tags(verse), e = s.enemy;
    let concept = categories[0] || 'word', multiplier = 1;
    for (const weak of e.weaknesses) if (categories.includes(weak.concept) && weak.multiplier > multiplier) { concept = weak.concept; multiplier = weak.multiplier; }
    if (multiplier === 1 && categories.includes(ARCHETYPES[e.archetype].resist)) { concept = ARCHETYPES[e.archetype].resist; multiplier = .8; }
    if (multiplier > 1 && e.turn % 3 === 2) multiplier += .15;
    if (multiplier > 1 && has(p,'testing-faith')) multiplier *= 1.2;
    return {concept,multiplier,categories};
  }
  function attack(s, p, action) {
    demand(s.phase === 'battle' && s.enemy.hp > 0, 'This encounter has already ended.');
    const verse = parseReference(action.reference);
    demand(verse, 'Reference not found. Use the Verse Finder or enter a complete reference.');
    const key = refKey(verse);
    demand(!s.used[key], `That verse was used by ${s.used[key]?.name || 'a teammate'}. Choose another.`);
    // Validate before claiming: invalid or duplicate attempts spend no HP or turns.
    s.used[key] = {playerId:p.id,name:p.name};
    const {concept,multiplier,categories} = weaknessInfo(s,p,verse), book = normalizeBook(verse.book);
    let raw = p.baseDamage + Math.min(8,p.streak);
    if (has(p,'first-light') && !p.battle.attacks) raw *= 1.15;
    if (has(p,'faith-spark') && categories.includes('faith') && !p.battle.faithUsed) { raw *= 1.2; p.battle.faithUsed = true; }
    if (has(p,'ancient-scroll') && oldBooks.has(book)) raw *= 1.1;
    p.bookCombo = p.previousBook && p.previousBook !== book ? Math.min(4,p.bookCombo + 1) : 0;
    if (has(p,'many-books')) raw *= 1 + .05 * p.bookCombo;
    p.previousBook = book;
    if (p.epistleBuff) { raw *= 1.08; p.epistleBuff = 0; }
    if (p.inspiration?.turns > 0) { if (categories.includes(p.inspiration.category)) raw *= 1.3; if (--p.inspiration.turns === 0) p.inspiration = null; }
    if (p.study) { raw *= 1.15; p.study = false; }
    if (s.enemy.armor && (categories.includes('truth') || categories.includes('justice'))) { s.enemy.armor = false; log(s,'Truth breaks the enemy armor.', {effect:'truth',target:'enemy'}); }
    const critical = random(s) < .05 + (has(p,'proverb-eye') && book === 'proverbs' ? .15 : 0);
    let damage = Math.max(1,Math.round(raw * multiplier * (critical ? 1.5 : 1) * (s.enemy.armor ? .75 : 1)));
    if (has(p,'psalm-mastery') && book === 'psalms') { damage += 5; heal(s,p,.03); }
    if (has(p,'gospel-guard') && isGospel(verse)) p.shield = Math.min(30,p.shield + 3);
    if (has(p,'epistle-seal') && !oldBooks.has(book) && !isGospel(verse) && !['acts','revelation'].includes(book)) p.epistleBuff = 1;
    if (has(p,'wisdom-lens') && categories.includes('wisdom')) s.enemy.revealed = true;
    if (p.double) { damage += Math.round(damage * .45); p.double = false; }
    p.turn++; p.streak++; p.battle.attacks++;
    if (has(p,'scripture-strike') && p.streak % 3 === 0) damage += 10;
    const dealt = Math.min(s.enemy.hp,damage); s.enemy.hp -= dealt; s.score += dealt;
    if (!p.mastered.includes(key)) p.mastered.push(key);
    p.mastered = p.mastered.slice(-500);
    if (multiplier > 1) { p.battle.discovered ||= []; if (!p.battle.discovered.includes(concept)) p.battle.discovered.push(concept); }
    log(s, `${p.name} used ${displayRef(verse)} — ${dealt} damage${multiplier > 1 ? ` • ${concept.toUpperCase()} +${Math.round((multiplier-1)*100)}%` : multiplier < 1 ? ' • resisted' : ''}${critical ? ' • critical' : ''}`, {target:'enemy',effect:concept,amount:dealt,reference:key,playerId:p.id,critical});
    if (s.enemy.hp <= 0) finishBattle(s); else retaliate(s,p);
  }
  function hintVerse(s) {
    return verses.find(v => !s.used[refKey(v)] && s.enemy.weaknesses.some(w => matches(v,w.concept))) || null;
  }
  function recall(s, p, reference) {
    const verse = parseReference(reference), key = verse && refKey(verse);
    demand(key && s.used[key], 'Select a verse already used in this encounter.');
    demand(!s.recalled.includes(key), 'This verse has already been recalled this encounter.');
    s.recalled.push(key); delete s.used[key];
    log(s, `${p.name} recalled ${displayRef(verse)} — available to the team again.`);
  }
  function useAbility(s, p, action) {
    demand(s.phase === 'battle', 'Abilities are available during battle.');
    const id = action.ability, def = ABILITIES[id];
    demand(def && p.equipped.includes(id), 'Equip that ability between encounters first.');
    demand((p.cooldowns[id] || 0) <= p.turn, 'This ability is still recharging.');
    const target = s.players.find(ally => ally.id === action.target) || p;
    if (id === 'discernment') {
      demand(!p.battle.discernmentUsed, 'Already used in this encounter.'); p.battle.discernmentUsed = true; s.enemy.revealed = true;
    } else if (id === 'second-wind') {
      demand(target.id === p.id || target.hp <= 0, 'Second Wind heals you or revives a downed teammate.');
      demand(heal(s,target,.18 + (p.upgrades[id] || 0) * .04,true), 'Health is already full.');
    } else if (id === 'stand-firm') { demand(!p.guard,'You are already braced.'); p.guard = true;
    } else if (id === 'silence') { demand(!s.enemy.silenced,'The next special is already silenced.'); s.enemy.silenced = true;
    } else if (id === 'double-strike') { demand(!p.double,'Double Strike is already ready.'); p.double = true;
    } else if (id === 'recall') {
      demand(!p.battle.recallUsed,'Recall is once per encounter.'); recall(s,p,action.reference); p.battle.recallUsed = true;
    } else if (id === 'inspiration') {
      const viable = CATEGORY_KEYS.filter(key => verses.some(v => matches(v,key)));
      p.inspiration = {category:pick(s,viable) || 'faith',turns:3};
      log(s,`${p.name} is inspired: ${p.inspiration.category} +30% for 3 verses.`);
    } else if (id === 'challenge') {
      demand(!p.battle.attacks && !p.challenge, 'Accept this challenge before your first verse.'); p.challenge = true; p.challengeBroken = false;
    }
    const cooldown = Math.max(2,def.cooldown - (id === 'second-wind' ? 0 : p.upgrades[id] || 0));
    if (def.cooldown) p.cooldowns[id] = p.turn + cooldown;
    log(s,`${p.name}: ${def.name}.`,{target:p.id,effect:id === 'second-wind' ? 'healing' : 'wisdom'});
  }
  function useItem(s,p,action) {
    const id = action.item, def = items[id];
    demand(def && p.inventory.includes(id),'That item is no longer in your inventory.');
    const target = s.players.find(ally => ally.id === action.target) || p;
    let message = def.name;
    if (def.kind === 'heal') demand(heal(s,p,def.amount),'Health is already full.');
    else if (def.kind === 'revive') {
      demand(target.hp <= 0,'Choose a downed teammate.'); heal(s,target,.3,true);
    } else if (def.kind === 'reroll') {
      demand(s.phase === 'shop' && !p.roomDone,'Use a Merchant Token while shopping.'); rerollShop(s,p,true);
    } else {
      demand(s.phase === 'battle','Use this item in battle.');
      if (def.kind === 'shield') p.shield = Math.min(60,p.shield + 18);
      else if (def.kind === 'cleanse') { demand(p.poison || p.chill,'You have no negative status.'); p.poison = 0; p.chill = 0;
      } else if (def.kind === 'recharge') {
        const ability = action.ability;
        demand(p.equipped.includes(ability) && ABILITIES[ability].cooldown && p.cooldowns[ability] > p.turn,'Choose a recharging ability.');
        p.cooldowns[ability] = p.turn;
      } else if (def.kind === 'refresh' || def.kind === 'great-recall') {
        recall(s,p,action.reference);
        if (def.kind === 'great-recall') { heal(s,p,.35); p.shield = Math.min(60,p.shield + 18); }
      } else if (def.kind === 'category') {
        const existing = new Set(s.enemy.weaknesses.map(w => w.concept));
        const keys = shuffled(s,conceptKeys.filter(key => !existing.has(key) && verses.some(v => matches(v,key)))).slice(0,def.count);
        demand(keys.length,'All categories are already present.');
        s.enemy.weaknesses.push(...keys.map(concept => ({concept,multiplier:1.25}))); s.enemy.revealed = true;
        message += `: ${keys.join(', ')} are now weaknesses.`;
      } else {
        const v = hintVerse(s); demand(v,'No unused matching verse was found.');
        p.hint = def.kind === 'hint-words' ? v.text.split(/\s+/).slice(0,7).join(' ') : def.kind === 'hint-book' ? v.book : def.kind === 'hint-chapter' ? `${v.book} ${v.chapter}` : displayRef(v);
        message += ` — ${p.hint}`;
      }
    }
    p.inventory.splice(p.inventory.indexOf(id),1); log(s,`${p.name}: ${message}`);
  }
  function makeShop(s,p,merchant) {
    const offers = [], generation = (p.shop?.generation || 0) + 1;
    const add = (kind,id,cost) => offers.push({key:`${generation}-offer${offers.length}`,kind,id,cost:Math.max(1,Math.round(cost * (1 - p.discount))),sold:false});
    const premium = has(p,'narrow-gate'), count = premium ? 4 : 6;
    const candidates = shuffled(s,Object.keys(relics).filter(id => !p.relics.includes(id) && (merchant === 'Trial merchant' ? relics[id].risk : !relics[id].risk)));
    if (!premium) { add('item',merchant === 'Healer' ? 'restoration-flask' : 'healing-draught',merchant === 'Healer' ? 25 : 10); }
    if (candidates.length) { const id = candidates.shift(); add('relic',id,relics[id].cost); }
    const ability = pick(s,Object.keys(ABILITIES).filter(id => !p.abilities.includes(id)));
    if (ability) add('ability',ability,ABILITIES[ability].cost);
    const upgrade = pick(s,p.abilities.filter(id => ABILITIES[id].cooldown && (p.upgrades[id] || 0) < 2));
    if (upgrade) add('upgrade',upgrade,30 + (p.upgrades[upgrade] || 0) * 15);
    while (offers.length < count) {
      if ((premium || merchant === 'Relic keeper' || merchant === 'Trial merchant') && candidates.length) { const id = candidates.shift(); add('relic',id,relics[id].cost); }
      else if (merchant === 'Verse specialist' && !offers.some(o => o.kind === 'training')) add('training','verse-training',38);
      else if (p.relics.length && !offers.some(o => o.kind === 'replace')) add('replace','replace-relic',22);
      else if (!offers.some(o => o.kind === 'mystery')) add('mystery','mystery',24);
      else { const id = rollItem(s, s.floor > 8); add('item',id,items[id].cost); }
    }
    return {merchant,offers,generation,rerolls:p.shop?.rerolls || 0};
  }
  function rerollShop(s,p,free = false) {
    const price = 10 + p.shop.rerolls * 5;
    demand(free || p.gold >= price,`Reroll costs ${price} gold.`);
    if (!free) p.gold -= price;
    const count = p.shop.rerolls + 1, merchant = p.shop.merchant;
    p.shop = makeShop(s,p,merchant); p.shop.rerolls = count;
    log(s,`${p.name} rerolled their shop.`);
  }
  function buy(s,p,action) {
    demand(s.phase === 'shop' && !p.roomDone,'The shop visit has ended.');
    const offer = p.shop.offers.find(o => o.key === action.offer);
    demand(offer && !offer.sold,'That offer is already sold.'); demand(p.gold >= offer.cost,'Not enough gold.');
    const id = offer.id;
    if (offer.kind === 'item') p.inventory.push(id);
    else if (offer.kind === 'relic') { demand(!has(p,id),'You already own that relic.'); addRelic(s,p,id);
    } else if (offer.kind === 'ability') {
      demand(!p.abilities.includes(id),'You already know that ability.'); p.abilities.push(id);
    } else if (offer.kind === 'upgrade') {
      demand((p.upgrades[id] || 0) < 2,'This ability is fully upgraded.'); p.upgrades[id] = (p.upgrades[id] || 0) + 1;
    } else if (offer.kind === 'training') p.baseDamage += 2;
    else if (offer.kind === 'replace') {
      demand(p.relics.includes(action.relic),'Choose an owned relic to replace.');
      const replacement = rollRelic(s,p); demand(replacement,'No replacement relic is available.');
      p.relics.splice(p.relics.indexOf(action.relic),1); addRelic(s,p,replacement);
    } else if (offer.kind === 'mystery') {
      if (random(s) < .4) addRelic(s,p,rollRelic(s,p)); else p.inventory.push(rollItem(s,true));
    }
    p.gold -= offer.cost; offer.sold = true; log(s,`${p.name} spent ${offer.cost} gold.`);
  }
  function enterRoom(s,type,now) {
    s.floor++; s.room = {id:`${s.id}:${s.floor}`,type};
    if (['battle','elite','boss'].includes(type)) { enterBattle(s,type); return; }
    s.phase = type; s.used = {}; s.recalled = [];
    const merchant = s.floor < 4 ? MERCHANTS[0] : pick(s,MERCHANTS);
    if (type === 'scripture') s.room.challenge = createChallenge(s,null,now);
    for (const p of s.players) {
      p.roomDone = false; p.roomClaimed = false; p.roomResult = ''; p.poison = 0; p.chill = 0;
      if (type === 'shop') { p.shop = null; p.shop = makeShop(s,p,merchant); }
      if (type === 'treasure') p.treasure = rollRelic(s,p);
      if (type === 'risk') p.riskOffer = rollRelic(s,p,true);
    }
    log(s,`Floor ${s.floor}: ${type === 'shop' ? merchant : type}.`);
  }
  function answerCorrect(q,answer,now) {
    if (['find','timed'].includes(q.kind)) {
      const v = parseReference(answer);
      return !!v && (!q.deadline || now <= q.deadline) && tags(v).includes(q.category);
    }
    return normalizeName(answer) === normalizeName(q.answer);
  }
  function answerChallenge(s,p,action) {
    const inBattle = action.type === 'quiz', q = inBattle ? p.quiz : s.room.challenge;
    demand(q && (inBattle ? s.phase === 'battle' : s.phase === 'scripture' && !p.roomClaimed),'That challenge is no longer available.');
    const correct = answerCorrect(q,action.answer,action.now);
    if (inBattle) {
      p.quiz = null;
      if (correct) {
        const amount = q.relic === 'book-hunter' ? 15 : q.relic === 'testament-seal' ? 6 : 12;
        const dealt = Math.min(s.enemy.hp,amount); s.enemy.hp -= dealt;
        if (q.relic === 'testament-seal') p.shield = Math.min(60,p.shield + 8);
        if (q.relic === 'verse-completion') heal(s,p,.06);
        log(s,`${p.name}: correct! ${dealt} bonus damage.`,{target:'enemy',effect:'wisdom',amount:dealt});
      } else log(s,`${p.name}: the answer was ${q.answer}. No combat penalty.`);
    } else {
      p.roomClaimed = true;
      p.roomResult = correct ? 'Correct! +25 gold, 10% healing and 15% shop discount.' : `Not this time. ${q.answer ? 'Answer: ' + q.answer + '.' : 'Find a verse matching the requested category before time expires.'}`;
      if (correct) { awardGold(p,25); heal(s,p,.1); p.discount = .15; if (random(s) < .2) addRelic(s,p,rollRelic(s,p)); }
      log(s,`${p.name}: ${p.roomResult}`);
    }
    if (correct && has(p,'wisdom-shield')) p.shield = Math.min(60,p.shield + 8);
    if (inBattle) finishBattle(s);
  }
  function continueRoom(s,p,activeIds) {
    demand(['shop','rest','treasure','scripture','risk'].includes(s.phase),'Choose a path first.');
    p.roomDone = true;
    const active = s.players.filter(ally => ally.hp > 0 && activeIds.includes(ally.id));
    if (active.every(ally => ally.roomDone)) setPaths(s);
    else log(s,`${p.name} is ready. Waiting for the party’s room choices.`);
  }
  function create({names,id,seed,now = 0}) {
    const roster = [...new Map(names.map(n => [normalizeName(n),String(n).trim()])).values()];
    demand(roster.length > 0 && roster.length <= 5,'Roguelike supports 1–5 players.');
    demand(verses.length && enemies.length,'Bible and enemy data must load first.');
    const state = {version:VERSION,id,seed:(seed >>> 0) || 17391,revision:0,floor:1,phase:'battle',score:0,bosses:0,
      players:roster.map(makePlayer),enemy:null,room:null,used:{},recalled:[],paths:[],events:[],eventSeq:0,actions:[],createdAt:now};
    enterBattle(state,'battle'); return state;
  }
  function reduce(snapshot,action,{activeIds = snapshot.players.map(p => p.id)} = {}) {
    demand(snapshot.version === VERSION,'This run needs a compatible game version.');
    if (snapshot.actions.includes(action.id)) return snapshot;
    demand(action.runId === snapshot.id && action.roomId === snapshot.room.id,'The party moved on. Choose an action for the current room.');
    demand(typeof action.id === 'string' && action.id.length <= 100,'Missing action ID.');
    const s = clone(snapshot), p = s.players.find(ally => ally.id === action.playerId);
    demand(p,'You are spectating this run.'); demand(s.phase !== 'ended','This run has ended.');
    demand(p.hp > 0,'You are downed. A teammate can revive you, or win the encounter.');
    if (action.type === 'attack') attack(s,p,action);
    else if (action.type === 'ability') useAbility(s,p,action);
    else if (action.type === 'item') useItem(s,p,action);
    else if (action.type === 'path') {
      demand(s.phase === 'path' && s.paths.includes(action.path),'That path is no longer available.'); enterRoom(s,action.path,action.now);
    } else if (action.type === 'equip') {
      demand(s.phase !== 'battle','Change active slots between encounters.');
      demand(p.abilities.includes(action.ability) && Number.isInteger(action.slot) && action.slot >= 0 && action.slot < 3,'Invalid ability slot.');
      demand(!p.equipped.includes(action.ability) || p.equipped[action.slot] === action.ability,'That ability is already equipped.');
      p.equipped[action.slot] = action.ability;
    } else if (action.type === 'class') {
      demand(classOptions(p).includes(action.classId),'That calling is not unlocked.'); p.classId = action.classId; p.classHistory.push(action.classId);
      log(s,`${p.name} became ${classes[action.classId].name}.`);
    } else if (action.type === 'buy') buy(s,p,action);
    else if (action.type === 'reroll') { demand(s.phase === 'shop' && !p.roomDone,'The shop visit has ended.'); rerollShop(s,p); }
    else if (action.type === 'answer' || action.type === 'quiz') answerChallenge(s,p,action);
    else if (action.type === 'insight') {
      demand(s.phase === 'battle' && has(p,'verse-insight') && !p.battle.insightUsed,'Verse Insight is unavailable.');
      const verse = hintVerse(s); demand(verse,'No matching verse remains.'); p.battle.insightUsed = true;
      p.hint = verses.filter(v => v.book === verse.book && v.chapter === verse.chapter && Math.abs(v.verse - verse.verse) <= 1).map(v => `${displayRef(v)} — ${v.text}`).join('\n\n');
    } else if (action.type === 'claim') {
      demand(['rest','treasure','risk'].includes(s.phase) && !p.roomClaimed && !p.roomDone,'That room reward has already been claimed.');
      if (s.phase === 'rest') {
        demand(['heal','study'].includes(action.choice),'Choose rest or study.');
        if (action.choice === 'heal') heal(s,p,.28); else { p.study = true; log(s,`${p.name} studied: next verse +15%.`); }
      } else if (s.phase === 'treasure') {
        demand(['gold','relic'].includes(action.choice),'Choose gold or a relic.');
        if (action.choice === 'gold') awardGold(p,32); else addRelic(s,p,p.treasure);
      } else {
        demand(['accept','pass'].includes(action.choice),'Accept the relic or pass.');
        if (action.choice === 'accept') { demand(p.riskOffer,'No risk relic remains.'); addRelic(s,p,p.riskOffer); awardGold(p,25); }
      }
      p.roomClaimed = true; p.roomResult = 'Choice saved. Continue when the party is ready.';
    } else if (action.type === 'continue') continueRoom(s,p,activeIds);
    else throw new Error('Unknown action.');
    s.revision++; s.actions.push(action.id); s.actions = s.actions.slice(-100);
    return s;
  }
  function migrateLegacy(payload,{name,id,seed,now = 0}) {
    const old = payload?.state;
    if (payload?.version !== 2 || !old || old.dead || old.health <= 0) return null;
    const s = create({names:[name],id,seed,now}), p = s.players[0];
    s.floor = Math.max(1,Math.floor(Number(old.floor) || 1)); s.score = Math.max(0,Number(old.score) || 0);
    for (const [next,previous] of [['hp','health'],['maxHp','maxHealth'],['level','level'],['xp','xp'],['baseDamage','baseDamage']]) {
      if (Number.isFinite(old[previous]) && old[previous] >= 0) p[next] = old[previous];
    }
    p.gold += Math.round((Number(old.jewels) || 0) * 2); p.shield = Math.min(60,(Number(old.itemShields) || 0) * 18);
    p.inventory = (old.inventory || []).filter(id => items[id]).slice(-60); p.relics = (old.relics || []).filter(id => relics[id]);
    if (classes[old.classId]) { p.classId = old.classId; p.classHistory = old.classHistory || [old.classId]; }
    p.mastered = (old.runMastered || []).map(parseReference).filter(Boolean).map(refKey).slice(-500);
    enterBattle(s,s.floor % 5 === 0 ? 'boss' : old.enemy?.elite ? 'elite' : 'battle');
    if (old.enemy?.hp > 0 && enemies.some(e => e.id === old.enemy.id)) {
      const base = enemies.find(e => e.id === old.enemy.id);
      Object.assign(s.enemy,{id:base.id,name:base.name,art:base.art,desc:base.desc,archetype:ENEMY_ARCHETYPES[base.id] || 'chaos',hp:old.enemy.hp,maxHp:Math.max(old.enemy.hp,old.enemy.maxHp || old.enemy.hp),weaknesses:clone(old.enemy.weaknesses || base.weaknesses)});
    }
    for (const key of old.used || []) { const v = parseReference(key); if (v) s.used[refKey(v)] = {playerId:p.id,name:p.name}; }
    return s;
  }
  return {create,reduce,migrateLegacy,parseReference,tags,matches,verseMap,items,relics,classes,classOptions,weaknessInfo};
}
