import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
import {createRules} from './core.mjs';
const page = await readFile(new URL('../biblegame.html',import.meta.url),'utf8');
export const books = vm.runInNewContext(page.match(/const BOOK_ORDER=(\[[^;]+\]);/)[1]);
export const enemies = vm.runInNewContext(page.match(/const ENEMIES=(\[[\s\S]*?\n\]);/)[1],{W:(concept,multiplier=1)=>({concept,multiplier})});
const dataImport = async path => import('data:text/javascript;base64,' + Buffer.from(await readFile(new URL(path,import.meta.url),'utf8')).toString('base64'));
export const concepts = await dataImport('../biblegame-concepts.js');
export const legacy = await dataImport('../biblegame-rpg-items.js');
export const verses = [
  {book:'John',chapter:3,verse:16,text:'For God so loved the world, that he gave his only begotten Son, that whosoever believeth in him should not perish, but have everlasting life.'},
  {book:'Philippians',chapter:4,verse:13,text:'I can do all things through Christ which strengtheneth me.'},
  {book:'Psalms',chapter:23,verse:4,text:'Yea, though I walk through the valley of the shadow of death, I will fear no evil: for thou art with me; thy rod and thy staff they comfort me.'},
  {book:'Proverbs',chapter:3,verse:5,text:'Trust in the LORD with all thine heart; and lean not unto thine own understanding.'},
  {book:'James',chapter:4,verse:7,text:'Submit yourselves therefore to God. Resist the devil, and he will flee from you.'},
  {book:'Romans',chapter:8,verse:28,text:'And we know that all things work together for good to them that love God, to them who are the called according to his purpose.'},
  {book:'John',chapter:8,verse:32,text:'And ye shall know the truth, and the truth shall make you free.'},
  {book:'Isaiah',chapter:41,verse:10,text:'Fear thou not; for I am with thee: be not dismayed; for I am thy God: I will strengthen thee; yea, I will help thee.'},
  {book:'James',chapter:5,verse:16,text:'Confess your faults one to another, and pray one for another, that ye may be healed. The effectual fervent prayer of a righteous man availeth much.'},
  {book:'Romans',chapter:12,verse:12,text:'Rejoicing in hope; patient in tribulation; continuing instant in prayer;'},
  {book:'James',chapter:1,verse:12,text:'Blessed is the man that endureth temptation: for when he is tried, he shall receive the crown of life.'},
  {book:'Micah',chapter:6,verse:8,text:'He hath shewed thee, O man, what is good; and what doth the LORD require of thee, but to do justly, and to love mercy, and to walk humbly with thy God?'},
  {book:'Psalms',chapter:95,verse:6,text:'O come, let us worship and bow down: let us kneel before the LORD our maker.'},
  {book:'Genesis',chapter:1,verse:1,text:'In the beginning God created the heaven and the earth.'}
].map((v,i)=>({...v,id:i+1}));
export const options = {verses,enemies:[...enemies,...legacy.JOURNEY_ENEMIES],books,classes:legacy.CLASS_DEFS,legacyItems:legacy.ITEM_DEFS,legacyRelics:legacy.STORE_RELICS,conceptKeys:concepts.MULTIPLAYER_CATEGORY_KEYS,matchVerseConcept:concepts.matchVerseConcept,classifyVerseKeys:concepts.classifyVerseKeys};
export const rules = createRules(options);
export const make = (count=1,seed=317)=>rules.create({names:['Sean','Alex','John','Mary','Pat'].slice(0,count),seed,id:'run-test',now:100000});
let nextId = 0;
export const command = (state,type,fields={})=>({id:`cmd-${++nextId}`,runId:state.id,roomId:state.room.id,playerId:'p0',type,now:100001,...fields});
export const act = (state,type,fields={},context)=>rules.reduce(state,command(state,type,fields),context);
export function forceRoom(state,type) { const copy=structuredClone(state); copy.phase='path';copy.paths=[type];return act(copy,'path',{path:type}); }
