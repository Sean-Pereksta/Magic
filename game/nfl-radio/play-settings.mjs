import {storagePrefix} from './league.mjs';
import {ANNOUNCEMENT_MODES,PLAY_TYPES,normalizeAnnouncementMode} from './play-formatter.mjs';
export const STORAGE_KEY=`${storagePrefix}:livePlayByPlay`;
export const DEFAULT_SCORE_INTERVAL_MINUTES=5;
export const MAX_SCORE_INTERVAL_MINUTES=120;
export function normalizeScoreInterval(value){
  const number=Math.round(Number(value));
  return !Number.isFinite(number)||number<0?DEFAULT_SCORE_INTERVAL_MINUTES:Math.min(number,MAX_SCORE_INTERVAL_MINUTES);
}
export function readPlayByPlaySettings(storage=globalThis.localStorage){
  let saved={};try{saved=JSON.parse(storage?.getItem(STORAGE_KEY)||'null')||{};}catch{}
  const selectedPlayersByGame=Object.create(null);
  for(const [game,players] of Object.entries(saved.selectedPlayersByGame||{})){
    if(!Array.isArray(players))continue;
    selectedPlayersByGame[game]=players.filter(p=>p&&typeof p.id==='string'&&typeof p.name==='string').map(p=>({id:p.id,name:p.name,team:typeof p.team==='string'?p.team:''}));
  }
  const modeByGame=Object.create(null);
  for(const [game,mode] of Object.entries(saved.modeByGame||{})){
    if(mode==='off'||ANNOUNCEMENT_MODES.includes(mode))modeByGame[game]=mode;
  }
  const playTypes=Array.isArray(saved.playTypes)?[...new Set(saved.playTypes.filter(type=>Object.hasOwn(PLAY_TYPES,type)))]:[];
  return {
    enabled:!!saved.enabled,mode:normalizeAnnouncementMode(saved.mode),modeByGame,playTypes,selectedPlayersByGame,
    duckRadio:saved.duckRadio!==false,includeRadioGame:saved.includeRadioGame!==false,
    scoreIntervalMinutes:normalizeScoreInterval(saved.scoreIntervalMinutes??DEFAULT_SCORE_INTERVAL_MINUTES),
    scoreScope:saved.scoreScope==='all'?'all':'rotation'
  };
}
export function settingsForGame(settings,gameId){
  const id=String(gameId);
  return {...settings,mode:settings.modeByGame?.[id]||settings.mode,selectedPlayerIds:(settings.selectedPlayersByGame?.[id]||[]).map(p=>p.id)};
}
export function selectedGameIds(storage=globalThis.localStorage){
  try{const value=JSON.parse(storage?.getItem(`${storagePrefix}:rotation`)||'[]');return Array.isArray(value)?value.filter(id=>typeof id==='string'):[];}catch{return [];}
}
