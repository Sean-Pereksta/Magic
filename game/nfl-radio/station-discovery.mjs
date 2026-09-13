// Public, no-login station stream discovery for the NFL Radio Dial.
//
// This deliberately does NOT scrape iHeart/Audacy/TuneIn session URLs or proxy
// restricted audio. It searches the open Radio Browser directory for public
// station-origin HTTPS streams, then lets the browser audio element prove whether
// each candidate actually plays. NFL game carriage is never inferred from a
// working general station stream: a broadcaster can substitute other programming
// online because of rights, blackout, or location rules.

const MIRRORS = [
  'https://de1.api.radio-browser.info',
  'https://nl1.api.radio-browser.info'
];

// Primary/alternate flagship names and call signs. Several teams have multiple
// entries because 2026 network changes or simulcasts mean more than one station is
// worth checking. These are search keys, not hard-coded stream URLs.
export const TEAM_STATION_SEARCHES = {
  ARI:['KMVP','Arizona Sports 98.7'],
  ATL:['WZGC','92.9 The Game'],
  BAL:['WIYY','98 Rock Baltimore','WBAL','ESPN 630','El Zol 107.9'],
  BUF:['WGRF','97 Rock Buffalo','WSKO 1260','WAIO','Rock 95.1 Rochester','WEPN 1050'],
  CAR:['WRFX','99.7 The Fox Charlotte'],
  CHI:['ESPN 1000 Chicago','WMVP','WBBM 780','WCFS 105.9'],
  CIN:['WCKY 1530','ESPN 1530','WEBN','102.7 WEBN','WLW 700'],
  CLE:['WKRK','92.3 The Fan Cleveland','WNCX 98.5','WKNR 850'],
  DAL:['KRLD-FM','105.3 The Fan Dallas','KMVK','La Grande 107.5'],
  DEN:['KOA 850','KOA 94.1','103.5 The Fox Denver'],
  DET:['WXYT-FM','97.1 The Ticket'],
  GB:['WRIT','95.7 BIG FM','WIBA 1310','WIXX 101.1'],
  HOU:['KILT 610','SportsRadio 610 Houston','KILT-FM 100.3','KLOL 101'],
  IND:['WFNI','107.5 The Fan Indianapolis','93.5 The Fan Indianapolis','WLHK','97.1 HANK FM'],
  JAX:['WJXL','1010 XL Jacksonville','92.5 FM Jacksonville'],
  KC:['KFNZ','96.5 The Fan Kansas City','WDAF-FM','106.5 The Wolf Kansas City'],
  LV:['KOMP','92.3 KOMP','KRLV','Raider Nation Radio 920'],
  LAC:['KLAC','AM 570 LA Sports','KYSR','ALT 98.7'],
  LAR:['KSPN','ESPN LA 710','KCBS-FM','93.1 Jack FM'],
  MIA:['WQAM','560 WQAM','WBGG','Big 105.9','WINZ 940','Fox Sports 940','WZTU','Tu 94.9'],
  MIN:['KFXN-FM','KFAN 100.3','KDWB 101.3','KQQL','Kool 108','KTCZ','Cities 97.1','KEEY','K102'],
  NE:['WBZ-FM','98.5 The Sports Hub'],
  NO:['WWL 870','WWL-FM 105.3','WWL New Orleans'],
  NYG:['WFAN','WFAN-FM','WFAN Sports Radio New York'],
  NYJ:['WAXQ','Q104.3','WEPN','ESPN New York 1050','98.7 ESPN New York'],
  PHI:['WIP-FM','SportsRadio 94WIP'],
  PIT:['WDVE','102.5 DVE','WBGG 970','Fox Sports 970'],
  SEA:['KIRO 710','Seattle Sports 710','KIRO-FM 97.3'],
  SF:['KNBR 680','KSAN','107.7 The Bone'],
  TB:['WXTB','98ROCK Tampa Bay','WDAE','95.3 WDAE','620 WDAE'],
  TEN:['WGFX','104.5 The Zone'],
  WAS:['WBIG-FM','BIG 100','WTEM','910 The Fan Richmond','WRVA 1140']
};

const clean = value => String(value || '').toLowerCase().replace(/[^a-z0-9]+/g,' ').trim();
const codecScore = codec => ({MP3:5,AAC:4,'AAC+':4,OGG:2,OPUS:2}[String(codec||'').toUpperCase()] || 0);

export function isUsablePublicStream(station){
  const streamUrl = station?.url_resolved || station?.url || '';
  if(Number(station?.lastcheckok) !== 1) return false;
  if(Number(station?.ssl_error) === 1) return false;
  if(!/^https:\/\//i.test(streamUrl)) return false;
  if(!codecScore(station?.codec)) return false;
  return true;
}

function score(station,search){
  const name=clean(station?.name), q=clean(search);
  const exact=name===q?100:name.includes(q)||q.includes(name)?55:0;
  const callsign=q.split(' ').find(v=>/^[kw][a-z]{2,4}(?:fm|am)?$/i.test(v));
  const callMatch=callsign&&name.replace(/\s/g,'').includes(callsign.replace(/\s/g,''))?45:0;
  const votes=Math.min(25,Math.log2(Number(station?.votes||0)+1)*4);
  const bitrate=Math.min(12,Number(station?.bitrate||0)/16);
  return exact+callMatch+votes+bitrate+codecScore(station?.codec);
}

function toFeed(station,teamId,search){
  const streamUrl=station.url_resolved||station.url;
  return {
    id:`radio-browser:${station.stationuuid||encodeURIComponent(streamUrl)}`,
    name:station.name||search,
    team:teamId,
    kind:'station',
    language:'en',
    access:'public-direct',
    publicDirectory:true,
    gameAudio:false,
    streamUrl,
    sourceUrl:station.homepage||'https://www.radio-browser.info/',
    codec:String(station.codec||'').toUpperCase(),
    bitrate:Number(station.bitrate||0),
    hls:Number(station.hls||0)===1,
    stationUuid:station.stationuuid||'',
    search,
    directoryCheckedAt:station.lastchecktime_iso8601||station.lastcheckoktime_iso8601||null,
    note:'FREE IN APP · Public station stream. NFL game carriage may be replaced or unavailable online because the broadcaster controls rights and location rules.'
  };
}

async function fetchJson(url,fetchImpl,timeout=7000){
  const controller=new AbortController();
  const timer=setTimeout(()=>controller.abort(),timeout);
  try{
    const response=await fetchImpl(url,{cache:'no-store',signal:controller.signal,headers:{Accept:'application/json'}});
    if(!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.json();
  } finally { clearTimeout(timer); }
}

async function searchOne(search,fetchImpl){
  const params=new URLSearchParams({name:search,countrycode:'US',hidebroken:'true',limit:'25',order:'votes',reverse:'true'});
  let lastError=null;
  for(const mirror of MIRRORS){
    try{
      const data=await fetchJson(`${mirror}/json/stations/search?${params}`,fetchImpl);
      return Array.isArray(data)?data:[];
    }catch(error){lastError=error;}
  }
  if(lastError) throw lastError;
  return [];
}

export async function discoverTeamStreams(teamId,{fetchImpl=globalThis.fetch}={}){
  const searches=TEAM_STATION_SEARCHES[teamId]||[];
  if(!searches.length||typeof fetchImpl!=='function') return [];
  const settled=await Promise.allSettled(searches.map(search=>searchOne(search,fetchImpl).then(rows=>({search,rows}))));
  const candidates=[];
  for(const result of settled){
    if(result.status!=='fulfilled') continue;
    const {search,rows}=result.value;
    for(const station of rows){
      if(!isUsablePublicStream(station)) continue;
      candidates.push({feed:toFeed(station,teamId,search),score:score(station,search)});
    }
  }
  const byUrl=new Map();
  for(const item of candidates){
    const current=byUrl.get(item.feed.streamUrl);
    if(!current||item.score>current.score) byUrl.set(item.feed.streamUrl,item);
  }
  return [...byUrl.values()].sort((a,b)=>b.score-a.score||b.feed.bitrate-a.feed.bitrate||a.feed.name.localeCompare(b.feed.name)).map(item=>item.feed);
}

export function mergeInAppQueues(...queues){
  const seen=new Set(), out=[];
  for(const feed of queues.flat()){
    if(!feed?.streamUrl||seen.has(feed.streamUrl)) continue;
    seen.add(feed.streamUrl);out.push(feed);
  }
  return out;
}
