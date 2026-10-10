// A page navigation tears down polling/audio before entering the other league.
export const college = new URLSearchParams(globalThis.location?.search || '').get('league') === 'college';
export const leagueName = college ? 'College Football' : 'NFL';
export const storagePrefix = college ? 'college-dial' : 'nfl-dial';
export const API = `https://site.api.espn.com/apis/site/v2/sports/football/${college?'college-football':'nfl'}`;
// Explicit all-groups request avoids ESPN's default ranked-only college slate.
export const SCOREBOARD_URL = `${API}/scoreboard${college?'?groups=80&limit=1000':''}`;
export const isTop25 = game => [game.homeRank,game.awayRank].some(rank=>Number.isInteger(rank)&&rank>=1&&rank<=25);
