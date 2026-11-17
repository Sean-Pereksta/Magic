export const HUB_SERVICE = 'https://hub.catnmice.com';
export const TRASH_SERVICE = 'https://trash-tactics.catnmice.com';
export async function getServiceData(path, origin = HUB_SERVICE, fetchImpl = fetch) {
  const response = await fetchImpl(new URL(path, origin), {signal:AbortSignal.timeout(6000),credentials:'omit',redirect:'error',cache:'no-store'});
  if (!response.ok) throw new Error('Service unavailable (' + response.status + ').');
  return response.json();
}
export const getCounts = () => getServiceData('/v1/counts');
export function getRankings(game,season) {
  if (game==='trashtactics'||game==='trash-tactics') return getServiceData('/api/leaderboard',TRASH_SERVICE);
  if (!['bannerlands','catandmouse'].includes(game)) throw new Error('Unknown queue.');
  const params=new URLSearchParams({game});
  if(season)params.set('season',season);
  return getServiceData('/v1/leaderboard?'+params);
}
export const getTrashLobbies = () => getServiceData('/api/lobbies',TRASH_SERVICE);
