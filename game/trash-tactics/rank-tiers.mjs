export const STARTING_ELO = 1000;

const TIERS = [
  {min: 1900, title: 'Master', color: '#ed92d7'},
  {min: 1650, title: 'Diamond', color: '#8dbaff'},
  {min: 1400, title: 'Platinum', color: '#72e1ce'},
  {min: 1200, title: 'Gold', color: '#f8ce73'},
  {min: 1000, title: 'Silver', color: '#c3d2df'},
  {min: 0, title: 'Bronze', color: '#dc9d79'}
];

const UNRANKED = {title: 'Unranked', color: '#9da8b8'};

export function trashTacticsRank(stats) {
  if (!stats?.games || !Number.isSafeInteger(stats.rating)) return UNRANKED;
  return TIERS.find(tier => stats.rating >= tier.min) || UNRANKED;
}
