// Simulation ownership is independent of the browser viewing the campaign.
export const DEFAULT_HOUSE = 'ashen';
export function controllerForHouse(s, houseId) {
  return s.controllers?.[houseId] || { kind: houseId === DEFAULT_HOUSE ? 'human' : 'ai', uid: null };
}
export const isHumanHouse = (s, id) => controllerForHouse(s, id).kind === 'human';
export const isAiHouse = (s, id) => !isHumanHouse(s, id) || !!controllerForHouse(s, id).substitute;
export const humanControlledHouseIds = s => s.kingdoms.filter(k => isHumanHouse(s,k.id)).map(k => k.id);
export const aiControlledHouseIds = s => s.kingdoms.filter(k => isAiHouse(s,k.id)).map(k => k.id);
export const localHouseId = s => s.viewHouseId || DEFAULT_HOUSE;
// Legacy schema-3 saves retain their existing shape. Online courts are per actor.
export function court(s, actorHouseId = DEFAULT_HOUSE) {
  if (!s.controllers) return { conversations: s.conversations, offers: s.diplomacy.offers, messages: s.diplomacy.messages };
  s.courts ||= {};
  return s.courts[actorHouseId] ||= { conversations: {}, offers: {}, messages: { turn:s.turn,regular:0,hosts:{} } };
}
export function resetMessages(s) {
  s.diplomacy.messages = { turn:s.turn,regular:0,hosts:{} };
  for (const c of Object.values(s.courts || {})) c.messages = { turn:s.turn,regular:0,hosts:{} };
}
