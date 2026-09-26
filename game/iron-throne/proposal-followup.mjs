import { consumeMessage } from './living.mjs';
import { validateIntent } from './diplomacy.mjs';
import { councilActive } from './council-state.mjs';

export const privateConversation = ruler => `private:${ruler}`;
const key = (actor, ruler) => `${actor}:${ruler}`;
export function expireFollowups(s, actor, conversation) {
  for (const [id,c] of Object.entries(s.proposalFollowups || {}))
    if (c.turn !== s.turn || c.actor === actor && (!conversation || c.conversation === conversation)) delete s.proposalFollowups[id];
}
export function requestedTerms(text) {
  if (/\b(?:do not|don't|never|no need to)\s+(?:send|make|offer|put|submit)/i.test(text)) return false;
  return /\b(?:send|submit|bring|put)\b.{0,45}\b(?:proposal|terms|agreement|offer)\b|\bwhat terms\b|\b(?:consider|review)\s+(?:your\s+|the\s+)?formal terms\b|\bmake me an offer\b/i.test(text);
}
// Ground an invitation in the subject actually discussed. No generic unlimited
// credit is issued for a greeting or an unrelated model suggestion.
export function topicIntent(s, message) {
  const target = s.kingdoms.find(k => new RegExp(`\\b${k.id}\\b`, 'i').test(message));
  const tile = message.match(/\b\d{1,3},\d{1,3}\b/)?.[0];
  const type = /\b(?:attack|invad\w*|campaign|war|strike)\b/i.test(message) && target ? 'JOINT_WAR'
    : /\bdefend\b/i.test(message) && tile ? 'DEFEND'
    : /\b(?:position|rally|move)\b/i.test(message) && tile ? 'POSITION'
    : /\b(?:build|fort)\b/i.test(message) && tile ? 'BUILD_DEFENSES'
    : /\b(?:vassal|allegiance|capitulation|submit)\b/i.test(message) ? 'VASSALAGE'
    : /\b(?:alliance|ally|renew|pact)\b/i.test(message) ? 'ALLIANCE'
    : /\b(?:peace|truce)\b/i.test(message) ? 'PEACE'
    : /\b(?:aid|gift|resources|supplies)\b/i.test(message) ? 'AID'
    : /\btrad\w*\b/i.test(message) ? 'TRADE' : null;
  return type ? validateIntent({type, targetId:['JOINT_WAR'].includes(type)?target.id:['DEFEND','POSITION','BUILD_DEFENSES'].includes(type)?tile:''}) : null;
}
export function grantFollowup(s, actor, ruler, conversation, message, response, { paid = false, requestedIntent = null } = {}) {
  if (!paid || !requestedTerms(response.reply || response.message || '')) return false;
  const intent = topicIntent(s, message) || validateIntent(requestedIntent);
  if (!intent) return false;
  s.proposalFollowups ||= {};
  s.proposalFollowups[key(actor,ruler)] = { actor, ruler, conversation, turn:s.turn, type:intent.type, targetId:intent.targetId };
  return true;
}
export function followupCredit(s, actor, ruler, conversation = privateConversation(ruler), proposal = null) {
  const c = s.proposalFollowups?.[key(actor,ruler)];
  if (!c || c.turn !== s.turn || c.conversation !== conversation) return null;
  if (conversation.startsWith('council-')) {
    const council = s.allianceCouncils?.find(x => x.id === conversation);
    if (!councilActive(s,council) || ![actor,ruler].every(id => council.participants.includes(id))) return null;
  }
  const i = proposal && validateIntent(proposal);
  return !proposal || i && i.type === c.type && i.targetId === c.targetId ? c : null;
}
export function consumeDiplomaticMessage(s, ruler, actor, proposal = null, conversation = privateConversation(ruler)) {
  if (proposal && !validateIntent(proposal)) return {ok:false,error:'Invalid proposal terms.'};
  if (proposal && followupCredit(s,actor,ruler,conversation,proposal)) {
    delete s.proposalFollowups[key(actor,ruler)];
    return {ok:true,free:true};
  }
  const spent = consumeMessage(s,ruler,actor);
  if (spent.ok) expireFollowups(s,actor,conversation);
  return spent;
}
