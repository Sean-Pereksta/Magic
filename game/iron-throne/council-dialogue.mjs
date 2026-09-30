// Interpret conversation, not commands. These hints never move troops, grant
// trust, transfer resources, or turn an unconfirmed offer into a commitment.
const normalize = text => String(text).toLowerCase().replace(/[’‘]/g, "'");
export const dialogueKey = text => normalize(text).replace(/[^a-z0-9]+/g, ' ').trim();
const escapeRE = text => text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const mentions = (text, p) => [p.id, p.name.replace(/^House\s+/i, ''), p.ruler, p.ruler.replace(/^\S+\s+/, '')]
  .filter(Boolean).some(alias => new RegExp(`\\b${escapeRE(alias)}\\b`, 'i').test(text));

export function recentCouncilHistory(c, actor, message, turn) {
  const history = c.messages.filter(m => m.turn >= turn - 2 && !m.agreement);
  // Single-player has already appended this utterance; multiplayer has not.
  if (history.at(-1)?.speakerHouseId === actor && history.at(-1)?.message === message.trim()) return history.slice(0, -1);
  return history;
}

function aidOffer(text) {
  // Require a first-person offer in its own clause, rather than a bare 'aid'
  // keyword (which also occurs in requests, refusals and third-party claims).
  return normalize(text).split(/[.!?;]|\bbut\b/).some(clause => {
    const offer = clause.match(/\b(?:i|we)(?:'ll|'d|\s+(?:can|could|will|would|shall|am willing to|are willing to|want to|offer to))\s+(.{0,180})/);
    const offered = offer?.[1].split(/\b(?:if|unless|provided|as long as)\b/)[0];
    return offered && !/\b(?:not|no|never|cannot|can't|won't|wouldn't|don't)\b/.test(offered) &&
      /^(?:(?:provide|offer|send|bring|give)\b.{0,65}\b(?:aid|assistance|help|support|reinforcements|troops|soldiers|supplies|food|gold|wood|iron)\b|(?:help|aid|assist|support|reinforce|defend|protect)\b)/.test(offered);
  });
}

export function councilDiscussion(facts, c, actor, message, turn) {
  const history = recentCouncilHistory(c, actor, message, turn), text = normalize(message);
  const others = facts.participants.filter(p => p.id !== actor);
  let addressed = others.filter(p => mentions(text, p)).map(p => p.id);
  const explicitOffer = aidOffer(text);
  const previousPlayer = history.findLast(m => m.speakerHouseId === actor);
  const clarification = /\b(?:i (?:said|meant|offered)|already (?:said|offered)|my offer|our offer|you misunderstood)\b/.test(text);
  const details = /\b\d+\s*(?:troops|soldiers|spearmen|archers|cavalry|food|gold|wood|iron|turns?)\b|\b(?:next turn|this turn|where|when|how many|how much|what do you need)\b/.test(text);
  const withdrawn = /\b(?:cannot|can't|won't|will not|no longer|withdraw|retract)\b/.test(text);
  // Only a direct continuation can inherit an offer, never an unrelated topic
  // or a historical promise. A refusal overrides that inherited interpretation.
  const changedTopic = /\b(?:instead|peace|truce|trade|attack|invade|declare war|renew|marriage)\b/.test(text);
  const continuation = !withdrawn && !changedTopic && (clarification || details) && previousPlayer && aidOffer(previousPlayer.message);
  const offeringAid = explicitOffer || !!continuation;
  if (!addressed.length && continuation) addressed = others.filter(p => mentions(previousPlayer.message, p)).map(p => p.id);
  if (!addressed.length && offeringAid && !/\b(?:all of you|everyone|each house|one another)\b/.test(text)) {
    // 'I can help you' answers the most recent ruler who asked for relief.
    const last = history.findLast(m => m.speakerHouseId !== actor);
    if (last && /\b(?:frontier|survival|need help|need food|short of food|defend our|relief)\b/i.test(last.message)) addressed = [last.speakerHouseId];
  }
  const offerText = continuation ? `${previousPlayer.message} ${text}` : text;
  return {history, addressed, offeringAid, continuation:!!continuation, clarification,
    amount:offerText.match(/\b\d{1,4}\s*(?:troops|soldiers|spearmen|archers|cavalry|food|gold|wood|iron)\b/i)?.[0],
    timing:offerText.match(/\b(?:(?:next|this) turn|(?:in|within) \d{1,2} turns?|by turn \d{1,5})\b/i)?.[0],
    conditional:/\b(?:if|unless|provided|as long as|on condition|in return)\b/.test(continuation ? `${previousPlayer.message} ${text}` : text),
    details, withdrawn, question:/\?|\b(?:why|what|where|when|how)\b/.test(text)};
}

// Stable across reloads and clients: select unused phrasing from actual shared
// history, not Math.random or hidden state. At exhaustion avoid the last reply.
export function freshCouncilLine(history, speaker, lines) {
  const prior = history.filter(m => m.speakerHouseId === speaker).map(m => dialogueKey(m.message));
  return lines.find(line => !prior.includes(dialogueKey(line))) ||
    lines.find(line => dialogueKey(line) !== prior.at(-1)) || lines[0];
}

export function aidCouncilLines(p, discussion, recipient) {
  if (recipient && recipient.id !== p.id) return [
    `${recipient.ruler}, that offer of aid is addressed to you. What support would let you hold your ground while we consider the wider campaign?`,
    `Before we debate the next offensive, let ${recipient.ruler} and our host settle what help is possible. How would that change the duties you ask of my House?`,
    `I hear the offer to help ${recipient.name}. Let us establish its scope and timing before building a campaign around it.`
  ];
  const opening = discussion.clarification ? 'You are offering me help; I understand. ' : discussion.continuation ? 'Let us work through your offer of help. ' : 'Your offer of aid is welcome. ';
  const condition = discussion.conditional ? 'The conditions of your offer still need agreement. ' : '';
  const cautious = p.personality.paranoia >= .7 ? 'I must judge that support by what arrives. ' : '';
  const need = p.concern === 'food shortage' ? 'Food is my immediate concern.'
    : ['war survival','frontier threatened'].includes(p.concern) ? 'Help defending my frontier would address my immediate concern.'
    : p.concern === 'forces committed elsewhere' ? 'My existing duties still stand; relief could help us make room for a shared effort.'
    : 'Let us agree how our Houses can support one another.';
  const detail = discussion.amount ? `You mention ${discussion.amount}${discussion.timing ? ` ${discussion.timing}` : ''}; that is an offer we can discuss. ` : '';
  const question = discussion.amount && discussion.timing ? 'Where do you propose to send that help?'
    : discussion.amount ? 'When could that help arrive, and where do you propose to send it?'
    : discussion.details ? 'Are you considering troops or supplies? Name what you can spare so we can discuss where it would help.'
    : 'Are you offering troops to defend our lands, supplies, or support in a joint campaign?';
  return [
    `${opening}${condition}${cautious}${need} ${detail}${question}`,
    `${condition}I hear your offer of assistance. ${need} ${detail}${question} We can discuss the next offensive after that.`,
    `${opening}${condition}${need} ${detail}Let us settle the remaining terms of that aid. Until we agree and act, I cannot count it among my defenses.`
  ];
}
