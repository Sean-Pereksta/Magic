import { isSupportedGameEvent } from "./event-system.js";
import { isBaseEvolutionTrigger } from "./base-evolution.js";

export const VALIDATION_LEVELS = Object.freeze({
  ERROR: "ERROR",
  WARNING: "WARNING",
  INFO: "INFO"
});

export const SUPPORTED_EFFECT_KEYS = Object.freeze(new Set([
  "trade", "combat", "heal", "authority", "draw", "opponentDiscard", "forceDiscard",
  "stun", "disable", "destroyBase", "damageAll", "lifelink", "scrapMarket", "marketErase",
  "scrapOwn", "purge", "shield", "tradePerBase", "combatPerBase", "healPerBase", "drawPerBase",
  "createToken", "drawFromDrawPile", "resurrect", "graveEcho", "reclaim", "redeploy", "coolHeat",
  "addHeat", "harvestHeat", "moveHeat", "armor", "repair", "advanceConstruction", "selfDamage",
  "topdeckFromHand", "topdeckFromDiscard", "repeatPrimaryChoice", "sacrificeRequired", "peekTop",
  "purgeAndDraw", "combatAgainstBases", "chooseAdditionalDifferentOption", "moveTrackedCreatedToken",
  "target", "authorityDamage", "preventDestruction", "cancelEvent", "reduceDiscard",
  "or", "choice", "chooseOne", "choose_one", "options"
]));

const CARD_EFFECT_FIELDS = new Set([
  "effect", "ally", "doubleAlly", "double_ally", "ally2", "sacrifice", "onPurchase",
  "constructionEffect", "additionalEffect", "ifChangedEffect", "effectPerHeat", "effectPerUnit"
]);
const VALID_CARD_TYPES = new Set(["ship", "base", "attachment"]);

function issue(level, code, message, cardId = "", path = "") {
  return { level, code, message, cardId, path };
}

function transformTargets(transform = {}) {
  const raw = Array.isArray(transform.choose)
    ? transform.choose
    : transform.choose && typeof transform.choose === "object"
      ? Object.values(transform.choose)
      : transform.into
        ? [transform.into]
        : [];
  return raw.map(option => typeof option === "string" ? option : option?.into).map(String).filter(Boolean);
}

function referencedEffects(value, path = "card", key = "") {
  if (!value || typeof value !== "object") return [];
  if (Array.isArray(value)) {
    return value.flatMap((entry, index) => referencedEffects(entry, `${path}[${index}]`, key));
  }
  const found = [];
  if (CARD_EFFECT_FIELDS.has(key)) found.push({ effect: value, path });
  for (const [childKey, child] of Object.entries(value)) {
    found.push(...referencedEffects(child, `${path}.${childKey}`, childKey));
  }
  return found;
}

function mentionedHeatThreshold(card) {
  const text = [card?.text, card?.heatText, card?.transformText].filter(Boolean).join(" ");
  const matches = [...text.matchAll(/(?:at|upon reaching)\s+heat\s+(\d+)/gi)];
  return [...new Set(matches.map(match => Number(match[1])).filter(Number.isFinite))];
}

function validateTransformGraph(cardsById, issues) {
  const graph = new Map();
  for (const card of cardsById.values()) {
    const targets = transformTargets(card.transform);
    if (targets.length) graph.set(card.id, targets);
    for (const targetId of targets) {
      const target = cardsById.get(targetId);
      if (!target) {
        issues.push(issue("ERROR", "TRANSFORM_TARGET_MISSING", `${card.id} transforms into missing card ${targetId}.`, card.id, "transform"));
        continue;
      }
      if (!target.transformedFrom) {
        issues.push(issue("WARNING", "MISSING_TRANSFORMED_FROM", `${targetId} is a transform target but has no transformedFrom value.`, targetId, "transformedFrom"));
      }
      if (card.type === "base" && target.type !== "base") {
        issues.push(issue("ERROR", "INVALID_BASE_EVOLUTION", `${card.id} is a Base but evolves into non-Base ${targetId}.`, card.id, "transform"));
      }
    }
  }

  const visiting = new Set();
  const visited = new Set();
  const visit = (cardId, path = []) => {
    if (visiting.has(cardId)) {
      issues.push(issue("ERROR", "TRANSFORM_CYCLE", `Transform cycle detected: ${[...path, cardId].join(" → ")}.`, cardId, "transform"));
      return;
    }
    if (visited.has(cardId)) return;
    visiting.add(cardId);
    for (const next of graph.get(cardId) || []) visit(next, [...path, cardId]);
    visiting.delete(cardId);
    visited.add(cardId);
  };
  for (const cardId of graph.keys()) visit(cardId);
}

function validateTrigger(trigger, card, path, issues) {
  if (!trigger || typeof trigger !== "object") return;
  const event = String(trigger.event || "");
  if (!event) {
    issues.push(issue("ERROR", "TRIGGER_EVENT_MISSING", `${card.id} has a trigger without an event.`, card.id, path));
  } else if (!isSupportedGameEvent(event)) {
    issues.push(issue("ERROR", "UNSUPPORTED_TRIGGER_EVENT", `${card.id} uses unsupported trigger event ${event}.`, card.id, path));
  }
  if (!trigger.effect || typeof trigger.effect !== "object") {
    issues.push(issue("WARNING", "TRIGGER_EFFECT_MISSING", `${card.id}'s ${event || "unnamed"} trigger has no effect.`, card.id, path));
  }
}

function validateReaction(reaction, card, path, issues) {
  if (!reaction || typeof reaction !== "object") return;
  const event = String(reaction.event || "");
  if (!event || !isSupportedGameEvent(event)) {
    issues.push(issue("ERROR", "UNSUPPORTED_REACTION_EVENT", `${card.id} uses unsupported Reaction event ${event || "(missing)"}.`, card.id, path));
  }
  if (!reaction.effect || typeof reaction.effect !== "object") {
    issues.push(issue("WARNING", "REACTION_EFFECT_MISSING", `${card.id}'s ${event || "unnamed"} Reaction has no effect.`, card.id, path));
  }
}

function validateBosses(bosses, cardsById, issues) {
  const bossIds = new Set();
  for (const boss of bosses || []) {
    if (!boss?.id) {
      issues.push(issue("ERROR", "BOSS_ID_MISSING", "Campaign boss is missing an id.", "", "bosses"));
      continue;
    }
    if (bossIds.has(boss.id)) issues.push(issue("ERROR", "DUPLICATE_BOSS_ID", `Duplicate campaign boss id: ${boss.id}.`, boss.id, "bosses"));
    bossIds.add(boss.id);
    if (boss.bossCard && !cardsById.has(boss.bossCard)) {
      issues.push(issue("ERROR", "INVALID_BOSS_CARD", `${boss.id} references missing boss card ${boss.bossCard}.`, boss.id, "bossCard"));
    } else if (boss.bossCard && cardsById.get(boss.bossCard)?.campaignOnly !== true) {
      issues.push(issue("WARNING", "BOSS_CARD_NOT_CAMPAIGN_ONLY", `${boss.id}'s Boss card ${boss.bossCard} is not marked campaignOnly.`, boss.id, "bossCard"));
    }
    for (const baseId of boss.startingBases || []) {
      const base = cardsById.get(typeof baseId === "string" ? baseId : baseId?.id);
      if (!base || base.type !== "base") {
        issues.push(issue("ERROR", "INVALID_BOSS_STARTING_BASE", `${boss.id} references invalid starting Base ${typeof baseId === "string" ? baseId : baseId?.id}.`, boss.id, "startingBases"));
      }
    }
    for (const [phaseIndex, phase] of (boss.phases || []).entries()) {
      if (phase.createBase) {
        const base = cardsById.get(phase.createBase);
        if (!base || base.type !== "base") {
          issues.push(issue("ERROR", "INVALID_BOSS_PHASE_BASE", `${boss.id} phase ${phase.id || phaseIndex} creates invalid Base ${phase.createBase}.`, boss.id, `phases[${phaseIndex}].createBase`));
        }
      }
    }
  }
}

export function validateCardLibrary({ cards = [], starterCards = {}, factions = {}, bosses = [] } = {}) {
  const issues = [];
  const cardsById = new Map();
  const validFactions = new Set(Object.keys(factions || {}));

  for (const [index, card] of [...Object.values(starterCards || {}), ...(Array.isArray(cards) ? cards : [])].entries()) {
    const path = `cards[${index}]`;
    if (!card?.id) {
      issues.push(issue("ERROR", "CARD_ID_MISSING", `Card at ${path} is missing an id.`, "", path));
      continue;
    }
    if (cardsById.has(card.id)) {
      issues.push(issue("ERROR", "DUPLICATE_CARD_ID", `Duplicate card id: ${card.id}.`, card.id, path));
    } else cardsById.set(card.id, card);
    if (!String(card.name || "").trim()) issues.push(issue("ERROR", "CARD_NAME_MISSING", `${card.id} is missing a name.`, card.id, "name"));
    if (!String(card.image || "").trim()) issues.push(issue("WARNING", "CARD_IMAGE_MISSING", `${card.id} is missing an image.`, card.id, "image"));
    if (!validFactions.has(card.faction)) issues.push(issue("ERROR", "UNKNOWN_FACTION", `${card.id} uses unknown faction ${card.faction}.`, card.id, "faction"));
    if (!VALID_CARD_TYPES.has(card.type)) issues.push(issue("ERROR", "UNKNOWN_CARD_TYPE", `${card.id} uses unknown card type ${card.type}.`, card.id, "type"));
    if (card.token && card.collectible !== false) issues.push(issue("ERROR", "TOKEN_IN_NORMAL_POOL", `${card.id} is a Token but is not excluded from normal collection pools.`, card.id, "collectible"));
    if (card.campaignOnly && card.collectible !== false) issues.push(issue("ERROR", "CAMPAIGN_CARD_IN_NORMAL_POOL", `${card.id} is campaign-only but is not excluded from normal pools.`, card.id, "collectible"));
    if (card.type === "attachment" && (!card.attachment || typeof card.attachment !== "object")) {
      issues.push(issue("ERROR", "INVALID_ATTACHMENT", `${card.id} is an Attachment without attachment configuration.`, card.id, "attachment"));
    }
    if (card.type !== "base" && card.attachmentSlots !== undefined) {
      issues.push(issue("WARNING", "ATTACHMENT_SLOTS_ON_NON_BASE", `${card.id} defines attachmentSlots but is not a Base.`, card.id, "attachmentSlots"));
    }
    if (card.type === "base" && card.attachmentSlots !== undefined && (!Number.isInteger(card.attachmentSlots) || card.attachmentSlots < 0)) {
      issues.push(issue("ERROR", "INVALID_ATTACHMENT_SLOTS", `${card.id} must define attachmentSlots as a non-negative integer.`, card.id, "attachmentSlots"));
    }
    if (card.type === "base" && card.transform && !isBaseEvolutionTrigger(card.transform.trigger)) {
      issues.push(issue("ERROR", "UNSUPPORTED_BASE_EVOLUTION_TRIGGER", `${card.id} uses unsupported Base evolution trigger ${card.transform.trigger || "(missing)"}.`, card.id, "transform.trigger"));
    }

    for (const { effect, path: effectPath } of referencedEffects(card, path)) {
      for (const key of Object.keys(effect)) {
        if (["label", "name", "text", "id"].includes(key)) continue;
        if (!SUPPORTED_EFFECT_KEYS.has(key)) {
          issues.push(issue("WARNING", "UNKNOWN_EFFECT_KEY", `${card.id} uses unknown effect key ${key}.`, card.id, `${effectPath}.${key}`));
        }
      }
    }

    const triggers = [card.trigger, ...(Array.isArray(card.triggers) ? card.triggers : [])].filter(Boolean);
    triggers.forEach((trigger, triggerIndex) => validateTrigger(trigger, card, `triggers[${triggerIndex}]`, issues));
    const reactions = [card.reaction, ...(Array.isArray(card.reactions) ? card.reactions : [])].filter(Boolean);
    reactions.forEach((reaction, reactionIndex) => validateReaction(reaction, card, `reactions[${reactionIndex}]`, issues));

    if (card.transform?.trigger === "heat") {
      const required = Math.max(1, Math.floor(Number(card.transform.required) || 1));
      const mentioned = mentionedHeatThreshold(card);
      if (mentioned.some(value => value !== required)) {
        issues.push(issue("WARNING", "HEAT_TEXT_MISMATCH", `${card.id} transforms at Heat ${required}, but its text mentions ${mentioned.map(value => `Heat ${value}`).join(", ")}.`, card.id, "transform.required"));
      }
    }
  }

  validateTransformGraph(cardsById, issues);
  validateBosses(bosses, cardsById, issues);
  issues.push(issue("INFO", "VALIDATION_SUMMARY", `Validated ${cardsById.size} War Realms card definitions.`, "", "cards"));

  const errors = issues.filter(item => item.level === VALIDATION_LEVELS.ERROR);
  const warnings = issues.filter(item => item.level === VALIDATION_LEVELS.WARNING);
  const info = issues.filter(item => item.level === VALIDATION_LEVELS.INFO);
  return { valid: errors.length === 0, errors, warnings, info, issues };
}

export function reportCardValidation(result, logger = console) {
  const grouped = {
    ERROR: result?.errors || [],
    WARNING: result?.warnings || [],
    INFO: result?.info || []
  };
  for (const [level, entries] of Object.entries(grouped)) {
    const method = level === "ERROR" ? "error" : level === "WARNING" ? "warn" : "info";
    for (const entry of entries) {
      logger?.[method]?.(`[War Realms ${level}] ${entry.code}: ${entry.message}`);
    }
  }
  return result;
}
