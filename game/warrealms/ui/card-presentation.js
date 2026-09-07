const ACTION_STATUS = Object.freeze({
  "play-card": { icon: "▶", label: "Tap to play" },
  "buy-market": { icon: "◆", label: "Tap to acquire" },
  "forced-discard": { icon: "↘", label: "Tap to discard" },
  "attack-base": { icon: "⚔", label: "Tap to attack" },
  "attack-player": { icon: "⚔", label: "Tap to attack" }
});

function whole(value) {
  return Math.max(0, Math.floor(Number(value) || 0));
}

function status(kind, icon, value, label, priority) {
  return Object.freeze({ kind, icon, value: value == null ? "" : String(value), label, priority });
}

export function buildCompactCardStatuses(input = {}) {
  const statuses = [];
  if (input.heat != null) {
    const heat = whole(input.heat);
    statuses.push(status("heat", "♨", heat, `${heat} stored Heat`, 0));
  }
  if (input.charge != null) {
    const charge = whole(input.charge);
    statuses.push(status("charge", "⚡", charge, `${charge} stored Charge${charge === 1 ? "" : "s"}`, 10));
  }
  if (input.ascension) {
    const remaining = whole(input.ascension.remaining);
    statuses.push(status(
      remaining ? "ascension" : "ready",
      "↟",
      remaining || "✓",
      remaining
        ? `${remaining} owner turn${remaining === 1 ? "" : "s"} until ${input.ascension.nextName || "transformation"}`
        : `Ready to transform into ${input.ascension.nextName || "a new form"}`,
      20
    ));
  } else if (input.transformReady) {
    statuses.push(status("ready", "✦", "", "Evolution or transformation choice ready", 20));
  }
  if (input.cooldown > 0) statuses.push(status("cooldown", "◷", whole(input.cooldown), `${whole(input.cooldown)} turn cooldown remaining`, 25));
  if (input.activated) statuses.push(status("activated", "✓", "", "Activated this turn", 28));
  if (input.disabled) statuses.push(status("disabled", "⏸", "", "Disabled until the next turn", 30));
  if (input.armor > 0) statuses.push(status("armor", "⬡", whole(input.armor), `${whole(input.armor)} Armor`, 40));
  if (input.health) {
    const current = whole(input.health.current);
    const maximum = Math.max(1, whole(input.health.maximum));
    statuses.push(status("health", "♥", current, `${current} of ${maximum} persistent Health`, 50));
  }
  if (input.construction > 0) statuses.push(status("construction", "⚒", whole(input.construction), `${whole(input.construction)} Construction remaining`, 60));
  if (input.attachments) {
    const count = whole(input.attachments.count);
    const capacity = whole(input.attachments.capacity);
    statuses.push(status("attachment", "⌁", `${count}/${capacity}`, `${count} of ${capacity} Attachment slots used`, 70));
  }
  if (input.isAttachment) statuses.push(status("attachment", "⌁", "", "Attachment", 75));
  if (input.token) statuses.push(status("token", "◇", "", "Token", 80));
  if (input.personalDiscount) statuses.push(status("discount", "◆", "−1", "Your personal Trade discount", 90));
  if (input.directAction) {
    const action = ACTION_STATUS[input.directAction] || { icon: "●", label: "Tap to act" };
    statuses.push(status("action", action.icon, "", action.label, 100));
  }
  return statuses.sort((left, right) => left.priority - right.priority);
}

export function transformationPresentation(input = {}) {
  const fromName = String(input.fromName || "Card");
  const toName = String(input.toName || "New Form");
  const explicit = String(input.presentation || "").toLowerCase();
  const method = String(input.method || "").toLowerCase();
  const names = `${fromName} ${toName}`.toLowerCase();
  let eyebrow = "TRANSFORMATION";
  let complete = "TRANSFORMATION COMPLETE";
  if (explicit === "hatch" || names.includes("egg")) {
    eyebrow = "HATCHING";
    complete = "HATCH COMPLETE";
  } else if (explicit === "ascension" || method.includes("ownerturn") || names.includes("ascend")) {
    eyebrow = "ASCENSION";
    complete = "ASCENSION COMPLETE";
  } else if (explicit === "evolution" || explicit === "evolve") {
    eyebrow = "EVOLUTION";
    complete = "EVOLUTION COMPLETE";
  }
  return Object.freeze({ eyebrow, complete, fromName, toName });
}

export function transformationTimings(reducedMotion = false) {
  return reducedMotion
    ? Object.freeze({ travelIn: 140, build: 90, flash: 70, reveal: 220, travelOut: 160 })
    : Object.freeze({ travelIn: 360, build: 620, flash: 140, reveal: 700, travelOut: 430 });
}
