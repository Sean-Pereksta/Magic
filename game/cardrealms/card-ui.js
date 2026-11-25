/* Shared presentation for Orbital Rivals. Game rules stay in each game's engine. */
(function (root) {
  "use strict";

  const BASIC_EFFECTS = {
    trade: ["trade", "Trade", "trade to buy cards"],
    combat: ["combat", "Combat", "combat to spend on attacks"],
    authority: ["authority", "Health", "authority (player health)"],
    draw: ["draw", "Draw", "cards now"]
  };
  const FACTION_NAMES = {
    tf: "Trade Federation", blob: "Blob", se: "Star Empire", mc: "Machine Cult",
    sd: "Solar Dominion", vs: "Void Syndicate", n: "Neutral"
  };
  const signed = n => n > 0 ? `+${n}` : String(n);
  const plural = (n, singular, multiple = `${singular}s`) => Math.abs(n) === 1 ? singular : multiple;
  const basicDescription = (type, amount) => type === "draw"
    ? `Draw ${amount} ${plural(amount, "card")} now.`
    : `Gain ${amount} ${BASIC_EFFECTS[type][2]}.`;

  // Only primary effects are summarized together. Ally and scrap call this
  // separately, so conditional rewards can never inflate the printed gains.
  function effectSummary(steps) {
    const result = [];
    for (const step of steps || []) {
      if (!step || typeof step.t !== "string") continue;
      const n = Number.isFinite(Number(step.n)) ? Number(step.n) : 1;
      const basic = BASIC_EFFECTS[step.t];
      if (basic) {
        const old = result.find(item => item.type === step.t);
        if (old) {
          old.amount += n;
          old.value = signed(old.amount);
          old.description = basicDescription(step.t, old.amount);
        } else {
          result.push({ type: step.t, icon: basic[0], amount: n, value: signed(n), label: basic[1], description: basicDescription(step.t, n) });
        }
        continue;
      }
      let effect;
      switch (step.t) {
        case "opp_discard":
          effect = { icon: "discard", value: String(n), label: "Enemy discard", description: `Your opponent discards ${n} ${plural(n, "card")}.` };
          break;
        case "opp_next_draw":
          effect = { icon: "nextDraw", value: signed(n), label: "Enemy next draw", description: `Your opponent draws ${Math.abs(n)} ${n < 0 ? "fewer" : "more"} ${plural(n, "card")} next turn.` };
          break;
        case "scrap_hand":
          effect = { icon: "scrap", value: `${n}H`, label: "Scrap from hand", description: `Scrap ${n} ${plural(n, "card")} from your hand, if available.` };
          break;
        case "scrap_discard":
          effect = { icon: "scrap", value: `${n}D`, label: "Scrap from discard", description: `Scrap ${n} ${plural(n, "card")} from your discard pile, if available.` };
          break;
        case "scrap_trade_row":
          effect = { icon: "market", value: `${n || 1}`, label: "Scrap market", description: `Scrap ${n || 1} ${plural(n || 1, "card")} from the trade row, if available; empty slots refill.` };
          break;
        case "copy_next_ship":
          effect = { icon: "copy", value: `${n || 1}`, label: "Copy next ship", description: `Copy the primary effects of the next ${n || 1} ${plural(n || 1, "ship")} you play this turn (excluding additional copy effects).` };
          break;
        case "gain_per_base": {
          const stat = BASIC_EFFECTS[step.stat || "trade"] || BASIC_EFFECTS.trade;
          const per = n || 1;
          const target = step.outpostsOnly ? "outpost" : "base";
          effect = { icon: stat[0], value: `${signed(per)}/${step.outpostsOnly ? "O" : "B"}`, label: `${stat[1]} per ${target}`, description: `Gain ${per} ${stat[2]} for each of your ${target}s in play.` };
          break;
        }
        default:
          effect = { icon: "special", value: "!", label: "Special effect", description: "See the full card rules below." };
      }
      result.push({ type: step.t, amount: n, ...effect });
    }
    return result;
  }

  function defenseSummary(card, context) {
    if (card.type !== "base") return null;
    const max = Number(card.defense) || 0;
    const inBattle = context === "myBase" || context === "enemyBase";
    const current = inBattle ? (card.curDefense ?? max) : max;
    return {
      type: "defense", icon: "defense", value: inBattle ? `${current}/${max}` : String(max), label: "Defense",
      description: inBattle ? `${current} of ${max} defense remains.` : `${max} defense when deployed.`
    };
  }

  function el(tag, className, text) {
    const node = root.document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function artIcon(name) {
    const node = el("span", "cr-icon");
    node.setAttribute("aria-hidden", "true");
    node.innerHTML = root.CardRealmsArt.icon(name);
    return node;
  }

  function statBadge(effect, detail = false) {
    const node = el("span", `${detail ? "cr-detail-stat" : "cr-stat"} cr-stat-${effect.type}`);
    node.append(artIcon(effect.icon));
    if (detail) {
      node.append(el("span", "cr-detail-text", effect.description));
    } else {
      node.append(el("strong", "cr-stat-value", effect.value));
      node.title = `${effect.label}: ${effect.description}`;
      node.setAttribute("aria-label", `${effect.label}: ${effect.description}`);
    }
    return node;
  }

  let inspector = null;
  let backdrop = null;
  let active = null;
  let dismissedAnchor = null;
  let closeTimer = null;
  let hoverTimer = null;
  let suppressFocus = false;
  let touchInput = false;
  let listenersInstalled = false;

  function clearCloseTimer() {
    if (closeTimer !== null) root.clearTimeout(closeTimer);
    closeTimer = null;
  }

  function clearHoverTimer() {
    if (hoverTimer !== null) root.clearTimeout(hoverTimer);
    hoverTimer = null;
  }

  function closeInspector(restoreFocus = false) {
    clearCloseTimer();
    clearHoverTimer();
    const previous = active;
    active = null;
    if (inspector) inspector.hidden = true;
    if (backdrop) backdrop.hidden = true;
    if (previous) {
      if (restoreFocus) dismissedAnchor = previous.anchor;
      previous.anchor.removeAttribute("aria-describedby");
      previous.anchor.querySelector(".cr-primary")?.removeAttribute("aria-describedby");
      previous.info.setAttribute("aria-expanded", "false");
    }
    if (restoreFocus && previous && previous.returnFocus && previous.returnFocus.isConnected) {
      suppressFocus = true;
      previous.returnFocus.focus({ preventScroll: true });
      suppressFocus = false;
    }
  }

  function scheduleClose() {
    clearCloseTimer();
    if (!active || active.pinned) return;
    closeTimer = root.setTimeout(() => {
      if (!active || active.pinned) return;
      const focused = root.document.activeElement;
      if (active.anchor.contains(focused) || inspector.contains(focused)) return;
      closeInspector();
    }, 180);
  }

  function placeInspector() {
    if (!active || !inspector || inspector.hidden) return;
    if (!active.anchor.isConnected) { closeInspector(); return; }
    const width = root.innerWidth || root.document.documentElement.clientWidth;
    const height = root.innerHeight || root.document.documentElement.clientHeight;
    const margin = 10;
    const anchor = active.anchor.getBoundingClientRect();
    let panelWidth = Math.min(328, Math.max(0, width - margin * 2));
    let panelHeight = Math.max(0, height - margin * 2);
    let hoverSide = null;
    let groupLeft = anchor.left, groupRight = anchor.right;
    if (!active.pinned) {
      // Put hover details outside the whole card group, so moving to another
      // card never requires clicking through its neighbor's inspector.
      const siblings = active.anchor.parentElement?.children || [];
      for (const sibling of siblings) {
        if (!sibling.classList.contains("cr-card")) continue;
        const rect = sibling.getBoundingClientRect();
        groupLeft = Math.min(groupLeft, rect.left);
        groupRight = Math.max(groupRight, rect.right);
      }
      const leftSpace = groupLeft - margin * 2;
      const rightSpace = width - groupRight - margin * 2;
      if (Math.max(leftSpace, rightSpace) >= 220) {
        hoverSide = rightSpace >= leftSpace ? "right" : "left";
        panelWidth = Math.min(panelWidth, Math.max(leftSpace, rightSpace));
      } else {
        // Narrow screens have room above or below a card. Bound the panel to
        // that space and let its details scroll instead of covering the card.
        const above = anchor.top - margin * 2;
        const below = height - anchor.bottom - margin * 2;
        hoverSide = below >= above ? "below" : "above";
        panelHeight = Math.max(0, Math.max(above, below));
      }
    }
    inspector.style.width = `${panelWidth}px`;
    inspector.style.maxHeight = `${panelHeight}px`;
    const panel = inspector.getBoundingClientRect();
    let left, top;
    if (hoverSide) {
      if (hoverSide === "left" || hoverSide === "right") {
        left = hoverSide === "right" ? groupRight + margin : groupLeft - panel.width - margin;
        top = anchor.top;
      } else {
        left = anchor.left + (anchor.width - panel.width) / 2;
        top = hoverSide === "above" ? anchor.top - panel.height - margin : anchor.bottom + margin;
      }
    } else if (active.pinned && width <= 600) {
      left = (width - panel.width) / 2;
      top = height - panel.height - margin;
    } else {
      left = anchor.right + margin;
      top = anchor.top;
      if (left + panel.width > width - margin) {
        left = anchor.left - panel.width - margin;
        if (left < margin) {
          left = anchor.left + (anchor.width - panel.width) / 2;
          top = anchor.top - panel.height - margin;
          if (top < margin) top = anchor.bottom + margin;
        }
      }
    }
    left = Math.max(margin, Math.min(left, width - panel.width - margin));
    top = Math.max(margin, Math.min(top, height - panel.height - margin));
    inspector.style.left = `${left}px`;
    inspector.style.top = `${top}px`;
  }

  function ensureInspector() {
    if (inspector) return;
    backdrop = el("div", "cr-inspector-backdrop");
    backdrop.hidden = true;
    backdrop.setAttribute("aria-hidden", "true");
    backdrop.addEventListener("click", event => {
      event.preventDefault();
      event.stopPropagation();
      closeInspector(true);
    });
    // Dismissal never reaches the game below; prevent scroll-through as well.
    backdrop.addEventListener("pointerdown", event => event.preventDefault());
    backdrop.addEventListener("wheel", event => event.preventDefault(), { passive: false });
    backdrop.addEventListener("touchmove", event => event.preventDefault(), { passive: false });

    inspector = el("section", "cr-inspector");
    inspector.id = "cr-inspector";
    inspector.hidden = true;
    inspector.tabIndex = -1;
    inspector.style.position = "fixed";
    inspector.style.overflowY = "auto";
    inspector.addEventListener("pointerenter", () => {
      clearCloseTimer();
      clearHoverTimer();
    });
    inspector.addEventListener("pointerleave", scheduleClose);
    inspector.addEventListener("focusin", clearCloseTimer);
    inspector.addEventListener("focusout", scheduleClose);
    inspector.addEventListener("click", event => event.stopPropagation());
    root.document.body.append(backdrop, inspector);
    if (listenersInstalled) return;
    listenersInstalled = true;
    root.document.addEventListener("pointerdown", event => {
      touchInput = event.pointerType === "touch";
      if (active && !active.pinned && !inspector.contains(event.target) && !active.anchor.contains(event.target)) closeInspector();
    }, true);
    root.document.addEventListener("keydown", event => {
      touchInput = false;
      if (!active) return;
      if (event.key === "Escape") {
        event.preventDefault();
        event.stopImmediatePropagation();
        dismissedAnchor = active.anchor;
        closeInspector(active.pinned);
      } else if (event.key === "Tab" && active.pinned) {
        // The inspector has a single control. Keep keyboard focus in the pinned
        // dialog until Close/Escape returns it to the original card.
        event.preventDefault();
        inspector.querySelector(".cr-inspector-close").focus();
      }
    }, true);
    root.addEventListener("resize", placeInspector);
    root.addEventListener("scroll", event => {
      if (!active || (event.target && event.target.nodeType && inspector.contains(event.target))) return;
      if (active.pinned) placeInspector();
      else closeInspector();
    }, true);
  }

  function detailSection(title, effects, help) {
    const section = el("div", "cr-inspector-section");
    section.append(el("h4", "", title));
    if (help) section.append(el("p", "cr-inspector-note", help));
    for (const effect of effects) section.append(statBadge(effect, true));
    return section;
  }

  function openInspector(anchor, card, options, info, pinned = false, returnFocus = info) {
    ensureInspector();
    clearCloseTimer();
    clearHoverTimer();
    if (!pinned && dismissedAnchor === anchor) return;
    if (active && active.pinned && !pinned) return;
    if (active && active.anchor === anchor && active.pinned === pinned) return;
    dismissedAnchor = null;
    closeInspector();
    const context = options.context || "generic";
    const faction = options.factionName || FACTION_NAMES[card.faction] || FACTION_NAMES.n;
    const type = card.type === "base" ? (card.outpost ? "Outpost" : "Base") : "Ship";
    const defense = defenseSummary(card, context);
    const hasTriggered = card.allyTriggered && ["inPlay", "enemyInPlay", "myBase", "enemyBase"].includes(context);
    active = { anchor, card, options, info, pinned, returnFocus };
    inspector.replaceChildren();
    inspector.dataset.pinned = String(pinned);
    inspector.setAttribute("role", pinned ? "dialog" : "region");
    if (pinned) inspector.setAttribute("aria-modal", "true");
    else inspector.removeAttribute("aria-modal");
    inspector.setAttribute("aria-labelledby", "cr-inspector-title");
    inspector.style.setProperty("--cr-faction", root.CardRealmsArt.factionColor(card.faction));
    const header = el("div", "cr-inspector-header");
    const heading = el("div");
    const title = el("h3", "cr-inspector-title", card.name);
    title.id = "cr-inspector-title";
    heading.append(title, el("p", "cr-inspector-meta", `${faction} · ${type} · Cost ${card.cost ?? 0} trade`));
    const close = el("button", "cr-inspector-close");
    close.type = "button";
    close.title = "Close card details";
    close.setAttribute("aria-label", "Close card details");
    close.append(artIcon("close"));
    close.addEventListener("click", () => {
      dismissedAnchor = anchor;
      closeInspector(pinned);
    });
    header.append(heading, close);
    const art = el("div", "cr-inspector-art");
    art.innerHTML = root.CardRealmsArt.illustration(card);
    inspector.append(header, art);
    if (defense) {
      inspector.append(detailSection(type, [defense], card.outpost ? "Protects your authority: destroy all outposts before attacking player health." : "Stays in play between turns until destroyed."));
    }
    inspector.append(detailSection(card.type === "base" ? "On deploy / each turn" : "On play", effectSummary(card.play)));
    if (card.ally && card.ally.length) {
      inspector.append(detailSection(hasTriggered ? "Ally · resolved this turn" : "Ally · 2+ same faction", effectSummary(card.ally), "Triggers once when you have played two or more cards of this faction this turn."));
    }
    if (card.scrap && card.scrap.length) {
      inspector.append(detailSection("Optional scrap", effectSummary(card.scrap), "Permanently remove this card from play to gain this effect."));
    }
    if (card.text) {
      const rules = el("div", "cr-inspector-rules");
      rules.append(el("h4", "", "Full card rules"), el("p", "", card.text));
      inspector.append(rules);
    }
    inspector.append(el("p", "cr-inspector-help", pinned ? "Close or press Esc to return to the board." : "Press I or tap the info button to keep these details open."));
    anchor.setAttribute("aria-describedby", inspector.id);
    anchor.querySelector(".cr-primary")?.setAttribute("aria-describedby", inspector.id);
    info.setAttribute("aria-expanded", "true");
    backdrop.hidden = !pinned;
    inspector.hidden = false;
    placeInspector();
    if (pinned) close.focus({ preventScroll: true });
  }

  function createCard(card, options = {}) {
    const context = options.context || "generic";
    const faction = options.factionName || FACTION_NAMES[card.faction] || FACTION_NAMES.n;
    const type = card.type === "base" ? (card.outpost ? "Outpost" : "Base") : "Ship";
    const typeIcon = type.toLowerCase();
    const node = el("div", `card cr-card cr-context-${context} ${card.faction || "n"}`);
    node.dataset.cardKey = card.key || "";
    node.dataset.cardUid = String(card.uid ?? "");
    node.style.setProperty("--cr-faction", root.CardRealmsArt.factionColor(card.faction));
    node.setAttribute("role", "group");
    node.setAttribute("aria-label", card.name);
    if (card.outpost) node.classList.add("cr-outpost");
    const effects = effectSummary(card.play);
    const defense = defenseSummary(card, context);
    if (defense) effects.push(defense);
    const summary = effects.map(effect => effect.description).join(" ");
    const action = { hand: "Play", trade: "Buy", enemyBase: "Attack", modal: "Select" }[context] || "Inspect";
    const primary = el("button", "cr-primary");
    primary.type = "button";
    primary.setAttribute("aria-disabled", "false");
    primary.setAttribute("aria-label", `${action} ${card.name}. ${faction} ${type}. Cost ${card.cost ?? 0} trade. ${summary} Press I for full details.`);
    node.append(primary);

    const top = el("div", "cr-card-top");
    const cost = el("span", "cr-cost");
    cost.title = `Costs ${card.cost ?? 0} trade`;
    cost.setAttribute("aria-label", cost.title);
    cost.append(artIcon("trade"), el("b", "", card.cost ?? 0));
    const badge = el("span", "cr-type");
    badge.append(artIcon(typeIcon), el("span", "", type));
    top.append(cost, badge);
    const art = el("div", "cr-art");
    art.innerHTML = root.CardRealmsArt.illustration(card);
    const mark = el("span", "cr-faction");
    mark.title = faction;
    mark.setAttribute("aria-label", faction);
    mark.innerHTML = root.CardRealmsArt.factionMark(card.faction);
    art.append(mark);
    const heading = el("div", "cr-heading");
    const name = el("div", "cr-name", card.name);
    const info = el("button", "cr-info");
    info.type = "button";
    info.title = `Inspect ${card.name}`;
    info.setAttribute("aria-label", info.title);
    info.setAttribute("aria-haspopup", "dialog");
    info.setAttribute("aria-expanded", "false");
    info.append(artIcon("info"));
    info.addEventListener("click", event => {
      event.preventDefault();
      event.stopPropagation();
      openInspector(node, card, options, info, true, info);
    });
    heading.append(name, info);
    const gains = el("div", "cr-effects");
    gains.setAttribute("aria-label", "Primary effects");
    effects.forEach(effect => gains.append(statBadge(effect)));
    node.append(top, art, heading, gains);

    const triggers = el("div", "cr-triggers");
    for (const trigger of ["ally", "scrap"]) {
      if (!card[trigger] || !card[trigger].length) continue;
      const effects = effectSummary(card[trigger]);
      const row = el("div", `cr-trigger ${trigger === "ally" ? "cr-ally" : "cr-scrap-effect"}`);
      const triggered = trigger === "ally" && card.allyTriggered && ["inPlay", "enemyInPlay", "myBase", "enemyBase"].includes(context);
      if (trigger === "ally") row.dataset.triggered = String(!!triggered);
      if (triggered) row.classList.add("is-triggered");
      const label = trigger === "ally" ? (triggered ? "Ally resolved" : "Ally with 2+ same faction") : "Optional scrap of this card";
      row.title = `${label}: ${effects.map(effect => effect.description).join(" ")}`;
      row.setAttribute("aria-label", row.title);
      const triggerLabel = el("span", "cr-trigger-label");
      triggerLabel.append(artIcon(trigger), el("small", "", trigger === "ally" ? (triggered ? "✓" : "2+") : ""));
      const values = el("span", "cr-trigger-values");
      effects.forEach(effect => values.append(statBadge(effect)));
      row.append(triggerLabel, values);
      triggers.append(row);
    }
    if (triggers.childElementCount) node.append(triggers);

    if (context === "inPlay" && card.scrap && card.scrap.length && typeof options.onScrap === "function") {
      const scrap = el("button", "cr-scrap");
      scrap.type = "button";
      scrap.disabled = options.canScrap === false;
      scrap.append(artIcon("scrap"), el("span", "", "Scrap"));
      scrap.setAttribute("aria-label", `Scrap ${card.name} permanently for its scrap effect`);
      scrap.addEventListener("click", event => {
        event.preventDefault();
        event.stopPropagation();
        if (!scrap.disabled) {
          closeInspector();
          options.onScrap();
        }
      });
      node.append(scrap);
    }

    // Callers attach their existing onclick for play/buy/attack/select. Capture
    // guards keep those actions separate from inspecting a disabled card.
    node.addEventListener("click", event => {
      if (event.target.closest(".cr-info, .cr-scrap")) return;
      if (node.classList.contains("disabled")) {
        event.preventDefault();
        event.stopImmediatePropagation();
      }
    }, true);
    node.addEventListener("click", event => {
      if (event.target.closest(".cr-info, .cr-scrap")) return;
      if (!node.onclick) openInspector(node, card, options, info, true, primary);
    });
    node.addEventListener("keydown", event => {
      if (event.target !== primary) return;
      if (event.key.toLowerCase() === "i") {
        event.preventDefault();
        event.stopPropagation();
        openInspector(node, card, options, info, true, primary);
      }
    });
    node.addEventListener("pointerenter", event => {
      if (event.pointerType === "touch") return;
      clearCloseTimer();
      clearHoverTimer();
      // A small intent delay lets the pointer cross neighboring cards on its
      // way into the inspector without replacing the details being read.
      hoverTimer = root.setTimeout(() => {
        hoverTimer = null;
        openInspector(node, card, options, info);
      }, 160);
    });
    node.addEventListener("pointerdown", event => { touchInput = event.pointerType === "touch"; }, true);
    node.addEventListener("pointerleave", () => {
      clearHoverTimer();
      if (dismissedAnchor === node) dismissedAnchor = null;
      scheduleClose();
    });
    node.addEventListener("focusin", () => {
      if (!suppressFocus && !touchInput) openInspector(node, card, options, info);
    });
    node.addEventListener("focusout", event => {
      if (dismissedAnchor === node && !node.contains(event.relatedTarget)) dismissedAnchor = null;
      scheduleClose();
    });
    return node;
  }

  function setDisabled(node, value = true) {
    node.classList.toggle("disabled", !!value);
    const primary = node.querySelector(".cr-primary");
    if (primary) primary.setAttribute("aria-disabled", String(!!value));
    return node;
  }

  const api = { createCard, closeInspector, effectSummary, defenseSummary, setDisabled };
  root.CardRealmsUI = api;
  if (root.document && root.CardRealmsArt) {
    root.document.querySelectorAll("[data-cr-icon]").forEach(node => {
      node.innerHTML = root.CardRealmsArt.icon(node.dataset.crIcon);
      node.setAttribute("aria-hidden", "true");
    });
  }
  if (typeof module === "object" && module.exports) module.exports = api;
})(typeof globalThis === "object" ? globalThis : window);
