/* Shared, side-effect-free poker rules for the solo and live Balatle tables. */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.BalatleCore = factory();
})(typeof globalThis !== "undefined" ? globalThis : this, function () {
  "use strict";

  const HANDS = Object.freeze({
    "High Card": Object.freeze({ tier: 1, baseChips: 5, baseMult: 1, detail: "Highest card scores" }),
    "Pair": Object.freeze({ tier: 2, baseChips: 10, baseMult: 2, detail: "Matching pair scores" }),
    "Two Pair": Object.freeze({ tier: 3, baseChips: 20, baseMult: 2, detail: "Both pairs score" }),
    "Three of a Kind": Object.freeze({ tier: 4, baseChips: 30, baseMult: 3, detail: "Three matching cards score" }),
    "Straight": Object.freeze({ tier: 5, baseChips: 30, baseMult: 4, detail: "Five consecutive ranks" }),
    "Flush": Object.freeze({ tier: 6, baseChips: 35, baseMult: 4, detail: "Five cards of one suit" }),
    "Full House": Object.freeze({ tier: 7, baseChips: 40, baseMult: 4, detail: "Three of a kind and a pair" }),
    "Four of a Kind": Object.freeze({ tier: 8, baseChips: 60, baseMult: 7, detail: "Four matching cards score" }),
    "Straight Flush": Object.freeze({ tier: 9, baseChips: 100, baseMult: 8, detail: "Consecutive ranks in one suit" }),
    "Five of a Kind": Object.freeze({ tier: 10, baseChips: 120, baseMult: 12, detail: "Five matching ranks" }),
    "Flush House": Object.freeze({ tier: 11, baseChips: 140, baseMult: 14, detail: "Full House in one suit" }),
    "Flush Five": Object.freeze({ tier: 12, baseChips: 160, baseMult: 16, detail: "Five matching ranks in one suit" })
  });
  const HAND_KEYS = Object.freeze(Object.keys(HANDS));
  const SUITS = Object.freeze(["S", "H", "D", "C"]);
  const ANTE_BASES = Object.freeze([300, 800, 2000, 5000, 11000, 20000, 35000, 50000]);

  function rankOf(card) {
    if (!card) return NaN;
    if (card.rv != null) return Number(card.rv);
    const faces = { J: 11, Q: 12, K: 13, A: 14 };
    return faces[card.rank] || Number(card.rank);
  }

  function validCards(cards) {
    return Array.isArray(cards) && cards.length > 0 && cards.length <= 5 && cards.every(function (card) {
      const rank = rankOf(card);
      return card && Number.isInteger(rank) && rank >= 2 && rank <= 14 && (card.wild || SUITS.includes(card.suit));
    });
  }

  function groupsFor(cards) {
    const groups = new Map();
    for (const card of cards) {
      const rank = rankOf(card);
      if (!groups.has(rank)) groups.set(rank, { rank: rank, cards: [] });
      groups.get(rank).cards.push(card);
    }
    return Array.from(groups.values()).sort(function (a, b) {
      return b.cards.length - a.cards.length || b.rank - a.rank;
    });
  }

  // Returning the original card objects preserves their enhancement data and play order.
  // Boss rules can ask for a downgraded hand's scoring cards without re-evaluating it.
  function scoringCards(cards, handName) {
    if (!validCards(cards) || !HANDS[handName]) return [];
    const groups = groupsFor(cards);
    let picked;
    if (handName === "High Card") {
      let highest = cards[0];
      for (const card of cards) if (rankOf(card) > rankOf(highest)) highest = card;
      picked = [highest];
    } else if (handName === "Pair" || handName === "Three of a Kind" || handName === "Four of a Kind") {
      const count = handName === "Pair" ? 2 : handName === "Three of a Kind" ? 3 : 4;
      const group = groups.find(function (entry) { return entry.cards.length >= count; });
      picked = group ? group.cards.slice(0, count) : [];
    } else if (handName === "Two Pair") {
      const pairs = groups.filter(function (entry) { return entry.cards.length >= 2; }).slice(0, 2);
      picked = pairs.length === 2 ? pairs.flatMap(function (entry) { return entry.cards.slice(0, 2); }) : [];
    } else {
      picked = cards;
    }
    const selected = new Set(picked);
    return cards.filter(function (card) { return selected.has(card); });
  }

  function evaluateHand(cards) {
    if (!validCards(cards)) return null;
    const groups = groupsFor(cards);
    const ranks = Array.from(new Set(cards.map(rankOf))).sort(function (a, b) { return a - b; });
    const fiveCards = cards.length === 5;
    // A wild card is any suit, not any rank. All natural cards must agree on one suit.
    const flush = fiveCards && SUITS.some(function (suit) {
      return cards.every(function (card) { return card.wild || card.suit === suit; });
    });
    const straight = fiveCards && ranks.length === 5 && (
      ranks.every(function (rank, index) { return rank === ranks[0] + index; }) ||
      ranks.join(",") === "2,3,4,5,14"
    );
    const fiveKind = fiveCards && groups[0].cards.length === 5;
    const fullHouse = fiveCards && groups[0].cards.length === 3 && groups[1].cards.length === 2;
    let name = "High Card";
    if (fiveKind && flush) name = "Flush Five";
    else if (fullHouse && flush) name = "Flush House";
    else if (fiveKind) name = "Five of a Kind";
    else if (straight && flush) name = "Straight Flush";
    else if (groups[0].cards.length === 4) name = "Four of a Kind";
    else if (fullHouse) name = "Full House";
    else if (flush) name = "Flush";
    else if (straight) name = "Straight";
    else if (groups[0].cards.length === 3) name = "Three of a Kind";
    else if (groups[0].cards.length === 2 && groups[1] && groups[1].cards.length === 2) name = "Two Pair";
    else if (groups[0].cards.length === 2) name = "Pair";
    return Object.assign({ name: name }, HANDS[name], {
      scoringIds: scoringCards(cards, name).map(function (card) { return card.id; })
    });
  }

  function targetForRound(round, difficulty) {
    const safeRound = Number.isFinite(Number(round)) ? Math.max(1, Math.floor(Number(round))) : 1;
    const ante = Math.ceil(safeRound / 3);
    const blindIndex = (safeRound - 1) % 3;
    const endless = Math.max(0, ante - ANTE_BASES.length);
    const base = endless
      ? ANTE_BASES[ANTE_BASES.length - 1] * (1 + endless * (0.44 + 0.055 * endless))
      : ANTE_BASES[ante - 1];
    const difficultyScale = difficulty === "easy" ? 0.88 : difficulty === "hard" ? 1.22 : 1;
    return Math.floor(capScore(base * [1, 1.5, 2][blindIndex] * difficultyScale));
  }

  function capScore(value) {
    return Math.min(Number.MAX_SAFE_INTEGER, Math.max(0, value || 0));
  }

  function nonnegativeInteger(value) {
    return Number.isFinite(Number(value)) ? Math.max(0, Math.floor(Number(value))) : 0;
  }

  // Interest is calculated on savings before this blind's payout, never on new rewards.
  // Bigger Wallet multiplies the $1-per-$5 yield and its normal $5 cap.
  // `total` is the payout to add to the balance, not the resulting balance.
  function settlement(options) {
    options = options || {};
    const reward = nonnegativeInteger(options.reward);
    const handBonus = nonnegativeInteger(options.handsLeft);
    const savings = nonnegativeInteger(options.money);
    const boost = nonnegativeInteger(options.interestBoost);
    let interest = Math.min(5, Math.floor(savings / 5)) * (1 + boost);
    if (options.halveInterest) interest = Math.floor(interest / 2);
    return { reward: reward, handBonus: handBonus, interest: interest, total: reward + handBonus + interest };
  }

  return Object.freeze({
    HANDS: HANDS,
    HAND_KEYS: HAND_KEYS,
    ANTE_BASES: ANTE_BASES,
    evaluateHand: evaluateHand,
    scoringCards: scoringCards,
    targetForRound: targetForRound,
    capScore: capScore,
    settlement: settlement
  });
});
