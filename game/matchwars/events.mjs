/** Rare board events are independent of combat and never occur in the initial fill. */
export const BOARD_EVENT_RATE = .006;
export const BOARD_EVENT_COOLDOWN = 8;
export const CURSE_FUSE = 4;
export const BOARD_EVENT_MIN_STAGE = 5;
export const BOARD_EVENTS = Object.freeze({
  supply: Object.freeze({ type: 'supply', name: 'Supply Cache', symbol: '◆', color: '#efc67a', description: 'Match this rune or clear it with a special to collect 20 gold.', gold: 20 }),
  frost: Object.freeze({ type: 'frost', name: 'Frost Cell', symbol: '❄', color: '#9ddff5', description: 'This rune cannot be swapped. Match its color, blast it, or clear an adjacent rune to thaw it.' }),
  curse: Object.freeze({ type: 'curse', name: 'Curse Rune', symbol: '⌛', color: '#d29de7', description: 'Purify this rune by matching or blasting it within four successful moves. Otherwise one enemy reinforcement arrives.', fuse: CURSE_FUSE }),
});
