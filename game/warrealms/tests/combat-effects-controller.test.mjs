import test from "node:test";
import assert from "node:assert/strict";
import { createCombatEffects } from "../ui/combat-effects.js";

// A minimal DOM/clock adapter tests controller lifecycle without a browser binary.
// Rendering and CSS remain covered by combat-effects.browser.mjs.
function environment(width = 1280) {
  let time = 0, id = 0;
  const frames = new Map(), listeners = new Map(), saved = new Map();
  class Element {
    constructor(tag = "div") {
      this.tagName = tag; this.children = []; this.dataset = {}; this.attributes = {};
      this.className = ""; this.id = ""; this.parentElement = null; this.textContent = "";
      this.style = { setProperty(k, v) { this[k] = v; } };
      this.rect = { left: 100, top: 200, width: 100, height: 140, right: 200, bottom: 340 };
      this.classList = {
        contains: name => this.className.split(" ").includes(name),
        add: (...names) => { this.className = [...new Set([...this.className.split(" "), ...names])].filter(Boolean).join(" "); },
        remove: (...names) => { this.className = this.className.split(" ").filter(n => !names.includes(n)).join(" "); },
        toggle: (name, value) => { (value ?? !this.classList.contains(name)) ? this.classList.add(name) : this.classList.remove(name); }
      };
    }
    get isConnected() { return this === document.documentElement || !!this.parentElement?.isConnected; }
    setAttribute(k, v) { this.attributes[k] = v; if (k === "id") this.id = v; }
    removeAttribute(k) { delete this.attributes[k]; if (k === "id") this.id = ""; }
    getBoundingClientRect() { return this.rect; }
    append(...nodes) { for (const node of nodes) { node.remove(); node.parentElement = this; this.children.push(node); } }
    remove() { if (this.parentElement) this.parentElement.children = this.parentElement.children.filter(n => n !== this); this.parentElement = null; }
    replaceChildren(...nodes) { this.children.forEach(n => { n.parentElement = null; }); this.children = []; this.append(...nodes); }
    matches(selector) {
      if (selector === "*") return true;
      if (selector.startsWith("#")) return this.id === selector.slice(1);
      const className = selector.match(/^\.([\w-]+)/)?.[1];
      if (className && !this.classList.contains(className)) return false;
      const attr = selector.match(/\[([^=\]]+)(?:="([^"]*)")?\]/);
      if (attr) {
        const key = attr[1].replace(/^data-/, "").replace(/-([a-z])/g, (_, c) => c.toUpperCase());
        const value = attr[1] === "id" ? this.id || undefined : this.attributes[attr[1]] ?? (attr[1].startsWith("data-") ? this.dataset[key] : undefined);
        if (attr[2] === undefined ? value === undefined : value !== attr[2]) return false;
      }
      return !!(className || attr) || this.tagName === selector;
    }
    querySelectorAll(selector) {
      const matches = [], selectors = selector.split(",");
      const visit = node => { for (const child of node.children) { if (selectors.some(s => child.matches(s))) matches.push(child); visit(child); } };
      visit(this); return matches;
    }
    querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
    closest(selector) { return selector.split(",").some(s => this.matches(s)) ? this : this.parentElement?.closest(selector) || null; }
    cloneNode(deep) {
      const copy = new Element(this.tagName); copy.className = this.className; copy.id = this.id;
      copy.attributes = { ...this.attributes }; copy.dataset = { ...this.dataset }; copy.rect = { ...this.rect };
      if (deep) copy.append(...this.children.map(n => n.cloneNode(true))); return copy;
    }
    animate() { return { cancel() {} }; }
  }
  const document = {
    hidden: false, createElement: tag => new Element(tag),
    addEventListener() {}, removeEventListener() {},
    querySelectorAll: selector => document.documentElement.querySelectorAll(selector),
    querySelector: selector => document.documentElement.querySelector(selector),
    getElementById: id => document.querySelector(`#${id}`)
  };
  document.documentElement = new Element("html");
  document.head = new Element("head"); document.body = new Element("body");
  document.documentElement.append(document.head, document.body);
  const globals = {
    document, innerWidth: width, innerHeight: 1000, CSS: { escape: String },
    localStorage: { getItem: () => null }, matchMedia: () => ({ matches: false }),
    performance: { now: () => time },
    getComputedStyle: () => ({ visibility: "visible", getPropertyValue: () => "#66aaff" }),
    requestAnimationFrame: callback => { frames.set(++id, callback); return id; },
    cancelAnimationFrame: id => frames.delete(id),
    addEventListener: (name, callback) => listeners.set(name, callback),
    removeEventListener: name => listeners.delete(name)
  };
  Object.entries(globals).forEach(([key, value]) => { saved.set(key, Object.getOwnPropertyDescriptor(globalThis, key)); Object.defineProperty(globalThis, key, { value, configurable: true }); });
  const add = (parent, className, attributes = {}) => { const node = new Element(); node.className = className; Object.assign(node, attributes); parent.append(node); return node; };
  const board = add(document.body, "battleShell");
  const hud = add(board, "", { id: "commanderAuthority" }); hud.rect = { ...hud.rect, left: 450, top: 720 };
  const card = add(board, "gameCard", { dataset: { instanceId: "fort" } });
  add(card, "battleCardVisual");
  return {
    document, card,
    async advance(ms) {
      const end = time + ms;
      while (time < end) { time += 16; const callbacks = [...frames.values()]; frames.clear(); callbacks.forEach(cb => cb(time)); await Promise.resolve(); }
    },
    resize: () => listeners.get("resize")?.(),
    restore() { for (const [key, value] of saved) value ? Object.defineProperty(globalThis, key, value) : delete globalThis[key]; }
  };
}

test("controller preserves razed art, sequences damage, and removes every transient node", async () => {
  const env = environment(); const fx = createCombatEffects();
  try {
    fx.capture([], "me");
    fx.capture([
      { id: "hit", type: "base-damage", instanceId: "fort", targetId: "enemy", actorId: "me", amount: 8 },
      { id: "destroy", type: "card-destroy", instanceId: "fort", targetId: "enemy", method: "combat" }
    ], "me");
    env.card.remove(); fx.flush();
    assert.equal(env.document.querySelectorAll(".wrRazeGhost").length, 1);
    await env.advance(350);
    assert.ok(env.document.querySelectorAll(".wrCombatNumber").some(n => n.textContent === "−8"));
    assert.equal(env.document.querySelector(".wrRazeGhost").classList.contains("burning"), false);
    await env.advance(420);
    assert.equal(env.document.querySelector(".wrRazeGhost").classList.contains("burning"), true);
    await env.advance(1600);
    assert.equal(fx.debug().nodes, 0); assert.equal(fx.debug().queued, 0); assert.equal(fx.debug().running, false);
  } finally { fx.dispose(); env.restore(); }
});

test("controller aggregates rapid hits, deduplicates snapshots, and respects both node budgets", async () => {
  for (const width of [1280, 390]) {
    const env = environment(width); const fx = createCombatEffects();
    try {
      fx.capture([], "me");
      const events = Array.from({ length: 80 }, (_, i) => ({ id: `hit${i}`, type: "health-loss", targetId: "me", amount: 3 }));
      fx.capture(events, "me"); fx.flush(); const size = fx.debug().queued;
      fx.capture(structuredClone(events), "me"); fx.flush(); assert.equal(fx.debug().queued, size);
      let peak = 0, combined = false;
      for (let i = 0; i < 24; i++) {
        await env.advance(250);
        const debug = fx.debug(); peak = Math.max(peak, debug.tracked);
        combined ||= env.document.querySelectorAll(".wrCombatNumber").some(n => n.textContent === "−219 ×73");
      }
      assert.ok(combined, "all congested damage survives in one exact total");
      assert.ok(peak <= (width === 390 ? 80 : 160));
      assert.equal(fx.debug().nodes, 0); assert.equal(fx.debug().running, false);
    } finally { fx.dispose(); env.restore(); }
  }
});

test("reset cancels pending work and geometry changes retain event deduplication", async () => {
  const env = environment(); const fx = createCombatEffects();
  try {
    fx.capture([], "me");
    fx.capture([{ id: "one", type: "health-loss", targetId: "me", amount: 8 }], "me"); fx.flush();
    env.resize(); assert.equal(fx.debug().queued, 0);
    fx.capture([{ id: "one", type: "health-loss", targetId: "me", amount: 8 }, { id: "two", type: "health-gain", targetId: "me", amount: 5 }], "me"); fx.flush();
    assert.equal(fx.debug().queued, 1, "first new event after resizing is retained");
    await env.advance(80); fx.reset(); await env.advance(2000);
    assert.equal(fx.debug().nodes, 0); assert.equal(fx.debug().running, false);
  } finally { fx.dispose(); env.restore(); }
});
