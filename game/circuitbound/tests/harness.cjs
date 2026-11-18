const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const E = require('../engineering.js');
const S = require('../survival.js');
const Workshop = require('../workshop.js');
const RoboticsCatalog = require('../robotics-catalog.js');
const DeepIndustry = require('../deep-industry.js');
const FoundryAutomation = require('../foundry-automation.js');
const FoundryCombat = require('../foundry-combat.js');

const root = path.join(__dirname, '..');
const source = fs.readFileSync(path.join(root, 'app.js'), 'utf8');
const html = fs.readFileSync(path.join(root, '../circuitbound.html'), 'utf8');

function fixture(width = 40, height = 24) {
  const context = vm.createContext({
    E, CircuitSurvival: S, CircuitWorkshop: Workshop, CircuitRoboticsCatalog: RoboticsCatalog,
    CircuitDeepIndustry: DeepIndustry, CircuitFoundryAutomation: FoundryAutomation, CircuitFoundryCombat: FoundryCombat
  });
  const catalog = source.slice(source.indexOf('  const T='), source.indexOf('  const palette='));
  vm.runInContext(catalog + '\nglobalThis.catalog = { T, defs, items, recipes };', context);
  const { T, defs, items, recipes } = context.catalog;
  const world = new Uint8Array(width * height), meta = new Map();
  const get = (x, y) => x >= 0 && y >= 0 && x < width && y < height ? world[x + y * width] : T.STONE;
  const sim = new E.Simulation({ width, height, get, meta: () => meta, defs, T });
  const put = (x, y, id, m = {}) => {
    const key = x + y * width;
    world[key] = id;
    if (id) meta.set(key, { rot: 0, ...m });
    else meta.delete(key);
    sim.changed(x, y);
    return meta.get(key);
  };
  return { T, defs, items, recipes, world, meta, get, sim, put, key: (x, y) => x + y * width };
}

function documentFixture() {
  const ids = new Map(), listeners = {};
  const context = new Proxy({}, {
    get: (obj, key) => key in obj ? obj[key] : /^create.*Gradient$/.test(key) ?
      () => ({ addColorStop() {} }) : () => {},
    set: (obj, key, value) => (obj[key] = value, true)
  });
  class Element {
    constructor(tag) {
      this.tagName = tag.toUpperCase();
      this.attributes = {};
      this.children = [];
      this.dataset = {};
      this.listeners = {};
      this.style = { setProperty() {}, removeProperty() {} };
      this.className = '';
      this.textContent = '';
      this.hidden = false;
      this.disabled = false;
      this.classList = {
        add: (...names) => { this.className = Array.from(new Set([...this.className.split(/\s+/), ...names])).join(' ').trim(); },
        remove: (...names) => { this.className = this.className.split(/\s+/).filter(name => !names.includes(name)).join(' '); },
        contains: name => this.className.split(/\s+/).includes(name),
        toggle: (name, force = !this.classList.contains(name)) => {
          if (force) this.classList.add(name);
          else this.classList.remove(name);
          return force;
        }
      };
    }
    append(...elements) {
      for (const item of elements) {
        if (item?.tagName === '#FRAGMENT') { this.append(...item.children); continue; }
        if (!(item instanceof Element)) {
          if (typeof item === 'string') this.textContent += item;
          continue;
        }
        item.remove();
        item.parentNode = this;
        this.children.push(item);
      }
    }
    appendChild(item) { this.append(item); return item; }
    get firstChild() { return this.children[0] ?? null; }
    get firstElementChild() { return this.firstChild; }
    get parentElement() { return this.parentNode; }
    prepend(...elements) { this.append(...elements); this.children.unshift(...this.children.splice(-elements.length)); }
    before(...elements) {
      const parent = this.parentNode;
      if (!parent) return;
      parent.append(...elements);
      parent.children.splice(parent.children.indexOf(this), 0, ...parent.children.splice(-elements.length));
    }
    replaceChildren(...elements) { this.children.slice().forEach(item => item.remove()); this.append(...elements); }
    get innerHTML() { return this.markup || ''; }
    set innerHTML(value) { this.markup = value; this.replaceChildren(); }
    remove() {
      if (!this.parentNode) return;
      this.parentNode.children.splice(this.parentNode.children.indexOf(this), 1);
      this.parentNode = null;
    }
    contains(item) { return item === this || this.children.some(child => child.contains(item)); }
    setAttribute(key, value) {
      this.attributes[key] = String(value);
      if (key === 'id') { this.id = value; ids.set(value, this); }
      if (key === 'class') this.className = value;
      if (key === 'value') this.value = value;
      if (key === 'hidden' || key === 'disabled' || key === 'checked') this[key] = true;
      if (key.startsWith('data-')) this.dataset[key.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] = String(value);
    }
    getAttribute(key) {
      if (key.startsWith('data-')) return this.dataset[key.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] ?? null;
      return this.attributes[key] ?? null;
    }
    removeAttribute(key) { delete this.attributes[key]; }
    addEventListener(name, listener) { (this.listeners[name] ||= []).push(listener); }
    removeEventListener(name, listener) { this.listeners[name] = this.listeners[name]?.filter(fn => fn !== listener) || []; }
    click() {
      if (!this.disabled) {
        const event = { target: this, currentTarget: this, preventDefault() {}, stopPropagation() {} };
        this.onclick?.(event);
        for (const listener of this.listeners.click || []) listener(event);
      }
    }
    focus() { document.activeElement = this; }
    scrollIntoView() {}
    getContext() { return context; }
    getBoundingClientRect() { return { left: 0, top: 0, width: 1280, height: 800 }; }
    getClientRects() { return this.hidden ? [] : [this.getBoundingClientRect()]; }
    setPointerCapture() {}
    releasePointerCapture() {}
    matches(selectors) {
      return selectors.split(',').some(selector => {
        const chain = selector.trim().split(/\s+/);
        const matchesPart = (node, part) => {
          if (part.includes(':not(:disabled)') && node.disabled) return false;
          part = part.replace(':not(:disabled)', '');
          for (const [, key, value] of part.matchAll(/\[([^=\]]+)(?:="?([^"\]]+)"?)?\]/g)) {
            const actual = node.getAttribute(key);
            if (actual === null || (value !== undefined && actual !== value)) return false;
          }
          part = part.replace(/\[[^\]]+\]/g, '');
          const id = part.match(/#([\w-]+)/)?.[1], tag = part.match(/^[\w-]+/)?.[0];
          return (!id || node.id === id) && (!tag || node.tagName === tag.toUpperCase()) &&
            [...part.matchAll(/\.([\w-]+)/g)].every(([, name]) => node.classList.contains(name));
        };
        if (!matchesPart(this, chain.pop())) return false;
        let parent = this.parentNode;
        while (chain.length) {
          const part = chain.pop();
          while (parent && !matchesPart(parent, part)) parent = parent.parentNode;
          if (!parent) return false;
          parent = parent.parentNode;
        }
        return true;
      });
    }
    closest(selector) { return this.matches(selector) ? this : this.parentNode?.closest(selector) ?? null; }
    querySelectorAll(selector) {
      const all = [];
      const walk = node => { for (const child of node.children) { if (child.matches(selector)) all.push(child); walk(child); } };
      walk(this);
      return all;
    }
    querySelector(selector) { return this.querySelectorAll(selector)[0] ?? null; }
  }
  const tree = new Element('document');
  const stack = [tree];
  const voidTags = new Set(['area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'param', 'source', 'track', 'wbr']);
  for (const [, closing, tag, attributes] of html.matchAll(/<(\/?)([a-z][\w-]*)([^<>]*)>/gi)) {
    if (closing) { if (stack.at(-1).tagName === tag.toUpperCase()) stack.pop(); continue; }
    const element = new Element(tag);
    for (const [, key, value] of attributes.matchAll(/([\w-]+)(?:="([^"]*)")?/g)) element.setAttribute(key, value ?? '');
    stack.at(-1).append(element);
    if (!voidTags.has(tag)) stack.push(element);
  }
  const document = {
    activeElement: null, currentScript: null, body: tree.querySelector('body'), documentElement: tree.querySelector('html'),
    createElement: tag => new Element(tag), createDocumentFragment: () => new Element('#fragment'), createTextNode: text => String(text),
    getElementById: id => ids.get(id) || tree.querySelector('#' + id) || null,
    querySelector: selector => tree.querySelector(selector), querySelectorAll: selector => tree.querySelectorAll(selector),
    addEventListener: (name, fn) => { (listeners[name] ||= []).push(fn); }, removeEventListener() {}
  };
  return { document, ids };
}

function game(saved) {
  const { document, ids } = documentFixture(), listeners = {};
  const storage = new Map(saved ? [['circuitbound-save-v1', JSON.stringify(saved)]] : []);
  const env = {
    console, URL, structuredClone, Image: class {}, Uint8Array, Map, Set, Math, Date, JSON, btoa, atob,
    innerWidth: 1280, innerHeight: 800, devicePixelRatio: 1,
    document, navigator: { maxTouchPoints: 0 }, location: { href: 'https://fixture.invalid/game/circuitbound.html' },
    matchMedia: () => ({ matches: false, addEventListener() {}, removeEventListener() {} }),
    localStorage: { getItem: key => storage.get(key) || null, setItem: (key, value) => storage.set(key, value), removeItem: key => storage.delete(key) },
    addEventListener: (name, fn) => { listeners[name] = fn; }, removeEventListener() {},
    requestAnimationFrame() {}, queueMicrotask() {}, setInterval: () => 0, setTimeout: () => 0, clearTimeout() {}, confirm: () => true
  };
  env.window = env;
  const context = vm.createContext(env);
  for (const match of html.matchAll(/<script\b[^>]*\bsrc="(?:\.\/circuitbound\/|\/game\/circuitbound\/)([^"]+)"[^>]*>/g)) {
    if (match[1] === 'app.js') break;
    document.currentScript = { src: 'https://fixture.invalid/game/circuitbound/' + match[1] };
    vm.runInContext(fs.readFileSync(path.join(root, match[1]), 'utf8'), context, { filename: match[1] });
  }
  const instrumented = source.replace(/\}\)\(\);\s*$/, `globalThis.api={T,defs,items,recipes,engine,survival,player,inventory,stats,keys,camera,defaultMeta,set,get,idx,simulateNetworks,updateMachines,moveGroup,rotateAssembly,finishLink,saveGame,loadGame,generate,render,tick,toggleModal,mineStep,interactItems,dropHeldItem,collectDrops,configureComponent,movePlayer,placeLine,placeBlueprint,blueprintStatus,placementValid,inventoryRoom,lineOfSight,simulationStep,interactAtPointer,selectItem,placeAtAim,doPrimary,clearActions,workshop,
    get blueprint(){return blueprint},set blueprint(value){blueprint=value},get buildDrag(){return buildDrag},set buildDrag(value){buildDrag=value},
    get meta(){return meta},get world(){return world},get pointer(){return pointer},get linkDrag(){return linkDrag},set linkDrag(value){linkDrag=value},set linkMode(value){linkMode=value},set selectedAssembly(value){selectedAssembly=value},get paused(){return paused}};})();`);
  vm.runInContext(instrumented, context, { filename: 'app.js' });
  if (ids.get('welcomePanel').classList.contains('show')) ids.get('startBtn').click();
  function clearTerrain(){
    const world = context.api.world;
    world.natural = () => context.api.T.AIR;
    world.features.clear();
    world.chunks.clear();
    world.edits.clear();
  }
  return { ...context.api, api: context.api, document, ids, storage, listeners, context, clearTerrain, run: code => vm.runInContext(code, context) };
}

module.exports = { fixture, game, E };
