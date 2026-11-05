const TRANSIENT_CLASSES = new Set(['moving','flash','queued']);
const STRUCTURES = ['compact-battle','compact-shell','arena-panel','compact-scorebar','arena','board'];
const KEYED_SELECTOR=['[id]','[data-unit-token]','[data-cell]','.command-dock','[data-mode]','[data-command]',...STRUCTURES.map(name=>`.${name}`)].join(',');
const ELEMENT = 1;
// Cache source markup, rather than live markup: effects and countdown fills may
// change between renders without requiring a walk through unchanged artwork.
const LAST_MARKUP = new WeakMap();
const children = node => Array.from(node.childNodes || []);
const attribute = (node,name) => node.getAttribute?.(name);
const classes = node => new Set((attribute(node,'class') || '').split(/\s+/).filter(Boolean));
const transientChild = node => node.nodeType===ELEMENT && [...classes(node)].some(name=>name==='effect'||name==='burst');

function key(node) {
  if(node.nodeType!==ELEMENT)return null;
  const id=attribute(node,'id');if(id)return `id:${id}`;
  const token=attribute(node,'data-unit-token');if(token)return `unit:${token}`;
  const cell=attribute(node,'data-cell');if(cell!==null)return `cell:${cell}`;
  const names=classes(node),side=attribute(node,'data-side') || '';
  if(names.has('command-dock'))return `dock:${side}`;
  const mode=attribute(node,'data-mode');if(mode!==null)return `mode:${side}:${mode}:${attribute(node,'data-skill-index') || ''}`;
  const command=attribute(node,'data-command');if(command!==null)return `command:${side}:${command}`;
  const structure=STRUCTURES.find(name=>names.has(name));
  return structure?`structure:${structure}`:null;
}

function sameKind(a,b) {
  return a?.nodeType===b?.nodeType && (a.nodeType!==ELEMENT || (a.nodeName===b.nodeName && a.namespaceURI===b.namespaceURI));
}

function liveDialog(node) {
  return node.nodeType===ELEMENT && node.nodeName.toUpperCase()==='DIALOG' && (node.open || node.hasAttribute('open'));
}

function syncAttributes(actual,fresh,keepDialogOpen) {
  const keepClasses=[...classes(actual)].filter(name=>TRANSIENT_CLASSES.has(name));
  const wanted=new Map(Array.from(fresh.attributes || [],item=>[item.name,item.value]));
  if(keepClasses.length){const names=new Set((wanted.get('class') || '').split(/\s+/).filter(Boolean));keepClasses.forEach(name=>names.add(name));wanted.set('class',[...names].join(' '));}
  if(actual.hasAttribute('data-node-id'))wanted.set('data-node-id',actual.getAttribute('data-node-id'));
  if(keepDialogOpen)wanted.set('open',actual.getAttribute('open') || '');
  for(const item of Array.from(actual.attributes || []))if(!wanted.has(item.name))actual.removeAttribute(item.name);
  for(const [name,value] of wanted)if(actual.getAttribute(name)!==value)actual.setAttribute(name,value);
}

/**
 * Morph only the battle container. Stable cells, controls, and unit tokens retain
 * identity; tokens can move globally between cells without first being detached.
 * The browser's native parser owns HTML parsing. No animations are started here.
 */
export function patchBattleMarkup(container,html) {
  const started=globalThis.performance?.now?.() ?? Date.now();
  const document=container.ownerDocument;
  const template=document.createElement('template');template.innerHTML=html;
  const oldNodes=[],oldKeyed=new Map(),claimed=new Set(),desiredByParent=new Map(),cleanup=[],keys=new WeakMap(),invalidated=new Set();
  const nodeKey=node=>{if(!keys.has(node))keys.set(node,key(node));return keys.get(node);};
  const nativeIndex=typeof container.querySelectorAll==='function';
  const collect=(node,protectedSubtree=false)=>{for(const child of node.childNodes || []){oldNodes.push(child);if(!nativeIndex){const value=nodeKey(child);if(!protectedSubtree&&value&&!oldKeyed.has(value))oldKeyed.set(value,child);}collect(child,protectedSubtree||liveDialog(child));}};
  collect(container);
  // The native selector engine can find ~100 meaningful keys without asking
  // JavaScript to inspect attributes on every terrain path and sprite detail.
  if(nativeIndex)for(const node of container.querySelectorAll(KEYED_SELECTOR)){
    const dialog=node.closest?.('dialog[open]');if(dialog&&dialog!==node)continue;
    const value=nodeKey(node);if(value&&!oldKeyed.has(value))oldKeyed.set(value,node);
  }
  const oldSet=new Set(oldNodes),active=document.activeElement;
  const preserveFocus=active&&container.contains(active);
  const stats={created:0,reused:0,removed:0,ms:0};

  function place(parent,node,reference) {
    if(node===reference)return;
    // A token claimed by an earlier destination changes its former ancestors,
    // even when one of those ancestors has otherwise identical source markup.
    if(node.parentNode!==parent)for(let ancestor=node.parentNode;ancestor&&ancestor!==container;ancestor=ancestor.parentNode)invalidated.add(ancestor);
    // moveBefore preserves animation/focus state on supporting browsers.
    if(typeof parent.moveBefore==='function' && node.nodeType===ELEMENT && node.isConnected && parent.isConnected){
      try{parent.moveBefore(node,reference);return;}catch{/* Older implementations fall back to the standard DOM move. */}
    }
    parent.insertBefore(node,reference);
  }

  function claimChildren(parent) {
    for(const node of parent.childNodes || []){claimed.add(node);if(oldSet.has(node))stats.reused++;claimChildren(node);}
  }

  function morph(parent,freshParent) {
    const oldChildren=children(parent),usedHere=new Set(),desired=[];
    let reference=parent.firstChild;
    for(const [index,fresh] of children(freshParent).entries()){
      const value=nodeKey(fresh);
      let actual=value?oldKeyed.get(value):null;
      if(actual&&(claimed.has(actual)||!sameKind(actual,fresh)||actual===parent||actual.contains?.(parent)))actual=null;
      if(!actual&&!value){
        const candidate=oldChildren[index];
        if(candidate&&candidate.parentNode===parent&&!claimed.has(candidate)&&!usedHere.has(candidate)&&!transientChild(candidate)&&!nodeKey(candidate)&&sameKind(candidate,fresh))actual=candidate;
        if(!actual)actual=oldChildren.find(node=>node.parentNode===parent&&!claimed.has(node)&&!usedHere.has(node)&&!transientChild(node)&&!nodeKey(node)&&sameKind(node,fresh));
      }
      if(!actual){actual=fresh.cloneNode(false);stats.created++;}
      else if(oldSet.has(actual))stats.reused++;
      claimed.add(actual);usedHere.add(actual);desired.push(actual);
      if(actual.parentNode!==parent||actual!==reference)place(parent,actual,reference);
      reference=actual.nextSibling;
      if(actual.nodeType===ELEMENT){
        const preserveDialog=liveDialog(actual);
        // Tile/control markup is small; serializing the entire board at every
        // ancestor would cost more than it saves. Skip only keyed leaf regions.
        const markup=value&&!value.startsWith('structure:')&&!preserveDialog?fresh.outerHTML:undefined;
        if(markup!==undefined&&LAST_MARKUP.get(actual)===markup&&!invalidated.has(actual)){
          claimChildren(actual);continue;
        }
        syncAttributes(actual,fresh,preserveDialog);
        if(!preserveDialog)morph(actual,fresh);
        if(markup!==undefined)LAST_MARKUP.set(actual,markup);
      }else if(actual.nodeValue!==fresh.nodeValue)actual.nodeValue=fresh.nodeValue;
    }
    const wanted=new Set(desired);
    // Floating damage/heal numbers and burst rings own their timeout lifecycle.
    // Keep them attached until that lifecycle removes them, never reuse them as UI.
    for(const node of children(parent))if(transientChild(node))wanted.add(node);
    desiredByParent.set(parent,wanted);
    for(const node of children(parent))if(!wanted.has(node))cleanup.push({parent,node});
  }

  morph(container,template.content);
  // Defer removals until every destination has had a chance to claim its token.
  for(const {parent,node} of cleanup)if(node.parentNode===parent&&!desiredByParent.get(parent)?.has(node))parent.removeChild(node);
  if(cleanup.length)stats.removed=oldNodes.filter(node=>!container.contains(node)).length;
  if(preserveFocus&&container.contains(active)&&document.activeElement!==active)active.focus?.({preventScroll:true});
  stats.ms=(globalThis.performance?.now?.() ?? Date.now())-started;
  return stats;
}
