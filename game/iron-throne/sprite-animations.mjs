// Presentation only. This module never writes to campaign objects or rolls RNG.
const troops = ['levy','spearman','menAtArms','heavyInfantry','archer','veteranArcher','crossbow','scout','lightCavalry','cavalry','knight','ram','catapult','trebuchet','siege'];
const actions = move => ({idle:{row:0,frames:8,fps:4},[move]:{row:1,frames:8,fps:8},attack:{row:2,frames:8,fps:8}});
const sheet = (path,width,height,rows,animation,anchor) => ({path,cell:{width,height},columns:8,rows,anchor,actions:animation});
export const SPRITE_ANIMATIONS = {
  troops:Object.fromEntries(troops.map(id=>[id,sheet(`sprites/troops/${id}.png`,160,160,3,actions('march'),{x:80,y:146})])),
  ships:Object.fromEntries(Object.entries({warCanoe:'war_canoe',transport:'transport',warship:'warship'}).map(([id,file])=>[id,sheet(`sprites/ships/${file}.png`,192,160,3,actions('sailing'),{x:96,y:136})])),
  construction:Object.fromEntries([1,2,3].map(stage=>[stage,sheet(`sprites/construction/stage_${stage}.png`,192,192,1,{build:{row:0,frames:8,fps:6}},{x:96,y:174})]))
};
export const ANIMATION_MANIFEST = {version:1,...SPRITE_ANIMATIONS};
export const SPRITE_PATHS = Object.values(SPRITE_ANIMATIONS).flatMap(group=>Object.values(group).map(spec=>spec.path));
export const spriteVisible = tile => !!tile && (!tile.fog || tile.fog==='visible');
export const constructionStage = project => !project || !(project.total>0) || !(project.remaining>0) ? null : Math.min(3,Math.max(1,Math.floor((1-project.remaining/project.total)*3)+1));

export function spriteFrame(spec,action='idle',elapsed=0,reduced=false) {
  const animation=spec.actions[action]||Object.values(spec.actions)[0];
  const frame=reduced?0:Math.floor(Math.max(0,elapsed)*animation.fps/1000)%animation.frames;
  return {x:frame*spec.cell.width,y:animation.row*spec.cell.height,width:spec.cell.width,height:spec.cell.height,frame,row:animation.row};
}
export const validSpriteSheet = (image,spec) => image?.naturalWidth===spec.cell.width*spec.columns && image?.naturalHeight===spec.cell.height*spec.rows;
const eventKey = e => e.id ?? JSON.stringify([e.turn,e.action,e.attacker,e.defender,e.tile,e.from,e.before,e.after]);
const snapshot = (entities,s) => new Map((entities||[]).filter(e=>spriteVisible(s.tiles[e.tile])).map(e=>[e.id,{
  id:e.id,owner:e.owner,tile:e.tile,node:e.node,target:e.target,attackTile:e.attackTile,order:e.order,
  pathEnd:e.path?.at(-1),units:{...e.units}
}]));
const orderChanged = (old,e) => old.order!==e.order || old.target!==e.target || old.attackTile!==e.attackTile || old.pathEnd!==e.pathEnd;
const orderAction = (kind,e) => {
  if(['attack','ranged','bombard'].includes(e.order))return 'attack';
  if(e.order==='move' || kind==='fleet' && e.pathEnd && ['escort','intercept','unload','blockade'].includes(e.order))return kind==='army'?'march':'sailing';
  return null;
};

export class SpritePlayback {
  constructor() { this.seed=null;this.viewer=null;this.turn=-1;this.seen=new Set();this.armies=new Map();this.fleets=new Map();this.active=new Map(); }
  ingest(s,now) {
    const armies=snapshot(s.armies,s),fleets=snapshot(s.fleets,s),events=(s.militaryEvents||[]).slice(-100);
    const viewer=s.knowledgeView||s.viewHouseId||null;
    if(this.seed!==s.seed || this.viewer!==viewer || s.turn<this.turn || this.turn<0) {
      this.active.clear();this.seen=new Set(events.map(eventKey));
    } else {
      // Actions belong to the displayed campaign turn, not a wall-clock timeout.
      if(s.turn!==this.turn)this.active.clear();
      for(const [kind,current,previous] of [['army',armies,this.armies],['fleet',fleets,this.fleets]]) {
        for(const [id,old] of previous) {
          const e=current.get(id);
          if(!e || old.owner!==e.owner || orderChanged(old,e))this.active.delete(`${kind}:${id}`);
        }
      }
      // Position changes are completed movement, including the last path step.
      // Keep that action even when resolution automatically resets its order.
      for(const [kind,current,previous,action] of [['army',armies,this.armies,'march'],['fleet',fleets,this.fleets,'sailing']]) {
        for(const [id,e] of current) {const old=previous.get(id);if(old && old.owner===e.owner && (old.tile!==e.tile || old.node!==e.node))this.play(kind,id,orderAction(kind,e)||action,now);}
      }
      for(const e of events) if(!this.seen.has(eventKey(e)) && e.turn>=s.turn-1) {
        if(!['battle','siege','structure','naval'].includes(e.action))continue;
        for(const side of ['attacker','defender']) {
          const owner=e[side];
          if(e.action!=='naval') {
            // Ship bombardments reuse attackerArmyId with the actual fleet ID.
            const id=e[`${side}ArmyId`];
            if(id!==undefined) {
              if(armies.get(id)?.owner===owner)this.play('army',id,'attack',now);
              else if(fleets.get(id)?.owner===owner)this.play('fleet',id,'attack',now);
            } else if(side==='attacker' && e.from) {
              const matches=[...armies.values()].filter(a=>a.owner===owner && (a.tile===e.from || this.armies.get(a.id)?.tile===e.from));
              if(matches.length===1)this.play('army',matches[0].id,'attack',now);
            }
          } else {
            // Legacy naval events have no fleet IDs. Require an unambiguous
            // location/order match; never infer combat merely from a war.
            const candidates=[];
            for(const [kind,current,previous] of [['fleet',fleets,this.fleets],['army',armies,this.armies]]) {
              for(const entity of current.values()) {
                if(entity.owner!==owner)continue;
                const old=previous.get(entity.id);
                const atClash=entity.tile===e.tile || old?.tile===e.tile;
                const ordered=[old,entity].some(a=>a && (a.attackTile===e.tile || a.target===e.tile && ['attack','ranged','bombard','intercept'].includes(a.order)));
                const routed=e.retreatOwner===owner && entity.tile===e.retreat && old?.tile===e.tile;
                if(atClash||ordered||routed)candidates.push({kind,id:entity.id});
              }
            }
            if(candidates.length===1)this.play(candidates[0].kind,candidates[0].id,'attack',now);
          }
        }
      }
    }
    // Standing orders keep their loop across turns and after loading a campaign.
    // Only authorized visible snapshots are used; observed enemies hide orders.
    for(const [kind,current] of [['army',armies],['fleet',fleets]])for(const [id,e] of current) {
      const action=orderAction(kind,e);
      if(action && !this.active.has(`${kind}:${id}`))this.play(kind,id,action,now);
    }
    this.seed=s.seed;this.viewer=viewer;this.turn=s.turn;this.armies=armies;this.fleets=fleets;this.seen=new Set(events.map(eventKey));
    for(const [key,value] of this.active)if(!(value.kind==='army'?armies:fleets).has(value.id))this.active.delete(key);
  }
  play(kind,id,action,now) {
    this.active.set(`${kind}:${id}`,{kind,id,action,start:now});
  }
  resolve(kind,group,now,type) {
    const active=group.map(e=>this.active.get(`${kind}:${e.id}`)).filter(e=>e && (kind!=='army'||!type||this.armies.get(e.id)?.units[type]>0));
    return active.find(e=>e.action==='attack') || active[0] || {action:'idle',start:0};
  }
}

// One timeout for all visible sprites. Input still requests an immediate RAF.
export class SpriteScheduler {
  constructor(redraw,{setTimer=(fn,ms)=>setTimeout(fn,ms),clearTimer=id=>clearTimeout(id)}={}) {this.redraw=redraw;this.setTimer=setTimer;this.clearTimer=clearTimer;this.timer=null;this.due=Infinity;}
  begin(now,reduced=false,hidden=false) {this.cancel();this.now=now;this.paused=hidden;this.reduced=reduced;this.due=Infinity;}
  use(spec,action,elapsed) {
    if(this.paused||this.reduced)return;
    const animation=spec.actions[action]||Object.values(spec.actions)[0],interval=1000/animation.fps;
    this.due=Math.min(this.due,interval-Math.max(0,elapsed)%interval);
  }
  defer(delay) {if(!this.paused && Number.isFinite(delay))this.due=Math.min(this.due,Math.max(16,delay));}
  end() {if(!this.paused && Number.isFinite(this.due))this.timer=this.setTimer(()=>{this.timer=null;this.redraw();},Math.max(16,this.due));}
  cancel() {if(this.timer!==null)this.clearTimer(this.timer);this.timer=null;}
}
