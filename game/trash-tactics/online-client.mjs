import {Game} from './engine.js';
import {ONLINE_VERSION,RECONNECT_MS,SNAPSHOT_INTERVAL} from './online-rules.mjs';

const orders=['train','research','build','removeWall','reverseConveyor','loadTransport','loadNearest','dropTransports','ability','useAbility','command','rally'];

function unpack(encoded,target) {
  const raw=atob(encoded);
  for (let i=0;i<target.length;i++) target[i]=(raw.charCodeAt(i>>3)>>(i&7))&1;
}

export class RemoteGame extends Game {
  constructor({map,team,connection,onEvent}) {
    super({map,online:true,onEvent});
    this.localTeam=team;
    this.connection=connection;
    this.memory=[];
    this.motion=new Map();
    this.receivedAt=0;
    for (const method of orders) this[method]=(...args)=>connection.order(method,args);
  }
  _ensureVision() {}
  getRememberedEntities() { return this.memory; }
  getMatchReport() { return this.report || null; }
  apply(state) {
    const now=performance.now();
    this.time=state.time;
    this.teams=state.teams;
    this.effects=state.effects;
    this.projectiles=state.projectiles;
    this.forceFields=state.forceFields;
    this.divisions=state.divisions;
    const prior=new Map(this.entities.map(e=>[e.id,e]));
    this.motion=new Map();
    for (const entity of state.entities) {
      const last=prior.get(entity.id);
      if (this.receivedAt && last && !entity.transportedBy && !last.transportedBy && Math.hypot(last.x-entity.x,last.y-entity.y)<120) {
        this.motion.set(entity.id,{x:last.x,y:last.y,toX:entity.x,toY:entity.y});
      }
    }
    this.entities=state.entities;
    this.receivedAt=now;
    if (state.fog) {
      const view=this.fog.teams[this.localTeam];
      unpack(state.fog.visible,view.visible);
      unpack(state.fog.explored,view.explored);
      this.fog.revision=state.fog.revision;
    }
    if (state.memory) this.memory=state.memory;
    if (state.field?.gateOpen && this.map.gate && !this.map.gate.open) {
      this.map.gate.open=true;
      this.map.obstacles=this.map.obstacles.filter(obstacle=>obstacle!==this.map.gateObstacle);
    }
    for (const [key,values] of [['stations',state.field?.stations],['conveyors',state.field?.conveyors]]) {
      for (const value of values || []) {
        const item=this.map[key]?.find(entry=>entry.id===value.id);
        if (item) Object.assign(item,value);
      }
    }
    for (const event of state.events) this.onEvent(event);
  }
  update() {
    const t=Math.min(1,(performance.now()-this.receivedAt)/(1000*SNAPSHOT_INTERVAL));
    for (const [id,from] of this.motion) {
      const e=this.getEntity(id);
      if (e) {e.x=from.x+(from.toX-from.x)*t;e.y=from.y+(from.toY-from.y)*t;}
    }
  }
}

export class OnlineConnection extends EventTarget {
  constructor(origin) {
    super();
    this.origin=origin;
    this.seq=0;
    this.ended=false;
    this.attempt=0;
  }
  emit(type,detail) { this.dispatchEvent(new CustomEvent(type,{detail})); }
  send(data) {
    if (this.socket?.readyState!==WebSocket.OPEN || this.socket.bufferedAmount>128*1024) return false;
    this.socket.send(JSON.stringify(data));
    return true;
  }
  open(request) {
    const url=new URL('/api/play',this.origin);
    url.protocol=url.protocol==='https:' ? 'wss:' : 'ws:';
    const socket=new WebSocket(url);
    this.socket=socket;
    const timeout=setTimeout(()=>socket.close(),6500);
    socket.addEventListener('message',event=>{
      if (this.ended || this.socket!==socket) return;
      let data;
      try {data=JSON.parse(event.data);} catch {return;}
      if (data.type==='hello') {
        clearTimeout(timeout);
        if (data.version!==ONLINE_VERSION) {
          this.emit('error',{message:'This lobby uses a different game version. Refresh the page.'});
          this.close(false);return;
        }
        if (this.seat) this.send({type:'resume',room:this.seat.room,session:this.seat.session});
        else if (request) {this.send(request);request=null;}
        return;
      }
      if (data.type==='seat') {
        this.seat=data;this.attempt=0;this.retryUntil=0;this.emit('connection',{connected:true});
      }
      if (data.type==='error' && data.code==='SESSION_EXPIRED') this.close(false);
      if (data.type==='closed') this.close(false);
      this.emit(data.type,data);
    });
    socket.addEventListener('close',()=>{
      clearTimeout(timeout);
      if (this.ended || this.socket!==socket) return;
      if (!this.seat) {this.emit('error',{message:'Could not connect to the game host.'});return;}
      this.retryUntil ||= Date.now()+RECONNECT_MS;
      this.emit('connection',{connected:false});
      const delay=Math.min(4200,500*2**this.attempt++);
      this.retry=setTimeout(()=>{
        if (Date.now()>=this.retryUntil) {
          this.close(false);
          this.emit('error',{message:'The reconnect window expired.'});
          this.emit('closed',{message:'Connection lost.'});
        } else this.open();
      },delay);
    });
  }
  order(method,args) {
    if (this.send({type:'order',seq:++this.seq,method,args})) return {ok:true,pending:true};
    return {ok:false,reason:'Waiting for the connection to recover.'};
  }
  close(leave=true) {
    if (leave) this.send({type:'leave'});
    this.ended=true;
    clearTimeout(this.retry);
    this.socket?.close(1000);
  }
}
