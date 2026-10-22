import {delta} from './gps.mjs';
const RAD=Math.PI/180,EARTH=6371008.8;
// Absolute coordinates remain inside this object and the encrypted device-only
// radar slot. Cloud game state contains only species identity and challenge data.
export class EncounterRadar {
  constructor(){this.vault=null;this.anchor=null;this.fix=null;this.generation=0;this.pending=Promise.resolve();}
  async bind(vault){this.vault=vault;this.fix=null;const gen=++this.generation;const data=await vault.loadDeviceData('radar');if(gen!==this.generation)return;if(data&&typeof data.id==='string'&&Number.isFinite(data.lat)&&Number.isFinite(data.lon)&&Math.abs(data.lat)<=90&&Math.abs(data.lon)<=180)this.anchor=data;else this.anchor=null;}
  observe(fix,state){
    if(!(fix.meters>0||fix.positionValid)||![fix.lat,fix.lon,fix.timestamp].every(Number.isFinite))return;
    this.fix={lat:fix.lat,lon:fix.lon,timestamp:fix.timestamp};
    const u=state.run.ultra;if(!u||u.safeAlternative||['claimed','battle'].includes(u.status))return;
    if(this.anchor?.id===u.id)return;
    if(!(fix.meters>0))return;
    // A device without the private anchor starts at the original offset. It
    // cannot claim approach distance by reopening a cloud save on another phone.
    const bearing=u.bearing*RAD,distance=Math.max(100,u.spawnDistance||250);
    this.anchor={id:u.id,lat:fix.lat+Math.cos(bearing)*distance/EARTH/RAD,lon:((fix.lon+Math.sin(bearing)*distance/(EARTH*Math.max(.01,Math.cos(fix.lat*RAD)))/RAD+540)%360)-180};
    const anchor={...this.anchor},vault=this.vault;
    this.pending=this.pending.catch(()=>{}).then(()=>vault?.saveDeviceData('radar',anchor));
  }
  view(state,now=Date.now()){
    const u=state?.run.ultra,valid=!!u&&this.anchor?.id===u.id&&!!this.fix;
    const vector=valid?delta(this.fix,this.anchor):{east:Math.sin((u?.bearing||0)*RAD)*(u?.spawnDistance||250),north:Math.cos((u?.bearing||0)*RAD)*(u?.spawnDistance||250),meters:u?.spawnDistance||250};
    const gpsFresh=valid&&now-this.fix.timestamp<=10000&&now>=this.fix.timestamp-2000;
    return {east:vector.east,north:vector.north,distance:vector.meters,bearing:(Math.atan2(vector.east,vector.north)/RAD+360)%360,gpsFresh,withinRadius:gpsFresh&&vector.meters<=100*.3048,calibrating:!valid};
  }
  clear(){this.generation++;this.anchor=null;this.fix=null;const vault=this.vault;this.pending=this.pending.catch(()=>{}).then(()=>vault?.saveDeviceData('radar',null));}
}
