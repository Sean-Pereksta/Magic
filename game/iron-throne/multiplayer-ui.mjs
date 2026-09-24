import { mapOptions, MAP_PROFILES } from './map-profiles.mjs';
import {HOUSES} from './data.mjs';
import {alive,treaty} from './core.mjs';
import {present,seatFor,requiredRulers,millis,GRACE_MS} from './multiplayer-rounds.mjs';
import {describeIntent} from './diplomacy.mjs';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
export function humanProposals(s,other,actor){
  return (s.humanProposals||[]).filter(p=>[p.from,p.to].includes(actor)&&[p.from,p.to].includes(other)&&p.status==='pending'&&p.expires>=s.turn).map(p=>`<article class="proposal"><h4>${p.from===actor?'PROPOSAL SENT':'PROPOSAL RECEIVED'}</h4><p>${esc(describeIntent(p.intent))}</p><p class="fine">Expires turn ${p.expires}. Resources are checked again on acceptance.</p>${p.to===actor?`<div class="button-row"><button data-human-accept="${esc(p.id)}">Accept agreement</button><button data-human-decline="${esc(p.id)}">Decline</button></div>`:'<p class="fine">Waiting for the receiving ruler.</p>'}</article>`).join('');
}
export class MultiplayerUI {
  constructor(network,toast){
    this.network=network;this.toast=toast;
    this.bar=document.createElement('section');this.bar.className='online-bar';this.bar.setAttribute('aria-label','Online campaign');
    document.querySelector('.resources').before(this.bar);
    this.dialog=document.createElement('dialog');this.dialog.id='online-lobby';this.dialog.className='online-lobby';
    this.dialog.innerHTML='<div class="dialog-body"><span class="eyebrow">IRON THRONES ONLINE</span><h2>Choose your House</h2><p id="online-lobby-status" role="status">Connecting to the campaign…</p><div id="online-seats" class="online-seats"></div><div id="online-options"></div><p class="fine">Every unclaimed kingdom becomes an independent AI ruler. One to six humans can play.</p><div class="button-row"><button id="online-start" class="primary" hidden>Begin campaign</button><button id="online-retry">Reconnect</button><button id="online-copy">Copy campaign link</button><a class="hub" href="../../lobby/lobby.html">Return to lobby</a></div></div>';
    document.body.append(this.dialog);this.dialog.addEventListener('cancel',e=>e.preventDefault());this.dialog.showModal();
    const run=fn=>Promise.resolve().then(fn).catch(e=>toast(e.message));
    this.dialog.addEventListener('click',e=>{
      const b=e.target.closest('button');if(!b||b.disabled)return;
      if(b.dataset.claim)run(()=>network.setup('claim',{houseId:b.dataset.claim}));
      if(b.dataset.seat)run(()=>network.setup('seat',{houseId:b.dataset.seat,kind:b.dataset.kind}));
      if(b.id==='online-start')run(()=>network.start());
      if(b.id==='online-retry')run(()=>network.connected?network.tick():network.connect());
      if(b.id==='online-copy')run(async()=>{await navigator.clipboard.writeText(location.href);toast('Campaign link copied. Invited players join through the shared lobby before it starts.');});
    });
    this.dialog.addEventListener('submit',e=>{
      if(e.target.id!=='online-settings')return;e.preventDefault();const f=new FormData(e.target);
      run(()=>network.setup('configure',{seed:Number(f.get('seed')),preset:f.get('preset'),timerSeconds:Number(f.get('timerSeconds')),absent:f.get('absent')}));
    });
    this.bar.addEventListener('click',e=>{
      const b=e.target.closest('[data-takeover]');if(!b)return;
      const permanent=b.dataset.permanent==='true';
      if(!permanent||confirm('Permanently surrender this absent ruler’s House to AI? Their reserved seat will be released.'))run(()=>network.submit('takeover',{houseId:b.dataset.takeover,permanent}));
    });
    this.clock=setInterval(()=>this.render(this.status,this.state),1000);
  }
  render(status,state){
    if(!status)return;this.status=status;this.state=state;
    const {meta,uid,online,pending,presence}=status;
    const button=document.getElementById('end-turn');
    for(const b of document.querySelectorAll('[data-found-city]'))b.disabled=b.dataset.siteValid!=='true'||!online||!this.network.state||this.network.lastVersion!==meta?.stateVersion||pending>0||meta?.phase!=='founding';
    if(!meta){this.bar.textContent='CONNECTING…';button.disabled=true;return;}
    const actor=seatFor(meta,uid),host=meta.hostUid===uid,now=this.network.now(),setup=meta.phase==='setup';
    const statusFor=id=>{
      const seat=meta.seats[id];
      if(seat.kind==='open')return 'Open';if(seat.kind==='ai')return 'AI';
      if(meta.phase==='founding')return `${seat.name} · ${state?.founding?.houses[id]?.founded?'Capital founded':'Choosing a home'}`;
      if(state&&!alive(state,id))return `${seat.name} · House fallen`;
      return `${seat.name} · ${seat.substitute?'AI substitute':present(presence[seat.uid],now)?meta.ready[id]?'Ready':'Planning':'Disconnected'}`;
    };
    if(setup){
      if(!this.dialog.open)this.dialog.showModal();
      document.getElementById('online-lobby-status').textContent=online?'Choose an open House. The host begins the campaign when everyone is seated.':'RECONNECTING… Your House remains reserved.';
      const signature=JSON.stringify([meta.seats,meta.hostUid,uid]);
      if(signature!==this.seatSignature){
        this.seatSignature=signature;
        document.getElementById('online-seats').innerHTML=HOUSES.map(h=>{const seat=meta.seats[h.id];return `<article class="house-card" style="--house:${h.color}"><span class="house-sigil">${h.sigil}</span><h3>${esc(h.name)}</h3><p>${esc(statusFor(h.id))}</p><button data-claim="${h.id}" ${seat.kind==='ai'||seat.uid&&seat.uid!==uid?'disabled':''}>${seat.uid===uid?'Your House':'Claim House'}</button>${host&&!seat.uid?`<button data-seat="${h.id}" data-kind="${seat.kind==='ai'?'open':'ai'}">${seat.kind==='ai'?'Open to humans':'Assign AI'}</button>`:''}</article>`;}).join('');
      }
      const optSig=JSON.stringify([host,meta.options]);
      if(optSig!==this.optionSignature){
        this.optionSignature=optSig;
        const o=meta.options;
        document.getElementById('online-options').innerHTML=host?`<form id="online-settings"><div class="form-row"><label>World<select name="preset">${mapOptions(o.preset)}</select></label><label>Map seed<input name="seed" type="number" min="1" max="4294967295" value="${o.seed}" required></label></div><div class="form-row"><label>Round timer<select name="timerSeconds">${[0,120,300,600].map(n=>`<option value="${n}" ${o.timerSeconds===n?'selected':''}>${n?n/60+' minutes':'No timer'}</option>`).join('')}</select></label><label>Absent rulers<select name="absent"><option value="hold" ${o.absent==='hold'?'selected':''}>Hold remaining orders</option><option value="ai" ${o.absent==='ai'?'selected':''}>Temporary AI substitute</option></select></label></div><button>Save campaign options</button></form>`:`<p>Map seed ${o.seed} · ${o.preset} · ${o.timerSeconds?o.timerSeconds/60+' minute rounds':'No timer'}</p>`;
      }
      document.getElementById('online-start').hidden=!host;
      document.getElementById('online-start').disabled=!actor||!online;
    }else if(this.dialog.open)this.dialog.close();
    const ready=requiredRulers(meta,presence,now,state),count=ready.filter(id=>meta.ready[id]).length;
    const seconds=meta.deadline?Math.max(0,Math.ceil((meta.deadline-now)/1000)):0;
    const remaining=meta.deadline?`${Math.floor(seconds/60)}:${String(seconds%60).padStart(2,'0')} left`:'No timer';
    const expanded=this.bar.querySelector('details')?.open;
    this.bar.innerHTML=`<div role="status"><strong>${!online?'RECONNECTING…':meta.phase==='resolving'?'Resolving Turn…':meta.phase==='ended'?'Campaign complete':setup?'Kingdom selection':meta.phase==='founding'?'Found your kingdom':`Turn ${meta.turn} · Planning`}</strong><span>${actor?`${esc(meta.seats[actor].name)} · ${esc(HOUSES.find(h=>h.id===actor).name)}`:'Choose a House'} · ${meta.phase==='founding'?`${Object.values(state?.founding?.houses||{}).filter(h=>h.founded).length}/6 capitals · ${esc(MAP_PROFILES[meta.mapProfile]?.name||'')} · Seed ${meta.seed}`:`${count} / ${ready.length} rulers ready · ${remaining}`}${pending?` · ${pending} pending order${pending===1?'':'s'}`:''}</span></div><details ${expanded?'open':''}><summary>Players / Kingdoms</summary><div class="online-rulers">${HOUSES.map(h=>{const s=meta.seats[h.id],absent=s.uid&&!present(presence[s.uid],now),canTake=host&&meta.phase==='planning'&&s.kind==='human'&&now-Math.max(millis(presence[s.uid]?.at),meta.startedAt??meta.planningAt)>=GRACE_MS;return `<div style="--house:${h.color}"><strong>${h.sigil} ${esc(h.name)}</strong><span>${esc(statusFor(h.id))}${state&&actor!==h.id&&treaty(state,actor,h.id,'alliance')?' · Allied':''}</span>${canTake?`<button data-takeover="${h.id}">AI substitute next round</button><button data-takeover="${h.id}" data-permanent="true">Surrender to AI…</button>`:absent?'<small>Seat reserved</small>':''}</div>`;}).join('')}</div></details>`;
    button.textContent=meta.phase==='founding'?'Found all kingdoms':meta.phase==='resolving'?'Resolving…':meta.ready[actor]?'Unready':'Ready';
    button.disabled=!online||!this.network.state||this.network.state.turn!==meta.turn||pending>0||meta.phase!=='planning'||!actor||!!meta.seats[actor]?.substitute||state&&!alive(state,actor);
    document.getElementById('save-status').textContent=online?meta.phase==='founding'?'Online founding progress saved':`Online campaign saved · turn ${meta.turn}`:'RECONNECTING… Orders paused';
  }
}
