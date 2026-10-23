import {ITEMS,MOVES,SPECIES,ELEMENTS,stats} from './core.mjs';
import {portrait,pickupIcon} from './art.mjs';
import {availableMoves,bossHealth,duelStake} from './social-combat.mjs';

const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const number=value=>Math.max(0,Number(value)||0),count=value=>Math.floor(number(value)).toLocaleString();
const idOf=p=>typeof p==='string'?p:p?.id||p?.uid||p?.playerId||'';
const displayName=p=>p?.name||p?.nickname||SPECIES[p?.species]?.name||'Trail companion';
const action=(name,label,{id='',disabled=false,cls='',extra=''}={})=>`<button type="button" data-social-action="${name}"${id?` data-id="${esc(id)}"`:''}${disabled?' disabled':''}${cls?` class="${cls}"`:''}${extra?` ${extra}`:''}>${label}</button>`;
const list=value=>Array.isArray(value)?value:Object.values(value||{});
const meId=model=>idOf(model.me)||model.uid||'';
const profileFor=(uid,model)=>[model.me,...list(model.members),...list(model.nearby)].find(p=>idOf(p)===uid);
const playerName=(uid,model)=>{const p=profileFor(uid,model);return p?displayName(p):uid||'Trail player';};
const walletOf=p=>p?.wallet||{fruit:0,items:{}};
const assets=['fruit',...Object.keys(ITEMS)];
const assetName=id=>id==='fruit'?'Fruit':ITEMS[id]?.name||id;
const assetOptions=(selected='fruit')=>assets.map(id=>`<option value="${id}" ${selected===id?'selected':''}>${esc(assetName(id))}</option>`).join('');
const select=(name,label,options)=>`<label>${label}<select name="${name}">${options}</select></label>`;
const amountInput=(name='amount',label='Amount')=>`<label>${label}<input name="${name}" type="number" min="1" max="9999" step="1" value="1" required inputmode="numeric"></label>`;
function submit(label){return `<button type="submit" class="primary">${label}</button>`;}
const SYMBOLS={
 leaf:'<path d="M25 45Q18 19 49 18Q50 44 25 45Zm0 0 18-18M34 36v-8m0 8h8"/>',
 flame:'<path d="M39 12c3 13-12 15-7 28 4-2 6-7 5-11 15 10 17 23 7 30-12 8-29 0-27-14 1-9 8-15 12-22-1 7 1 9 4 11 6-8 7-15 6-22Z"/>',
 moon:'<path d="M48 50A22 22 0 0 1 28 16a24 24 0 1 0 20 34Z"/>',
 bolt:'<path d="m39 15-18 24h13l-3 18 20-27H38Z"/>',
 crown:'<path d="m17 25 10 9 9-17 9 17 10-9-5 27H22ZM23 46h26"/>',
 paw:'<path d="M23 47c0-7 8-9 13-15 5 6 13 8 13 15 0 12-10 4-13 4s-13 8-13-4Z"/><ellipse cx="22" cy="29" rx="5" ry="7"/><ellipse cx="33" cy="23" rx="5" ry="7"/><ellipse cx="45" cy="26" rx="5" ry="7"/><ellipse cx="53" cy="36" rx="4" ry="6"/>'
};
export const EMBLEM_SYMBOLS=Object.keys(SYMBOLS);
export const EMBLEM_COLORS=['#365d45','#78629a','#bd654d','#3f7f95','#b08c36','#68763f'];
export const EMBLEM_SHAPES=['shield','circle','diamond'];
export function emblemSVG(config={}){
 const shape=EMBLEM_SHAPES.includes(config.shape)?config.shape:'shield',symbol=SYMBOLS[config.symbol]?config.symbol:'leaf',color=/^#[0-9a-f]{6}$/i.test(config.color||'')?config.color:EMBLEM_COLORS[0];
 const outline=shape==='circle'?'<circle cx="36" cy="36" r="32"/>':shape==='diamond'?'<path d="m36 3 32 33-32 33L4 36Z"/>':'<path d="M7 10 36 3l29 7v23c0 19-18 30-29 36C25 63 7 52 7 33Z"/>';
 return `<svg class="tribe-emblem" viewBox="0 0 72 72" role="img" aria-label="${esc(symbol)} tribe emblem"><g fill="${color}" stroke="#f1df9e" stroke-width="3">${outline}</g><g fill="#f8eed0" stroke="#f8eed0" stroke-width="2" stroke-linejoin="round">${SYMBOLS[symbol]}</g></svg>`;
}
function emblemFields(emblem={}){
 return `<div class="social-form-grid emblem-fields">${select('shape','Decal shape',EMBLEM_SHAPES.map(v=>`<option value="${v}" ${v===emblem.shape?'selected':''}>${v[0].toUpperCase()+v.slice(1)}</option>`).join(''))}${select('symbol','Symbol',EMBLEM_SYMBOLS.map(v=>`<option value="${v}" ${v===emblem.symbol?'selected':''}>${v[0].toUpperCase()+v.slice(1)}</option>`).join(''))}${select('color','Tribe color',EMBLEM_COLORS.map((v,i)=>`<option value="${v}" ${v===emblem.color?'selected':''}>${['Forest','Violet','Ember','River','Gold','Olive'][i]}</option>`).join(''))}</div>`;
}
function profileOnline(p,model){
 const now=number(model.now)||Date.now(),seen=p?.seenAt?.toMillis?.()??p?.seenAt??p?.updatedAt?.toMillis?.()??p?.updatedAt??0;
 return p?.online===true&&now-number(seen)<90000&&(!p.expiresAt||number(p.expiresAt?.toMillis?.()??p.expiresAt)>now);
}
function canChallenge(p,model){return idOf(p)!==meId(model)&&profileOnline(p,model)&&!p.busy&&!p.activeDuel&&!p.activeRaid&&!model.busy&&!model.me?.busy&&!model.me?.activeDuel&&!model.me?.activeRaid&&!model.duel&&!model.raid;}
function memberCard(p,model){
 const online=profileOnline(p,model),busy=p.busy||p.activeDuel||p.activeRaid;
 return `<li class="social-member"><span class="social-status-dot ${online&&!busy?'online':''}" aria-hidden="true"></span><div><strong>${esc(displayName(p))}${idOf(p)===meId(model)?' <small>You</small>':''}</strong><small>${online?(busy?'In a battle':'Online · ready'):'Offline'}${p.averageLevel?` · team Lv ${count(p.averageLevel)}`:''}</small></div>${idOf(p)!==meId(model)?action('challenge','Challenge',{id:idOf(p),disabled:!canChallenge(p,model),cls:'small'}):''}</li>`;
}
function tribeSetup(){
 return `<section class="panel social-panel"><span class="eyebrow">FIND YOUR PEOPLE</span><h2>Build a tribe. Share the trail.</h2><p>Up to ten adventurers share supplies, take on a growing tribe boss, and challenge one another.</p><details class="social-disclosure" open><summary>Create your tribe</summary><form data-social-form="create-tribe"><label>Tribe name<input name="name" maxlength="32" minlength="3" placeholder="Canopy companions" required></label>${emblemFields()}${submit('Create tribe')}</form></details><details class="social-disclosure"><summary>Join with a tribe code</summary><form data-social-form="join-tribe"><label>Tribe code<input name="code" maxlength="24" autocomplete="off" autocapitalize="characters" spellcheck="false" required placeholder="Enter their code"></label>${submit('Join tribe')}</form></details></section>`;
}
function memberSection(model){
 const t=model.tribe,members=list(model.members||t.members),owner=(t.ownerId||t.ownerUid||t.owner)===meId(model),code=t.code||t.id||model.me?.tribeId||'';
 return `<section class="panel social-panel"><div class="tribe-heading">${emblemSVG(t.emblem)}<div><span class="eyebrow">YOUR TRIBE · ${members.length} / 10</span><h2>${esc(t.name)}</h2><p class="tribe-code">Invite code <strong>${esc(code)}</strong></p></div></div><ul class="social-members">${members.map(p=>memberCard(p,model)).join('')}</ul>${owner?`<details class="social-disclosure"><summary>Edit tribe decal</summary><form data-social-form="update-emblem">${emblemFields(t.emblem)}${submit('Save emblem')}</form></details>`:''}${action('leave-tribe','Leave tribe',{cls:'social-subtle'})}</section>`;
}
function sharedBoss(model){
 const t=model.tribe,b=t.boss||t.raid||{},level=number(b.level||t.bossLevel)||1,max=number(b.maxHp)||bossHealth(level),hp=b.hp==null?number(t.bossHp)||max:number(b.hp),next=number(model.me?.raidReadyAt||model.me?.nextRaidAt||(model.me?.lastRaidAt?model.me.lastRaidAt+3600000:0)),remaining=Math.max(0,next-(number(model.now)||Date.now()));
 return `<section class="panel social-panel"><div class="social-section-head"><div><span class="eyebrow">EVERY MEMBER HELPS</span><h2>Tribe boss · ${count(level)}</h2></div><span class="social-tag">10 turns</span></div><p>Send your three companions in for ten turns. The boss cannot hurt them. Your damage stays on the tribe’s shared boss.</p>${healthMeter(hp,max,'Shared boss health')}<div class="social-boss-stats"><span><strong>${count(hp)}</strong> HP remaining</span><span>Next attempt ${remaining?`in ${Math.ceil(remaining/60000)} min`:'ready'}</span></div>${action('start-raid',remaining?'Resting between attempts':'Fight the tribe boss',{disabled:remaining>0||model.busy||model.me?.busy||!!model.duel,cls:'primary wide'})}<p class="fine">One attempt per hour. Defeat the boss together to reward every member, then face a stronger one. Rewards wait for offline members too.</p></section>`;
}
function walletSection(model){
 const w=walletOf(model.me),items=w.items||{},hasItems=Object.entries(items).filter(([,v])=>number(v)>0);
 return `<section class="panel social-panel"><span class="eyebrow">YOUR TRIBE POUCH</span><h2>Trading supplies</h2><div class="social-wallet"><span>${pickupIcon('fruit')}<strong>${count(w.fruit)}</strong> fruit</span>${hasItems.map(([id,n])=>`<span>${pickupIcon(id)}<strong>${count(n)}</strong> ${esc(assetName(id))}</span>`).join('')}</div><p class="fine">Move supplies into your tribe pouch to trade, or move them back whenever available. Friendly battle stakes move from your expedition pack into this pouch automatically when needed. If an invitation is declined or expires, that fruit stays yours here and can be moved back.</p><details class="social-disclosure"><summary>Move supplies between packs</summary><div class="social-pack-transfer"><form data-social-form="deposit"><h3>From expedition</h3><p class="fine">${count(model.localFruit)} fruit in your expedition</p><div class="social-form-grid">${select('asset','Supply',assetOptions())}${amountInput()}</div>${submit('Move into tribe pouch')}</form><form data-social-form="withdraw"><h3>Back to expedition</h3><div class="social-form-grid">${select('asset','Supply',assetOptions())}${amountInput()}</div>${submit('Move back to expedition')}</form></div></details></section>`;
}
function tradingSection(model){
 const members=list(model.members||model.tribe?.members).filter(p=>idOf(p)!==meId(model)),options=members.map(p=>`<option value="${esc(idOf(p))}">${esc(displayName(p))}</option>`).join(''),offers=list(model.offers).filter(o=>o.status==='open'||o.status==='pending'),mine=meId(model);
 return `<section class="panel social-panel"><span class="eyebrow">PASS IT ALONG</span><h2>Gifts & trades</h2>${members.length?`<details class="social-disclosure"><summary>Give a teammate supplies</summary><form data-social-form="gift">${select('toId','Teammate',options)}<div class="social-form-grid">${select('asset','Gift',assetOptions())}${amountInput()}</div>${submit('Send gift')}</form></details><details class="social-disclosure"><summary>Offer a trade</summary><form data-social-form="offer-trade">${select('toId','Teammate',options)}<h3>You give</h3><div class="social-form-grid">${select('giveAsset','Supply',assetOptions())}${amountInput('giveAmount')}</div><h3>You receive</h3><div class="social-form-grid">${select('wantAsset','Supply',assetOptions('berry'))}${amountInput('wantAmount')}</div>${submit('Offer trade')}</form></details>`:'<p>Invite another member to begin exchanging supplies.</p>'}<div class="social-offers">${offers.length?offers.map(o=>{
 const incoming=(o.toId||o.to)===mine,give=o.give||{},want=o.want||{},otherName=playerName(incoming?(o.fromId||o.from):(o.toId||o.to),model);
 return `<article class="social-offer"><strong>${incoming?'From':'To'} ${esc(otherName)}</strong><p>${count(give.amount)} ${esc(assetName(give.asset))} ⇄ ${count(want.amount)} ${esc(assetName(want.asset))}</p><div class="button-row">${incoming?action('accept-trade','Accept trade',{id:o.id,cls:'primary'}):''}${action('decline-trade',incoming?'Decline':'Cancel offer',{id:o.id})}</div></article>`;
 }).join(''):'<p class="fine">No pending trades.</p>'}</div></section>`;
}
function challengeCard(c,model){
 const incoming=(c.toId||c.to||c.recipientId)===meId(model),otherId=incoming?(c.fromId||c.from):(c.toId||c.to),other=c.fromName||c.name||playerName(otherId,model),ownStake=duelStake(model.me?.team||[]),otherProfile=profileFor(otherId,model),rivalStake=otherProfile?.team?duelStake(otherProfile.team):null;
 return `<article class="social-challenge"><div><strong>${incoming?`${esc(other)} challenged you`:'Challenge sent'}</strong><small>Your stake up to ${count(ownStake)} fruit${rivalStake!==null?` · rival up to ${count(rivalStake)}`:''} · both gain XP</small></div><div class="button-row">${incoming?action('accept-challenge','Accept',{id:c.id,disabled:model.busy||model.me?.busy||!!model.duel,cls:'primary'}):''}${action('decline-challenge',incoming?'Decline':'Cancel',{id:c.id})}</div></article>`;
}
function nearbySection(model){
 const nearby=list(model.nearby).filter(p=>canChallenge(p,model)),pending=list(model.challenges).filter(c=>(!c.status||c.status==='pending')&&(!c.expiresAt||number(c.expiresAt)>(number(model.now)||Date.now()))),discoverable=!!(model.me?.discoverable||model.discoverable);
 return `<section class="panel social-panel"><div class="social-section-head"><div><span class="eyebrow">FRIENDLY RIVALS</span><h2>Players near you</h2></div><span class="social-tag">Within 1 mile</span></div><label class="social-discovery-option"><input type="checkbox" data-social-action="discoverability" ${discoverable?'checked':''}><span><strong>Let nearby players find me</strong><small>Visible only while online and available. No location pin or route is shown.</small></span></label>${model.locationMessage?`<p class="fine">${esc(model.locationMessage)}</p>`:''}<ul class="social-members">${nearby.length?nearby.map(p=>memberCard(p,model)).join(''):'<li class="social-empty">No available nearby players right now. Tribe members can challenge from anywhere.</li>'}</ul><p class="fine">Friendly battles use separate teams. Your expedition health, energy, items and run stay unchanged. Only fruit changes hands, and both players gain XP. The stake is at most 10 fruit or twice the team’s average level, whichever is lower. Play again as often as you like after returning.</p>${pending.map(c=>challengeCard(c,model)).join('')}</section>`;
}
function healthMeter(hp,max,label){
 const value=Math.max(0,Math.min(number(hp),number(max))),percent=max?value/max*100:0;
 return `<div class="social-meter-label"><span>${esc(label)}</span><strong>${count(value)} / ${count(max)}</strong></div><div class="social-health" role="progressbar" aria-label="${esc(label)}" aria-valuemin="0" aria-valuemax="${number(max)}" aria-valuenow="${value}"><i style="width:${percent}%"></i></div>`;
}
function fighter(p,label){
 if(!p)return '';
 const maxHp=p.maxHp||p.combatStats?.hp||stats(p).hp,maxEnergy=p.maxEnergy||p.combatStats?.maxEnergy||stats(p).maxEnergy;
 return `<article class="social-fighter"><small>${esc(label)}</small>${portrait(p,{state:p.hp<=0?'knockout':'idle'})}<strong>${esc(displayName(p))}</strong><span>Lv ${count(p.level)} · ${(p.types||[]).map(t=>esc(ELEMENTS[t]?.name||t)).join(' + ')||'Adaptable'}</span>${healthMeter(p.hp,maxHp,`${displayName(p)} health`)}<small>${count(p.energy)} / ${count(maxEnergy)} energy${Object.keys(p.status||{}).length?` · ${Object.keys(p.status).map(esc).join(', ')}`:''}</small></article>`;
}
function moveButtons(p,turns,members,kind,disabled,sequence){
 if(!p)return '';
 const available=availableMoves(p,turns,members);
 return `<div class="social-moves">${available.map(info=>{const m=MOVES[info.id];return action(`${kind}-move`,`<strong>${esc(m.name)}</strong><span>${number(m.cost)} energy${m.power?` · ${m.power} power`:''}</span><small>${esc(info.available?m.desc:info.reason||'Unavailable')}</small>`,{id:info.id,disabled:disabled||!info.available,cls:'social-move',extra:`data-sequence="${number(sequence)}"`});}).join('')}</div>`;
}
function teamSwitches(members,current,kind,disabled,sequence){
 return `<div class="social-team-switches" aria-label="Your three companions">${members.map((p,index)=>action(`${kind}-switch`,`${portrait(p,{state:p.hp<=0?'knockout':'idle'})}<span><strong>${esc(displayName(p))}</strong><small>${p.hp<=0?'Knocked out':`${count(p.hp)} HP`}${index===current?' · Active':''}</small></span>`,{id:String(index),disabled:disabled||index===current||p.hp<=0,cls:index===current?'is-active':'',extra:`data-index="${index}" data-sequence="${number(sequence)}"`})).join('')}</div>`;
}
function combatLog(log){return `<details class="social-disclosure social-combat-log"><summary>Battle log</summary><ol>${list(log).slice(-8).map(line=>`<li>${esc(typeof line==='string'?line:line.text||line.message||'')}</li>`).join('')}</ol></details>`;}
function duelView(model){
 const wrapper=model.duel,d=wrapper.state||wrapper.duel||wrapper,my=meId(model),side=d.players?.indexOf(my),mine=side<0?0:side,opponent=1-mine,members=d.teams?.[mine]||[],their=d.teams?.[opponent]||[],a=d.active?.[mine]||0,b=d.active?.[opponent]||0,finished=d.status==='finished'||wrapper.status==='finished',myTurn=d.turn===mine&&side>=0&&!finished,waiting=!!model.pendingAction;
 const title=finished?(d.winner===my?'A friendly victory':d.winner?'A good match':'An even match'):myTurn?'Your turn':'Waiting for your rival';
 return `<section class="panel social-panel social-battle"><div class="social-section-head"><div><span class="eyebrow">FRIENDLY MONKEY BATTLE</span><h2>${title}</h2></div><span class="social-tag">Turn ${count((d.sequence||0)+1)}</span></div><p class="social-battle-assurance">Your expedition and original monkey health are safe.</p><div class="social-battlefield">${fighter(members[a],d.names?.[mine]||'Your team')}${fighter(their[b],d.names?.[opponent]||'Rival team')}</div>${finished?`<div class="social-result"><strong>${d.winner===my?'You won this round of the rivalry.':d.winner?'There is always the next friendly match.':'Both teams held their ground.'}</strong><p>Both players gain XP. Fruit is settled from the agreed stake; your expedition health, energy and supplies are unchanged.</p>${action('return','Return to the trail',{cls:'primary wide'})}<small>You become available for new challenges after returning.</small></div>`:`<p class="social-turn-state" role="status">${waiting?'Sending your action…':myTurn?'Choose one action. Your rival acts next.':'Your rival is choosing an action. The match stays synced.'}</p>${moveButtons(members[a],d.turnCounts?.[mine]||0,members,'duel',!myTurn||waiting,d.sequence)}${teamSwitches(members,a,'duel',!myTurn||waiting||number(members[a]?.effectUntil?.root)>number(d.turnCounts?.[mine]),d.sequence)}<div class="social-battle-actions">${action('surrender','Concede friendly match',{disabled:waiting,cls:'social-subtle'})}${model.canClaimTimeout?action('duel-timeout','Claim disconnected match',{cls:'small'}):''}</div>`}${combatLog(d.log)}</section>`;
}
function raidView(model){
 const wrapper=model.raid,r=wrapper.state||wrapper.attempt||wrapper,members=r.team||[],active=r.active||0,finished=r.status==='finished'||r.status==='complete'||number(r.turns)>=10;
 return `<section class="panel social-panel social-battle"><div class="social-section-head"><div><span class="eyebrow">TRIBE BOSS ATTEMPT</span><h2>${finished?'Damage ready for your tribe':'Ten turns. Every hit helps.'}</h2></div><span class="social-tag">${count(r.turns)} / 10 turns</span></div><div class="social-raid-score"><strong>${count(r.totalDamage)}</strong><span>damage for the tribe</span></div><div class="social-battlefield">${fighter(members[active],'Your active companion')}${fighter(r.boss,'Tribe boss')}</div><p class="fine">The boss never attacks. Your companions return exactly as they were before this attempt.</p>${r.submitted?`<div class="social-result"><strong>${r.result?.defeated?'Your tribe defeated the boss!':r.result?.damageApplied===0?'Your tribe already defeated that boss.':'Your damage is on the board.'}</strong><p>${count(r.result?.damageApplied??r.totalDamage)} damage added to the shared boss.${r.result?.defeated?' Rewards are waiting in every member’s tribe pouch.':''}</p>${action('return','Return to the trail',{cls:'primary wide'})}</div>`:finished?action('submit-raid',model.pendingAction?'Adding your damage…':'Add damage to the tribe boss',{disabled:!!model.pendingAction,cls:'primary wide'}):`${moveButtons(members[active],r.turns,members,'raid',!!model.pendingAction,r.sequence)}${teamSwitches(members,active,'raid',!!model.pendingAction,r.sequence)}${action('return','End attempt & return',{disabled:!!model.pendingAction,cls:'social-subtle'})}`}${combatLog(r.log)}</section>`;
}
export function renderSocialBanner(model={}){
 if(!model.me||model.status!=='ready')return '';
 if(model.busy||model.me.busy||model.me.activeDuel||model.me.activeRaid||model.duel||model.raid)return `<div class="social-banner is-busy"><span>Monkey battle in progress</span><small>New challenges are paused until you return.</small></div>`;
 const nearby=list(model.nearby).filter(p=>canChallenge(p,model)),incoming=list(model.challenges).filter(c=>(c.toId||c.to||c.recipientId)===meId(model)&&(!c.status||c.status==='pending')&&(!c.expiresAt||number(c.expiresAt)>(number(model.now)||Date.now())));
 if(!nearby.length&&!incoming.length)return '';
 return `<div class="social-banner">${nearby.length?action('open-social',`${nearby.length} nearby ${nearby.length===1?'player':'players'} online`,{cls:'social-nearby-link'}):''}${incoming.slice(0,2).map(c=>challengeCard(c,model)).join('')}</div>`;
}
export function renderRewardNotices(receipts=[]){
 return `<div class="social-reward-notices" aria-live="polite">${list(receipts).filter(r=>!r.dismissed&&!r.acknowledged&&!r.seen).slice(0,5).map(r=>{
  const payload=r.payload||r.reward||r,fruit=number(payload.fruit),xp=number(payload.xp),items=payload.items||{},title=r.title||(r.type==='raid'?'Your tribe defeated a boss!':r.type==='duel'?'Friendly battle rewards':'Supplies from your tribe');
  return `<article class="social-reward-notice"><div class="social-reward-icon" aria-hidden="true">${pickupIcon('fruit')}</div><div><strong>${esc(title)}</strong>${r.message?`<p>${esc(r.message)}</p>`:''}<small>${[fruit?`${count(fruit)} fruit`:'',xp?`${count(xp)} XP`:'',...Object.entries(items).filter(([,n])=>number(n)>0).map(([id,n])=>`${count(n)} ${assetName(id)}`)].filter(Boolean).map(esc).join(' · ')||'Your tribe pouch has been updated.'}</small></div>${action('dismiss-reward','×',{id:r.id,cls:'social-reward-close',extra:'aria-label="Dismiss reward notification"'})}</article>`;
 }).join('')}</div>`;
}
export function renderSocialView(model={}){
 if(model.status==='practice')return `<section class="panel social-panel"><span class="eyebrow">TRIBES & FRIENDLY BATTLES</span><h2>A shared adventure awaits</h2><p>Open a regular GPS trail to join a tribe, exchange supplies, and challenge friends. Practice stays separate.</p></section>`;
 if(model.status!=='ready')return `<section class="panel social-panel"><span class="eyebrow">TRIBES & FRIENDLY BATTLES</span><h2>${model.status==='connecting'?'Opening the tribe trail…':'Connect with other adventurers'}</h2><p>${esc(model.error||'Connect your regular trail to meet a tribe and share rewards.')}</p>${action('connect','Connect to tribes',{disabled:model.status==='connecting',cls:'primary'})}</section>`;
 const error=model.error?`<p class="social-error" role="alert">${esc(model.error)}</p>`:'';
 if(model.duel)return `<div class="social-view">${error}${duelView(model)}</div>`;
 if(model.raid)return `<div class="social-view">${error}${raidView(model)}</div>`;
 return `<div class="social-view"><div class="social-page-heading"><span class="eyebrow">BETTER TOGETHER</span><h1>Tribes & friendly rivals</h1><p>Share discoveries. Take on a giant. Meet your next rival.</p></div>${model.error?`<p class="social-error" role="alert">${esc(model.error)}</p>`:''}<div class="social-grid">${model.tribe?memberSection(model):tribeSetup()}${model.tribe?sharedBoss(model):''}${nearbySection(model)}${walletSection(model)}${model.tribe?tradingSection(model):''}</div></div>`;
}
