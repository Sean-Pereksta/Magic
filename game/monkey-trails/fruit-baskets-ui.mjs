import {fruitBasketView} from './fruit-baskets.mjs';
const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const number=value=>Math.max(0,Math.ceil(Number(value)||0)).toLocaleString();
export function renderFruitBasket(s){
 const basket=fruitBasketView(s);if(!basket)return '';
 return `<section class="panel fruit-basket-card" data-fruit-basket="${esc(basket.id)}" aria-label="Fruit basket walking challenge"><span class="eyebrow">GROVE FIND · FRUIT BASKET</span><h3>🍎 A basket of ${basket.fruit} fruit</h3><p class="fruit-basket-remaining" data-fruit-basket-remaining role="status" aria-live="polite"><strong>${basket.ready?'Ready to collect':`${number(basket.remainingFeet)} ft left`}</strong></p><p class="fine">Walk 500 accepted feet after finding the basket, in any direction in this region. Progress pauses at camp and during battles.</p><progress data-fruit-basket-progress value="${basket.progressFeet}" max="${basket.goalFeet}" aria-label="Walking to collect fruit basket"></progress><button type="button" class="primary wide" data-action="fruit-basket-claim" data-id="${esc(basket.id)}" ${basket.canClaim?'':'disabled'}>${basket.ready?`Collect ${basket.fruit} fruit`:'Keep walking to collect'}</button></section>`;
}
export function refreshFruitBasket(s,root=globalThis.document){
 if(!root)return;const basket=fruitBasketView(s);
 for(const host of root.querySelectorAll('[data-fruit-basket-host]')){const current=host.querySelector('[data-fruit-basket]');if(!basket){if(current)host.innerHTML='';}else if(current?.dataset.fruitBasket!==basket.id)host.innerHTML=renderFruitBasket(s);}
 for(const node of root.querySelectorAll('[data-fruit-basket]')){
  if(!basket||node.dataset.fruitBasket!==basket.id){node.hidden=true;continue;}node.hidden=false;
  const text=node.querySelector('[data-fruit-basket-remaining] strong');if(text)text.textContent=basket.ready?'Ready to collect':`${number(basket.remainingFeet)} ft left`;
  const progress=node.querySelector('[data-fruit-basket-progress]');if(progress){progress.value=basket.progressFeet;progress.setAttribute('aria-valuetext',`${number(basket.remainingFeet)} feet left`);}
  const button=node.querySelector('[data-action="fruit-basket-claim"]');if(button){button.disabled=!basket.canClaim;button.textContent=basket.ready?`Collect ${basket.fruit} fruit`:'Keep walking to collect';}
 }
}
