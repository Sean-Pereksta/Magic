// Presentation only: keep the original action nodes, listeners, drafts and session.
export function installChatComposer(textarea){
  const form=textarea.closest('form');
  textarea.rows=1;form.classList.add('chat-composer');
  function resize(){
    const expanded=textarea===textarea.ownerDocument.activeElement;
    form.classList.toggle('composer-expanded',expanded);
    textarea.style.height=expanded?'auto':'40px';
    if(expanded)textarea.style.height=`${Math.min(128,Math.max(88,textarea.scrollHeight))}px`;
    else textarea.scrollTop=0;
  }
  textarea.addEventListener('focus',resize);textarea.addEventListener('input',resize);textarea.addEventListener('blur',resize);
  textarea.addEventListener('keydown',event=>{if(event.key==='Escape'){event.preventDefault();event.stopPropagation();textarea.blur();}});
  resize();return resize;
}
export function installChatOptions(details){
  const doc=details.ownerDocument,summary=details.querySelector('summary');
  summary.setAttribute('aria-expanded',String(details.open));
  details.addEventListener('toggle',()=>summary.setAttribute('aria-expanded',String(details.open)));
  doc.addEventListener('pointerdown',event=>{if(details.open&&!details.contains(event.target))details.open=false;});
  details.addEventListener('keydown',event=>{
    if(event.key==='Escape'&&details.open){event.preventDefault();event.stopPropagation();details.open=false;summary.focus();}
  });
  // Close the menu before invoking an action, so focus can move into its dialog,
  // map picker or draft. Selects, switches and nested disclosures stay usable.
  details.addEventListener('click',event=>{if(details.open&&event.target.closest('button')){details.open=false;summary.focus({preventScroll:true});}},true);
  details.closest('dialog')?.addEventListener('close',()=>{details.open=false;});
}
export function installPrivateChatWorkspace(doc,{connection}={}){
  const get=id=>doc.getElementById(id),dialog=get('diplomacy'),form=get('chat-form');
  const options=doc.createElement('details');options.id='private-chat-options';options.className='chat-options';
  options.innerHTML='<summary id="private-options-toggle" role="button" aria-controls="private-options-panel">Offer / Request <span aria-hidden="true">⌃</span></summary><div class="chat-options-panel" id="private-options-panel"><strong>Diplomatic actions</strong><div class="private-action-options"></div><div class="private-connection-options"><strong>Gemini connection</strong></div></div>';
  const actions=options.querySelector('.private-action-options'),settings=options.querySelector('.private-connection-options');
  const toolbar=doc.createElement('div');toolbar.className='chat-toolbar';
  toolbar.append(options,get('message-allowance'),get('send-chat'));
  const originalRow=form.querySelector('.button-row'),toggle=get('use-gemini').closest('label');
  settings.append(toggle,get('gemini-diagnostics'),get('retry-private-gemini'),get('retry-private-dispatch-gemini'),get('chat-notice'),get('privacy'));
  originalRow.replaceWith(toolbar);get('chat-notice').hidden=false;
  const offer=get('private-offer-request');offer.textContent='Make an offer / request';actions.append(offer);
  actions.append(dialog.querySelector('.council-tools'),get('relations-details'),get('expand-council'));
  // Court shortcuts are created on demand by renderCourtOptions next to the
  // original action row; its anchor now keeps those shortcuts in this menu too.
  for(const id of ['court-knowledge-options','court-marriage-readiness'])if(get(id))actions.append(get(id));
  get('relations-details').append(get('ruler-motto'));
  const status=doc.createElement('span');status.id='private-connection-status';status.className='chat-connection-status';status.setAttribute('role','status');
  const connectionRow=doc.createElement('div');connectionRow.className='chat-connection-row';toolbar.after(connectionRow);connectionRow.append(status);
  // Leave the shared widget's home marker and real widget in place.
  get('turnstile').classList.add('chat-verification');
  installChatComposer(get('chat-message'));installChatOptions(options);
  const refresh=()=>{const value=connection?.();if(value){status.textContent=value.text;status.title=value.detail||'';options.dataset.attention=String(!!value.attention);}};
  const observer=new doc.defaultView.MutationObserver(refresh);
  for(const node of [get('chat-notice'),get('gemini-diagnostics'),get('retry-private-gemini'),get('use-gemini'),get('turnstile')])observer.observe(node,{childList:true,subtree:true,characterData:true,attributes:true,attributeFilter:['hidden','disabled']});
  get('use-gemini').addEventListener('change',refresh);dialog.addEventListener('toggle',refresh);refresh();
  return {refresh};
}
