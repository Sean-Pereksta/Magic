import { RESOURCES } from './data.mjs';
import { tradeItems } from './trade-package.mjs';

export function mountTradeItems(form) {
  const containers={};
  for(const side of ['give','receive']){
    const initial=form.querySelector(`#${side}-amount`).closest('.form-row');
    const box=document.createElement('div');box.id=`${side}-items`;initial.replaceWith(box);box.append(initial);
    containers[side]=box;
    const more=document.createElement('button');more.type='button';more.textContent='+ Add Resource';more.dataset.addResource=side;box.append(more);
    initial.dataset.tradeRow=side;initial.classList.add('trade-item-row');
    initial.querySelector('input').dataset.amount='';initial.querySelector('select').dataset.resource='';
    const remove=document.createElement('button');remove.type='button';remove.textContent='×';remove.setAttribute('aria-label','Remove resource');remove.title='Remove resource';remove.dataset.removeResource=side;initial.append(remove);
  }
  function update(){
    for(const [side,box] of Object.entries(containers)){
      const rows=[...box.querySelectorAll('[data-trade-row]')],used=rows.map(r=>r.querySelector('select').value);
      rows.forEach((row,index)=>{
        row.querySelector('input').id=index===0?`${side}-amount`:'';row.querySelector('select').id=index===0?`${side}-resource`:'';
        row.querySelector('button').disabled=rows.length===1;
        for(const option of row.querySelectorAll('option'))option.disabled=used.includes(option.value)&&option.value!==row.querySelector('select').value;
      });
      box.querySelector('[data-add-resource]').disabled=rows.length>=RESOURCES.length;
    }
  }
  function add(side,item){
    const box=containers[side],row=document.createElement('div');row.className='form-row trade-item-row';row.dataset.tradeRow=side;
    const label=document.createElement('label');label.textContent='Amount';const input=document.createElement('input');input.type='number';input.min='1';input.max='1000';input.step='1';input.value=String(item.amount);input.dataset.amount='';label.append(input);
    const resLabel=document.createElement('label');resLabel.textContent='Resource';const select=document.createElement('select');select.dataset.resource='';
    for(const resource of RESOURCES){const option=document.createElement('option');option.value=resource;option.textContent=resource;select.append(option);}select.value=item.resource;resLabel.append(select);
    const remove=document.createElement('button');remove.type='button';remove.textContent='×';remove.setAttribute('aria-label','Remove resource');remove.title='Remove resource';remove.dataset.removeResource=side;
    row.append(label,resLabel,remove);box.insertBefore(row,box.querySelector('[data-add-resource]'));update();
  }
  form.addEventListener('click',e=>{
    const button=e.target.closest('button');if(!button)return;
    const side=button.dataset.addResource;
    if(side){const used=[...containers[side].querySelectorAll('select')].map(el=>el.value),resource=RESOURCES.find(r=>!used.includes(r));if(resource)add(side,{resource,amount:1});}
    if(button.dataset.removeResource){button.closest('[data-trade-row]').remove();update();}
  });
  form.addEventListener('change',e=>{if(e.target.matches('[data-resource]'))update();});
  const read=side=>[...containers[side].querySelectorAll('[data-trade-row]')].map(row=>({resource:row.querySelector('select').value,amount:Number(row.querySelector('input').value)}));
  return {
    read,
    load(i){for(const side of ['give','receive']){for(const row of containers[side].querySelectorAll('[data-trade-row]'))row.remove();for(const item of tradeItems(i,side))add(side,item);}},
    mode(enabled){for(const box of Object.values(containers)){for(const row of box.querySelectorAll('[data-trade-row]')){const first=row===box.querySelector('[data-trade-row]');row.hidden=!enabled&&!first;row.querySelector('button').hidden=!enabled;}box.querySelector('[data-add-resource]').hidden=!enabled;box.querySelector('input').min=enabled?'1':'0';}update();}
  };
}
