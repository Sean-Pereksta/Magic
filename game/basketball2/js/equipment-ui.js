// Appearance overrides live with the existing match/profile save. Store only
// supported, bounded values so old saves and edited storage remain usable.
const BASKETBALL_EQUIPMENT_REGIONS = {
  jersey:"Jersey",shorts:"Shorts",shoes:"Shoes",socks:"Socks",headband:"Headband",
  sleeve:"Arm sleeves",wristband:"Wristbands",accessory:"Accessories",accent:"Accents"
};
function sanitizeBasketballEquipment(value){
  const result={};
  if(!value||typeof value!=="object"||Array.isArray(value)) return result;
  for(const key of Object.keys(BASKETBALL_EQUIPMENT_REGIONS)){
    const source=value[key];
    if(!source||typeof source!=="object"||Array.isArray(source)) continue;
    const region={};
    for(const field of ["color","accent","sole"]){
      const color=source[field];
      if(typeof color==="string"&&/^#[0-9a-f]{6}$/i.test(color)) region[field]=color;
      else if(Number.isInteger(color)&&color>=0&&color<=0xffffff) region[field]=color;
    }
    if(typeof source.enabled==="boolean") region.enabled=source.enabled;
    if(["left","right","both"].includes(source.side)) region.side=source.side;
    if(Object.keys(region).length) result[key]=region;
  }
  return result;
}

function basketballEquipmentForSlot(slot){ return slot==="p2"?selectedP2Equipment:selectedP1Equipment; }
function buildEquipmentSelectors(){
  for(const slot of ["p1","p2"]){
    if(document.getElementById(`${slot}Equipment`)) continue;
    const host=document.getElementById(`${slot}Cards`)?.closest(".selectPanel");
    if(!host) continue;
    const details=document.createElement("details");
    details.id=`${slot}Equipment`;details.className="equipment-picker";
    details.innerHTML=`<summary>Equipment colors</summary><p>Customize each piece. Team rings identify your side.</p>`;
    const grid=document.createElement("div");grid.className="equipment-color-grid";
    for(const [region,label] of Object.entries(BASKETBALL_EQUIPMENT_REGIONS)){
      const row=document.createElement("label");
      row.innerHTML=`<input type="checkbox" data-region="${region}" aria-label="Use custom ${label.toLowerCase()}"><span>${label}</span><input type="color" data-color="${region}" aria-label="${slot.toUpperCase()} ${label.toLowerCase()} color">`;
      const enable=row.querySelector('[type="checkbox"]'),color=row.querySelector('[type="color"]');
      const existing=basketballEquipmentForSlot(slot)[region];
      enable.checked=!!existing&&existing.enabled!==false;
      color.value=typeof existing?.color==="string"?existing.color:toHex(existing?.color??0xffffff);
      color.disabled=!enable.checked;
      const update=()=>{
        const equipment=basketballEquipmentForSlot(slot);
        // Unchecking accessories hides them; unchecking clothing restores its item palette.
        if(enable.checked) equipment[region]={...(equipment[region]||{}),color:color.value,enabled:true};
        else if(["headband","sleeve","wristband","accessory"].includes(region)) equipment[region]={enabled:false};
        else delete equipment[region];
        color.disabled=!enable.checked;
        saveMatchSetup();refreshSneakerSelectorUI();refreshJerseySelectorUI();
        const active=slot==="p1"?p1:p2;
        if(active&&typeof updateBasketballPlayerEquipment==="function") updateBasketballPlayerEquipment(active,equipment);
      };
      enable.onchange=update;color.oninput=update;grid.appendChild(row);
    }
    const reset=document.createElement("button");reset.type="button";reset.className="optionBtn";reset.textContent="Use equipped item colors";
    reset.onclick=()=>{
      if(slot==="p1") selectedP1Equipment={};else selectedP2Equipment={};
      details.remove();buildEquipmentSelectors();saveMatchSetup();refreshSneakerSelectorUI();refreshJerseySelectorUI();
      const active=slot==="p1"?p1:p2;
      if(active&&typeof updateBasketballPlayerEquipment==="function") updateBasketballPlayerEquipment(active,{});
    };
    details.append(grid,reset);host.appendChild(details);
  }
}
