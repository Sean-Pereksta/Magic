import {ELEMENTS} from './core.mjs';
// Lightweight vector portraits: no image downloads, canvas loop, or map service.
export function portrait(p,{enemy=false}={}){
  const palettes={capuchin:['#805b3d','#f4d6a2'],macaque:['#9b7960','#efb5a0'],langur:['#babeb9','#354644'],mandrill:['#776655','#dcaaa0'],spider:['#59453e','#ecc9a0'],howler:['#ba7343','#593b30']};
  const [fur,face]=palettes[p.species]||palettes.capuchin;
  const a=ELEMENTS[p.types?.[0]]?.color||'#e8ca83',b=ELEMENTS[p.types?.[1]]?.color||a,stage=p.stage||0;
  const mask=p.species==='mandrill'?'<path d="M104 99l-6 31m39-31 6 31" stroke="#69cce4" stroke-width="8"/><path d="M120 105v28" stroke="#e47873" stroke-width="9"/>':'';
  const mane=p.species==='howler'?`<path d="M77 121q-9 51 43 59 53-10 44-59l-25 21-19-3-18 3z" fill="${fur}" stroke="#263c36" stroke-width="4"/>`:'';
  const tuft=p.species==='langur'?'<path d="M92 70q17-20 20-36l10 24 15-17 8 36z" fill="#dadeda"/>':`<path d="M96 73l9-23 13 12 15-17 11 28" fill="${fur}" stroke="#263c36" stroke-width="4"/>`;
  let aura='';
  if(p.types?.includes('fire'))aura+='<path d="M33 159q-18-28 4-48-2 20 12 23 3-13 7-18 16 39-23 43z" fill="#ff986d"/><path d="M184 76q-9-17 9-35-3 14 9 24 10 17-18 11z" fill="#ffd084"/>';
  if(p.types?.includes('leaf'))aura+='<path d="M29 96q-13-35 22-39 5 30-22 39m156 72q-5-29 31-32-2 31-31 32" fill="#76d99c"/><path d="M31 90l14-24m145 96 19-19" stroke="#255947" stroke-width="3"/>';
  if(p.types?.includes('water'))aura+='<path d="M33 112q-23-24 0-47 24 25 0 47m164 38q-19-22 0-43 20 23 0 43" fill="#7acbff"/><path d="M28 103q-6-7 0-16" fill="none" stroke="#dcf5ff" stroke-width="3"/>';
  if(p.types?.includes('storm'))aura+='<path d="M34 63l-12 38h15l-4 24 25-41H42l10-21zm162 46-12 26h13l-5 23 27-36h-14l9-13z" fill="#d8b4ff"/>';
  return `<svg class="monkey-art${enemy?' rival':''}" viewBox="0 0 240 240" role="img" aria-label="${p.species} monkey, evolution stage ${stage}">
    <ellipse cx="120" cy="219" rx="74" ry="12" fill="#000" opacity=".18"/>
    <circle cx="120" cy="118" r="88" fill="${a}" opacity=".07"/>
    <g class="aura">${aura}</g>
    <g class="creature">
    <path d="M155 188q63 4 56-39-4-23-22-13" fill="none" stroke="#263c36" stroke-width="17" stroke-linecap="round"/>
    <path d="M155 188q63 4 56-39-4-23-22-13" fill="none" stroke="${fur}" stroke-width="10" stroke-linecap="round"/>
    <path d="M92 178l-12 29q-19 8-15 15h38l12-37m29-7 12 29q19 8 15 15h-38l-12-37" fill="${fur}" stroke="#263c36" stroke-width="4"/>
    <path d="M82 145q-18 12-21 44l14 7 24-33m60-18q18 12 21 44l-14 7-24-33" fill="${fur}" stroke="#263c36" stroke-width="4"/>
    <ellipse cx="120" cy="163" rx="38" ry="44" fill="${fur}" stroke="#263c36" stroke-width="4"/>
    <ellipse cx="120" cy="169" rx="25" ry="30" fill="${face}" opacity=".85"/>
    <circle cx="73" cy="104" r="19" fill="${fur}" stroke="#263c36" stroke-width="4"/><circle cx="167" cy="104" r="19" fill="${fur}" stroke="#263c36" stroke-width="4"/>
    <circle cx="73" cy="104" r="10" fill="${face}"/><circle cx="167" cy="104" r="10" fill="${face}"/>
    ${mane}${tuft}<path d="M76 100q0-40 44-40t44 40v17q0 32-44 35-44-3-44-35z" fill="${fur}" stroke="#263c36" stroke-width="4"/>
    <path d="M89 100q-2-20 20-18l11 10 11-10q22-2 20 18l-5 27q-26 28-52 0z" fill="${face}"/>
    ${mask}<ellipse cx="103" cy="105" rx="5" ry="7" fill="#112625"/><ellipse cx="137" cy="105" rx="5" ry="7" fill="#112625"/>
    <circle cx="105" cy="103" r="1.7" fill="#fff"/><circle cx="139" cy="103" r="1.7" fill="#fff"/>
    <path d="M113 120q7-5 14 0l-7 6z" fill="#263c36"/><path d="M108 132q12 10 24 0" fill="none" stroke="#263c36" stroke-width="3" stroke-linecap="round"/>
    ${stage?`<path d="M82 144l38 17 38-17-6 15-32 16-32-16z" fill="${a}"/><path d="M120 161v14l32-16 6-15z" fill="${b}"/><path d="M115 163l5-6 5 6-5 8z" fill="#fff" opacity=".8"/>`:''}
    ${stage>=2?`<path d="M92 76l-4-18 16 7 16-16 16 16 16-7-4 18z" fill="${a}" stroke="#263c36" stroke-width="3"/>`:''}
    ${stage>=3?`<path d="M113 61l7-15 7 15-7 9z" fill="${b}" stroke="#fff" stroke-width="2"/><path d="M58 189l19 5m86 0 19-5" stroke="${a}" stroke-width="7"/>`:''}
    </g></svg>`;
}
