// Original repository-owned vector artwork. Run with Node to regenerate assets.
import { writeFile } from 'node:fs/promises';
import { BUILDINGS, RESOURCES, UNITS } from '../data.mjs';
const root=new URL('./',import.meta.url);
const line=(d,color='#e4ce96',width=3)=>`<path d="${d}" fill="none" stroke="${color}" stroke-width="${width}" stroke-linecap="round" stroke-linejoin="round"/>`;
const path=(d,fill='#a89570')=>`<path d="${d}" fill="${fill}" stroke="#263b40" stroke-width="1.6" stroke-linejoin="round"/>`;
const rect=(x,y,w,h,fill)=>`<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="1" fill="${fill}"/>`;
const circle=(x,y,r,fill)=>`<circle cx="${x}" cy="${y}" r="${r}" fill="${fill}"/>`;
const house=(x,y,w=32,h=25,roof='#9c5746')=>path(`M${x} ${y}v${-h}l${w/2} -7 ${w/2} 7v${h}l${-w/2} 7Z`,'#c1ae80')+path(`M${x+w/2} ${y+7}v${-h}l${w/2} -7v${h}Z`,'#7c8877')+path(`M${x-3} ${y-h}l${w/2+3} -18 ${w/2+3} 18 -${w/2+3} 7Z`,roof)+rect(x+w*.25,y-14,7,15,'#263b40')+rect(x+w*.66,y-h+8,5,6,'#f7d98d');
const tower=(x,y,h=45)=>rect(x,y-h,18,h,'#aeb5a1')+rect(x+12,y-h,6,h,'#71878a')+Array.from({length:4},(_,i)=>rect(x+i*5,y-h-5,4,8,'#d7d4b2')).join('')+line(`M${x+8} ${y-h+13}v10`,'#31434a',4);
const banner=(x,y)=>line(`M${x} ${y}v-29`,'#d8c594',2)+path(`M${x} ${y-28}l19 4 -5 6 5 4 -19 -3Z`,'#bb4940');
const wheel=(x,y)=>circle(x,y,8,'#24363b')+circle(x,y,5,'#a78252')+line(`M${x-5} ${y}h10M${x} ${y-5}v10`,'#dcc594',1);
const tree=(x,y)=>rect(x-2,y-14,4,20,'#715642')+path(`M${x} ${y-44}l-14 26h8l-12 15h36l-12 -15h8Z`,'#4d7860');
const horse=(x,y)=>path(`M${x} ${y}l-9 -3 -5 -11 -10 1 -4 13 -20 -2 -8 -9 3 24 7 -4 1 20 6 0 3 -21 19 0 5 21 6 -2 -2 -29 12 -3 -3 -9 -6 2Z`,'#947359')+line(`M${x-25} ${y+2}l7 7`,'#edcf8f',3);
const shield=(x,y,color='#a75243')=>path(`M${x} ${y}h20v20l-10 10 -10 -10Z`,color)+line(`M${x+10} ${y+4}v20M${x+4} ${y+11}h12`,'#e4cf99',2);
const bow=(x,y)=>line(`M${x} ${y}q28 23 0 46`,'#b88e57',4)+line(`M${x} ${y}v46m-8 -23h40m-5 -4l5 4 -5 4`,'#e4d7aa',1.5);
const siege=(type)=>type==='ram'?wheel(43,98)+wheel(85,98)+house(30,88,57,24,'#765d43')+line('M20 83h87','#594537',9)+circle(104,83,6,'#adb4a7'):wheel(41,99)+wheel(83,99)+line('M32 92h60M50 90l17 -51 15 51M38 38l49 30','#9e7b50',7)+line(type==='trebuchet'?'M31 24l57 56M31 24v23':'M44 42l37 30M43 41q-18 -12 -10 -20','#ddc393',4)+circle(type==='trebuchet'?31:33,type==='trebuchet'?49:19,7,'#a6b0ad');
function structure(id,l){
 let a='';
 if(id==='farm'){a=path('M12 98l30 -22 71 21 -32 23Z','#8f954f')+Array.from({length:5},(_,i)=>line(`M${27+i*11} ${89+i*3}l29 10`,'#dbc46a',2)).join('')+house(42,71,34+l*4,22+l*3);}
 else if(id==='lumber'){a=tree(29,79)+tree(92,78)+house(49,90,28+l*6,20)+line('M18 105l35 8m-30 -16 36 8','#9b7847',7);if(l>1)a+=circle(73,78,13,'#b7c2b9')+circle(73,78,5,'#394e50');}
 else if(['mine','quarry'].includes(id)){a=path('M10 101l9 -38 24 -15 21 10 23 -35 28 80Z','#708588')+path('M42 103v-23q14 -21 28 0v23Z','#203b45')+line('M40 102V80q16 -26 32 0v22','#bdaa7a',5)+line('M43 98l-10 15m33 -15 10 15','#bca778',3);if(id==='mine')a+=path('M24 64l10 -13 7 14 -11 8Z','#92aaa9');if(l>1)a+=tower(91,103,20+l*8);}
 else if(['ranch','stable','greatStable'].includes(id)){a=house(27,73,50+l*4,21,'#68747c')+line('M15 98h98M19 91v21m25 -21v21m48 -21v21m19 -21v21','#c3a575',3)+horse(89,77);}
 else if(id==='road'){a=path('M20 120l23 -32 31 -28 17 -55 21 6 -17 57 -32 31 -17 23Z',l===1?'#b99e71':'#aeb4a6')+line('M33 116l20 -24 30 -31 17 -54','#e5d2a0',2);if(l>1)a+=Array.from({length:5},(_,i)=>line(`M${49+i*8} ${96-i*17}l16 6`,'#71817f',2)).join('');}
 else if(['fort','wall','watchtower'].includes(id)){a=path('M24 103V72h82v31l-41 14Z','#92a39b')+tower(16,105,40+l*9)+tower(93,105,40+l*9);if(id!=='wall')a+=tower(54,98,38+l*13)+banner(63,53-l*8);if(id==='wall')a+=path('M52 109V89q13 -16 26 0v20Z','#263f46');}
 else if(['market','tradeOutpost','merchantGuild','harbor'].includes(id)){a=house(42,80,45,29+l*2,'#566d77')+path('M10 89l15 -21 27 8 8 17Z','#c6a468')+path('M65 99l12 -22 28 7 9 21Z','#a25342')+line('M14 89v19m41 -16v19m15 -11v15m40 -11v12','#ded0a3',3)+banner(66,37);if(id==='harbor')a+=path('M5 110q25 17 52 0l-8 13H17Z','#719da2');}
 else if(['workshop','armory','royalArsenal','siegeWorks','siegeFoundry'].includes(id)){a=house(19,105,57,30+l*5,'#59666c')+tower(78,93,43+l*3)+circle(55,78,13,'#d5bd83')+circle(55,78,7,'#354e54')+line('M55 61v10m0 15v10M38 78h10m14 0h11','#d5bd83',4);if(id.includes('siege')||id==='siegeFoundry')a+=`<g transform="translate(49 52) scale(.52)">${siege(l===3?'trebuchet':'catapult')}</g>`;else a+=shield(89,93);}
 else if(['storehouse','greatGranary'].includes(id)){a=house(18,99,56,42,'#aa8451')+tower(80,107,40+l*7)+line('M27 101v-31m12 31v-31m12 31v-31','#ede0b7',2)+circle(89,91,7,'#dfc27a');}
 else if(id==='range'){a=house(18,82,40,23,'#697d68')+circle(91,84,19,'#e2d0a0')+circle(91,84,12,'#b7614f')+circle(91,84,5,'#e2d0a0')+bow(64,57);}
 else if(id==='barracks'){a=house(20,102,69,33+l*5,'#95594c')+tower(90,104,34+l*6)+shield(46,75)+banner(53,46);}
 else {a=house(33,101,57,36+l*3,'#617f8b')+tower(15,105,33+l*10)+tower(95,105,43+l*9)+banner(62,39);if(id==='chancery'||id==='envoyOffice')a+=path('M49 86h30v17H49Z','#e5d8ae')+line('M49 86l15 11 15 -11','#917052',2);}
 if(l>1&&id!=='road')a+=house(7,110,19,12,'#977758');if(l>2&&id!=='road')a+=banner(101,63)+line('M19 115h90','#dfc68e',2);
 return a;
}
function unitArt(id){const u=UNITS[id];if(u.family==='siege')return siege(id==='ram'?'ram':id==='trebuchet'?'trebuchet':'catapult');let a=u.family==='mounted'?horse(101,78):'';const x=u.family==='mounted'?68:61,y=u.family==='mounted'?59:67;a+=circle(x,y-14,10,'#ccbea0')+path(`M${x-13} ${y}l26 0 5 28 -34 0Z`,id==='levy'?'#a0906f':'#81999e')+line(`M${x-7} ${y+27}v20m15 -20v20`,'#425c62',6);if(u.family==='ranged')a+=bow(x+22,y-9);else if(id==='spearman')a+=line('M95 24v83','#daca9c',3)+path('M95 12l-5 14h10Z','#c4d3cc');else a+=shield(x-28,y+5,id==='knight'?'#b58e50':'#a45546')+line(`M${x+20} ${y+22}l12 -39`,'#d0dad0',4);if(id==='knight'||id==='heavyInfantry')a+=path(`M${x-11} ${y-11}v-14l11 -7 11 7v14Z`,'#92aab0')+line(`M${x-8} ${y-15}h16`,'#233c48',3);return a;}
const frame=(title,body,w=128,h=128)=>`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${w} ${h}" width="${w}" height="${h}" role="img"><title>${title}</title>${body}</svg>\n`;
for(const [id,b] of Object.entries(BUILDINGS))for(const spec of b.levels)await writeFile(new URL(`structures/${id}_${spec.level}.svg`,root),frame(spec.name,`<ellipse cx="64" cy="111" rx="56" ry="12" fill="#102c39" opacity=".28"/>`+structure(id,spec.level)));
for(const [id,u] of Object.entries(UNITS))await writeFile(new URL(`units/${id}.svg`,root),frame(u.name,unitArt(id)));
const icons={food:line('M64 111V24m0 22L47 30m17 31L43 43m21 37L44 62m20 -17 18 -19m-18 36 24 -20m-24 39 24 -21','#dbc179',7),wood:tree(64,100),stone:path('M24 98l11 -46 32 -21 35 31 -1 41 -42 16Z','#8fa3a1'),iron:line('M34 33l61 61M20 66q30 -53 78 -35','#b8c9c1',9),gold:circle(64,71,34,'#c7a35e')+circle(64,71,24,'#e5c482'),horses:horse(99,66),tools:line('M35 103l55 -60M65 24l28 26 15 -16 -28 -23','#b8c2b0',10),arms:shield(45,62)+line('M84 107V18m-15 48h30','#cec7a0',8)};
for(const r of RESOURCES)await writeFile(new URL(`resources/${r}.svg`,root),frame(r,icons[r]));
for(const [i,title] of ['Kingdom','Trade','Construction','Army','Battle','Diplomacy','Treasury','Great Projects'].entries())await writeFile(new URL(`titles/${title.toLowerCase().replaceAll(' ','_')}.svg`,root),frame(title,`<path d="M1 3h510v82H1Z" fill="#172f3d"/><path d="M6 77h500M6 9h500" stroke="#b99862"/><g transform="translate(0 -5) scale(.68)">${structure(['city','market','workshop','barracks','fort','envoyOffice','storehouse','chancery'][i],2)}</g><text x="96" y="53" fill="#ebd5a0" font-family="Georgia,serif" font-size="29">${title}</text>`,512,88));
for(let l=1;l<=3;l++)await writeFile(new URL(`construction/stage_${l}.svg`,root),frame(`Construction stage ${l}`,l>1?structure('town',1)+line('M12 116V30h100v86M12 65h100M12 30l100 86','#cfac72',3):line('M18 113V72h91v41M18 72l91 41M18 93h91','#cfac72',4)));
