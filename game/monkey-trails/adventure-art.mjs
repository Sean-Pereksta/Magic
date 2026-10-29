// Original, self-contained vector illustrations for the adventurous world.
// Art is always available offline with the game. Text labels live beside the art.
const INK='#244d43';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const path=(d,fill,more='')=>`<path d="${d}" fill="${fill}" ${more}/>`;
const line=(d,color=INK,w=3,more='')=>path(d,'none',`stroke="${color}" stroke-width="${w}" stroke-linecap="round" stroke-linejoin="round" ${more}`);
const circle=(x,y,r,color,more='')=>`<circle cx="${x}" cy="${y}" r="${r}" fill="${color}" ${more}/>`;
const oval=(x,y,rx,ry,color,more='')=>`<ellipse cx="${x}" cy="${y}" rx="${rx}" ry="${ry}" fill="${color}" ${more}/>`;
const group=(x,y,s,body,more='')=>`<g transform="translate(${x} ${y}) scale(${s})" ${more}>${body}</g>`;
const shape=(d,color)=>path(d,color,`stroke="${INK}" stroke-opacity=".24" stroke-width="2" stroke-linejoin="round"`);
const star=(x,y,s=1,color='#fff0b4')=>group(x,y,s,path('M0-9 3-3 9 0 3 3 0 9-3 3-9 0-3-3Z',color));
const leaf=(x,y,s=1,color='#75ae74',r=0)=>group(x,y,s,`<g transform="rotate(${r})">${shape('M0 1Q-22-11-7-28Q17-30 0 1Z',color)}${line('M0 1-5-20','#d2dca0',1.3)}</g>`);
const flower=(x,y,s=1,color='#edaf9d')=>group(x,y,s,[0,72,144,216,288].map(a=>oval(0,-7,5,7,color,`transform="rotate(${a})"`)).join('')+circle(0,0,4,'#ffe3a0')+circle(-1,-1,1.3,'#fff6cf'));
const berry=(x,y,s=1,color='#da7180')=>group(x,y,s,circle(-5,-2,7,color)+circle(5,-2,7,color)+circle(0,7,7,color)+circle(-7,-5,2,'#ffe1bc')+leaf(0,-6,.45,'#637b49',35));
const fruit=(x,y,s=1,color='#e6a65e')=>group(x,y,s,shape('M0-9Q14-20 20-4Q25 17 6 25Q-15 21-19 4Q-22-14-6-10Z',color)+line('M0-8Q-1-18 7-22','#708049',3)+leaf(8,-15,.65,'#6d9956',65)+oval(-8,-1,3,7,'#ffe4a1','opacity=".7"'));
const tree=(x,y,s=1,color='#5f9a69',trunk='#99865a')=>group(x,y,s,line('M0 7Q-7-70 4-110M-1-44-32-78M1-64 32-96',trunk,13)+shape('M-7-145Q-34-157-43-130Q-73-139-78-109Q-100-82-74-66Q-53-45-28-59Q0-38 23-56Q57-40 73-70Q98-97 69-115Q68-140 38-138Q15-162-7-145Z',color)+line('M-60-111q13-18 27-11M14-136q17-5 28 8','#c6d18b',4,'opacity=".36"'));
const grass=(x,y,s=1,color='#64875b')=>group(x,y,s,line('M-9 2-17-13M0 2q-1-17 6-23M9 3 20-10',color,3));
const rock=(x,y,s=1,color='#9fa484')=>group(x,y,s,shape('M-29 4-20-21 3-31 27-15 32 5 8 14-19 11Z',color)+path('M3-31 5 0 27-15Z','#e2d6af','opacity=".35"')+line('M-20-21 5 0 32 5',INK,1,'opacity=".25"'));
const crystal=(x,y,s=1,color='#b1dace')=>group(x,y,s,shape('M0 8-18-17-10-47 6-57 26-29 18 0Z',color)+path('M6-57 4-13 0 8 18 0 26-29Z','#fff9df','opacity=".4"')+line('M-10-47 4-13 26-29',INK,1.3,'opacity=".25"'));
const cloud=(x,y,s=1)=>group(x,y,s,path('M-45 8Q-63 7-58-7Q-55-22-37-19Q-33-41-12-32Q8-42 20-27Q43-34 47-13Q66-12 62 3Q62 11 42 9Z','#fff4d5','opacity=".7"'));
const monkey=(x,y,s=1,{coat='#a68b61',face='#ead1a4',pose='sit',accent='#679365'}={})=>group(x,y,s,
 line(pose==='walk'?'M13 10Q44 34 47 2Q50-17 36-16':'M17 17Q57 30 52-2Q47-21 32-12',coat,9)+
 oval(0,11,22,28,coat)+oval(0,15,13,18,face)+
 line(pose==='walk'?'M-13 25-31 43M12 27 32 36':'M-14 27-27 31M14 27 26 31',coat,11)+
 line(pose==='hang'?'M-17 0-31-43M17 0 33-44':pose==='walk'?'M-17 0-29 14M16 0 29-11':'M-18 0-29 17M18 0 28 17',coat,9)+
 circle(-23,-20,11,coat)+circle(23,-20,11,coat)+circle(-23,-20,6,face)+circle(23,-20,6,face)+
 oval(0,-23,25,26,coat)+path('M-20-26Q-19-45-2-35Q18-46 21-26Q26-4 0 0Q-24-3-20-26Z',face)+
 oval(-8,-25,3,4,INK)+oval(8,-25,3,4,INK)+circle(-9,-26,1,'#fffbe2')+circle(7,-26,1,'#fffbe2')+
 line('M-4-10q4 4 8 0',INK,2)+oval(0,-17,3,2,'#99795c')+path('M-22-4Q0 5 22-5L17 4Q0 12-18 3Z',accent));
const lantern=(x,y,s=1)=>group(x,y,s,line('M-10-27q0-23 20 0','#887953',3)+shape('M-15-26H15L19 7H-19Z','#d8a15a')+path('M-10-20H10L12 1H-12Z','#ffe7a0')+line('M-20 8h40M-17-27h34','#6e7251',4)+circle(0,-8,5,'#fff5c7'));
const book=(x,y,s=1)=>group(x,y,s,shape('M0-20Q-28-38-55-25V36Q-24 28 0 41Q26 28 55 36V-25Q25-38 0-20Z','#e7d6a6')+shape('M0-26Q-25-40-51-31V28Q-24 21 0 35Q23 21 51 28V-31Q25-40 0-26Z','#fff0c8')+line('M0-25V34','#ab9f72',2)+line('M-41-18-12-12M-41-6-12 0M-41 6-18 11','#c7b88a',2)+path('M15-10 23-17 35-13 40 3 26 16 12 7Z','#a4bb86')+line('M16 5 23 0 29 6 37-4','#6e916c',2));
const basket=(x,y,s=1)=>group(x,y,s,shape('M-37-9H37L27 28H-27Z','#b99462')+line('M-31-8q0-36 31-36T31-8','#9c8155',6)+line('M-27 1h54M-24 12h48M-21 23h42M-17-4l4 30M0-4v30M17-4l-4 30','#e2c18b',3));
function setting(id,{sky='#c4dfbd',ground='#8eb275',far='#98bd8f',sun=true}={}){
 return `<defs><linearGradient id="${id}-sky" x2="0" y2="1"><stop stop-color="${sky}"/><stop offset="1" stop-color="#f3e8bc"/></linearGradient><linearGradient id="${id}-ground" x2="0" y2="1"><stop stop-color="${ground}"/><stop offset="1" stop-color="#638967"/></linearGradient></defs>`+
 path('M0 0H600V340H0Z',`url(#${id}-sky)`)+(sun?circle(448,61,32,'#fff0bb')+circle(448,61,48,'#fff0bb','opacity=".22"'):'')+
 path('M0 181Q68 104 149 157Q239 101 319 153Q431 89 514 143Q553 123 600 153V340H0Z',far)+
 path('M0 224Q121 165 223 203Q376 158 468 196Q538 174 600 202V340H0Z',`url(#${id}-ground)`);
}
function edgeFoliage(color='#457c58'){
 return path('M0 340V289Q20 260 40 293Q45 260 64 297Q84 277 116 340M600 340V285Q576 264 563 295Q545 272 530 311Q502 291 480 340Z',color)+
 [34,89,501,560].map((x,i)=>grass(x,326-(i%2)*12,.8,'#a1b978')).join('');
}
function berryGrove(id){return setting(id,{sky:'#bce5cc',ground:'#a6c976'})+cloud(142,48,.7)+
 path('M260 340Q208 297 284 259Q352 220 321 194Q304 178 333 165L350 171Q332 191 354 218Q379 260 310 293Q282 316 337 340Z','#ead293')+
 tree(96,246,1.18,'#5c9b5c')+tree(484,221,.94,'#6eaa62')+tree(211,181,.48,'#81ae70')+
 [[47,103],[109,118],[146,147],[74,166],[456,113],[502,96],[525,144],[470,159]].map(([x,y],i)=>fruit(x,y,.54,i%2?'#e6af5f':'#e7866b')).join('')+
 path('M111 300Q92 250 133 255Q156 230 179 261Q209 248 218 295Z','#4f855b')+
 [[128,268],[158,260],[183,279],[151,290],[398,268],[422,284]].map(([x,y])=>berry(x,y,.85)).join('')+
 flower(371,286,1)+flower(412,309,.8,'#f9d48d')+basket(352,302,.68)+fruit(342,285,.4)+fruit(364,286,.38,'#e87667')+
 line('M314 56 256 181M356 66 300 182','#fff7cb',18,'opacity=".13"')+star(318,245,.6)+edgeFoliage();}
function ancientRuins(id){return setting(id,{sky:'#d2d9ba',ground:'#93a87a',far:'#94a78d'})+cloud(105,67,.6)+
 path('M181 340 240 235H365L424 340Z','#cbbb91')+line('M208 308h190M221 280h163M235 251h133','#8a947b',3)+
 shape('M194 242V112L218 98V74L249 67V48H353V68L382 77V100L406 113V242Z','#bcb58b')+
 shape('M224 239V119Q300 31 376 119V239Z','#d1c79b')+path('M249 241V129Q302 66 353 130V241Z','#33594f')+
 path('M271 241V143Q301 106 329 143V241Z','#294c46')+line('M229 122h25M349 122h24M225 157h26M349 158h26M224 194h27M351 194h22M253 88l15 15M331 102l15-14','#8c9779',3)+
 shape('M275 76 281 46H320L326 76 302 91Z','#a5a784')+circle(290,65,5,'#4b6e58')+circle(313,65,5,'#4b6e58')+line('M292 77h19','#637b5e',3)+
 line('M233 71q-16 22-6 53t-8 58M373 86q17 23 3 53t11 44','#597e56',6)+
 [0,1,2,3].map(i=>leaf(229-(i%2)*7,91+i*22,.75,'#70a16a',i%2?100:35)+leaf(381,104+i*24,.7,'#739f63',i%2?-50:40)).join('')+
 rock(143,286,1.3,'#a2a986')+rock(445,267,.9,'#a9ad87')+crystal(315,222,.36,'#e9d27d')+star(311,199,.5)+
 tree(55,234,1.25,'#4d805c')+tree(548,253,1.1,'#547e5a')+flower(432,298,.8,'#dbb69e')+edgeFoliage('#426f53');}
function mistyCanopy(id){return setting(id,{sky:'#b7d3cf',ground:'#6c9e85',far:'#7fae9d',sun:false})+
 cloud(153,61,1.4)+circle(435,57,30,'#e5edcb','opacity=".6"')+tree(90,269,1.8,'#4c8275')+tree(536,277,1.8,'#5d9280')+
 path('M30 340V245Q182 206 300 246Q430 282 580 211V340Z','#7fafa0')+
 path('M160 287Q290 284 437 151L465 161Q305 305 161 310Z','#b69d70')+
 line('M158 269Q303 274 452 132M162 307Q308 310 465 166','#7e7858',4)+
 [0,1,2,3,4,5,6].map(i=>{const x=173+i*42,y=288-i*i*2.75;return line(`M${x} ${y}v-31M${x-9} ${y+11}l20-1`,'#d7c697',4)}).join('')+
 line('M446 169V124M463 157V108','#8a8763',6)+
 monkey(407,112,.44,{coat:'#586f69',face:'#a7bda0',pose:'hang'})+line('M392 91q35-28 57-14','#849d76',4)+
 path('M0 227Q110 201 214 228T427 223 600 207V229Q486 244 375 240T149 249 0 247Z','#e1eed8','opacity=".48"')+
 path('M0 305Q168 276 313 308T600 292V321Q449 344 303 328T0 329Z','#dfecd4','opacity=".5"')+
 leaf(54,36,1.7,'#4a7d68',20)+leaf(560,58,2,'#51846f',-70)+star(339,165,.6,'#ecedce');}
function bloom(id){return setting(id,{sky:'#dfdfb3',ground:'#a7ba76'})+cloud(119,57,.8)+
 line('M41 301Q134 195 251 171T530 71M193 187 175 101M339 142 389 209','#9a8359',17)+
 [[93,250],[155,203],[215,170],[287,165],[358,130],[435,111],[500,89]].map(([x,y],i)=>leaf(x,y,1.45,i%2?'#7f9e54':'#66915a',i%2?100:-30)).join('')+
 [[155,192],[244,178],[359,140],[427,125]].map(([x,y],i)=>fruit(x,y,1.1,i%2?'#d68d68':'#dba457')).join('')+
 [[178,115],[205,141],[308,143],[382,209],[460,89],[123,227]].map(([x,y],i)=>flower(x,y,1.3,i%2?'#f5c1a0':'#edb7b0')).join('')+
 basket(429,287,1.15)+fruit(409,263,.7)+fruit(446,260,.7,'#df8b68')+berry(434,280,.9)+
 [0,1,2,3,4].map(i=>flower(87+i*90,65+(i%2)*190,.45,'#ffefd0')).join('')+edgeFoliage();}
function migration(id){return setting(id,{sky:'#c9e0c5',ground:'#8faf72'})+cloud(117,51,.65)+tree(50,259,1.7,'#598969')+tree(553,262,1.7,'#507f63')+
 line('M59 124Q275 208 550 100','#7b8158',8)+line('M67 129Q278 216 546 108','#d9cf95',2)+
 monkey(192,132,.88,{coat:'#ad8c5f',pose:'walk',accent:'#dbac64'})+monkey(316,143,.7,{coat:'#a6aa7d',pose:'walk',accent:'#e2b67d'})+monkey(427,113,.6,{coat:'#7e8b77',pose:'walk',accent:'#d9b789'})+
 line('M129 135q-12-8-16-21M269 144q-12-8-17-19M386 119q-9-5-13-15','#fff0bd',3,'opacity=".8"')+
 leaf(69,120,1,'#91ad72',-40)+leaf(513,122,1.1,'#88a76b',40)+leaf(529,117,1,'#85a96f',120)+
 path('M112 340Q152 266 269 254Q361 246 417 206L451 210Q404 277 297 287Q225 291 214 340Z','#d9c997')+
 flower(454,281,.9,'#ecc19b')+flower(149,289,.7)+edgeFoliage();}
function winds(id){return setting(id,{sky:'#b8cdd6',ground:'#a0b39a',far:'#97ada8'})+cloud(126,60,.9)+
 path('M-30 221Q62 140 209 212T480 152 640 183','none','stroke="#e9dfad" stroke-width="24" opacity=".65"')+
 path('M-30 258Q99 180 237 247T481 198 640 217','none','stroke="#a9d4ce" stroke-width="22" opacity=".82"')+
 line('M-5 186Q98 106 244 182T527 107 628 146','#e4ebce',9)+line('M-5 279Q118 213 254 281T527 239 624 270','#bda9cb',13)+
 crystal(175,196,1.1,'#a4d2bb')+crystal(308,170,1.35,'#c1b7dd')+crystal(444,199,1,'#f0c590')+
 [122,223,362,489].map((x,i)=>star(x,103+(i%2)*138,i%2?.7:1)).join('')+
 leaf(95,170,.8,'#7ca486',65)+leaf(518,247,.9,'#a5ba85',90)+rock(107,306,.8)+rock(483,305,.95)+edgeFoliage('#678973');}
function guardian(id){return setting(id,{sky:'#b6cec3',ground:'#859a71',far:'#7f9e8d'})+circle(300,141,107,'#e4d6a3','opacity=".45"')+
 shape('M207 308 219 272H381L394 308Z','#aaa981')+shape('M225 273 239 239H361L378 273Z','#c0bb8c')+
 shape('M249 239Q225 191 256 156L252 116Q252 76 300 74Q348 76 348 116L344 156Q375 191 350 239Z','#a8ad85')+
 oval(300,196,36,40,'#c0bb91')+circle(249,120,19,'#9da47d')+circle(351,120,19,'#9da47d')+
 shape('M265 111Q278 90 300 105Q321 90 335 111L329 147Q300 166 271 148Z','#c5bf91')+
 line('M271 123h18M313 123h18','#f0d184',6)+circle(279,123,3,'#fff5c2')+circle(321,123,3,'#fff5c2')+line('M289 143h22','#677b61',4)+
 line('M260 174 225 228M340 174 377 228','#a0a780',22)+circle(225,230,13,'#bcc09a')+circle(376,230,13,'#bcc09a')+
 shape('M277 82 269 55 290 66 302 43 314 65 336 54 325 83Z','#c1ba84')+crystal(300,218,.5,'#e5cc83')+
 line('M248 89q-27 33-13 61M354 91q21 34 10 71','#688459',5)+leaf(248,97,.8,'#7c9b65',-40)+leaf(236,128,.8,'#7c9b65',20)+
 star(189,158,.7)+star(401,156,.8)+rock(125,293,1.1)+rock(470,291,1)+tree(45,225,1.2,'#507a5e')+tree(562,245,1.35,'#4f795e')+edgeFoliage();}
function merchant(id){return setting(id,{sky:'#e2d6ad',ground:'#a6b582',far:'#a3b38e'})+cloud(114,56,.65)+tree(70,236,1.3,'#5f8a60')+
 shape('M170 184H422V265H170Z','#aa8c60')+path('M179 192H413V244H179Z','#c4aa75')+line('M190 205h209M190 222h209','#a88e63',2)+
 circle(209,278,25,'#566d54')+circle(386,278,25,'#566d54')+circle(209,278,14,'#bdad79')+circle(386,278,14,'#bdad79')+
 line('M209 265v26M196 278h26M386 265v26M373 278h26','#657557',3)+
 line('M183 186V109M409 186V109','#8d7955',8)+shape('M153 116 183 67H411L443 116Z','#e3b16a')+
 path('M190 67H231L213 116H167ZM274 67H314L317 116H265ZM356 67H396L425 116H374Z','#e8cf94')+
 path('M153 116h290v12q-18 20-36 0-18 20-36 0-18 20-36 0-18 20-36 0-18 20-36 0-18 20-36 0-18 20-36 0-19 20-37 0Z','#b77854')+
 monkey(294,166,.85,{coat:'#9d8e68',face:'#eddbaf',accent:'#c19c64'})+
 basket(213,174,.55)+fruit(207,160,.36)+berry(226,165,.45)+crystal(379,181,.5,'#accfc3')+lantern(425,161,.65)+
 line('M423 240 486 233','#8d7955',9)+leaf(489,299,1.1,'#72945d',70)+flower(132,281,1,'#e8ac94')+edgeFoliage();}
function meal(id){return setting(id,{sky:'#e1caa5',ground:'#9eaa72'})+tree(70,241,1.3,'#697f59')+tree(535,251,1.2,'#697f59')+
 oval(303,290,112,28,'#6d825b','opacity=".4"')+line('M244 298 355 275M249 276 353 298','#937650',13)+
 shape('M265 286Q253 259 280 235Q270 260 300 221Q320 254 331 247Q351 272 334 286Z','#dfa06b')+path('M284 286Q279 266 300 251Q308 276 319 286Z','#ffe6a5')+
 line('M218 272 251 157H354L386 272','#776d50',7)+line('M302 157v33','#776d50',4)+
 shape('M256 200H348Q351 249 302 251Q253 247 256 200Z','#566d5b')+oval(302,200,47,12,'#a7b17c')+oval(302,198,38,7,'#dfbf78')+
 line('M281 175q-10-13 1-26M305 178q-13-16 0-32M327 176q-10-11 1-23','#fff0cd',4,'opacity=".75"')+
 basket(138,277,.9)+fruit(121,255,.55)+fruit(151,258,.55,'#d98e6a')+
 shape('M416 267h69q-5 34-34 34t-35-34Z','#d6b889')+oval(450,267,35,8,'#efcf8b')+berry(464,264,.6)+edgeFoliage();}
function study(id){return setting(id,{sky:'#bdc8b5',ground:'#8c9d74',sun:false})+circle(462,58,30,'#f1dfad')+
 tree(64,241,1.4,'#59785b')+tree(546,239,1.3,'#638460')+shape('M150 242 189 190H420L465 242 443 283H170Z','#9e8a60')+
 book(306,211,1.55)+lantern(158,201,1.1)+circle(155,195,48,'#ffdf98','opacity=".12"')+
 shape('M405 234 432 220 454 235 431 250Z','#d1be85')+circle(431,235,10,'#ebe0b0')+path('M431 226 435 235 427 235Z','#739377')+path('M431 244 435 235 427 235Z','#b98466')+
 [93,228,358,505].map((x,i)=>star(x,78+(i%2)*33,.5,'#f5e6b8')).join('')+flower(472,303,.8,'#d7b5a0')+edgeFoliage();}
function supplies(id){return setting(id,{sky:'#ccd7b7',ground:'#a4b681'})+tree(71,240,1.3,'#658b60')+tree(545,255,1.3,'#618360')+
 shape('M219 291V155Q222 115 264 115H331Q375 115 378 155V291Z','#bc9d6d')+
 shape('M226 153V128Q229 101 260 100H335Q367 101 371 128V153Q301 179 226 153Z','#d1b77d')+
 line('M274 100v-17q26-12 48 0v17','#8f8159',8)+shape('M243 226H354V282H243Z','#d3b67b')+line('M259 166v66M338 166v66','#7c805a',10)+
 shape('M250 202h18v20h-18ZM329 202h18v20h-18Z','#e2cc92')+leaf(301,264,1.1,'#729261',50)+
 basket(159,280,.72)+fruit(150,260,.47)+berry(169,266,.6)+
 shape('M412 259V213h30v46q-15 16-30 0Z','#b8cec1')+path('M416 239h22v18q-11 12-22 0Z','#a0baa2')+line('M416 213h22','#a28d63',8)+
 crystal(444,293,.5,'#d5c1d9')+edgeFoliage();}
function sanctuary(id,kind){return setting(id,{sky:kind==='moonwell'?'#9eb5c6':'#bfdcc0',ground:'#a8b97e',far:'#8bb08e'})+
 tree(68,235,1.45,'#578667')+tree(542,236,1.45,'#578465')+line('M67 121Q282 177 538 120','#8a9065',3)+
 [135,214,300,386,465].map((x,i)=>lantern(x,139+(i%2)*10,.36)).join('')+
 (['moonwell','river'].includes(kind)?oval(300,265,112,40,'#7fa99f')+oval(300,260,96,28,'#b4d6cb')+line('M237 256q61-22 125 0M261 270q39-13 78 0','#e0e9ce',3)+(kind==='moonwell'?circle(301,255,17,'#e6dfb4'):leaf(349,266,.8,'#8da979',100)):
 kind==='sunstone'?shape('M247 281 265 237H336L354 281Z','#c6ad70')+shape('M268 239 263 165 302 137 338 165 331 239Z','#d7b674')+circle(301,189,22,'#f6dda0')+[0,45,90,135].map(r=>line('M301 154v10M301 214v10','#f6dda0',3,`transform="rotate(${r} 301 189)"`)).join(''):
 kind==='crystal'?crystal(280,266,1,'#b8c9d7')+crystal(323,273,1.3,'#cdbbdb')+crystal(350,281,.6,'#aed6ca'):
 line('M196 174Q298 264 402 174','#d0bc87',5)+path('M196 174Q299 239 402 174Q344 272 299 259Q247 271 196 174Z','#b4bd87')+line('M197 174V153M401 174V153','#7f8760',9))+
 flower(155,294,1)+flower(429,290,1.1,'#eac58d')+flower(470,310,.6)+edgeFoliage();}
function technique(id,kind){const storm=kind==='storm-relay'||kind==='storm';return setting(id,{sky:storm?'#b7c6d2':'#c3d8ba',ground:'#93a78b',sun:false})+
 circle(300,177,103,storm?'#d3c9df':'#d9e5b5','opacity=".65"')+
 monkey(201,220,1.18,{coat:storm?'#8d8eaa':'#929c6e',accent:storm?'#c7b9da':'#73a781'})+monkey(401,220,1.18,{coat:storm?'#819a9b':'#a78f6b',accent:storm?'#9ec9c7':'#dab782'})+
 (storm?path('M184 129Q280 71 403 132','none','stroke="#b0d4d0" stroke-width="14"')+path('M298 104 267 172 299 166 278 228 338 150 309 156 333 104Z','#fff0b2')+line('M217 114q30-25 67-27M321 89q39 8 64 29','#e3e6c7',5):
 line('M206 138Q298 66 398 138','#a7bf7a',16)+leaf(300,153,2.1,'#77ae89',45)+path('M284 199Q307 171 328 195Q346 216 303 244Q260 220 284 199Z','#dfb487'))+
 [155,258,354,453].map((x,i)=>star(x,92+(i%2)*169,i%2?.75:1)).join('')+edgeFoliage('#628574');}
function badge(id,kind){const art=kind==='companions'?monkey(300,199,1.4,{accent:'#dfb671'}):kind==='guardian'?shape('M243 185 230 127 271 151 300 107 329 151 370 127 357 185Z','#d9b971')+shape('M249 188H351V233H249Z','#b99562')+star(300,207,1.2):kind==='crafting'?crystal(300,224,1.5,'#b3cec0')+line('M253 167 348 240','#a48b65',12):book(300,190,1.4);
 return setting(id,{sky:'#ddd8b3',ground:'#a9b58b',sun:false})+path('M259 225 242 314 282 294 300 314 309 222Z','#b28765')+path('M294 223 314 315 332 294 365 309 343 225Z','#d4b074')+circle(300,174,105,'#d2b47b')+circle(300,174,92,'#ead9a4')+circle(300,174,80,'#f7eac0')+art+leaf(211,228,1.1,'#87a46a',-35)+leaf(389,228,1.1,'#87a46a',75)+star(192,111,.8)+star(411,109,.8);}

const scenes={
 'berry-grove':{label:'A sunlit trail through fruit trees and berry bushes',draw:berryGrove},
 'ancient-ruins':{label:'An overgrown stone monkey arch with a hidden glowing treasure',draw:ancientRuins},
 'misty-canopy':{label:'A suspended rope trail through mist, with a monkey in the canopy',draw:mistyCanopy},
 'fruit-bloom':{label:'Flowering branches heavy with colorful fruit',draw:bloom},
 'monkey-migration':{label:'Three companions crossing the canopy together',draw:migration},
 'essence-winds':{label:'Elemental crystals carried by ribbons of wind',draw:winds},
 'guardian-stirring':{label:'An ancient monkey guardian awakening beneath the trees',draw:guardian},
 'traveling-merchant':{label:'A friendly monkey merchant at a striped-canopy supply cart',draw:merchant},
 meal:{label:'A warm pot of fruit stew shared around the campfire',draw:meal},
 study:{label:'An illustrated trail journal beside a glowing camp lantern',draw:study},
 supplies:{label:'A prepared expedition pack with fruit, healing supplies and essence',draw:supplies},
 canopy:{label:'A leafy sanctuary with a hanging hammock and lanterns',draw:id=>sanctuary(id,'canopy')},
 moonwell:{label:'A peaceful sanctuary pool reflecting moonlight',draw:id=>sanctuary(id,'moonwell')},
 sunstone:{label:'A golden sunstone monument in a welcoming sanctuary',draw:id=>sanctuary(id,'sunstone')},
 crystal:{label:'A crystal garden glowing beneath sanctuary lanterns',draw:id=>sanctuary(id,'crystal')},
 'journal-explorer':{label:'An open adventure journal on an explorer medal',draw:id=>badge(id,'explorer')},
 'journal-companions':{label:'A smiling monkey on a friendship medal',draw:id=>badge(id,'companions')},
 'journal-guardian':{label:'A golden crown on a guardian milestone medal',draw:id=>badge(id,'guardian')},
 'journal-crafting':{label:'An elemental crystal on a crafting medal',draw:id=>badge(id,'crafting')},
 'bloom-relay':{label:'Two companions joining leaf and water energy into a healing bloom',draw:id=>technique(id,'bloom')},
 'storm-relay':{label:'Two companions joining water and storm energy into a lightning arc',draw:id=>technique(id,'storm')}
};
// Save-stable feature IDs share the same illustration on every surface.
const aliases={
 'camp-meal':'meal','camp-study':'study','camp-pack':'supplies','camp-merchant':'traveling-merchant',
 'journal-friends':'journal-companions','sanctuary-clearing':'canopy',
 'decoration-lantern':'study','decoration-banner':'guardian-stirring','decoration-bloom':'fruit-bloom',
 'technique-dewfall-chorus':'bloom-relay'
};
const relay=id=>setting(id,{sky:'#d0d5b2',ground:'#9cae83',sun:false})+circle(300,177,105,'#e9d7a2','opacity=".62"')+
 monkey(191,221,1.14,{coat:'#8a9c72',accent:'#7da47a'})+monkey(410,221,1.14,{coat:'#af9368',accent:'#dbb06a'})+
 path('M173 124Q291 46 414 122L396 94M414 122 379 126','none','stroke="#d1ae60" stroke-width="13" stroke-linecap="round" stroke-linejoin="round"')+
 leaf(274,109,1.2,'#92b77d',58)+leaf(309,106,1.1,'#8dad72',100)+star(305,196,2.8,'#fff0b8')+
 line('M279 215 258 231M334 210 354 226M300 162v-18','#e4c87d',5)+edgeFoliage();
const dewfall=id=>technique(id,'bloom')+line('M212 154Q300 125 390 154','#a7d3ce',7,'opacity=".7"')+
 [[250,182],[348,190],[291,117],[374,142]].map(([x,y],i)=>group(x,y,i%2?.85:1.05,shape('M0-14Q-17 5-9 13Q0 23 9 13Q17 5 0-14Z','#a4d4ca')+line('M-5 5q-4 7 3 9','#e4f2dc',2))).join('');
export const ADVENTURE_ART=Object.freeze({...scenes,...Object.fromEntries(Object.entries(aliases).map(([key,value])=>[key,scenes[value]])),
 'sanctuary-river':{label:'A quiet riverbank sanctuary with lily leaves and lanterns',draw:id=>sanctuary(id,'river')},
 'decoration-stone':{label:'A welcoming leaf-carved stone in the sanctuary',draw:id=>setting(id)+rock(300,257,2.4,'#bac198')+leaf(305,224,1.7,'#79996b',30)+flower(214,286,.9)+edgeFoliage()},
 'technique-canopy-relay':{label:'A guarded companion passes a golden opening to its teammate',draw:relay},
 'technique-dewfall-chorus':{label:'Leaf and Water companions share restorative dew beneath a living shelter',draw:dewfall}
});
let serial=0;
export function adventureArt(kind,{uid,decorative=false,compact=false}={}){
 const row=ADVENTURE_ART[kind]||ADVENTURE_ART['journal-explorer'];
 const id=`adventure-${String(uid||++serial).replace(/[^a-zA-Z0-9_-]/g,'')}`;
 return `<svg class="adventure-art ${compact?'adventure-art-compact':''}" viewBox="0 0 600 340" xmlns="http://www.w3.org/2000/svg" ${decorative?'aria-hidden="true"':`role="img" aria-labelledby="${id}-title"`} focusable="false"><title id="${id}-title">${esc(row.label)}</title>${row.draw(id)}</svg>`;
}

export function decorationArt(kind,{uid,decorative=false}={}){
 const id=`decor-${String(uid||++serial).replace(/[^a-zA-Z0-9_-]/g,'')}`;
 const lanternKind=/lantern|lamp/.test(kind),bloomKind=/bloom|flower/.test(kind),crystalKind=/crystal/.test(kind),moonKind=/moon/.test(kind);
 const title=lanternKind?'A warm hanging sanctuary lantern':bloomKind?'A flowering sanctuary planter':crystalKind?'A sparkling crystal keepsake':moonKind?'A silver moonwell keepsake':'An earned sanctuary banner';
 const drawing=/stone/.test(kind)?rock(59,94,1.3,'#b6bc94')+leaf(62,77,.85,'#79996b',30):lanternKind?line('M60 110V11Q84 4 89 22','#849062',6)+lantern(88,45,.84):bloomKind?shape('M23 77H98L86 111H36Z','#b59162')+line('M42 80 35 51M63 80 64 36M80 80 86 53','#799563',4)+flower(35,45,1.2)+flower(64,31,1.4,'#e8be7c')+flower(87,49,1.1,'#e1a6b1'):crystalKind?crystal(48,110,1.1,'#b3cec8')+crystal(80,114,.8,'#c6b5db'):moonKind?circle(60,67,35,'#d6dcba')+circle(73,56,30, '#87aaa0')+star(25,30,.7):line('M28 111V10','#8d855f',6)+shape('M32 14H98V83L65 66 32 83Z','#c1ad76')+star(65,43,1.8,'#f6e6b4');
 return `<svg class="adventure-decoration-art" viewBox="0 0 120 120" xmlns="http://www.w3.org/2000/svg" ${decorative?'aria-hidden="true"':`role="img" aria-labelledby="${id}-title"`} focusable="false"><title id="${id}-title">${title}</title>${drawing}</svg>`;
}
