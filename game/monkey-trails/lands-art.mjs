// Original region illustrations, shared by the walking view and land previews.
// Everything is local vector geometry: no image downloads or map service.
const INK='#173d39';
const path=(d,fill,attrs='')=>`<path d="${d}" fill="${fill}" ${attrs}/>`;
const line=(d,color=INK,width=3,attrs='')=>path(d,'none',`stroke="${color}" stroke-width="${width}" stroke-linecap="round" stroke-linejoin="round" ${attrs}`);
const circle=(x,y,r,fill,attrs='')=>`<circle cx="${x}" cy="${y}" r="${r}" fill="${fill}" ${attrs}/>`;
const oval=(x,y,rx,ry,fill,attrs='')=>`<ellipse cx="${x}" cy="${y}" rx="${rx}" ry="${ry}" fill="${fill}" ${attrs}/>`;
const group=(x,y,s,body,attrs='')=>`<g transform="translate(${x} ${y}) scale(${s})" ${attrs}>${body}</g>`;
const outlined=(d,fill,color=INK,width=2)=>path(d,fill,`stroke="${color}" stroke-opacity=".38" stroke-width="${width}" stroke-linejoin="round"`);
const sparkle=(x,y,s=1,color='#fff2bb')=>group(x,y,s,path('M0-8 2-2 8 0 2 2 0 8-2 2-8 0-2-2Z',color));
const cloud=(x,y,s=1,color='#fff3d7')=>`<g class="land-cloud">${group(x,y,s,path('M-43 5Q-60 4-56-8Q-53-21-37-18Q-33-38-13-32Q0-46 16-31Q39-37 42-17Q62-18 62-3Q64 7 43 7Z',color,'opacity=".82"'))}</g>`;
const grass=(x,y,s=1,color='#527c46')=>group(x,y,s,line('M-11 0q-1-9-6-13M0 2q0-15 5-22M10 1q4-8 10-10',color,3));
const flower=(x,y,s=1,color='#f4ab91')=>group(x,y,s,[0,72,144,216,288].map(r=>oval(0,-6,4.5,6.5,color,`transform="rotate(${r})"`)).join('')+circle(0,0,3.5,'#ffe7a1'));
const rock=(x,y,s=1,color='#a6b18c')=>group(x,y,s,outlined('M-25 1-18-20 2-29 24-13 32 2 10 10-14 9Z',color)+path('M2-29 4-3 24-13Z','#fff2cc','opacity=".22"')+line('M-18-20 4-3 32 2',INK,1.5,'opacity=".2"'));
const crystal=(x,y,s=1,color='#a0e7e0')=>group(x,y,s,outlined('M0 0-16-32-5-66 12-74 29-42 16-5Z',color)+path('M12-74 9-21 0 0 16-5 29-42Z','#fff6e0','opacity=".38"')+line('M-5-66 9-21 29-42M9-21 16-5',INK,1.5,'opacity=".24"'));
const crystals=(x,y,s=1,color='#a0e7e0')=>group(x,y,s,crystal(-21,1,.63,color)+crystal(16,4,.77,color)+crystal(-2,0,1,color));
const pine=(x,y,s=1,color='#568a7d',snow=false)=>group(x,y,s,line('M0-4V-86','#7b795b',8)+outlined('M0-113-23-77h11l-24 32h16L-45-7Q0 7 45-7L20-45h16L12-77h11Z',color)+(snow?path('M0-113-23-77q13 5 23-2 13 8 23 2ZM0-78-27-45q13 6 27-1 14 7 27 1ZM0-43-35-8Q0 3 35-8Z','#e8f5dc'):''));
const palm=(x,y,s=1,color='#488c62',flip=false)=>group(x,y,s,`<g transform="scale(${flip?-1:1} 1)">${line('M0 4Q-4-48 16-90','#a68b50',13)}${line('M-2-9 7-13M-2-27 9-31M1-46 14-49M7-65 19-67','#776c45',2.5)}${outlined('M15-87Q-25-119-66-81Q-20-91 15-87M15-87Q-3-142 36-147Q21-123 15-87M15-87Q47-131 85-103Q48-109 15-87M15-87Q72-92 86-53Q58-74 15-87M15-87Q-10-98-36-49Q-25-87 15-87',color)}${circle(11,-83,7,'#c8a366')}${circle(23,-85,6,'#ac8c55')}</g>`);
const broadTree=(x,y,s=1,color='#4d9365',trunk='#9a7955')=>group(x,y,s,line('M0 7V-79m0 40-23-27m23 15 24-26',trunk,11)+outlined('M-5-124Q-24-134-38-115Q-63-118-66-95Q-90-75-68-55Q-51-41-30-48Q-5-27 15-46Q40-33 58-51Q84-68 64-90Q65-117 40-117Q24-141 7-123Z',color)+path('M-54-96q11-20 29-11m39-6q17-9 30 7', 'none', 'stroke="#e9edb1" stroke-opacity=".25" stroke-width="5" stroke-linecap="round"'));
const acacia=(x,y,s=1)=>group(x,y,s,line('M-2 8 5-46m-1 9-26-34m27 24 27-30','#94724d',10)+outlined('M-61-72Q-72-91-46-97Q-35-115-12-104Q6-118 27-103Q54-111 67-94Q90-84 66-71Q4-55-61-72Z','#82914e')+line('M-43-83h30m22-7h38','#d0c16d',3,'opacity=".35"'));
const mushroom=(x,y,s=1,color='#c299cf')=>group(x,y,s,outlined('M-5 0-3-22h9l3 22Z','#e6d6ae')+outlined('M-28-20Q-25-50 0-51Q26-48 31-20Q0-9-28-20Z',color)+circle(-10,-30,4,'#fff0cf')+circle(10,-37,5,'#fff0cf')+circle(20,-25,3,'#fff0cf'));
const reed=(x,y,s=1)=>group(x,y,s,line('M0 2v-49m-2 39-13-18M1-17 13-31','#638565',3)+line('M0-48v-17','#bd9b60',7));
const temple=(x,y,s=1,color='#b6b784')=>group(x,y,s,outlined('M-53 5h106V-7H43v-16H32v-17H23v-29H-23v29h-9v17h-11v16h-10Z',color)+path('M-11 5v-38q11-15 22 0V5Z','#3e6550')+line('M-43-7h86m-75-16h64m-55-17h46m-32-21h18','#f0d594',2.5)+path('M-29-69 0-90 29-69Z','#d2bd7d')+circle(0,-71,5,'#ebcc75'));
const banner=(x,y,s=1,color='#efb77b')=>group(x,y,s,line('M0 2V-68','#9e8757',3)+outlined('M2-67h28v29L17-45 2-38Z',color)+sparkle(15,-55,.55,'#fff0b9'));
const bridge=(x,y,s=1)=>group(x,y,s,path('M-70-6Q0 11 70-6v9Q0 20-70 3Z','#ac956a')+line('M-68-8v-23m23 28v-20m23 23v-20m23 21v-19m23 18v-20m23 17v-20m23 17v-23','#7b775a',3)+line('M-68-29Q0-8 68-29','#dfca91',3));

const palettes={
 'emerald-jungle':{sky:['#b7e4c3','#f2edba'],ground:['#95bc66','#648e55'],distant:'#8ebd80',water:['#7fd7ca','#3eaeac'],trail:'#e8cf8e'},
 'sunstone-savannah':{sky:['#f1cf98','#ffe9b7'],ground:['#dfbd69','#bba85c'],distant:'#c9b47a',water:['#b9dac1','#6ca898'],trail:'#f6db9b'},
 'crystal-caverns':{sky:['#3c4b73','#7b80a1'],ground:['#84839b','#5c7089'],distant:'#626783',water:['#a4e5de','#5fa8c3'],trail:'#c7b8c7'},
 'whispering-marsh':{sky:['#b9c9cb','#eed6c2'],ground:['#9ab38b','#6b9580'],distant:'#8eaaa0',water:['#a4d6c4','#69afa9'],trail:'#d8ca9c'},
 'frostpeak-mountains':{sky:['#b0d8e0','#f2e9cd'],ground:['#e6ecd9','#afccc1'],distant:'#a9c8c6',water:['#c2eeeb','#80bcc5'],trail:'#d8d2bd'},
 'thunder-highlands':{sky:['#667e9f','#c2ced5'],ground:['#9ba18c','#778c86'],distant:'#80919d',water:['#b3dddf','#809cbd'],trail:'#e3c7a8'},
 'ember-volcano':{sky:['#c17b91','#f1b58e'],ground:['#b48980','#78686e'],distant:'#b28a8c',water:['#ffdc8b','#ed9565'],trail:'#d9baa0'},
 'moonlit-forest':{sky:['#414e7a','#9b93ad'],ground:['#8f9e96','#627f7a'],distant:'#747e9f',water:['#b6c7db','#7a9dbb'],trail:'#d7c6c6'},
 'celestial-ruins':{sky:['#a7cfdb','#efe6ca'],ground:['#c4c9ad','#96b39f'],distant:'#aebeca',water:['#d2e9e2','#8ebdc8'],trail:'#e8dcc3'},
 'ancient-monkey-kingdom':{sky:['#97cdd0','#f1dfb4'],ground:['#c2b675','#9b9f60'],distant:'#9db69b',water:['#aedbd0','#78b6aa'],trail:'#efd69a'}
};

function trail(color,{stone=false}={}){
 return path('M202 354Q190 308 262 274Q323 245 306 216Q289 190 345 173Q389 160 402 145L425 149Q408 175 369 188Q326 203 340 224Q371 267 301 303Q264 323 287 354Z',color,'stroke="#fff1c5" stroke-width="2" stroke-opacity=".4"')+
 (stone?line('M237 321l37 11m-19-28 38 10m-9-29 35 7m-3-24 26 1m-4-24 24-5m-25-19 26-9m-29-13 23-7m-7-13 29-3m-3-12 20 1','#a99d8b',2,'opacity=".44"'):
 line('M230 321q9-17 35-29m61-58q-2-11-9-17m58-37 18-10','#fff5d1',3,'opacity=".43"'));
}

function jungle(p,id){
 return cloud(98,61,.8)+cloud(335,44,.56)+circle(466,66,31,'#fff0b7','opacity=".8"')+
 path('M-10 184Q60 105 127 150Q199 80 265 150Q345 105 420 143Q506 87 610 145V340H-10Z',p.distant)+
 path('M-10 226Q95 165 200 203Q313 137 420 175Q521 153 610 195V350H-10Z',`url(#${id}-ground)`)+
 path('M464 163Q410 190 471 212Q550 234 516 264Q488 293 548 341H615V194Q532 196 487 164Z',`url(#${id}-water)`)+
 `<g class="land-water">${line('M464 189q-13 9 13 17m19 18q48 12 34 25m-10 52 22 13','#c4f3d8',3,'opacity=".65"')}</g>`+
 temple(432,161,.64)+trail(p.trail)+broadTree(106,199,.8,'#528e5c')+palm(30,257,1.24,'#3e8158')+palm(554,241,1.18,'#4c8f60',true)+
 path('M-12-5H92Q87 17 64 19Q77 46 41 46Q26 72-12 58Z','#3f8059')+
 `<g class="land-leaves">${line('M61 0q-20 38 4 67t-8 46m4-75q-18 2-22 13','#91ab67',4)}${oval(52,41,7,13,'#c2d17e','transform="rotate(-38 52 41)"')}${oval(64,73,7,13,'#a8c875','transform="rotate(35 64 73)"')}</g>`+
 rock(370,278,.65,'#88a06e')+rock(149,250,.58,'#a8b67c')+
 [65,100,155,392,454,574].map((x,i)=>grass(x,300+(i%3)*11,.7,'#4a794a')).join('')+
 flower(93,277,.8,'#f1b9a2')+flower(428,249,.7,'#f5d88d')+flower(397,309,.9,'#e8acb2')+
 path('M-10 350V303Q22 265 39 300Q49 263 79 308Q103 293 125 350m485 0v-54q-29-26-42 10-19-35-38 21-25-19-40 23Z','#3e7954')+
 line('M21 342q12-25 23-31m505 28 22-17','#85b46f',4);
}

function savannah(p,id){
 return circle(410,71,45,'#fff0ba')+circle(410,71,59,'#ffe7ac','opacity=".24"')+cloud(99,53,.62)+cloud(526,104,.55)+
 path('M-10 181 58 147 91 151 112 133 167 135 190 170 284 154 321 171 432 144 473 156 538 128 612 163V350H-10Z',p.distant)+
 path('M-10 215Q99 171 213 206Q338 154 446 182Q550 158 612 204V350H-10Z',`url(#${id}-ground)`)+
 path('M420 159h35l9-40-13-23-23 7-12 31Z','#e7b86a','stroke="#aa9258" stroke-width="2"')+line('M438 113v31m-8-18h19','#fff2b7',3)+sparkle(440,130,.65,'#fff4c2')+
 trail(p.trail)+acacia(116,201,1.05)+acacia(486,224,.83)+acacia(218,169,.39)+
 rock(404,259,.77,'#c5a66d')+rock(417,253,.4,'#d7bd7e')+
 [38,76,142,173,398,450,525,565].map((x,i)=>grass(x,281+(i%3)*20,.9,'#a18d43')).join('')+
 line('M19 337q24-41 24-61m-9 12 10 11m-17 8 14 8m503 24q-14-33-10-52m0 10 11-11','#d3ba66',4)+
 path('M0 341v-27q52-27 101 8 40-6 60 19m439 0v-18q-57-27-107 18Z','#aaa052')+
 flower(151,290,.6,'#fff0b8')+flower(462,304,.7,'#f3d58a');
}

function caverns(p,id){
 return [44,104,191,305,380,511,567].map((x,i)=>circle(x,63+(i%3)*25,1.8,'#d8e9dd','opacity=".65"')).join('')+
 path('M-10 168 48 125 111 151 170 111 247 159 325 128 389 150 471 117 533 151 610 123V350H-10Z',p.distant)+
 path('M-10 231Q100 173 219 209Q325 164 416 193Q536 163 610 201V350H-10Z',`url(#${id}-ground)`)+
 path('M405 204Q460 172 527 196Q572 208 612 201V340H457Q403 308 448 278Q506 250 405 239Z',`url(#${id}-water)`)+
 `<g class="land-water">${line('M465 217q48-17 95 1m-67 72q30-12 65 3m-77 23 28-4','#e6ffff',2.5,'opacity=".6"')}</g>`+
 trail(p.trail,{stone:true})+crystals(438,165,.65,'#e8beed')+crystals(101,240,1.48,'#9ddad5')+crystals(542,286,1.48,'#b6b4e8')+crystals(174,172,.47,'#c0cbe9')+
 `<g class="land-glow">${oval(104,240,65,13,'#a8f0e1','opacity=".17"')}${oval(532,284,63,17,'#d8d0f6','opacity=".2"')}${sparkle(116,126,1.1,'#e3fff0')}${sparkle(534,190,.9,'#f2e4ff')}</g>`+
 path('M-8-8H611V30l-27-6-15 70-16-72-37 18-17-32-51 30-18-25-52 20-9 64-19-57-47 3-29 42-21-57-31 18-10 64-21-74-42 9-31-24-29 18-13 61-19-64-40-6-31 83-18-87-34 21Z','#364c64')+
 path('M-4 26q37 33 39 112l-17 46 19 55-12 103H-4m610-308q-39 17-42 91l20 36-10 48 16 133h16Z','#445e70')+
 rock(186,298,.7,'#9a97b4')+mushroom(379,287,.43,'#9fc6cd')+mushroom(404,292,.32,'#afcfe3')+
 sparkle(231,131,.55,'#e0efff')+sparkle(373,102,.7,'#dcd5ff');
}

function marsh(p,id){
 return circle(440,67,34,'#f9e4ba','opacity=".75"')+cloud(171,57,.7)+cloud(501,104,.7)+
 path('M-8 181Q92 136 179 166Q302 120 399 163Q510 125 610 164V350H-8Z',p.distant)+
 path('M-10 204Q106 173 218 203Q333 168 441 192Q549 177 611 198V350H-10Z',`url(#${id}-water)`)+
 path('M-10 350V245Q39 208 116 232Q192 197 260 221Q323 192 369 211Q399 237 347 267L319 350Z',`url(#${id}-ground)`)+
 path('M316 184q53-42 118-15 38-8 49 7-20 23-80 13-50 20-87-5Z','#aeb993')+
 trail(p.trail)+bridge(326,220,.53)+
 group(98,235,1,line('M0 5q-11-41 8-83m-5 37-31-23m34 1 22-18','#7a795a',12)+outlined('M8-122q-54-13-75 27 24-5 17 17 24-6 31 9 18-12 28 0 23-9 34 4 8-22 35-15-5-45-70-42Z','#759b78')+line('M-46-86v39m14-44v31m20-28v48m29-49v45m22-45v29m21-31v43','#99b68c',4))+
 broadTree(518,211,.66,'#829e83','#7d7b62')+
 [25,56,145,425,472,561,582].map((x,i)=>reed(x,281+(i%3)*23,.55+(i%2)*.2)).join('')+
 oval(467,264,22,6,'#86a881')+path('M467 258v9l17-8Z','#bdc28b')+flower(466,258,.6,'#eebcb5')+
 oval(411,307,25,7,'#81a87d')+oval(550,325,34,10,'#82ab7d')+path('M553 316v13l24-10Z','#b1c28a')+
 `<g class="land-water">${line('M391 229h32m44 59h47m-144 37h33m99-109h38','#d4e6c8',2.5,'opacity=".65"')}</g>`+
 `<g class="land-glow">${[[-12,0],[25,24],[53,-19],[298,-18],[354,35],[429,11]].map(([x,y])=>circle(88+x,164+y,3,'#f6e7a6','opacity=".85"')).join('')}</g>`+
 path('M-8 350v-30q38-26 71 5 31-20 65 25Z','#638d70');
}

function frost(p,id){
 return circle(99,65,28,'#fff4cf')+cloud(228,62,.55)+cloud(520,91,.62)+
 outlined('M-20 219 110 48 194 170 280 24 405 182 486 64 620 221Z','#a5c2c2','#789c9f',1.5)+
 path('M110 48 70 104l27-6 16 17 17-15 16 6Zm170-24-52 76 31-10 20 18 21-23 34 14ZM486 64l-39 58 24-7 17 13 16-16 26 13Z','#f7f3da')+
 path('M-8 244Q111 160 222 210Q315 166 422 195Q521 169 610 217V350H-8Z',`url(#${id}-ground)`)+
 path('M418 226Q478 191 543 212L610 234V350H419Q405 316 451 294Q502 261 418 244Z',`url(#${id}-water)`)+
 line('M472 239 497 263 484 284m13-21 31 2m-75 47 28-12 17 9','#ecfbef',2.5)+
 trail(p.trail)+pine(84,238,1.1,'#67998d',true)+pine(151,193,.54,'#75a194',true)+pine(540,251,1.08,'#67998d',true)+pine(393,182,.43,'#7aa898',true)+
 bridge(421,172,.43)+banner(402,160,.55,'#d6a695')+rock(156,286,.73,'#b3c7bf')+path('M136 273q13-14 30 1l6 8q-18-7-36 0Z','#f3f3df')+
 `<g class="land-spark">${[[39,79],[174,108],[335,79],[456,137],[572,46],[201,260],[520,288]].map(([x,y])=>sparkle(x,y,.35,'#fff9e7')).join('')}</g>`+
 path('M-10 340v-32q70-11 126 32m496 0v-30q-68 5-99 30Z','#d9e8d9');
}

function thunder(p,id){
 return cloud(82,75,1.2,'#b7c4ce')+cloud(353,47,1.15,'#d4d8d5')+cloud(539,95,.95,'#a7b8c4')+
 `<g class="land-glow">${outlined('M466 75 440 119h19l-9 33 41-50h-23l13-27Z','#fff0a9','#fff3bf',2)}</g>`+
 path('M-10 211 35 162h58l11-42 48-1 12 46 67 18 56-50h57l17 29 71-1 23-22h54l26 59 85 19v130H-10Z',p.distant)+
 path('M-10 245 59 223l28-35h76l29 25 56 3 34-23h86l29 15 72-13 141 54v101H-10Z',`url(#${id}-ground)`)+
 path('M-10 247 78 235l39 7 44-8 33 18 41 4-7 50-28 34H-10m620-85-84-28-34 12-17 43 30 67h105Z','#657f80','opacity=".7"')+
 trail(p.trail,{stone:true})+outlined('M420 153h33v-44h-6v-15h-21v15h-6Z','#b5ba9e')+circle(436,111,9,'#e9daa0')+sparkle(436,111,.6,'#fff5c7')+
 pine(101,236,.85,'#687f83')+pine(530,252,.95,'#586f7b')+crystals(163,279,.55,'#c4d6cc')+crystals(397,299,.67,'#b4d0d4')+
 rock(146,150,.4,'#98aa9f')+rock(354,106,.34,'#a8b7b0')+line('M141 173v9m207-56v9','#d4dec7',2,'opacity=".5"')+
 [42,208,382,469,570].map((x,i)=>grass(x,295+(i%2)*24,.77,'#627c77')).join('')+
 `<g class="land-spark">${sparkle(156,208,.5)}${sparkle(378,249,.6)}${sparkle(505,173,.45)}</g>`;
}

function volcano(p,id){
 return circle(112,80,38,'#f4cd9b','opacity=".8"')+cloud(78,50,.65,'#f2bcaa')+
 path('M-10 205 89 134 177 194 235 137 300 195 364 121 431 166 517 112 611 180V350H-10Z',p.distant)+
 outlined('M302 210 397 84 432 95 472 85 574 215Z','#91747a')+path('M397 84q32 18 75 1l-11 18q-31 18-73 1Z','#f0ad7c')+
 path('M420 103q-2 43 29 65l39 38-27 23-52-61-5-59Z','#f7be76')+line('M419 108q5 35 23 49l36 49','#ffdb95',5)+
 `<g class="land-cloud">${circle(431,61,22,'#d9a6a0','opacity=".8"')}${circle(409,47,16,'#e5b4a6','opacity=".7"')}${circle(437,26,28,'#e6b8ad','opacity=".6"')}${circle(465,8,21,'#e9bfac','opacity=".45"')}</g>`+
 path('M-10 253Q100 199 212 220Q323 182 423 216Q528 191 612 237V350H-10Z',`url(#${id}-ground)`)+
 path('M464 212q-44 29 26 46 50 13 25 32-23 20 21 53h77v-50q-19-26-50-34-40-13-67-48Z',`url(#${id}-water)`)+
 `<g class="land-water">${line('M480 231q-9 9 21 19m39 35q-18 12 10 37','#ffe1a0',5,'opacity=".75"')}</g>`+
 trail(p.trail,{stone:true})+rock(107,247,1.4,'#756974')+rock(160,294,.72,'#96837e')+rock(409,297,.76,'#837079')+crystals(543,256,.55,'#dc9f8f')+
 line('M60 218v-56m0 25-18-20m18 8 15-23m-15 57-17-9M520 194v-54m0 25 18-18','#6f646c',7)+
 `<g class="land-spark">${[[52,129],[178,188],[355,114],[487,154],[573,200],[404,242]].map(([x,y],i)=>sparkle(x,y,.4+i%2*.15,'#ffdfa1')).join('')}</g>`+
 path('M-10 340v-28l26-18 22 14 25-9 31 41m518 0v-17l-34-18-27 15-21 20Z','#665e68');
}

function moonlit(p,id){
 return circle(135,65,35,'#f3e4b8')+circle(149,53,30,p.sky[0])+
 [[49,50],[217,64],[305,35],[368,86],[466,46],[548,82],[574,32]].map(([x,y],i)=>sparkle(x,y,.4+(i%2)*.15,'#e9e4c4')).join('')+
 cloud(323,102,.75,'#b4abc3')+path('M-10 198Q69 130 155 167Q252 112 344 163Q466 104 610 172V350H-10Z',p.distant)+
 path('M-10 245Q104 173 216 213Q325 163 414 193Q526 172 610 228V350H-10Z',`url(#${id}-ground)`)+
 path('M418 217Q475 191 543 212l67 27v110H472q-73-23-20-49 42-18-10-35-22-9-24-48Z',`url(#${id}-water)`)+
 `<g class="land-water">${oval(515,246,34,3,'#dbe4e6','opacity=".6"')}${oval(526,309,41,3,'#dbe4e6','opacity=".45"')}</g>`+
 trail(p.trail)+broadTree(77,242,1.1,'#777b9e','#b4b6ac')+broadTree(529,231,1.04,'#666f91','#aeb5ab')+broadTree(209,177,.43,'#8f91a9','#b4bfb0')+
 line('M62 186q12-7 23-3m435-6 12-5','#eee2b8',2,'opacity=".5"')+
 mushroom(153,284,.86,'#b8a2c8')+mushroom(187,292,.55,'#dfb3bf')+mushroom(383,286,.65,'#a7c9c3')+mushroom(410,296,.42,'#c7c2df')+
 `<g class="land-glow">${[[118,203],[192,222],[237,167],[375,195],[451,183],[497,272]].map(([x,y])=>circle(x,y,3,'#e6e8a6','opacity=".9"')+circle(x,y,9,'#e6e8a6','opacity=".1"')).join('')}</g>`+
 [24,215,397,565].map((x,i)=>grass(x,309+i%2*11,.8,'#69847c')).join('')+
 path('M-10 342v-26q25-30 46 2 31-27 69 24m505 0v-28q-30-21-46 11-32-16-61 17Z','#4d6e6d');
}

function ruins(p,id){
 return circle(435,90,53,'#fff3cf','opacity=".7"')+circle(435,90,40,'none',`stroke="#f8edbf" stroke-width="5"`)+circle(435,90,27,'none','stroke="#fff7dd" stroke-width="2"')+
 cloud(95,62,.8)+cloud(315,54,.58)+cloud(555,144,.8)+
 path('M-10 203Q94 137 197 180Q312 122 405 170Q520 124 610 183V350H-10Z',p.distant)+
 group(125,137,.62,path('M-47 0q50-24 99 0L26 24H-24Z','#9da9a8')+path('M-47 0q49-21 99 0Z','#d5d4b2')+outlined('M-23-5v-44q23-28 46 0v44H11v-42q-11-10-22 0v42Z','#e6d7b0'))+
 path('M-10 248Q100 185 218 218Q325 161 426 194Q524 168 610 223V350H-10Z',`url(#${id}-ground)`)+
 group(431,173,.76,outlined('M-43 0v-76h14v76m58 0v-76h14V0Z','#e9dbb7')+outlined('M-49-76h98v-14H-49Z','#e9dbb7')+path('M-38-64h7v53h-7m70-53h7v53h-7Z','#bbbea6')+sparkle(0,-52,1.2,'#fff2c3')+line('M-55 6H55','#d5c498',9))+
 trail(p.trail,{stone:true})+
 outlined('M88 254V153h21v101Z','#e0d3af')+outlined('M80 155h38v-12H80m1 111h36v13H81Z','#e8dab8')+line('M94 168v73m9-73v73','#b7b69b',2)+
 group(518,272,1,outlined('M-14 0V-66l27-13v79Z','#ddd3b5')+line('M-7-54v43m13-50v50','#b7b79e',2)+outlined('M-20-69-7-82 19-77 24-65Z','#ebe1c3'))+
 crystals(153,292,.61,'#b2ddd7')+crystals(376,272,.4,'#d1c5e2')+rock(541,180,.27,'#c5c9b0')+rock(331,118,.24,'#c3c9b9')+
 flower(66,281,.6,'#efe8c9')+flower(461,313,.7,'#e8d7c6')+
 `<g class="land-glow">${sparkle(225,146,.6)}${sparkle(487,217,.7)}${sparkle(345,103,.45)}</g>`+
 path('M-10 341v-27q36-19 62 1 32-9 72 26m487 0v-21q-40-17-80 21Z','#8eaa90');
}

function kingdom(p,id){
 const monkeyFace=group(0,-61,1,circle(-20,0,12,'#b79a58')+circle(20,0,12,'#b79a58')+outlined('M-25-6q1-27 25-27T25-6Q30 25 0 28-30 25-25-6Z','#d8ba72')+path('M-19-8q9-12 19-2 11-10 20 2l-2 19Q0 33-18 11Z','#ead49b')+circle(-9,0,3,'#5a6750')+circle(10,0,3,'#5a6750')+line('M-7 13q7 6 14 0','#8e8054',2.5));
 return circle(106,68,32,'#fff0bd')+cloud(244,54,.63)+cloud(529,80,.6)+
 path('M-10 195Q108 105 201 173Q304 110 392 157Q505 106 610 170V350H-10Z',p.distant)+
 path('M-10 249Q96 179 217 211Q325 151 428 190Q525 167 610 226V350H-10Z',`url(#${id}-ground)`)+
 group(431,173,.93,outlined('M-78 13H78V0H65v-17H52v-16H43v-72H-43v72h-9v16h-13V0h-13Z','#cab06d')+path('M-16 8v-32q16-20 32 0V8Z','#596e51')+line('M-65 0h130m-116-17H52m-95-16h86m-73-61h60','#f1d78f',3)+outlined('M-50-106 0-130 50-106Z','#dfc17d')+monkeyFace)+
 banner(357,164,.72,'#e0a18a')+banner(507,163,.72,'#e0a18a')+
 trail(p.trail,{stone:true})+path('M217 337q-7-17 8-31m55-24 24 8m24-40 20 1m-27-46 17-6','#f7e5ac','opacity=".7"')+
 group(109,239,.86,outlined('M-36 10v-68q0-39 36-39 38 0 38 39v68Z','#aaa16c')+circle(-26,-51,12,'#bdb17b')+circle(27,-51,12,'#bdb17b')+outlined('M-24-55q1-23 24-23 25 1 25 24v23Q0-9-24-31Z','#d2bf85')+circle(-9,-49,4,'#647456')+circle(10,-49,4,'#647456')+line('M-9-31q9 8 18 0','#7d8055',3)+path('M-31 10q28-17 62 0Z','#c6b579'))+
 palm(541,255,.84,'#9eaa65',true)+palm(32,221,.75,'#98a66a')+rock(404,282,.61,'#b6aa73')+
 flower(159,285,.7,'#edb693')+flower(463,296,.6,'#f2db9b')+
 [66,188,377,562].map((x,i)=>grass(x,304+i%2*17,.8,'#849352')).join('')+
 `<g class="land-glow">${sparkle(433,64,.75,'#fff1b5')}${sparkle(328,136,.45)}${sparkle(500,221,.45)}</g>`+
 path('M-10 342v-25q39-25 72 7 40-22 82 18m466 0v-26q-37-20-71 10-27-8-53 16Z','#849757');
}

const scenes={
 'emerald-jungle':jungle,'sunstone-savannah':savannah,'crystal-caverns':caverns,
 'whispering-marsh':marsh,'frostpeak-mountains':frost,'thunder-highlands':thunder,
 'ember-volcano':volcano,'moonlit-forest':moonlit,'celestial-ruins':ruins,
 'ancient-monkey-kingdom':kingdom
};

export function landArt(id,{uid='land',position={east:0,north:0},compact=false}={}){
 const key=Object.hasOwn(scenes,id)?id:'emerald-jungle',p=palettes[key];
 const safeUid=String(uid).replace(/[^a-zA-Z0-9_-]/g,'-')||'land';
 const prefix=`land-${safeUid}-${key}`;
 // A subtle bounded parallax response; these illustrations are fantasy scenery.
 const east=Number(position?.east)||0,north=Number(position?.north)||0;
 const dx=Number.isFinite(east)?Math.max(-4,Math.min(4,-east/80)):0;
 const dy=Number.isFinite(north)?Math.max(-2,Math.min(2,north/120)):0;
 return `<svg xmlns="http://www.w3.org/2000/svg" class="land-art${compact?' land-art-compact':''}" viewBox="0 0 600 340" preserveAspectRatio="xMidYMid slice" aria-hidden="true" focusable="false" data-land="${key}"><defs><linearGradient id="${prefix}-sky" x2="0" y2="1"><stop stop-color="${p.sky[0]}"/><stop offset="1" stop-color="${p.sky[1]}"/></linearGradient><linearGradient id="${prefix}-ground" x2="0" y2="1"><stop stop-color="${p.ground[0]}"/><stop offset="1" stop-color="${p.ground[1]}"/></linearGradient><linearGradient id="${prefix}-water" x2="0" y2="1"><stop stop-color="${p.water[0]}"/><stop offset="1" stop-color="${p.water[1]}"/></linearGradient><pattern id="${prefix}-grain" width="31" height="27" patternUnits="userSpaceOnUse"><circle cx="3" cy="8" r=".8" fill="#fff8d6" opacity=".19"/><circle cx="21" cy="23" r=".75" fill="#173d39" opacity=".11"/></pattern><clipPath id="${prefix}-clip"><path d="M0 0h600v340H0Z"/></clipPath></defs><g clip-path="url(#${prefix}-clip)"><path d="M0 0h600v340H0Z" fill="url(#${prefix}-sky)"/><g transform="translate(${dx.toFixed(2)} ${dy.toFixed(2)})">${scenes[key](p,prefix)}</g><path d="M0 0h600v340H0Z" fill="url(#${prefix}-grain)" pointer-events="none"/></g></svg>`;
}
