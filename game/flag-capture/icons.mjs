const drawings={
  move:'<path d="M5 23C5 13 16 24 18 12" fill="none"/><path d="m12 12 7-7 7 7-8 1" fill="#96d6f7"/><circle cx="6" cy="24" r="3" fill="#d7f5ff"/>',
  attack:'<path d="m8 26 4-11L24 4l4 1-1 5-12 10z" fill="#f2c6ac"/><path d="m5 18 10 9M7 25l-3 4" fill="none"/><path d="m16 15 8-8" stroke="#fff2d5"/>',
  guard:'<path d="M16 3 28 8v10c-1 7-7 11-12 13C10 28 5 24 4 18V8z" fill="#9ac9d1"/><path d="m10 16 4 5 9-10" fill="none" stroke="#e5fff4" stroke-width="3"/>',
  bloomcall:'<path d="M15 28V17" fill="none"/><path d="M15 23Q3 27 5 15q9-1 10 8m0-1q13 2 12-9-9 0-12 9" fill="#8be5a6"/><path d="M16 5c9-9 17 5 7 10 4 10-12 13-14 3C-3 18 0 4 10 8c-4-9 8-11 6-3" fill="#f2a6d3"/><circle cx="15" cy="13" r="5" fill="#fff2a6"/><path d="M15 10v6m-3-3h6" stroke="#46846e"/>',
  sporehop:'<path d="M12 26V15h7v11" fill="#ffe2b7"/><path d="M3 16Q5 1 15 4q12 0 15 13z" fill="#8bd8ef"/><circle cx="12" cy="10" r="2" fill="#ebfff0"/><circle cx="22" cy="12" r="2" fill="#ebfff0"/><path d="M3 26q7 7 20 1m-2-4 4 4-3 4" fill="none"/>',
  belch:'<path d="M3 9q12 12 1 20 18 0 18-14Q14 4 3 9Z" fill="#90d9b7"/><path d="m5 12 8 4-5 4 6 4" fill="#fff1b7"/><circle cx="25" cy="11" r="5" fill="#b692ee"/><path d="m21 2 3 2m6 12-3 2" fill="none"/>',
  sporeburst:'<path d="m16 5 4 7 10-1-5 8 5 8-10-2-5 7-3-10-10-3 9-5z" fill="#f3a3cf"/><circle cx="16" cy="18" r="6" fill="#8ae3bf"/><circle cx="7" cy="6" r="2" fill="#f8e497"/><circle cx="27" cy="4" r="2" fill="#f8e497"/>',
  wrongnote:'<path d="M17 24V6l11-2v16" fill="none" stroke="#f4b9c8" stroke-width="4"/><ellipse cx="12" cy="25" rx="6" ry="4" fill="#f5c575"/><ellipse cx="24" cy="21" rx="5" ry="4" fill="#ef98b3"/><path d="m3 8 5 3-4 4 5 3" fill="none"/>',
  blinkbite:'<path d="M4 12q12-11 24 0l-4 16H8Z" fill="#b18ddd"/><path d="m7 13 5 10 2-12m4 0 2 12 5-10" fill="#fff0c8"/><path d="m2 5 6-3m19 1 3 5" fill="none"/>',
  foldhorn:'<path d="M4 26C29 29 30 3 15 3 5 3 5 19 17 15c-2 7-6 6-13 11" fill="#f5c394"/><path d="M16 7q-8 5-1 5" fill="none"/><path d="m23 21 5 2-4 6" fill="#ef93ad"/>',
  omen:'<path d="M5 17h20l-3 12H8Z" fill="#b194d9"/><path d="M25 19q9-2 4 6h-6M11 14Q5 7 12 3m5 11Q23 7 17 2" fill="none"/><path d="M3 10q12-11 25 0-12 11-25 0" fill="#f4cc8a"/><circle cx="16" cy="10" r="4" fill="#7260a5"/>',
  concordance:'<circle cx="16" cy="15" r="10" fill="#f8d27f"/><path d="M13 21q-15-2-12-15 8 9 13 3m5 12Q34 19 31 6q-8 9-13 3" fill="#fff3d1"/><circle cx="16" cy="15" r="6" fill="#b5dce3"/><path d="M16 11v5l4 2" fill="none"/>',
  mercy:'<path d="M16 27C-8 12 8-2 16 10 24-2 40 12 16 27" fill="#9ae3b9"/><path d="M2 10 8 6l1 13m21-9-6-4-1 13" fill="#fff4cb"/><path d="M16 13v9m-4-4h8" stroke="#f8ffea" stroke-width="3"/>',
  sanctuary:'<path d="M16 2 28 7v12q-2 9-12 12Q6 28 4 19V7z" fill="#f4d18b"/><path d="M9 22q6-3 3-12 5-6 9 0-2 9 3 12Z" fill="#fff4d4"/><circle cx="16" cy="25" r="2" fill="#c7a5d9"/>',
  ray:'<path d="M3 15q10-11 21 0-10 11-21 0" fill="#fff2bf"/><circle cx="14" cy="15" r="5" fill="#ad91db"/><path d="m22 15 9-2m-7 8 6 4m-6-15 6-5M4 4l3 4M2 26l5-5" fill="none" stroke="#ffe6a0" stroke-width="3"/>',
  seedgrove:'<path d="M16 29V13" fill="none"/><path d="M16 20Q0 19 3 5q14 0 13 15m0 3Q31 22 30 9q-14-1-14 14" fill="#9bdbb3"/><path d="m7 10 9 10 9-6" fill="none" stroke="#eaffd4"/>',
  nectar:'<path d="M16 2C13 9 3 16 5 23c3 12 23 10 23-1C28 15 20 8 16 2" fill="#97e1bb"/><path d="M16 15v11m-5-5h10" stroke="#faffdf" stroke-width="3"/><path d="m9 16-2 6" stroke="#dbffed"/>',
  laughinghex:'<path d="M6 7q10 7 20 0l-1 16-9 8-9-8Z" fill="#cc9ae0"/><path d="m3 10 2-8 8 7m6-1 7-7 4 10" fill="#f5c58e"/><path d="m10 15 3 2m7-2 3 2m-11 6q5 5 10-1" fill="none"/>',
  crookedswap:'<path d="M3 10h23l-5-6m5 18H4l6 6" fill="none" stroke="#f1b68e" stroke-width="4"/><path d="m22 5 7 5-7 5M9 17l-7 5 7 5" fill="#b897e4"/><circle cx="15" cy="16" r="3" fill="#ffdf9c"/>',
  starstep:'<ellipse cx="16" cy="17" rx="13" ry="10" fill="#a1cfe9"/><ellipse cx="16" cy="17" rx="8" ry="6" fill="#524f83"/><path d="m17 1 3 7 8 1-6 5 1 8-7-5-7 3 3-8-5-6 8 1Z" fill="#ffe7a0"/>',
  wardray:'<path d="M16 8 26 12v9q-2 7-10 10-8-3-10-10v-9Z" fill="#f2d28f"/><path d="M10 11Q0 10 1 3q8 4 12 1m9 7Q32 10 31 3q-8 4-12 1" fill="#fff4d2"/><path d="M16 13v11m-4-6h8" stroke="#fff9da" stroke-width="3"/>',
  broodlings:'<path d="M4 20Q5 5 13 6q9 1 10 15-2 9-11 9-7-1-8-10Z" fill="#a4e6c6"/><path d="m5 17 6 4 5-6 6 4" fill="none"/><circle cx="24" cy="10" r="7" fill="#97d5ef"/><circle cx="22" cy="9" r="1.5" fill="#173740"/><path d="m23 15 5 3m-3-14 2-3" fill="none"/>',
  pocketmaws:'<path d="M2 11q6-9 13 0v12q-6 7-13 0ZM17 17q6-9 13 0v10q-6 6-13 0Z" fill="#c39be6"/><path d="m4 12 3 6 3-6m9 7 3 6 3-6" fill="#fff0c7"/><path d="m19 7 3-5 6 3" fill="none" stroke="#ffbd9f" stroke-width="3"/>',
  bellmotes:'<path d="M11 25q7-4 5-14 5-7 10 0-2 10 5 14Z" fill="#f7d68f"/><circle cx="21" cy="28" r="3" fill="#e9adbc"/><path d="M15 13Q2 16 2 6q8 5 13 1m10 5Q34 12 31 4l-8 4" fill="#fff6d6"/><ellipse cx="19" cy="3" rx="7" ry="2" fill="#fff0ae"/>'
};
export function abilityIcon(kind){return `<svg class="ability-icon" viewBox="0 0 34 34" aria-hidden="true" fill="none" stroke="#173740" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">${drawings[kind]||drawings.guard}</svg>`;}
export const ABILITY_ICON_KEYS=Object.freeze(Object.keys(drawings));
