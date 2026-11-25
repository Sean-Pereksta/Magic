// Picking follows the dimensions used by Renderer.unit. Heroes' wider wings
// and weapons are part of the target, not an accidental ground-move command.
function spriteBounds(unit) {
  if (unit.kind === 'tower') return {left:-35,right:35,top:-108,bottom:35};
  if (unit.kind === 'core') return {left:-70,right:70,top:-125,bottom:70};
  let width, height;
  if (unit.kind === 'hero') {
    const id=unit.heroId||unit.subtype||'arc';
    const expanded=['bulwark','ember','seraph'].includes(id);
    width=(id==='seraph'?72:['brinehook','vela'].includes(id)?60:['rivet','solstice'].includes(id)?48:expanded?48:40)*1.5;
    height=(expanded?48:44)*1.5;
  } else if (unit.kind === 'footman' || unit.kind === 'archer') {
    width=34;height=42;
  } else {
    const big=unit.kind==='beast',r=unit.r||(big?35:20);
    width=big?Math.min(140,Math.max(94,r*2.8)+Math.min(6,(unit.tier||1)-1)*2):Math.max(48,r*2.5)*(unit.scale||1);
    height=width*44/48;
  }
  return {left:-width/2,right:width/2,top:7-height,bottom:7};
}

/** Pick only visible enemies under the pointer, allowing a fixed CSS-pixel margin. */
export function pickEnemyAt({units,team,point,worldToScreen,zoom,pointerType='mouse',canSee=()=>true}) {
  const padding=pointerType==='touch'?14:7;
  const candidates=[];
  for (const unit of units) {
    if (!unit.alive || unit.team===team || !canSee(unit)) continue;
    const anchor=worldToScreen(unit.x,unit.y),bounds=spriteBounds(unit);
    const x=point.x-anchor.x,y=point.y-anchor.y;
    const outsideX=Math.max(bounds.left*zoom-x,0,x-bounds.right*zoom);
    const outsideY=Math.max(bounds.top*zoom-y,0,y-bounds.bottom*zoom);
    if (Math.hypot(outsideX,outsideY)>padding) continue;
    // Keep structure silhouettes narrow above their broad ground platforms.
    if (unit.kind==='tower' || unit.kind==='core') {
      const core=unit.kind==='core',baseRadius=(core?70:35)*zoom+padding;
      const onBase=Math.hypot(x,y)<=baseRadius;
      const onBody=Math.abs(x)<=(core?44:27)*zoom+padding
        && y>=(core?-125:-108)*zoom-padding && y<=padding;
      if (!onBase && !onBody) continue;
    }
    const radius=unit.r||20;
    const [width,height,lift]=({hero:[22,35,31],tower:[32,55,48],core:[64,80,42],footman:[17,23,17],archer:[17,23,17]})[unit.kind]
      ||[Math.max(22,radius*1.35),Math.max(26,radius*1.5),Math.max(20,radius*1.2)];
    const distance=Math.hypot(x/(width*zoom+padding),(y+lift*zoom)/(height*zoom+padding));
    // A troop's body wins over a wide hero's weapon or wing fringe.
    candidates.push({unit,distance,fringe:distance>1});
  }
  candidates.sort((a,b)=>Number(a.fringe)-Number(b.fringe)||a.distance-b.distance||a.unit.id-b.unit.id);
  return candidates[0]?.unit;
}
