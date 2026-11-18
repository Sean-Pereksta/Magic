(function (factory) {
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  else globalThis.CircuitStarship=api;
})(function () {
  'use strict';

  const SHIP='starship',FUEL='flightCell';
  const FUEL_MAX=100,FLIGHT_SPEED=400,CLIMB_SPEED=265,LANDING_SPEED=24;
  const clamp=(value,low,high)=>Math.max(low,Math.min(high,value));

  function create({T,defs,player,inventory,get,getMeta,inside,tile,width,height,inventoryRoom,
    canOccupy,notify=()=>{},save=()=>{}}) {
    const bodyW=tile*1.95,bodyH=tile*.94;
    let ship=null,thrust=0,braking=false;

    function clear(x,y,w,h) {
      const x0=Math.floor(x/tile),x1=Math.floor((x+w-.01)/tile);
      const y0=Math.floor(y/tile),y1=Math.floor((y+h-.01)/tile);
      for(let ty=y0;ty<=y1;ty++)for(let tx=x0;tx<=x1;tx++){
        if(!inside(tx,ty))return false;
        const def=defs[get(tx,ty)];
        if(def?.solid&&!(def.door&&getMeta(tx,ty)?.open))return false;
      }
      return true;
    }

    function ground(body=ship) {
      return !!body&&!clear(body.x,body.y+1.5,bodyW,bodyH);
    }

    function parked() {
      return !!ship&&ground()&&Math.abs(ship.vx)<LANDING_SPEED&&Math.abs(ship.vy)<LANDING_SPEED;
    }

    function pilotDistance(x,y) {
      return Math.hypot(x-(player.x+player.w/2),y-(player.y+player.h/2))/tile;
    }

    function reachable() {
      return !!ship&&pilotDistance(ship.x+bodyW/2,ship.y+bodyH/2)<=3;
    }

    function syncPilot() {
      player.x=ship.x+(bodyW-player.w)/2;
      player.y=Math.max(0,ship.y+bodyH-player.h);
      player.vx=player.vy=0;
      player.facing=ship.facing;
      player.onGround=ground();
      player.climbing=false;
    }

    function exitPosition() {
      const y=ship.y+bodyH-player.h+.01;
      const candidates=[ship.x+bodyW+2,ship.x-player.w-2,ship.x+(bodyW-player.w)/2];
      for(const x of candidates){
        if(clear(x,y,player.w,player.h)&&(canOccupy?.(x,y,player.w,player.h)??true)&&
          !clear(x,y+3,player.w,player.h))return {x,y};
      }
      return null;
    }

    function padSpot(x,y) {
      return {x:(x+.5)*tile-bodyW/2,y:y*tile-bodyH-.02};
    }

    function padIssue(x,y) {
      if(!inside(x,y)||get(x,y)!==T.STARSHIP_PAD)return 'Aim at a Starship Launch Pad.';
      if(pilotDistance((x+.5)*tile,(y+.5)*tile)>6)return 'Move within reach of the launch pad.';
      const meta=getMeta(x,y);
      if(meta?.overload)return 'Launch grid overloaded. Supply the pad with higher-capacity conductors.';
      if(meta?.signalOK===false||meta?.disabled)return 'The launch pad is disabled by its control signal.';
      if(!meta?.powered)return 'Supply the launch pad with 12 power.';
      const point=padSpot(x,y);
      if(!clear(point.x,point.y,bodyW,bodyH)||!clear(point.x,point.y-tile*.6,bodyW,bodyH))
        return 'Clear a two-tile-wide approach above the launch pad.';
      return null;
    }

    function fail(reason) {
      notify(reason);
      return false;
    }

    function board() {
      if(!ship||ship.piloted||!reachable())return fail('Move next to the landed starship.');
      if(!parked())return fail('Wait until the starship has landed.');
      if(!ship.fuel&&!(inventory[FUEL]>0))return fail('Bring a Flight Cell to power the starship.');
      if(!ship.fuel){inventory[FUEL]--;ship.fuel=FUEL_MAX;}
      ship.piloted=true;
      syncPilot();
      notify('Starship boarded. Fly with movement controls. Use to disembark after landing.');
      save();
      return true;
    }

    function launch(x,y) {
      const issue=padIssue(x,y);
      if(issue)return fail(issue);
      if(ship){
        if(reachable())return board();
        return fail('Your starship is already deployed. Retrieve it here or board it where it landed.');
      }
      if(!(inventory[SHIP]>0))return fail('Assemble a Starship at a Field Workbench using Robotics Foundry components.');
      if(!(inventory[FUEL]>0))return fail('Load a Flight Cell before launch.');
      const point=padSpot(x,y);
      ship={...point,vx:0,vy:-48,facing:player.facing<0?-1:1,fuel:FUEL_MAX,piloted:true};
      inventory[SHIP]--;
      inventory[FUEL]--;
      syncPilot();
      notify('Starship launched. Hold Up to climb or Down to land. Use to disembark.');
      save();
      return true;
    }

    function unboard() {
      if(!ship?.piloted)return false;
      if(!parked())return fail('Land the starship before disembarking.');
      const point=exitPosition();
      if(!point)return fail('Land beside a clear place to stand.');
      ship.piloted=false;
      ship.vx=ship.vy=0;
      Object.assign(player,point,{vx:0,vy:0,onGround:true});
      notify('Starship parked. Interact with it to board again.');
      save();
      return true;
    }

    function refuel() {
      if(!ship||!reachable()||!parked())return fail('Land and approach the starship to refuel.');
      if(ship.fuel>=FUEL_MAX)return fail('Flight Cell charge is full.');
      if(!(inventory[FUEL]>0))return fail('Bring a Flight Cell to refuel.');
      inventory[FUEL]--;
      ship.fuel=FUEL_MAX;
      notify('Starship refueled.');
      save();
      return true;
    }

    function recall(x,y) {
      const issue=padIssue(x,y);
      if(issue)return fail(issue);
      if(!ship||ship.piloted)return fail('Land and disembark before recalling a deployed starship.');
      if(onPad(x,y))return fail('The starship is already at this pad.');
      if(!(inventory[FUEL]>0))return fail('Remote recovery costs one Flight Cell.');
      inventory[FUEL]--;
      Object.assign(ship,padSpot(x,y),{vx:0,vy:0});
      notify('The pad retrieved your starship. Board it when ready.');
      save();
      return true;
    }

    function recover() {
      if(!ship||ship.piloted||!reachable()||!parked())return fail('Approach the landed starship on foot to pack it.');
      if(inventoryRoom&&inventoryRoom()<1)return fail('Make room for the starship in your backpack.');
      inventory[SHIP]=(inventory[SHIP]||0)+1;
      ship=null;
      notify('Starship packed. Its remaining fuel was discharged.');
      save();
      return true;
    }

    function hitTest(x,y) {
      return !!ship&&x*tile<ship.x+bodyW&&(x+1)*tile>ship.x&&y*tile<ship.y+bodyH&&(y+1)*tile>ship.y;
    }

    function onPad(x,y) {
      return !!ship&&Math.abs(ship.y+bodyH-y*tile)<2&&Math.abs(ship.x+bodyW/2-(x+.5)*tile)<tile*.8;
    }

    function interact(x,y) {
      if(ship?.piloted){unboard();return true;}
      if(hitTest(x,y)){board();return true;}
      if(inside(x,y)&&get(x,y)===T.STARSHIP_PAD){launch(x,y);return true;}
      return false;
    }

    function status() {
      if(!ship)return null;
      const landed=parked(),fuel=Math.ceil(ship.fuel);
      return {piloted:ship.piloted,landed,fuel,fuelPercent:fuel,nearby:reachable(),
        canRefuel:landed&&reachable()&&ship.fuel<FUEL_MAX&&inventory[FUEL]>0,
        canUnboard:ship.piloted&&landed&&!!exitPosition(),
        status:landed?'Landed':ship.fuel<=0||!ship.piloted?'Emergency descent':'In flight',
        detail:ship.piloted?(landed?'Use to disembark. Refuel with a Flight Cell.':'Move to fly, hold Down to land.'):
          landed?'Interact with the starship to board.':'Wait for the starship to land.'};
    }

    function describe(x,y) {
      if(!inside(x,y)||get(x,y)!==T.STARSHIP_PAD)return null;
      const issue=padIssue(x,y);
      if(issue)return {status:issue,detail:'A launch pad needs power and open space above it.'};
      const local=reachable(),ready=!!inventory[FUEL]&&!!inventory[SHIP]&&!ship;
      const detail=ship?(local?'Board, refuel, or pack the landed starship.':'Recover the deployed starship here for one Flight Cell.'):
        'Bring a Starship and a Flight Cell to launch.';
      return {status:ready?'Ready to launch':ship&&local?'Starship nearby':'Pad online',detail,
        canLaunch:ready,canBoard:!!ship&&!ship.piloted&&local&&parked()&&(ship.fuel>0||inventory[FUEL]>0),
        canRecall:!!ship&&!ship.piloted&&!onPad(x,y)&&inventory[FUEL]>0,canPack:!!ship&&!ship.piloted&&local&&parked(),
        canRefuel:!!ship&&local&&parked()&&ship.fuel<FUEL_MAX&&inventory[FUEL]>0};
    }

    function moveAxis(axis,delta) {
      if(!delta)return false;
      const steps=Math.ceil(Math.abs(delta)/3),distance=delta/steps;
      for(let i=0;i<steps;i++){
        const x=ship.x+(axis==='x'?distance:0),y=ship.y+(axis==='y'?distance:0);
        if(!clear(x,y,bodyW,bodyH))return true;
        ship.x=x;ship.y=y;
      }
      return false;
    }

    function onRespawn() {
      if(!ship?.piloted)return;
      ship.piloted=false;
      ship.vx=0;
      notify('The starship will land where you left it. Board it or recall it at a powered pad.');
      save();
    }

    function tick(dt,keys={}) {
      if(!ship)return;
      dt=Math.min(.06,Math.max(0,dt));
      if(ship.piloted){
        const expectedX=ship.x+(bodyW-player.w)/2,expectedY=Math.max(0,ship.y+bodyH-player.h);
        if(player.health<=0||Math.hypot(expectedX-player.x,expectedY-player.y)>tile*2.5)onRespawn();
      }
      const dx=Number(!!(keys.KeyD||keys.ArrowRight||keys.touchright))-Number(!!(keys.KeyA||keys.ArrowLeft||keys.touchleft));
      const dy=Number(!!(keys.KeyS||keys.ArrowDown||keys.touchdown))-Number(!!(keys.KeyW||keys.ArrowUp||keys.Space||keys.touchjump));
      const powered=ship.piloted&&ship.fuel>0;
      const supported=ground();
      thrust=powered&&(dx||dy<0||!supported)?(dx||dy<0?2:1):0;
      braking=powered&&dy>0;
      if(powered){
        const fuelBefore=ship.fuel;
        ship.vx+=(dx*FLIGHT_SPEED-ship.vx)*Math.min(1,dt*(dx?5:7));
        ship.vy+=(dy*(dy>0?180:CLIMB_SPEED)-ship.vy)*Math.min(1,dt*6);
        ship.fuel=Math.max(0,ship.fuel-dt*(thrust===2?1.7:thrust===1?.48:0));
        if(fuelBefore>0&&!ship.fuel)notify('Flight Cell depleted. The starship is gliding down to land.');
      }else{
        ship.vx*=Math.pow(.025,dt);
        ship.vy=clamp(ship.vy+dt*390,-CLIMB_SPEED,125);
      }
      if(Math.abs(ship.vx)>10)ship.facing=Math.sign(ship.vx);
      if(moveAxis('x',ship.vx*dt))ship.vx=0;
      if(moveAxis('y',ship.vy*dt))ship.vy=0;
      if(ship.piloted)syncPilot();
    }

    function draw(ctx,camera,vw,vh,art,time=0) {
      if(!ship)return false;
      const x=ship.x-camera.x+bodyW/2,y=ship.y-camera.y+bodyH/2;
      if(x<-bodyW||x>vw+bodyW||y<-bodyH||y>vh+bodyH)return false;
      const key=braking?'starshipBrake':thrust?(Math.floor(time*13)%2?'starshipThrust1':'starshipThrust2'):'starshipIdle';
      ctx.save();
      ctx.translate(Math.round(x),Math.round(y));
      ctx.scale(ship.facing,1);
      const w=bodyW+thrust*10,h=bodyH+7;
      if(!art?.vehicle?.(ctx,key,-w/2,-h/2,w,h)&&
        !art?.vehicle?.(ctx,SHIP,-w/2,-h/2,w,h)&&
        !art?.item?.(ctx,SHIP,-w/2,-w/2,w)){
        ctx.fillStyle='#63696d';
        ctx.beginPath();ctx.moveTo(-bodyW/2,1);ctx.lineTo(-bodyW*.28,-bodyH*.45);ctx.lineTo(bodyW*.26,-bodyH*.47);
        ctx.lineTo(bodyW/2,0);ctx.lineTo(bodyW*.31,bodyH*.37);ctx.lineTo(-bodyW*.31,bodyH*.37);ctx.closePath();ctx.fill();
        ctx.fillStyle='#6edacb';ctx.fillRect(1,-bodyH*.29,bodyW*.27,bodyH*.22);
        ctx.fillStyle='#e8b266';ctx.fillRect(-bodyW*.41,bodyH*.14,bodyW*.73,3);
      }
      ctx.restore();
      return true;
    }

    function restore(data) {
      ship=null;thrust=0;braking=false;
      if(!data||typeof data!=='object'||!Number.isFinite(data.x)||!Number.isFinite(data.y))return;
      const x=clamp(data.x,0,width*tile-bodyW),y=clamp(data.y,0,height*tile-bodyH);
      ship={x,y,piloted:data.piloted===true,facing:data.facing===-1?-1:1,
        vx:clamp(Number(data.vx)||0,-FLIGHT_SPEED,FLIGHT_SPEED),
        vy:clamp(Number(data.vy)||0,-CLIMB_SPEED,CLIMB_SPEED),
        fuel:clamp(Number(data.fuel)||0,0,FUEL_MAX)};
      if(ship.piloted)syncPilot();
    }

    return {get active(){return !!ship?.piloted;},status,describe,hitTest,launch,board,interact,unboard,refuel,recall,recover,tick,draw,onRespawn,
      canDismantle(x,y){return onPad(x,y)?'Move the starship off the launch pad first.':null;},
      export:()=>ship?{...ship}:null,restore,reset(){restore(null);}};
  }

  return {create};
});
