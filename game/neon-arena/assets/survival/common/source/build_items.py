"""Real GLB salvage pickups and deployable devices. Run alongside build_species.py."""
from pathlib import Path
import math
from geom import Scene,material
OUT=Path(__file__).resolve().parent.parent/'items'
steel=material('alloy','#425362',.7,.38);dark=material('recess','#111b25',.4,.5);white=material('ceramic','#ddeaf2',.15,.45)
def make(kind):
 s=Scene('Neon Breach '+kind);color={'health':'#ff537b','armor':'#77ccff','decoy':'#72f9ef','shockFence':'#e7c266','empMine':'#9f8cff','sentry':'#fcb956','shieldBeacon':'#6bcbff'}[kind];light=material('indicator',color,.1,.35,.7)
 if kind=='health':
  s.box('medical_case',(0,.3,0),(.62,.44,.4),white,.16);s.box('case_seal',(0,.3,0),(.66,.08,.43),dark,.1)
  s.box('medical_cross_vertical',(0,.39,.21),(.075,.22,.025),light,.1);s.box('medical_cross_horizontal',(0,.39,.21),(.23,.075,.025),light,.1)
  s.tube('carry_handle',[(-.15,.54,0),(-.15,.66,0),(.15,.66,0),(.15,.54,0)],[.03]*4,steel,sides=8)
 elif kind=='armor':
  s.ellipsoid('curved_armor_plate',(0,.39,0),(.38,.4,.13),steel,segments=14,rings=8)
  s.box('armor_indicator',(0,.49,.12),(.3,.08,.025),light,.1)
  for side in [-1,1]:s.box('armor_strap',(side*.24,.2,-.06),(.09,.29,.05),dark,.1)
 else:
  s.cylinder('deployment_base',(0,.12,0),.36,.18,steel,axis='y',sides=16)
  for side in [-1,1]:
   s.tube('tripod',[(0,.18,0),(side*.32,.04,.26)],[.045,.025],dark,sides=8)
   s.tube('tripod',[(0,.18,0),(side*.32,.04,-.26)],[.045,.025],dark,sides=8)
  if kind=='sentry':
   s.cylinder('turret_column',(0,.46,0),.085,.62,steel,axis='y',sides=12);s.box('turret_body',(0,.86,.06),(.32,.27,.44),steel,.18)
   s.cylinder('barrel',(0,.86,.41),.055,.5,dark,sides=12);s.cylinder('muzzle',(0,.86,.68),.075,.07,light,sides=12)
  elif kind=='shockFence':
   for side in [-1,1]:
    s.cylinder('electrode',(side*.49,.49,0),.065,.86,steel,axis='y',sides=10)
    for y in [.36,.66,.87]:s.torus('insulator',(side*.49,y,0),.08,.025,light,axis='y',segments=12,sides=6)
   s.box('power_cell',(0,.25,0),(.31,.25,.24),light,.1)
  elif kind=='empMine':
   s.ellipsoid('mine_shell',(0,.25,0),(.31,.14,.31),dark,segments=16,rings=6);s.torus('mine_rim',(0,.29,0),.23,.035,light,axis='y',segments=16,sides=6)
  else:
   s.cylinder('projector',(0,.37,0),.16,.47,steel,axis='y',sides=12);s.torus('energy_emitter',(0,.62,0),.22,.04,light,axis='y',segments=16,sides=6)
   s.ellipsoid('energy_core',(0,.66,0),(.12,.08,.12),light,segments=12,rings=6)
 s.ground();return s.save(OUT/(kind+'.glb'))
if __name__=='__main__':
 for k in ['health','armor','decoy','shockFence','empMine','sentry','shieldBeacon']:print(make(k))
