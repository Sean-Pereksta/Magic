"""Six original anatomical alien meshes; named rigid parts animate in NeonModels.
Run python build_species.py. Requires numpy and scipy. Feet at Y=0, +Z forward.
"""
import math, json
from pathlib import Path
from geom import Scene, material
OUT=Path(__file__).resolve().parent.parent/'aliens'
def build(kind):
    colors={'leaper':'#9471bc','stalker':'#846348','screecher':'#b87c72','artillery':'#698d55','lurker':'#5c6296','warder':'#678f9d'}
    s=Scene('Neon Breach '+kind)
    skin=material('dermis',colors[kind],.04,.7);plate=material('chitin','#263542',.17,.52);bone=material('ivory','#cfc8a3',.03,.4);dark=material('folds','#151c2c',.02,.65);glow=material('bioluminescence','#76f3e1',.03,.4,.65)
    def ell(n,p,r,m=skin):s.ellipsoid(n,p,r,m,segments=12,rings=7)
    def tube(n,p,r,m=skin):s.tube(n,p,r,m,sides=8)
    def eye(y,z,w=.18):
        for side in [-1,1]:ell('luminous_eye',(side*w,y,z),(.06,.065,.04),glow)
    def claws(n,x,y,z):
        for j in [-1,0,1]:tube(n,[(x+j*.07,y,z),(x+j*.08,y-.05,z+.24),(x+j*.07,y-.12,z+.34)],[.04,.035,.008],bone)
    if kind=='leaper':
        ell('chest_core',(0,1.03,0),(.25,.37,.37));ell('pelvis',(0,.68,-.25),(.30,.24,.31),plate)
        tube('neck',[(0,1.2,.16),(0,1.48,.36),(0,1.53,.53)],[.15,.14,.11]);ell('head',(0,1.54,.58),(.19,.19,.28));eye(1.59,.82,.13)
        tube('tail',[(0,.68,-.3),(0,.55,-.8),(0,.28,-1.4),(0,.19,-1.8)],[.14,.12,.08,.02])
        for side in [-1,1]:
            x=side*.29;ell('thigh',(x,.68,-.14),(.20,.28,.30),plate)
            tube('upper_leg',[(x,.68,-.14),(x,.35,-.49),(x,.12,.14)],[.14,.13,.065]);claws('foot',x,.13,.15)
            tube('arm',[(side*.22,1.15,.16),(side*.42,.91,.48),(side*.31,.82,.7)],[.085,.07,.035]);claws('foreclaw',side*.31,.82,.68)
            tube('crown_horn',[(side*.13,1.66,.5),(side*.2,1.91,.43)],[.055,.005],bone)
    elif kind=='stalker':
        for i in range(6):ell('carapace',(0,.62-i*.025,.5-i*.28),(.38-i*.025,.27,.24),plate)
        ell('head',(0,.55,.75),(.28,.17,.29));eye(.61,.97)
        for side in [-1,1]:
            tube('foreclaw',[(side*.28,.55,.5),(side*.58,.31,.79),(side*.55,.13,1.13)],[.14,.16,.08]);s.box('foreclaw_shovel',(side*.55,.12,1.08),(.30,.13,.38),bone,.15)
            for i in range(3):tube('rear_digging_leg',[(side*.29,.51,.2-i*.4),(side*.6,.22,.28-i*.4),(side*.69,.06,.42-i*.4)],[.08,.065,.025])
        for i in range(5):tube('dorsal_spike',[(0,.85,.32-i*.27),(0,1.04,.2-i*.27)],[.085,.005],bone)
    elif kind=='screecher':
        ell('chest_core',(0,.99,0),(.28,.37,.23));ell('throat_resonator',(0,1.34,.27),(.29,.24,.23),glow)
        ell('head',(0,1.66,.25),(.21,.26,.22));ell('jaw',(0,1.45,.47),(.20,.07,.19),bone);eye(1.7,.45,.14)
        for i in range(5):tube('crown_horn',[(0,1.8,.32-i*.10),(0,2.07+math.sin(i*.8)*.11,.35-i*.16)],[.09,.012],plate)
        for side in [-1,1]:
            tube('leg',[(side*.2,.8,0),(side*.28,.4,-.12),(side*.25,.08,.14)],[.1,.09,.04]);claws('foot',side*.25,.08,.13)
            tube('arm',[(side*.27,1.25,0),(side*.55,1.02,.05),(side*.68,.74,.22)],[.09,.07,.025]);claws('foreclaw',side*.68,.76,.2)
            ell('resonator_fin',(side*.29,1.40,.11),(.08,.30,.19),bone)
    elif kind=='artillery':
        ell('chest_core',(0,.75,0),(.48,.36,.63),plate);ell('acid_reservoir',(0,1.07,-.22),(.42,.42,.49));
        for i in range(4):s.torus('reservoir_rib',(0,1.08,-.48+i*.18),.34,.045,plate,segments=12,sides=6)
        tube('cannon',[(0,.92,.27),(0,1.36,.49),(0,1.55,.82)],[.18,.15,.18],bone);ell('acid_mouth',(0,1.55,.95),(.16,.17,.10),dark)
        ell('head',(0,.6,.64),(.23,.17,.25));eye(.65,.85,.15)
        for side in [-1,1]:
            for z in [-.45,.38]:tube('leg',[(side*.35,.7,z),(side*.64,.39,z+.08),(side*.69,.08,z+.19)],[.15,.13,.06]);claws('foot',side*.69,.08,z+.19)
            for i in range(3):ell('acid_gland',(side*.34,1.13,-.53+i*.28),(.13,.12,.13),glow)
    elif kind=='lurker':
        ell('chest_core',(0,1.10,0),(.20,.41,.24));ell('head',(0,1.68,.18),(.14,.24,.20),plate);eye(1.70,.35,.10)
        for side in [-1,1]:
            tube('leg',[(side*.13,.81,0),(side*.29,.42,-.16),(side*.24,.06,.17)],[.065,.055,.03]);claws('foot',side*.24,.06,.15)
            tube('arm',[(side*.20,1.4,0),(side*.48,1.03,.12),(side*.47,.72,.43)],[.07,.06,.025]);tube('foreclaw_blade',[(side*.47,.75,.43),(side*.49,.55,.85)],[.055,.008],bone)
            for i in range(3):tube('phase_fin',[(side*.18,1.2-i*.18,-.12),(side*(.52-i*.06),1.39-i*.14,-.4)],[.075,.009],glow)
        tube('tail',[(0,.9,-.13),(0,.67,-.56),(0,.54,-.86)],[.09,.07,.015])
    elif kind=='warder':
        ell('chest_core',(0,.99,0),(.38,.31,.47));ell('head',(0,1.15,.49),(.25,.15,.23));eye(1.20,.69)
        for side in [-1,1]:
            tube('wing',[(side*.27,1.05,-.05),(side*.75,1.16,-.13),(side*1.24,.95,-.33),(side*1.48,.73,-.5)],[.18,.14,.09,.009],plate)
            for i in range(4):tube('wing_vein',[(side*.4,1.03,-.16),(side*(.65+i*.17),.88,-.50-i*.1)],[.06,.014],glow)
            ell('shield_emitter',(side*.59,1.25,-.08),(.16,.11,.18),glow)
            tube('leg_tendril',[(side*.18,.82,-.12),(side*.33,.51,-.31),(side*.11,.26,-.42)],[.055,.04,.008])
        s.torus('shield_halo',(0,1.40,-.13),.46,.035,glow,axis='y',segments=20,sides=6)
        tube('tail',[(0,.89,-.33),(0,.76,-.84),(0,.6,-1.21)],[.12,.075,.01])
    # Anatomical microdetail belongs to the silhouette, rather than a shared recolor.
    for side in [-1,1]:
        for i in range(5):ell('dermal_scale',(side*.2,.9+i*.06,-.12),(.04,.025,.06),bone)
    s.ground();s.metadata={'kind':kind,'originalGeometry':True,'animatedNamedParts':True}
    return s.save(OUT/('alien_'+kind+'.glb'))
if __name__=='__main__':
    OUT.mkdir(parents=True,exist_ok=True)
    report={k:build(k) for k in ['leaper','stalker','screecher','artillery','lurker','warder']}
    (OUT/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
