#!/usr/bin/env python3
"""Rebuild the original Cats / Mice art pack using Python 3.10+, no dependencies.

Meshes are authored here, not transformed old operators. The shared builder only
provides mesh construction and glTF serialization helpers. Models use closed
bevelled hard surfaces, smooth anatomical surfaces, and semantic animation parts.
Portraits are vector projections of these very same meshes. No external art,
texture service, runtime model generation, or new server is required.
"""
from __future__ import annotations
import argparse, hashlib, importlib.util, json, math, struct
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('mesh_tools',Path(__file__).with_name('build-characters.py'))
geo=importlib.util.module_from_spec(spec);spec.loader.exec_module(geo)
Scene,material,add,sub,mul,cross,unit,dot=geo.Scene,geo.material,geo.add,geo.sub,geo.mul,geo.cross,geo.unit,geo.dot
VERSION='dominion-resistance-2026-10-18.1'

CHARACTERS=[
 dict(id='cat_assault',name='Cat Assault Trooper',faction='cats',role='assault',weapon='whisker',width=1,armor='344957',fur='b7a38b',trim='d9b67c'),
 dict(id='cat_heavy',name='Cat Heavy Enforcer',faction='cats',role='heavy',weapon='thunderpaw',width=1.22,armor='4c555b',fur='96948e',trim='d9a44f'),
 dict(id='cat_scout',name='Cat Scout',faction='cats',role='scout',weapon='purr90',width=.88,armor='355e63',fur='cbb497',trim='8ec8c6'),
 dict(id='cat_marksman',name='Cat Marksman',faction='cats',role='marksman',weapon='squeakshot',width=.98,armor='4d6252',fur='958f8a',trim='b9c39a'),
 dict(id='cat_stalker',name='Cat Night Stalker',faction='cats',role='stalker',weapon='burrower',width=.94,armor='263846',fur='4f505c',trim='79a3b0'),
 dict(id='cat_officer',name='Cat Officer',faction='cats',role='officer',weapon='whisker',width=1.03,armor='263f58',fur='d7ccba',trim='e1ba62'),
 dict(id='mouse_rifleman',name='Mouse Rifleman',faction='mice',role='assault',weapon='whisker',width=.91,armor='60714f',fur='ac9b86',trim='d4d4ad'),
 dict(id='mouse_runner',name='Mouse Runner',faction='mice',role='scout',weapon='purr90',width=.82,armor='3b786e',fur='d2bfaa',trim='b6dfcf'),
 dict(id='mouse_engineer',name='Mouse Engineer',faction='mice',role='engineer',weapon='clawbreaker',width=1.00,armor='a0753c',fur='8d827a',trim='eed29a'),
 dict(id='mouse_sniper',name='Mouse Recon Sniper',faction='mice',role='marksman',weapon='squeakshot',width=.90,armor='586b60',fur='bdbab1',trim='b9d4b2'),
 dict(id='mouse_shieldbearer',name='Mouse Shieldbearer',faction='mice',role='heavy',weapon='clawbreaker',width=1.12,armor='486d68',fur='94877d',trim='b4dfca'),
 dict(id='mouse_commander',name='Mouse Commander',faction='mice',role='officer',weapon='burrower',width=.98,armor='4f6f66',fur='d3c3ac',trim='e5bd75'),
]
WEAPONS=[
 dict(id='whisker',name='Whisker AR',kind='assault',length=1.14,color='687b84',accent='e1b45a'),
 dict(id='clawbreaker',name='Clawbreaker',kind='shotgun',length=1.18,color='5b666b',accent='e39a58'),
 dict(id='purr90',name='Purr-90',kind='smg',length=.76,color='5b7479',accent='87d5ca'),
 dict(id='burrower',name='Burrower',kind='suppressed',length=1.20,color='4e6056',accent='9cc5a2'),
 dict(id='squeakshot',name='Squeakshot',kind='marksman',length=1.46,color='687a62',accent='c7d09e'),
 dict(id='thunderpaw',name='Thunderpaw',kind='heavy',length=1.44,color='687078',accent='e5b057'),
]

def sculpt(s,name,center,sections,mat,sides=16):
    """Continuous shaped elliptical cross sections (Y, width, depth, Z offset)."""
    vertices=[];faces=[]
    for y,x,z,oz in sections:
        for n in range(sides):
            a=math.tau*n/sides;vertices.append((math.cos(a)*x,y,math.sin(a)*z+oz))
    for k in range(len(sections)-1):
        for n in range(sides):
            a=k*sides+n;b=k*sides+(n+1)%sides;faces.extend([(a,b+sides,b),(a,a+sides,b+sides)])
    # Closed caps with their own center vertices, retaining smooth shell normals.
    for k in (0,len(sections)-1):
        y,x,z,oz=sections[k];c=len(vertices);vertices.append((0,y,oz))
        for n in range(sides):
            a=k*sides+n;b=k*sides+(n+1)%sides;faces.append((c,a,b) if k==0 else (c,b,a))
    s.mesh(name,vertices,faces,mat,center,smooth=True)

def ear(s,side,fur,inner,cat):
    if cat:
        x=side*.215
        verts=[(-.105,-.08,.01),(.105,-.08,.01),(side*.065,.265,-.045),(-.08,-.07,-.10),(.08,-.07,-.10),(side*.06,.24,-.11)]
        # The helper orients convex faces around the mesh origin. Put that
        # origin inside this asymmetric ear rather than on its front plane.
        centroid=tuple(sum(v[i] for v in verts)/len(verts) for i in range(3))
        s.facets('ear_'+('L' if side<0 else 'R'),[sub(v,centroid) for v in verts],[[0,1,2],[3,5,4],[0,3,4,1],[1,4,5,2],[2,5,3,0]],fur,add((x,2.32,0),centroid))
        s.mesh('ear_inner_'+('L' if side<0 else 'R'),[(-.065,-.035,.016),(.064,-.035,.016),(side*.05,.195,-.023)],[(0,1,2)],inner,(x,2.32,.007))
    else:
        x=side*.31
        s.sphere('ear_'+('L' if side<0 else 'R'),(x,2.34,-.005),(.205,.225,.075),fur,16,8)
        s.sphere('ear_inner_'+('L' if side<0 else 'R'),(x,2.35,.058),(.153,.174,.025),inner,16,7)
        s.sphere('ear_fold_'+('L' if side<0 else 'R'),(x-side*.08,2.21,.073),(.048,.084,.035),fur,10,5)

def character(q):
    s=Scene(q['name']);w=q['width'];cat=q['faction']=='cats';role=q['role'];heavy=role=='heavy';light=role in ('scout','stalker');hood=role=='marksman'
    armor=material('Ceramic tactical plates',q['armor'],.42,.51);trim=material('Worn plated edges',q['trim'],.65,.38)
    suit=material('Woven undersuit','242e34' if cat else '394438',.02,.92);webbing=material('Canvas webbing','4c5150' if cat else '7a795d',.01,.94)
    steel=material('Gunmetal fasteners','647078',.85,.34);rubber=material('Rubber','161c20',0,.9)
    fur=material('Short fur',q['fur'],0,.98);muzzle=material('Muzzle fur','d0bfa7' if cat else 'c5b6a5',0,1);inner=material('Ear skin','ad7879',0,.96)
    black=material('Nose and pupils','161b20',.02,.45);eye=material('Amber eyes' if cat else 'Dark eyes','e4be6d' if cat else '20292a',.12,.25,.07)
    glow=material('Faction identification','f3ba64' if cat else '76dfbc',.1,.48,.38)
    # Tailored chest, anatomical abdomen, and reinforced combat trousers.
    sculpt(s,'torso',(0,1.45,0),[(-.39,.23*w,.14,0),(-.19,.25*w,.17,0),(.08,.35*w,.20,0),(.29,.38*w,.19,-.01),(.38,.25*w,.13,0)],suit)
    s.loft('carrier',(0,1.56,.025),[(-.23,.27*w,.19,.20),(.17,.34*w,.21,.20),(.25,.28*w,.16,.25)],webbing)
    for side in (-1,1):
        s.loft('chest_plate',(side*.153*w,1.64,.211),[(-.17,.14*w,.035,.24),(.13,.16*w,.042,.28),(.20,.10*w,.035,.30)],armor,rotation=(0,side*-.1,side*-.05))
        s.box('shoulder_strap',(side*.22*w,1.79,.105),(.095,.09,.35),webbing,.20,(-.25,0,0))
        s.box('carrier_buckle',(side*.22*w,1.76,.248),(.073,.07,.018),trim,.16)
        s.box('identification_tab',(side*.185*w,1.72,.259),(.12,.027,.012),glow,.15)
        s.box('rib_armor',(side*.296*w,1.42,.015),(.06,.30,.29),armor,.27,(0,0,side*.05))
        for i in range(2 if light else 3):
            x=side*(.06+i*.085)*w
            s.box('mag_pouch',(x,1.375,.223),(.073,.19,.07),webbing,.25,(-.07,0,0))
            s.box('mag_pouch_flap',(x,1.47,.264),(.075,.035,.022),armor,.18)
        s.box('hip_pouch',(side*.28*w,1.08,.025),(.11,.18,.21),webbing,.25)
        s.box('hip_pouch_clasp',(side*.34*w,1.10,.025),(.022,.04,.06),steel,.15)
    s.box('belt',(0,1.13,0),(.57*w,.075,.34),rubber,.20);s.box('belt_buckle',(0,1.13,.185),(.10,.07,.025),trim,.20)
    s.loft('pelvis',(0,1.02,-.01),[(-.13,.25*w,.15,.16),(.10,.25*w,.17,.18)],suit)
    # Leg geometry is completely separate from torso equipment and grounded.
    for side in (-1,1):
        x=side*.18*w
        sculpt(s,'thigh',(x,.81,-.015),[(-.23,.115*w,.115,0),(-.05,.145*w,.135,0),(.22,.151*w,.145,0)],suit,12)
        s.loft('thigh_plate',(x,.84,.119),[(-.19,.084*w,.027,.25),(.14,.115*w,.034,.25)],armor)
        s.box('thigh_strap',(x,.77,0),(.27*w,.035,.27),webbing,.15)
        s.sphere('knee',(x,.565,.005),(.115*w,.105,.12),rubber,10,5)
        s.loft('knee_guard',(x,.57,.119),[(-.085,.08*w,.025,.35),(.04,.106*w,.045,.30),(.09,.075*w,.025,.32)],armor)
        sculpt(s,'calf',(x,.345,-.015),[(-.15,.080*w,.085,0),(.05,.115*w,.11,0),(.17,.105*w,.105,0)],suit,12)
        s.loft('shin_plate',(x,.34,.084),[(-.15,.068*w,.022,.25),(.15,.084*w,.035,.30)],armor)
        s.box('boot',(x,.125,.04),(.22*w,.21,.34),rubber,.3)
        s.box('boot_sole',(x,.035,.055),(.235*w,.07,.365),rubber,.24)
        s.box('boot_toecap',(x,.12,.194),(.215*w,.13,.07),armor,.25)
        for z in (.015,.065,.115):s.box('boot_lace',(x,.235,z),(.135*w,.012,.013),webbing,.1)
        for y in (.32,.41):s.box('shin_clasp',(x+side*.07*w,y,.117),(.037,.025,.018),trim,.1)
    # Both hands are authored around the actual receiver / foregrip positions.
    # Arm-side tokens are explicit because the support hand crosses the body.
    for side in (-1,1):
        tag='L' if side<0 else 'R';shoulder=(side*.395*w,1.745,0);elbow=(side*.46*w,1.405,.10)
        wrist=(.27,1.38,.63) if side<0 else (.32,1.29,.36)
        s.tube('upper_arm_'+tag,[shoulder,elbow],[.115*w,.095*w],suit,10)
        s.tube('forearm_'+tag,[elbow,wrist],[.092*w,.066*w],suit,10)
        s.loft('shoulder_'+tag,shoulder,[(-.105,.14*w,.14,.27),(.08,.16*w,.15,.28),(.135,.10*w,.11,.30)],armor,rotation=(0,0,side*-.18))
        s.box('shoulder_insignia_'+tag,(side*.47*w,1.76,.142),(.075,.10,.018),glow,.3)
        s.sphere('elbow_'+tag,elbow,(.10,.10,.10),rubber,10,5)
        mid=mul(add(elbow,wrist),.5)
        s.tube('forearm_guard_'+tag,[add(mid,(0,.025,0)),add(wrist,(0,.025,-.03))],[.085*w,.068*w],armor,8)
        s.box('glove_'+tag,wrist,(.135,.13,.13),rubber,.3,(.25,0,0))
        for i in range(3):s.box('glove_knuckle_'+tag,(wrist[0]-.045+i*.041,wrist[1]+.055,wrist[2]+.035),(.033,.032,.045),armor,.25)
        s.tube('glove_thumb_'+tag,[add(wrist,(-.065,0,0)),add(wrist,(-.055,-.04,.07))],[.025,.021],rubber,7)
    # A smaller organic neck leaves a clean jaw silhouette above the blast collar.
    s.cylinder('head_neck',(0,1.915,0),.095,.23,fur,'y',12)
    s.loft('neck_guard',(0,1.89,-.07),[(-.05,.20,.10,.22),(.05,.18,.085,.25)],armor)
    if cat:
        sculpt(s,'head',(0,2.15,0),[(-.23,.102,.10,.045),(-.15,.169,.145,.015),(-.01,.215,.17,0),(.14,.188,.145,-.012),(.23,.113,.09,-.018)],fur,18)
        for side in (-1,1):
            s.sphere('head_cheek',(side*.13,2.08,.116),(.10,.09,.085),fur,12,6)
            s.sphere('snout',(side*.064,2.055,.197),(.079,.065,.091),muzzle,12,6)
            # Sculpted tapered cheek tufts, not a spherical animal mask.
            s.loft('head_cheek_tuft',(side*.18,2.06,.015),[(-.08,.035,.07,.3),(.05,.07,.11,.2)],fur,rotation=(0,0,side*-.8))
            s.sphere('head_eye_socket',(side*.109,2.184,.148),(.075,.049,.045),black,12,6)
            s.sphere('head_eye',(side*.109,2.185,.185),(.050,.029,.019),eye,12,6)
            s.sphere('head_pupil',(side*.109,2.184,.202),(.009,.025,.007),black,8,5)
            s.tube('head_brow',[(side*.06,2.222,.18),(side*.13,2.23,.18),(side*.173,2.214,.153)],[.018,.02,.013],fur,7)
        s.facets('snout_nose',[(-.039,.02,0),(.039,.02,0),(0,-.025,.012),(0,.012,.028)],[[0,1,3],[1,2,3],[2,0,3],[0,2,1]],inner,(0,2.075,.293))
        s.tube('head_mouth',[(0,2.05,.281),(0,2.025,.277),(-.039,2.015,.266)],[.005,.005,.003],black,6)
        s.tube('head_mouth',[(0,2.025,.277),(.039,2.015,.266)],[.005,.003],black,6)
    else:
        sculpt(s,'head',(0,2.145,-.015),[(-.20,.095,.10,.055),(-.13,.16,.135,.025),(.035,.19,.155,0),(.16,.15,.12,-.02),(.21,.08,.07,-.025)],fur,18)
        s.sphere('snout',(0,2.08,.168),(.108,.085,.16),muzzle,16,7)
        s.sphere('snout_nose',(0,2.094,.316),(.045,.034,.030),inner,12,6)
        for side in (-1,1):
            s.sphere('head_eye_socket',(side*.125,2.18,.11),(.057,.066,.049),muzzle,12,6)
            s.sphere('head_eye',(side*.128,2.183,.149),(.038,.046,.027),eye,14,7)
            s.sphere('head_eye_glint',(side*.12,2.195,.173),(.009,.011,.005),trim,8,4)
        s.tube('head_mouth',[(-.04,2.035,.276),(0,2.023,.285),(.04,2.035,.276)],[.004,.005,.004],black,6)
    for side in (-1,1):
        ear(s,side,fur,inner,cat)
        for i in range(3):
            s.tube('head_whisker',[(side*.075,2.06-i*.018,.252),(side*.21,2.075-i*.034,.27),(side*.32,2.10-i*.052,.24)],[.0045,.0032,.001],muzzle,5)
    # Ears protrude naturally through the sides of the helmet; face remains open.
    if not hood:
        s.loft('helmet_crown',(0,2.35,-.06),[(-.065,.175,.12,.35),(.025,.14,.105,.3),(.06,.09,.07,.3)],armor)
        s.box('helmet_brow',(0,2.29,.136),(.31,.038,.045),armor,.30)
        for side in (-1,1):
            s.box('helmet_side_rail',(side*.192,2.23,-.035),(.046,.095,.19),armor,.30)
            s.cylinder('helmet_headset',(side*.208,2.13,-.07),.07,.043,rubber,'x',12)
        s.tube('helmet_microphone',[(.21,2.12,-.06),(.235,2.05,.07),(.16,2.02,.20)],[.012,.009,.008],steel,7)
    else:
        # Segmented cloth hood wraps the back of the head and leaves animal face visible.
        s.loft('helmet_hood_back',(0,2.15,-.13),[(-.25,.19,.07,.16),(.13,.235,.095,.2),(.22,.15,.06,.2)],webbing)
        for side in (-1,1):s.tube('helmet_hood_rim',[(side*.18,1.96,.03),(side*.225,2.15,.025),(side*.18,2.36,.015)],[.032,.035,.026],webbing,8)
        s.loft('scarf',(0,1.87,.04),[(-.06,.19,.12,.25),(.06,.17,.115,.28)],webbing)
    # Curved tails with multiple genuine mesh sections; tails are cosmetic.
    if cat:
        points=[(0,1.06,-.15),(0,.99,-.32),(.06,.84,-.47),(.18,.74,-.53),(.31,.77,-.50),(.39,.89,-.45)]
        s.tube('tail',points,[.069,.069,.063,.055,.043,.017],fur,10)
        s.tube('tail_tip',points[-2:],[.044,.017],muzzle,10)
    else:
        points=[(0,1.04,-.14),(0,.89,-.33),(-.09,.69,-.46),(-.22,.54,-.52),(-.37,.52,-.47),(-.48,.61,-.38),(-.49,.72,-.31)]
        s.tube('tail',points,[.036,.034,.029,.024,.019,.013,.004],inner,9)
        for i in range(1,4):s.tube('tail_ring',[points[i],add(points[i],(.001,.008,0))],[.036-i*.004,.036-i*.004],muzzle,8)
    # Role-specific silhouettes and equipment, beyond palette swaps.
    pack_depth=.21 if heavy else .13
    s.box('backpack',(0,1.54,-.24),(.41*w,.40,pack_depth),webbing,.30)
    s.box('backpack_spine',(0,1.54,-.24-pack_depth/2),(.11,.32,.035),armor,.22)
    if role=='assault':
        for side in (-1,1):
            s.cylinder('grenade',(side*.285,1.40,.223),.041,.14,armor,'y',10)
            s.box('grenade_clip',(side*.285,1.48,.23),(.032,.04,.035),steel,.15)
        s.box('radio',( -.27,1.67,-.17),(.11,.20,.09),rubber,.20)
        s.tube('antenna',[(-.27,1.76,-.17),(-.29,2.02,-.19)],[.01,.005],steel,6)
    elif heavy:
        for side in (-1,1):
            s.loft('shoulder_extra_'+('L' if side<0 else 'R'),(side*.45*w,1.82,0),[(-.11,.17,.18,.25),(.08,.20,.17,.25),(.15,.12,.11,.25)],armor)
            s.loft('thigh_blast',(side*.19*w,.97,.17),[(-.15,.135,.045,.22),(.16,.13,.05,.28)],armor)
            s.cylinder('backpack_canister',(side*.14,1.56,-.39),.085,.39,steel,'y',10)
        if cat:
            for i in range(8):s.cylinder('ammo_bandolier',(-.26+i*.072,1.71-i*.038,.279),.025,.11,trim,'y',8)
            s.box('neck_guard_blast',(0,1.91,.18),(.30,.14,.06),armor,.28)
        else:
            s.loft('backpack_folded_shield',(-.15,1.46,-.48),[(-.49,.22,.045,.24),(.39,.27,.05,.25),(.48,.16,.045,.20)],armor,rotation=(0,0,.12))
            s.box('shield_marking',(-.15,1.48,-.54),(.05,.45,.015),glow,.1)
    elif role=='scout':
        for side in (-1,1):s.cylinder('backpack_water_tube',(side*.11,1.55,-.31),.054,.34,rubber,'y',10)
        s.tube('hydration_hose',[(-.13,1.75,-.31),(-.22,1.82,.02),(-.24,1.61,.23),(-.17,1.53,.24)],[.016]*4,rubber,7)
        s.box('thigh_utility_knife',(-.22,.81,-.14),(.075,.26,.06),armor,.15)
        s.box('head_goggle_strap',(0,2.305,.163),(.27,.065,.039),rubber,.15)
        for side in (-1,1):s.cylinder('head_goggle_lens',(side*.075,2.305,.19),.046,.022,steel,'z',10)
    elif role=='marksman':
        s.loft('backpack_cloak',(0,1.55,-.32),[(-.53,.32,.035,.20),(.14,.36,.045,.22),(.28,.21,.03,.20)],webbing)
        for i in range(6):s.box('cloak_camo_strip',(-.27+i*.108,1.55,-.37),(.075,.48+(i%2)*.15,.025),armor,.05,(0,0,(i-2.5)*.06))
        s.box('rangefinder',( .27,1.65,-.25),(.12,.15,.16),armor,.25)
        s.cylinder('rangefinder_lens',(.27,1.65,-.35),.04,.035,black,'z',10)
    elif role=='stalker':
        for side in (-1,1):
            s.cylinder('head_recon_optic',(side*.07,2.31,.22),.035,.13,rubber,'z',10)
            s.cylinder('head_recon_lens',(side*.07,2.31,.29),.025,.01,glow,'z',10)
        s.box('backpack_sensor',(0,1.79,-.32),(.27,.10,.09),armor,.20)
        s.cylinder('signal_suppressor',(.29,1.32,-.12),.04,.20,rubber,'y',9)
    elif role=='officer':
        s.box('helmet_officer_peak',(0,2.30,.19),(.34,.035,.13),armor,.20)
        s.box('helmet_rank',(0,2.382,.055),(.065,.065,.015),trim,.2)
        for side in (-1,1):
            s.box('shoulder_epaulette_'+('L' if side<0 else 'R'),(side*.395*w,1.89,.02),(.18,.035,.16),trim,.2)
            s.loft('uniform_coattail',(side*.13,1.05,-.16),[(-.31,.12,.035,.18),(.15,.14,.04,.2)],armor,rotation=(-.12,0,side*.07))
        s.tube('command_cord',[(-.23,1.78,.25),(-.30,1.58,.25),(-.17,1.49,.28),(-.08,1.64,.28)],[.012]*4,trim,7)
        for i in range(3):s.box('rank_bars',(.17,1.69-i*.042,.264),(.085,.019,.014),trim,.1)
        s.box('command_tablet',(-.32,1.17,.13),(.09,.21,.17),armor,.20)
    elif role=='engineer':
        s.box('toolpack',(0,1.55,-.39),(.37,.29,.12),armor,.25)
        for x in (-.10,0,.10):
            s.cylinder('tool_handle',(x,1.84,-.4),.024,.27,rubber,'y',8)
            s.box('tool_wrench_head',(x,2.0,-.4),(.074,.09,.03),steel,.18)
        for side in (-1,1):s.box('charge_pack',(side*.31,1.11,.17),(.09,.16,.13),armor,.2)
        s.box('helmet_work_lamp',(0,2.36,.10),(.13,.09,.12),rubber,.2)
        s.cylinder('helmet_lamp',(0,2.36,.167),.035,.02,glow,'z',12)
        s.tube('tool_wire',[(0,1.7,-.45),(.20,1.48,-.46),(.20,1.21,-.3)],[.012]*3,inner,6)
    return s

def weapon(q):
    s=Scene(q['name']);kind=q['kind'];smg=kind=='smg';heavy=kind=='heavy';shot=kind=='shotgun';marks=kind=='marksman';supp=kind=='suppressed'
    shell=material('Cerakote receiver',q['color'],.65,.43);metal=material('Machined gunmetal','303d46',.85,.28)
    trim=material('Wear edges','8e9ba0',.8,.38);rubber=material('Polymer grip','172329',.05,.84);accent=material('Weapon identification',q['accent'],.3,.52,.10)
    bore=material('Black bore','05090b',.05,.96);brass=material('Cartridge brass','c1a160',.75,.30);optic=material('Coated optic','579ea2',.65,.20,.15)
    # +Z is forward, receiver origin matches both first- and third-person code.
    front=.30 if smg else .46 if shot else .42
    length=q['length'];muzzle_z=length-.42
    s.box('receiver',(0,.02,0),(.135 if not heavy else .18,.16,.48),shell,.25)
    s.loft('receiver_lower',(0,-.095,-.04),[(-.045,.058,.18,.25),(.05,.067,.20,.2)],metal)
    s.box('receiver_top_cover',(0,.111,.01),(.115,.035,.40),metal,.22)
    s.box('receiver_side_plate',(-.074,.025,-.03),(.012,.092,.27),shell,.1)
    s.box('ejection_port',(.077,.036,.09),(.016,.064,.105),bore,.1)
    s.box('bolt_carrier',(.086,.038,.085),(.015,.040,.07),trim,.12)
    s.cylinder('bolt_handle',(.107,.045,.095),.012,.065,metal,'x',8)
    s.box('selector',(-.085,-.012,-.15),(.018,.025,.05),accent,.1,(0,0,.3))
    for z in (-.18,-.10,.17):
        for side in (-1,1):s.cylinder('receiver_pin',(side*.078,.035,z),.010,.008,trim,'x',8)
    # An open trigger guard built as a loop, not a solid block.
    s.tube('trigger_guard',[(-.035,-.105,-.08),(-.035,-.178,-.08),(-.035,-.185,.055),(-.035,-.10,.055)],[.009]*4,metal,7)
    s.tube('trigger',[(0,-.10,-.005),(0,-.148,-.018),(0,-.156,-.035)],[.006]*3,trim,7)
    s.loft('pistol_grip',(0,-.205,-.14),[(-.105,.043,.055,.25),(.075,.048,.066,.20)],rubber,rotation=(-.25,0,0))
    for y in (-.28,-.25,-.22,-.19):s.box('grip_texture',(0,y,-.187),(.080,.009,.008),metal,.1)
    stock_z=-.37 if smg else -.49
    s.box('stock_beam',(0,.01,-.34),(.062,.06,.28),metal,.18)
    s.loft('stock',(0,-.01,stock_z),[(-.12,.052,.085,.20),(.09,.06,.10,.30)],shell,rotation=(.10,0,0))
    s.box('stock_pad',(0,-.02,stock_z-.09),(.12,.20,.035),rubber,.25)
    s.box('cheek_rest',(0,.11,stock_z+.04),(.11,.035,.18),rubber,.2)
    # Genuine hollow muzzle with rings, exterior barrel, gas tube, rail teeth.
    s.cylinder('barrel',(0,.025,(front+muzzle_z)/2),.029 if not heavy else .038,max(.10,muzzle_z-front+.08),metal,'z',12)
    s.cylinder('gas_tube',(0,.079,front+.10),.012,.27,metal,'z',9)
    if not smg:
        s.box('handguard',(0,.015,.34),(.145,.13,.31),shell,.25)
        for z in (.22,.28,.34,.40,.46):
            for side in (-1,1):s.box('handguard_vent',(side*.075,.038,z),(.009,.045,.032),bore,.10)
            s.box('rail_tooth',(0,.102,z),(.105,.023,.022),metal,.12)
        s.box('foregrip',(0,-.13,.35),(.065,.16,.075),rubber,.25,(-.10,0,0))
    else:
        s.loft('bullpup_shell',(0,-.025,.12),[(-.10,.082,.22,.25),(.105,.09,.24,.28)],shell)
        s.box('magazine_top',(0,.15,-.03),(.115,.045,.47),rubber,.25)
        for z in (-.18,-.11,-.04,.03,.10,.17):s.box('magazine_round_window',(0,.175,z),(.076,.006,.034),brass,.1)
        s.box('foregrip',(0,-.175,.23),(.075,.15,.08),rubber,.24,(-.18,0,0))
    for z in (-.18,-.13,-.08,-.03,.02,.07,.12,.17):s.box('rail_tooth',(0,.145,z),(.095,.016,.019),metal,.1)
    if supp:
        s.cylinder('suppressor',(0,.025,muzzle_z-.06),.059,.30,metal,'z',16)
        for z in (muzzle_z-.17,muzzle_z-.12,muzzle_z-.07,muzzle_z-.02):s.cylinder('suppressor_band',(0,.025,z),.061,.008,shell,'z',16)
        muzzle_z+=.095
    else:
        s.cylinder('muzzle_brake',(0,.025,muzzle_z),.045,.10,metal,'z',12)
        for side in (-1,1):s.box('muzzle_brake_vent',(side*.042,.025,muzzle_z),(.009,.035,.04),bore,.1)
        muzzle_z+=.051
    s.cylinder('muzzle_bore',(0,.025,muzzle_z),.025,.002,bore,'z',14)
    s.cylinder('muzzle_rim',(0,.025,muzzle_z-.005),.034,.004,trim,'z',14)
    s.cylinder('muzzle_bore',(0,.025,muzzle_z+.001),.024,.002,bore,'z',14)
    if heavy:
        s.box('magazine_box',(-.035,-.21,.02),(.25,.25,.23),shell,.25)
        s.box('magazine_latch',(-.166,-.18,.02),(.015,.06,.06),accent,.1)
        for i in range(5):s.cylinder('ammo_belt',(-.14-i*.035,.025,.025+i*.01),.014,.09,brass,'z',8)
        s.tube('carry_handle',[(-.04,.14,-.12),(-.04,.25,-.12),(-.04,.25,.11),(-.04,.14,.11)],[.016]*4,rubber,8)
        for side in (-1,1):
            s.tube('bipod',[(side*.02,-.02,.54),(side*.11,-.18,.64),(side*.12,-.19,.70)],[.012,.009,.009],metal,8)
        for z in (.53,.58,.63,.68,.73):s.cylinder('barrel_cooling_ring',(0,.025,z),.044,.013,metal,'z',12)
    elif shot:
        s.cylinder('magazine_tube',(0,-.057,.44),.030,.58,metal,'z',12)
        s.box('pump',(0,-.038,.43),(.12,.10,.23),rubber,.28)
        for z in (.34,.38,.42,.46,.50):s.box('pump_rib',(0,-.045,z),(.13,.098,.010),metal,.1)
        for i in range(5):
            s.cylinder('shell_side_saddle',(-.10,.035,-.16+i*.055),.019,.10,accent,'y',8)
            s.cylinder('shell_rim',(-.10,.091,-.16+i*.055),.020,.014,brass,'y',8)
    elif not smg:
        # Curved magazine shell with feed lips and detachable base plate.
        s.loft('magazine',(0,-.22,.10),[(-.15,.047,.067,.22),(-.04,.050,.079,.20),(.12,.052,.083,.20)],rubber,rotation=(-.14,0,0))
        s.box('magazine_base',(0,-.373,.077),(.115,.027,.145),metal,.16,(-.14,0,0))
        for side in (-1,1):
            for z in (.06,.10,.14):s.box('magazine_rib',(side*.053,-.23,z),(.009,.20,.012),shell,.1,(-.14,0,0))
    if marks:
        for z in (-.13,.11):s.box('scope_mount',(0,.18,z),(.06,.085,.06),metal,.15)
        s.cylinder('scope',(0,.235,-.015),.047,.40,metal,'z',14)
        for z in (-.21,.18):s.cylinder('scope_bell',(0,.235,z),.067,.08,shell,'z',16)
        s.cylinder('scope_front_glass',(0,.235,.223),.052,.003,optic,'z',16)
        s.cylinder('scope_rear_glass',(0,.235,-.253),.051,.003,optic,'z',16)
        s.cylinder('scope_turret',(0,.294,-.01),.028,.05,metal,'y',10)
        s.cylinder('scope_windage',(.067,.235,-.01),.023,.045,metal,'x',10)
        for side in (-1,1):s.tube('bipod',[(side*.025,-.02,.49),(side*.075,-.15,.62)],[.011,.008],metal,8)
    else:
        s.box('sight_base',(0,.166,-.02),(.08,.03,.10),metal,.16)
        s.box('sight_left',(-.045,.215,-.02),(.012,.078,.065),metal,.15)
        s.box('sight_right',(.045,.215,-.02),(.012,.078,.065),metal,.15)
        s.box('sight_top',(0,.253,-.02),(.10,.012,.065),metal,.15)
        s.cylinder('sight_dot',(0,.21,.017),.004,.003,accent,'z',6)
        s.box('front_sight',(0,.14,.43 if not smg else .30),(.012,.055,.024),metal,.1)
    s.box('weapon_marking',(-.084,.05,-.08),(.008,.020,.080),accent,.1)
    return s

def write_glb(s,path,meta):
    data=bytearray();views=[];accessors=[];meshes=[];nodes=[];low=[math.inf]*3;high=[-math.inf]*3
    def acc(values,kind,target):
        integer=kind=='SCALAR';flat=values if integer else [x for p in values for x in p];offset=len(data)
        blob=struct.pack('<'+('H' if integer else 'f')*len(flat),*flat);data.extend(blob);data.extend(b'\0'*((-len(data))%4))
        views.append(dict(buffer=0,byteOffset=offset,byteLength=len(blob),target=target))
        a=dict(bufferView=len(views)-1,componentType=5123 if integer else 5126,count=len(values),type=kind)
        if not integer:a.update(min=[min(p[i] for p in values) for i in range(3)],max=[max(p[i] for p in values) for i in range(3)])
        accessors.append(a);return len(accessors)-1
    for p in s.parts:
        pa=acc(p['v'],'VEC3',34962);na=acc(p['n'],'VEC3',34962);ia=acc([i for f in p['f'] for i in f],'SCALAR',34963)
        meshes.append(dict(name=p['name'],primitives=[dict(attributes=dict(POSITION=pa,NORMAL=na),indices=ia,material=p['mat'],mode=4)]))
        nodes.append(dict(name=p['name'],mesh=len(meshes)-1,translation=list(p['center'])))
        for v in p['v']:
            for i in range(3):low[i]=min(low[i],v[i]+p['center'][i]);high[i]=max(high[i],v[i]+p['center'][i])
    j=dict(asset=dict(version='2.0',generator='Neon Breach original tactical art '+VERSION,extras=dict(neonBreach=meta['rig'])),scene=0,scenes=[dict(name=s.title,nodes=list(range(len(nodes))))],nodes=nodes,meshes=meshes,materials=s.materials,buffers=[dict(byteLength=len(data))],bufferViews=views,accessors=accessors,extras=meta)
    js=json.dumps(j,separators=(',',':')).encode();js+=b' '*((-len(js))%4);length=12+8+len(js)+8+len(data)
    blob=struct.pack('<III',0x46546c67,2,length)+struct.pack('<II',len(js),0x4e4f534a)+js+struct.pack('<II',len(data),0x004e4942)+data
    path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(blob)
    return dict(file=str(path.relative_to(OUT)).replace('\\','/'),bytes=len(blob),sha256=hashlib.sha256(blob).hexdigest(),triangles=sum(len(p['f']) for p in s.parts),nodes=len(nodes),materials=len(s.materials),bounds=dict(min=low,max=high),**meta)

def portrait(s,path,q,is_weapon=False):
    """Offline orthographic vector render of authored triangles, with depth sorting."""
    yaw=-.44 if not is_weapon else -1.10;pitch=.07 if not is_weapon else .18
    def view(p):return geo.rotated(p,(pitch,yaw,0))
    faces=[];light=unit((-.5,.8,.65))
    for part in s.parts:
        pts=[view(add(p,part['center'])) for p in part['v']];mat=s.materials[part['mat']];base=mat['pbrMetallicRoughness']['baseColorFactor']
        for ids in part['f']:
            t=[pts[i] for i in ids];n=cross(sub(t[1],t[0]),sub(t[2],t[0]));ln=math.sqrt(dot(n,n))
            if ln<1e-9:continue
            n=mul(n,1/ln)
            # Cull back faces in the technical portrait; all anatomy is closed
            # geometry and ear insets have explicitly front-facing triangles.
            if n[2]<=0:continue
            lum=.45+.55*max(0,dot(n,light));rgb=[min(255,int((base[i]*lum+mat['emissiveFactor'][i]*.25)*255)) for i in range(3)]
            faces.append((sum(p[2] for p in t)/3,t,'#'+''.join(f'{c:02x}' for c in rgb)))
    width,height=(640,360) if is_weapon else (400,520)
    allp=[p for f in faces for p in f[1]];minx=min(p[0] for p in allp);maxx=max(p[0] for p in allp);miny=min(p[1] for p in allp);maxy=max(p[1] for p in allp)
    scale=min((width-72)/(maxx-minx),(height-95)/(maxy-miny));cx=(minx+maxx)/2;cy=(miny+maxy)/2
    accent='#'+q.get('accent',q.get('trim','9bdabc'));faction=q.get('faction','FIELD ARMORY').upper()
    svg=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-label="{q["name"]} actual model preview"><defs><linearGradient id="b" x2="0.7" y2="1"><stop stop-color="#20303b"/><stop offset="1" stop-color="#09121c"/></linearGradient></defs><rect width="100%" height="100%" fill="url(#b)"/><path d="M20 80H{width-20}M20 {height-70}H{width-20}M45 20V{height-20}M{width-45} 20V{height-20}" stroke="{accent}" opacity=".14"/><circle cx="{width*.50}" cy="{height*.47}" r="{width*.35}" fill="none" stroke="{accent}" opacity=".15"/><ellipse cx="{width/2}" cy="{height-65}" rx="{width*.25}" ry="10" fill="#02080d" opacity=".75"/>']
    for depth,t,color in sorted(faces,key=lambda a:a[0]):
        points=' '.join(f'{width/2+(p[0]-cx)*scale:.2f},{height/2-10-(p[1]-cy)*scale:.2f}' for p in t)
        svg.append(f'<polygon points="{points}" fill="{color}" stroke="{color}" stroke-width=".25"/>')
    svg.append(f'<rect x="20" y="20" width="4" height="16" fill="{accent}"/><text x="32" y="33" fill="{accent}" font-family="Arial,sans-serif" font-size="11" letter-spacing="2">{faction}</text><rect y="{height-48}" width="100%" height="48" fill="#08121c" opacity=".96"/><text x="20" y="{height-21}" fill="#e9f2ef" font-family="Arial,sans-serif" font-size="15" font-weight="bold">{q["name"].upper()}</text></svg>')
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(''.join(svg),encoding='utf-8')

def icon(faction):
    cat=faction=='cats';color='#e8b66c' if cat else '#82dfbd'
    head='<path d="M25 54 23 25 43 39Q50 35 57 39L77 25 75 54Q74 77 50 85 26 77 25 54Z"/>' if cat else '<circle cx="26" cy="34" r="16"/><circle cx="74" cy="34" r="16"/><path d="M24 52Q25 29 50 36 75 29 76 52 74 74 50 87 26 74 24 52Z"/>'
    return f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><path d="M50 5 91 22V63L50 96 9 63V22Z" fill="#10232c" stroke="{color}" stroke-width="2"/><g fill="{color}">{head}</g><path d="M33 52 44 57M67 52 56 57" stroke="#10232c" stroke-width="5"/><path d="m44 66 6 6 6-6Z" fill="#10232c"/><path d="M14 63 38 65M16 71 38 69M62 65 86 63M62 69 84 71" stroke="{color}" stroke-width="2"/></svg>'

def art(out):
    for faction in ('cats','mice'):
        p=out/'factions'/faction/'icon.svg';p.write_text(icon(faction),encoding='utf-8')
    p=out/'campaign/ui';p.mkdir(parents=True,exist_ok=True)
    (p/'faction-war.svg').write_text('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 420"><defs><linearGradient id="b"><stop stop-color="#302d29"/><stop offset=".5" stop-color="#111e27"/><stop offset="1" stop-color="#1b3c36"/></linearGradient></defs><path fill="url(#b)" d="M0 0H1200V420H0Z"/><g stroke="#7ea095" opacity=".15" fill="none"><path d="M0 60H1200M0 120H1200M0 180H1200M0 240H1200M0 300H1200M0 360H1200M120 0V420M240 0V420M360 0V420M480 0V420M600 0V420M720 0V420M840 0V420M960 0V420M1080 0V420"/><circle cx="600" cy="210" r="155"/><circle cx="600" cy="210" r="130"/></g><path d="M600 20 555 125 625 190 574 286 605 400" stroke="#a2d6c4" stroke-width="2" opacity=".6"/><text x="60" y="85" fill="#e6b56a" font-family="Arial,sans-serif" font-size="15" letter-spacing="4">CAT DOMINION</text><text x="1140" y="85" text-anchor="end" fill="#81dfbd" font-family="Arial,sans-serif" font-size="15" letter-spacing="4">MOUSE RESISTANCE</text><text x="600" y="207" text-anchor="middle" fill="#eef5f0" font-family="Arial,sans-serif" font-weight="bold" font-size="64" letter-spacing="5">NEON BREACH</text><text x="600" y="253" text-anchor="middle" fill="#a0beb9" font-family="Arial,sans-serif" font-size="18" letter-spacing="8">THE TERRITORY WAR</text><path d="M60 335H420M780 335H1140" stroke="#a3c2b6" opacity=".4"/><text x="60" y="368" fill="#bec8c0" font-family="Arial,sans-serif" font-size="13" letter-spacing="2">DISCIPLINE · ARMOR · DOMINION</text><text x="1140" y="368" text-anchor="end" fill="#bec8c0" font-family="Arial,sans-serif" font-size="13" letter-spacing="2">INGENUITY · MOBILITY · RESISTANCE</text></svg>',encoding='utf-8')
    p=out/'conquest/objectives';p.mkdir(parents=True,exist_ok=True)
    for label in 'ABCDE':
        (p/f'sector-{label.lower()}.svg').write_text(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><path d="m50 5 42 45-42 45L8 50Z" fill="#10212a" stroke="#9ccaba" stroke-width="3"/><path d="M20 50H80M50 20V80" stroke="#7eb6a3" opacity=".2"/><text x="50" y="64" text-anchor="middle" font-family="Arial,sans-serif" font-size="42" fill="#e8f6ed">{label}</text></svg>',encoding='utf-8')

def main():
    global OUT
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,default=ROOT/'assets');args=parser.parse_args();OUT=args.output.resolve();models=[]
    for q in CHARACTERS:
        s=character(q);rig=dict(bodyHeight=2.39,hip=[.18*q['width'],1.02,0],shoulder=[.395*q['width'],1.745,0],headPivot=[0,1.94,0],aimHead=[0,2.15,.12],aimBody=[0,1.54,.025],tailPivot=[0,1.04,-.15],earPivot=[.215 if q['faction']=='cats' else .31,2.32,0],holdPose=True,weaponGrip=[.32,1.43,.43],cosmeticExtremities=True)
        entry=write_glb(s,OUT/'factions'/q['faction']/(q['id']+'.glb'),dict(id=q['id'],name=q['name'],faction=q['faction'],role=q['role'],rig=rig,buildVersion=VERSION))
        # Portraits include the selected equipment using the same world grip as
        # the arena. Guns stay separate from the character GLB for loadout swaps.
        held=weapon(next(g for g in WEAPONS if g['id']==q['weapon']))
        for part in held.parts:
            mat=held.materials[part['mat']]
            if mat not in s.materials:s.materials.append(mat)
            s.parts.append({**part,'v':[mul(v,.7) for v in part['v']],'center':add(mul(part['center'],.7),(.32,1.43,.43)),'mat':s.materials.index(mat)})
        preview=OUT/'factions'/q['faction']/(q['id']+'.svg');portrait(s,preview,q);entry['portrait']=str(preview.relative_to(OUT)).replace('\\','/');models.append(entry)
        print(f'{q["id"]}: {entry["triangles"]:,} triangles; {entry["bytes"]:,} bytes')
    for q in WEAPONS:
        s=weapon(q);entry=write_glb(s,OUT/'weapons/new'/(q['id']+'.glb'),dict(id=q['id'],name=q['name'],rig=dict(mechanicalParts=True),buildVersion=VERSION))
        preview=OUT/'weapons/new'/(q['id']+'.svg');portrait(s,preview,q,True);entry['portrait']=str(preview.relative_to(OUT)).replace('\\','/');models.append(entry)
        print(f'{q["id"]}: {entry["triangles"]:,} triangles; {entry["bytes"]:,} bytes')
    art(OUT)
    manifest=dict(schemaVersion=1,version=VERSION,source='tools/build-factions.py',format='glTF 2.0 embedded indexed triangles, authored normals, opaque materials',license='Original project meshes and vector artwork; no third-party source assets',models=models)
    (OUT/'factions/manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')

if __name__=='__main__':main()
