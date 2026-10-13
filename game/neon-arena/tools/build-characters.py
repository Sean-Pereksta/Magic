#!/usr/bin/env python3
"""Build the original Neon Breach Vanguard character collection.

Run with Python 3.10+: python tools/build-characters.py
No third-party libraries, remote assets, textures, skins, or compression are used.
The GLBs contain opaque PBR materials, indexed triangles, and vertex normals.
X points right, Y points up, +Z faces forward; feet touch Y=0. Character parts
have semantic names so NeonModels can animate them as rigid armored limbs.
"""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
from pathlib import Path
import struct

ROOT = Path(__file__).resolve().parents[1]
VERSION = "vanguard-2026-10-13.1"


def add(a, b): return tuple(x + y for x, y in zip(a, b))
def sub(a, b): return tuple(x - y for x, y in zip(a, b))
def mul(a, s): return tuple(x * s for x in a)
def dot(a, b): return sum(x * y for x, y in zip(a, b))
def cross(a, b): return (a[1]*b[2]-a[2]*b[1], a[2]*b[0]-a[0]*b[2], a[0]*b[1]-a[1]*b[0])
def unit(a):
    length = math.sqrt(dot(a, a))
    if length < 1e-12: raise ValueError("Degenerate normal")
    return mul(a, 1/length)


def rotated(p, angles):
    x, y, z = p
    for axis, a in enumerate(angles):
        c, s = math.cos(a), math.sin(a)
        if axis == 0: y, z = y*c-z*s, y*s+z*c
        elif axis == 1: x, z = x*c+z*s, -x*s+z*c
        else: x, y = x*c-y*s, x*s+y*c
    return (x, y, z)


def material(name, color, metal=.5, rough=.43, glow=0):
    rgb = [int(color.lstrip("#")[i:i+2], 16)/255 for i in (0, 2, 4)]
    return {"name":name, "pbrMetallicRoughness":{"baseColorFactor":rgb+[1], "metallicFactor":metal, "roughnessFactor":rough}, "emissiveFactor":[x*glow for x in rgb], "alphaMode":"OPAQUE"}


class Scene:
    def __init__(self, title): self.title, self.parts, self.materials = title, [], []

    def mesh(self, name, vertices, faces, mat, center=(0,0,0), rotation=(0,0,0), normals=None, smooth=False):
        vertices = [rotated(p, rotation) for p in vertices]
        if normals is not None: normals = [unit(rotated(n, rotation)) for n in normals]
        if normals is None and smooth:
            normals = [(0., 0., 0.) for _ in vertices]
            for a,b,c in faces:
                n = cross(sub(vertices[b], vertices[a]), sub(vertices[c], vertices[a]))
                for k in (a,b,c): normals[k] = add(normals[k], n)
            normals = [unit(n) for n in normals]
        if normals is None:
            vv, nn, ff = [], [], []
            for face in faces:
                n = unit(cross(sub(vertices[face[1]], vertices[face[0]]), sub(vertices[face[2]], vertices[face[0]])))
                ff.append(tuple(range(len(vv), len(vv)+3)))
                vv.extend(vertices[i] for i in face); nn.extend([n]*3)
            vertices, normals, faces = vv, nn, ff
        if mat not in self.materials: self.materials.append(mat)
        self.parts.append({"name":name+"_"+str(len(self.parts)+1), "v":vertices, "n":normals, "f":faces, "center":center, "mat":self.materials.index(mat)})

    def facets(self, name, vertices, polygons, mat, center=(0,0,0), rotation=(0,0,0)):
        faces = []
        for poly in polygons:
            c = mul(tuple(sum(vertices[k][j] for k in poly) for j in range(3)), 1/len(poly))
            n = cross(sub(vertices[poly[1]],vertices[poly[0]]), sub(vertices[poly[2]],vertices[poly[0]]))
            if dot(n,c)<0: poly=list(reversed(poly))
            faces.extend((poly[0],poly[i],poly[i+1]) for i in range(1,len(poly)-1))
        self.mesh(name, vertices, faces, mat, center, rotation)

    def box(self, name, center, size, mat, bevel=.12, rotation=(0,0,0)):
        h = tuple(x/2 for x in size); b = min(h)*min(.8,max(0.001,bevel))
        points, lookup, polygons = [], {}, []
        signs=list(itertools.product((-1,1),repeat=3))
        for sign in signs:
            for axis in range(3):
                lookup[(sign,axis)] = len(points)
                points.append(tuple(sign[j]*(h[j] if j==axis else h[j]-b) for j in range(3)))
        for axis in range(3):
            others=[j for j in range(3) if j!=axis]
            for side in (-1,1):
                p=[]
                for a,c in ((-1,-1),(1,-1),(1,1),(-1,1)):
                    s=[0,0,0];s[axis]=side;s[others[0]]=a;s[others[1]]=c;p.append(lookup[(tuple(s),axis)])
                polygons.append(p)
        for a,baxis in ((0,1),(1,2),(0,2)):
            free=3-a-baxis
            for sa,sb in itertools.product((-1,1),repeat=2):
                s=[0,0,0];s[a]=sa;s[baxis]=sb;s[free]=-1;t=s.copy();t[free]=1
                polygons.append([lookup[(tuple(s),a)],lookup[(tuple(s),baxis)],lookup[(tuple(t),baxis)],lookup[(tuple(t),a)]])
        for sign in signs: polygons.append([lookup[(sign,j)] for j in range(3)])
        self.facets(name, points, polygons, mat, center, rotation)

    def loft(self, name, center, sections, mat, rotation=(0,0,0)):
        """Faceted armor shells: (height, half-width, half-depth, bevel-ratio)."""
        vertices=[];polygons=[]
        for y,x,z,b in sections:
            vertices.extend([(-x+b*x,y,-z),(x-b*x,y,-z),(x,y,-z+b*z),(x,y,z-b*z),(x-b*x,y,z),(-x+b*x,y,z),(-x,y,z-b*z),(-x,y,-z+b*z)])
        for ring in range(len(sections)-1):
            for i in range(8):
                a=ring*8+i;b=ring*8+(i+1)%8;polygons.append([a,b,b+8,a+8])
        polygons.extend([list(range(7,-1,-1)),list(range((len(sections)-1)*8,len(sections)*8))])
        self.facets(name, vertices, polygons, mat, center, rotation)

    def sphere(self, name, center, radii, mat, segments=12, rings=6):
        vertices=[(0,radii[1],0)];faces=[]
        for j in range(1,rings):
            a=math.pi*j/rings
            for i in range(segments):
                b=math.tau*i/segments;vertices.append((radii[0]*math.sin(a)*math.cos(b),radii[1]*math.cos(a),radii[2]*math.sin(a)*math.sin(b)))
        bottom=len(vertices);vertices.append((0,-radii[1],0))
        for i in range(segments): faces.append((0,1+(i+1)%segments,1+i))
        for j in range(rings-2):
            for i in range(segments):
                a=1+j*segments+i;b=1+j*segments+(i+1)%segments;faces.extend([(a,b,b+segments),(a,b+segments,a+segments)])
        for i in range(segments): faces.append((bottom,1+(rings-2)*segments+i,1+(rings-2)*segments+(i+1)%segments))
        normals=[unit(tuple(p[j]/(radii[j]*radii[j]) for j in range(3))) for p in vertices]
        self.mesh(name,vertices,faces,mat,center,normals=normals)

    def tube(self, name, points, radii, mat, sides=10):
        vertices=[];faces=[];previous=None
        for i,p in enumerate(points):
            tangent=unit(sub(points[min(i+1,len(points)-1)],points[max(0,i-1)]))
            seed=(0,1,0) if abs(tangent[1])<.85 else (1,0,0)
            u=unit(cross(tangent,seed));v=cross(tangent,u)
            for j in range(sides):
                a=math.tau*j/sides;vertices.append(add(p,add(mul(u,math.cos(a)*radii[i]),mul(v,math.sin(a)*radii[i]))))
        for k in range(len(points)-1):
            for i in range(sides):
                a=k*sides+i;b=k*sides+(i+1)%sides;faces.extend([(a,b,b+sides),(a,b+sides,a+sides)])
        for ring,p in ((0,points[0]),(len(points)-1,points[-1])):
            index=len(vertices);vertices.append(p)
            for i in range(sides):
                a=ring*sides+i;b=ring*sides+(i+1)%sides
                faces.append((index,b,a) if ring==0 else (index,a,b))
        center=mul(add(points[0],points[-1]),.5)
        self.mesh(name,[sub(p,center) for p in vertices],faces,mat,center,smooth=True)

    def cylinder(self,name,center,radius,length,mat,axis='z',sides=12):
        d={'x':(length/2,0,0),'y':(0,length/2,0),'z':(0,0,length/2)}[axis]
        self.tube(name,[sub(center,d),add(center,d)],[radius,radius],mat,sides)

    def save(self,path,metadata):
        data=bytearray();views=[];accessors=[];meshes=[];nodes=[]
        def accessor(values,kind,target,integer=False):
            width=3 if kind=='VEC3' else 1; flat=[x for row in values for x in row] if width>1 else values
            offset=len(data);blob=struct.pack('<'+('H' if integer else 'f')*len(flat),*flat);data.extend(blob);data.extend(b'\0'*((-len(data))%4))
            views.append({'buffer':0,'byteOffset':offset,'byteLength':len(blob),'target':target})
            a={'bufferView':len(views)-1,'componentType':5123 if integer else 5126,'count':len(values),'type':kind}
            if kind=='VEC3':a.update(min=[min(p[j] for p in values) for j in range(3)],max=[max(p[j] for p in values) for j in range(3)])
            accessors.append(a);return len(accessors)-1
        low=[float('inf')]*3;high=[-float('inf')]*3
        for part in self.parts:
            pos=accessor(part['v'],'VEC3',34962);norm=accessor(part['n'],'VEC3',34962);ind=accessor([i for f in part['f'] for i in f],'SCALAR',34963,True)
            meshes.append({'name':part['name'],'primitives':[{'attributes':{'POSITION':pos,'NORMAL':norm},'indices':ind,'material':part['mat'],'mode':4}]})
            nodes.append({'name':part['name'],'mesh':len(meshes)-1,'translation':list(part['center'])})
            for p in part['v']:
                for j in range(3): low[j]=min(low[j],p[j]+part['center'][j]);high[j]=max(high[j],p[j]+part['center'][j])
        gltf={'asset':{'version':'2.0','generator':'Neon Breach original Vanguard builder '+VERSION,'extras':{'neonBreach':metadata['rig']}},'scene':0,'scenes':[{'name':self.title,'nodes':list(range(len(nodes)))}],'nodes':nodes,'meshes':meshes,'materials':self.materials,'buffers':[{'byteLength':len(data)}],'bufferViews':views,'accessors':accessors,'extras':{'title':self.title,'units':'metres','axes':'X right, Y up, +Z forward','staticNamedParts':True,**metadata}}
        js=json.dumps(gltf,separators=(',',':')).encode();js+=b' '*((-len(js))%4)
        length=12+8+len(js)+8+len(data)
        blob=struct.pack('<III',0x46546c67,2,length)+struct.pack('<II',len(js),0x4e4f534a)+js+struct.pack('<II',len(data),0x004e4942)+data
        path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(blob)
        assert abs(low[1])<1e-6,(path,low)
        assert 2.30<high[1]<3.0,(path,high)
        return {'file':'characters/'+path.name,'name':self.title,'bytes':length,'sha256':hashlib.sha256(blob).hexdigest(),'nodes':len(nodes),'triangles':sum(len(p['f']) for p in self.parts),'vertices':sum(len(p['v']) for p in self.parts),'materials':len(self.materials),'bounds':{'min':low,'max':high},**metadata}


CHARACTERS=[
    {'id':'player_one','name':'Vanguard One','style':'assault','armor':'345573','trim':'8eb7cc','glow':'3fe7ff','width':1.02,'description':'Broad tactical assault armor, angular visor, reactor harness and layered pauldrons.'},
    {'id':'player_two','name':'Vanguard Two','style':'striker','armor':'805831','trim':'e9bd70','glow':'ffbb48','width':1.00,'description':'Heavy amber striker armor with a divided faceplate and reinforced forearm braces.'},
    {'id':'bot_breacher','name':'Breacher','style':'breacher','armor':'7a342f','trim':'b99676','glow':'ff6e3b','width':1.10,'description':'Close assault armor with a blast collar, wide face shield, and stacked charge packs.'},
    {'id':'bot_sentinel','name':'Sentinel','style':'sentinel','armor':'42675a','trim':'a4c5a4','glow':'69ffb6','width':1.04,'description':'Sentry suit with elevated sensor dishes, an optical monocle and reinforced shoulder housings.'},
    {'id':'bot_vanguard','name':'Vanguard','style':'vanguard','armor':'3d497b','trim':'a9b5dd','glow':'8599ff','width':1.12,'description':'Command assault suit with a central crest, armored collar and symmetrical shield pauldrons.'},
    {'id':'bot_flanker','name':'Flanker','style':'flanker','armor':'493b6c','trim':'b59bd6','glow':'d76dff','width':.97,'description':'Powerful recon suit with swept back fins, twin thrusters and split visor optics.'},
    {'id':'bot_demolisher','name':'Demolisher','style':'demolisher','armor':'7b6333','trim':'c8bb85','glow':'f7e351','width':1.18,'description':'Bombardment frame with a drum reactor, heavy blast plates and shoulder ammunition tubes.'},
    {'id':'character_bulwark','name':'Bulwark','style':'bulwark','armor':'314d61','trim':'a4c5d7','glow':'55d7ff','width':1.24,'description':'Siege chassis with a towering left shield, slab pauldrons and piston reinforced legs.'},
    {'id':'character_ronin','name':'Ronin','style':'ronin','armor':'753c45','trim':'ce9774','glow':'ff5969','width':1.04,'description':'Armored duelist with a horned kabuto, segmented shoulder armor and a back mounted energy blade.'},
    {'id':'character_warden','name':'Warden','style':'warden','armor':'58666a','trim':'d6e0cc','glow':'a7ff5c','width':1.15,'description':'Industrial enforcer with a respirator cage, armored tabard and dual shoulder floodlights.'},
    {'id':'character_revenant','name':'Revenant','style':'revenant','armor':'3c354c','trim':'a794b7','glow':'d17aff','width':1.03,'description':'Spectral assault frame with a skull faceplate, bladed fins, spinal coils and clawed gauntlets.'},
    {'id':'character_praetorian','name':'Praetorian','style':'praetorian','armor':'8b693b','trim':'f1d69a','glow':'70ecff','width':1.14,'description':'Royal guard armor with a transverse crest, split armored skirt and circular shoulder shields.'},
    {'id':'character_overlord','name':'Overlord','style':'overlord','armor':'522e39','trim':'c5946a','glow':'ff493f','width':1.23,'description':'Massive warlord frame with a crown, asymmetric spike pauldrons and a twin reactor power pack.'},
]


def build(spec):
    s=Scene(spec['name']);w=spec['width'];style=spec['style']
    armor=material('Ceramic armor',spec['armor'],.55,.38)
    trim=material('Machined armor edges',spec['trim'],.72,.29)
    dark=material('Graphite pressure suit','111923',.24,.72)
    joint=material('Titanium joints','3b4854',.8,.33)
    rubber=material('Boot rubber','0b1119',.08,.88)
    black=material('Obsidian optics','050c16',.62,.20)
    light=material('Ion emissive',spec['glow'],.3,.25,.95)
    white=material('Identification marking','d4e0df',.15,.55)
    # Tapered armored torso; a large chest, narrow waist, and a broad stance.
    s.loft('torso',(0,1.46,0),[(-.34,.28*w,.19,.16),(-.11,.37*w,.24,.18),(.23,.45*w,.25,.22),(.36,.34*w,.18,.20)],dark)
    s.loft('thorax_carapace',(0,1.58,.045),[(-.19,.34*w,.23,.28),(.17,.43*w,.255,.27),(.27,.34*w,.20,.27)],armor)
    for side in (-1,1):
        s.box('pectoral_plate',(side*.215*w,1.67,.282),(.41*w,.24,.105),armor,.23,(0,side*-.13,side*-.11))
        s.box('pectoral_edge',(side*.21*w,1.79,.316),(.34*w,.035,.028),trim,.25,(0,0,side*-.11))
        s.box('pectoral_lumen',(side*.237*w,1.66,.347),(.22*w,.035,.022),light,.28,(0,0,side*-.11))
        s.box('rib_shell',(side*.315*w,1.42,.08),(.16*w,.33,.35),armor,.3,(0,0,side*-.12))
        for y in (1.30,1.39,1.48): s.box('abdominal_lamella',(side*.115*w,y,.252),(.205*w,.069,.075),joint,.18)
        s.box('hip_guard',(side*.27*w,1.115,.035),(.255*w,.24,.36),armor,.28,(0,0,side*-.10))
        s.box('utility_pouch',(side*.36*w,1.19,-.005),(.14*w,.18,.21),dark,.22)
        s.box('utility_latch',(side*.37*w,1.205,.106),(.085,.052,.024),trim,.16)
    s.box('pelvis',(0,1.105,0),(.53*w,.22,.35),dark,.24)
    s.loft('codpiece',(0,1.09,.215),[(-.145,.105*w,.045,.25),(.07,.19*w,.065,.25)],armor)
    s.box('belt',(0,1.235,.035),(.68*w,.085,.42),joint,.15)
    s.box('belt_buckle',(0,1.235,.263),(.14,.09,.048),trim,.19)
    s.box('belt_buckle_lumen',(0,1.235,.29),(.075,.025,.012),light,.2)
    s.cylinder('chest_core',(0,1.53,.322),.09,.065,trim)
    s.cylinder('chest_core_light',(0,1.53,.359),.056,.018,light,sides=10)
    # Solid boots and thick layered thighs/shins; each foot is planted on Y=0.
    for side in (-1,1):
        x=side*.255*w
        s.box('boot_sole',(x,.055,.085),(.315*w,.11,.48),rubber,.22)
        s.box('boot_toe',(x,.16,.105),(.315*w,.20,.44),armor,.23)
        s.box('boot_toe_cap',(x,.177,.295),(.275*w,.125,.075),trim,.18)
        s.box('boot_ankle',(x,.295,-.02),(.265*w,.205,.295),dark,.2)
        s.box('boot_ankle_guard',(x,.307,.135),(.26*w,.18,.105),armor,.2)
        s.loft('shin_armor',(x,.515,.018),[(-.18,.125*w,.13,.24),(.18,.155*w,.15,.24)],armor)
        s.box('shin_front_plate',(x,.525,.176),(.225*w,.285,.07),trim,.25)
        s.box('shin_lumen',(x,.535,.217),(.035,.195,.015),light,.2)
        s.cylinder('knee_joint',(x,.752,0),.13,.235*w,joint,'x')
        s.loft('knee_shield',(x,.75,.172),[(-.102,.105*w,.055,.25),(.07,.15*w,.075,.28),(.12,.115*w,.045,.20)],armor)
        s.box('knee_shield_detail',(x,.77,.25),(.15*w,.038,.022),light,.2)
        s.loft('thigh_suit',(x,.95,-.015),[(-.13,.13*w,.135,.2),(.13,.155*w,.16,.2)],dark)
        s.loft('thigh_armor',(x,.958,.055),[(-.15,.13*w,.145,.25),(.14,.173*w,.16,.25)],armor)
        s.box('thigh_front_trim',(x,.984,.222),(.22*w,.16,.038),trim,.2)
        s.box('thigh_outboard_guard',(x+side*.14*w,.955,.015),(.085*w,.205,.22),joint,.22)
        s.cylinder('calf_piston',(x+side*.105*w,.50,-.13),.033,.30,trim,'y',8)
    # Broad shoulders, heavy bracers, and closed armored hands.
    for side in (-1,1):
        x=side*.54*w
        s.sphere('shoulder_joint',(x,1.725,0),(.18*w,.18,.18),joint)
        s.loft('shoulder_pauldron',(side*.565*w,1.81,.015),[(-.12,.205*w,.23,.25),(.10,.235*w,.25,.22),(.17,.14*w,.17,.25)],armor,rotation=(0,0,side*-.12))
        s.box('shoulder_edge',(side*.655*w,1.91,.18),(.22*w,.056,.08),trim,.23,(0,0,side*-.12))
        s.box('shoulder_lumen',(side*.71*w,1.8,.25),(.075*w,.095,.028),light,.28)
        s.tube('upper_arm_suit',[(side*.55*w,1.66,0),(side*.59*w,1.42,.018)],[.115*w,.115*w],dark)
        s.box('upper_arm_plate',(side*.623*w,1.51,.055),(.235*w,.25,.245),armor,.28,(0,0,side*.07))
        s.cylinder('elbow_joint',(side*.60*w,1.34,.025),.105,.245*w,joint,'x')
        s.loft('forearm_bracer',(side*.615*w,1.185,.075),[(-.14,.135*w,.15,.22),(.14,.165*w,.165,.22)],armor,rotation=(.08,0,side*.025))
        s.box('forearm_faceplate',(side*.615*w,1.20,.248),(.21*w,.23,.055),trim,.23)
        s.box('forearm_lumen',(side*.615*w,1.20,.283),(.035,.145,.017),light,.2)
        s.box('glove',(side*.62*w,.986,.08),(.23*w,.19,.245),dark,.26)
        s.box('glove_knuckles',(side*.62*w,.973,.222),(.22*w,.103,.064),armor,.22)
        for off in (-.068,0,.068):s.box('glove_finger',(side*.62*w+off*w,.92,.185),(.05*w,.073,.095),joint,.18)
    # The sculpted full helmet includes a projecting brow and armored jaw.
    s.cylinder('neck_guard',(0,1.91,0),.17,.20,joint,'y',12)
    s.loft('neck_guard_collar',(0,1.89,-.005),[(-.08,.255*w,.19,.20),(.06,.205*w,.18,.22)],armor)
    s.loft('helmet',(0,2.15,0),[(-.22,.16,.165,.33),(-.10,.225,.20,.32),(.12,.22,.205,.32),(.22,.145,.14,.25)],armor)
    s.box('visor_recess',(0,2.165,.203),(.35,.13,.055),black,.28)
    s.box('visor',(0,2.177,.236),(.30,.052,.024),light,.32)
    s.box('helmet_brow',(0,2.257,.223),(.39,.055,.085),trim,.26)
    for side in (-1,1):
        s.box('helmet_cheek',(side*.154,2.045,.183),(.11,.15,.105),armor,.3,(0,side*-.15,side*.16))
        s.cylinder('helmet_ear',(side*.225,2.135,0),.079,.07,joint,'x')
        s.cylinder('helmet_ear_light',(side*.265,2.135,0),.042,.018,light,'x',10)
    s.box('helmet_jaw',(0,1.997,.165),(.22,.10,.15),joint,.28)
    for x in (-.055,0,.055):s.box('helmet_breather',(x,2.018,.252),(.02,.055,.018),black,.15)
    # A substantial rear power unit gives every suit a complete back silhouette.
    s.box('reactor_pack',(0,1.59,-.30),(.47*w,.46,.24),dark,.22)
    s.box('reactor_armored_cover',(0,1.61,-.443),(.38*w,.32,.08),armor,.22)
    for side in (-1,1):
        s.box('reactor_cooling_rail',(side*.175*w,1.61,-.498),(.055,.28,.035),trim,.2)
        for y in (1.50,1.59,1.68):s.box('reactor_cooling_lumen',(side*.082*w,y,-.493),(.083,.025,.018),light,.2)
        s.tube('reactor_cable',[(side*.19*w,1.43,-.37),(side*.31*w,1.32,-.22),(side*.31*w,1.24,-.12)],[.036,.036,.028],joint,8)

    if style=='assault':
        s.box('helmet_central_ridge',(0,2.32,-.005),(.09,.105,.32),joint,.24)
        s.box('identification_plate',(-.255*w,1.71,.355),(.09,.065,.018),white,.13)
        s.tube('sensor_mast',[(.25*w,1.76,-.32),(.25*w,2.09,-.32)],[.02,.014],joint,8)
        s.sphere('sensor_tip',(.25*w,2.09,-.32),(.028,.028,.028),light,8,4)
    elif style=='striker':
        s.box('helmet_nasal_bridge',(0,2.14,.269),(.045,.19,.05),trim,.25)
        for side in (-1,1):
            s.box('shoulder_upper_lamella',(side*.61*w,1.965,.01),(.33*w,.065,.34),joint,.26,(0,0,side*-.08))
            s.cylinder('forearm_impact_cap',(side*.615*w,1.13,.306),.08,.04,armor)
        s.box('reactor_charge_bank',(0,1.82,-.36),(.34,.14,.22),trim,.25)
    elif style=='breacher':
        s.loft('blast_collar',(0,1.91,.10),[(-.08,.32*w,.225,.25),(.08,.29*w,.21,.25)],joint)
        s.box('visor_blast_shield',(0,2.14,.257),(.30,.11,.037),light,.26)
        for side in (-1,1):
            for off in (-.055,.055):
                s.box('charge_pack',(side*.32*w,1.45+off*2,.29),(.13,.09,.10),dark,.18)
                s.box('charge_pack_label',(side*.32*w,1.45+off*2,.347),(.07,.025,.015),light,.15)
            s.box('shoulder_blast_plate',(side*.58*w,1.85,.29),(.35,.22,.075),joint,.25)
    elif style=='sentinel':
        s.cylinder('visor_optical_housing',(.115,2.17,.266),.085,.10,black)
        s.cylinder('visor_optical_lens',(.115,2.17,.324),.055,.026,light)
        for side in (-1,1):
            s.box('sensor_stalk',(side*.33*w,1.97,-.24),(.065,.30,.065),joint,.18)
            s.sphere('sensor_dish',(side*.33*w,2.105,-.20),(.12,.07,.105),armor)
            s.box('sensor_optic',(side*.33*w,2.10,-.087),(.11,.04,.03),light,.2)
    elif style=='vanguard':
        s.loft('helmet_command_crest',(0,2.38,-.035),[(-.05,.055,.16,.2),(.10,.034,.10,.2)],trim)
        for side in (-1,1):
            s.box('shoulder_herald_plate',(side*.58*w,1.82,.292),(.33,.22,.065),trim,.25)
            s.box('shoulder_herald_lumen',(side*.58*w,1.82,.333),(.16,.05,.02),light,.2)
    elif style=='flanker':
        for side in (-1,1):
            s.loft('reactor_swept_fin',(side*.30*w,1.78,-.40),[(-.23,.055,.13,.22),(.29,.035,.16,.2)],armor,rotation=(-.4,0,side*-.13))
            s.cylinder('reactor_thruster',(side*.235*w,1.47,-.42),.095,.32,joint,'y')
            s.cylinder('reactor_exhaust',(side*.235*w,1.31,-.42),.067,.03,light,'y')
            s.box('visor_split_beam',(side*.092,2.177,.258),(.08,.055,.024),white,.25)
        s.box('helmet_crown_blade',(0,2.36,-.06),(.065,.10,.32),armor,.23)
    elif style=='demolisher':
        s.cylinder('reactor_drum',(0,1.61,-.43),.24,.59*w,joint,'x',16)
        for side in (-1,1):
            s.cylinder('reactor_drum_cap',(side*.31*w,1.61,-.43),.19,.045,armor,'x')
            for i in (-1,0,1):
                s.cylinder('shoulder_ammo_tube',(side*(.46+.085*i)*w,1.99,.02),.047,.28,dark,'z',8)
                s.cylinder('shoulder_ammo_tip',(side*(.46+.085*i)*w,1.99,.165),.038,.025,light,'z',8)
        s.box('thorax_blast_apron',(0,1.46,.335),(.45*w,.33,.08),armor,.28)
        s.box('thorax_hazard_band',(0,1.48,.383),(.31,.035,.018),light,.2,(0,0,.18))
    elif style=='bulwark':
        # The shield travels with the left arm, not the torso.
        s.loft('forearm_tower_shield',(-.81*w,1.31,.20),[(-.48,.23,.12,.22),(.31,.29,.13,.20),(.51,.20,.10,.20)],armor)
        s.box('forearm_shield_rib',(-.81*w,1.34,.345),(.085,.80,.055),trim,.22)
        s.box('forearm_shield_lumen',(-.81*w,1.34,.379),(.032,.56,.015),light,.2)
        for side in (-1,1):
            s.box('shoulder_siege_slab',(side*.615*w,1.96,.02),(.44*w,.13,.48),joint,.22)
            s.cylinder('shin_hydraulic_rod',(side*.255*w+side*.12,.48,-.20),.046,.40,trim,'y',8)
        s.box('helmet_siege_brow',(0,2.27,.255),(.43,.11,.10),armor,.24)
    elif style=='ronin':
        for side in (-1,1):
            s.tube('helmet_horn',[(side*.14,2.30,.015),(side*.29,2.42,.035),(side*.32,2.57,.085)],[.07,.05,.008],trim,8)
            for layer in range(3):s.box('shoulder_lamellar',(side*(.54+.035*layer)*w,1.87-layer*.085,.06),(.37,.08,.41),armor,.24,(0,0,side*-.20))
            s.loft('hip_lamellar_skirt',(side*.30*w,1.07,-.02),[(-.25,.14,.18,.2),(.06,.13,.14,.2)],joint,rotation=(0,0,side*.2))
        s.tube('blade_scabbard',[(-.40,1.12,-.43),(.34,2.12,-.43)],[.055,.055],dark,8)
        s.tube('blade_hilt',[(.34,2.12,-.43),(.53,2.37,-.43)],[.035,.035],trim,8)
        s.box('blade_guard',(.34,2.12,-.43),(.28,.055,.10),light,.18,(0,0,-.64))
        s.box('helmet_nasal_guard',(0,2.10,.269),(.07,.19,.055),armor,.23)
    elif style=='warden':
        s.box('helmet_respirator',(0,2.03,.256),(.245,.13,.12),dark,.28)
        for side in (-1,1):
            s.cylinder('helmet_filter',(side*.105,2.035,.322),.058,.06,joint)
            s.cylinder('helmet_filter_lumen',(side*.105,2.035,.357),.028,.015,light)
            s.box('shoulder_floodlight_housing',(side*.48*w,2.005,.04),(.20,.17,.23),joint,.25)
            s.cylinder('shoulder_floodlight',(side*.48*w,2.005,.171),.068,.025,light)
            s.loft('armored_tabard',(side*.13*w,1.035,.23),[(-.29,.105,.045,.15),(.13,.12,.065,.2)],armor,rotation=(.10,0,side*.05))
        s.box('thorax_warden_cross',(0,1.69,.365),(.055,.205,.025),trim,.20)
        s.box('thorax_warden_crossbar',(0,1.69,.367),(.205,.055,.025),trim,.20)
    elif style=='revenant':
        s.loft('helmet_skull_mask',(0,2.08,.245),[(-.10,.095,.055,.3),(.075,.155,.065,.35)],trim)
        for side in (-1,1):
            s.box('visor_skull_eye',(side*.086,2.15,.311),(.08,.046,.02),light,.2,(0,0,side*.17))
            s.loft('shoulder_blade_fin',(side*.67*w,1.96,-.04),[(-.16,.065,.15,.2),(.31,.015,.075,.2)],armor,rotation=(0,0,side*-.30))
            for finger in (-1,0,1):s.tube('glove_talon',[(side*.62*w+finger*.06,.925,.24),(side*.62*w+finger*.06,.87,.30),(side*.62*w+finger*.06,.82,.27)],[.023,.019,.004],trim,7)
        for y in (1.42,1.52,1.62,1.72,1.82):
            s.box('reactor_spinal_link',(0,y,-.512),(.14,.06,.09),joint,.20)
            s.box('reactor_spinal_lumen',(0,y,-.565),(.055,.027,.02),light,.20)
        for x in (-.05,0,.05):s.box('helmet_skull_teeth',(x,2.005,.308),(.025,.068,.018),black,.15)
    elif style=='praetorian':
        s.box('helmet_crest_base',(0,2.38,0),(.39,.075,.14),joint,.22)
        for x in (-.18,-.12,-.06,0,.06,.12,.18):s.box('helmet_transverse_crest',(x,2.475-abs(x)*.26,-.015),(.05,.15,.13),trim,.22)
        for side in (-1,1):
            s.cylinder('shoulder_round_shield',(side*.63*w,1.80,.28),.22,.09,trim,'z',12)
            s.cylinder('shoulder_round_inset',(side*.63*w,1.80,.333),.16,.03,armor,'z',12)
            s.cylinder('shoulder_round_core',(side*.63*w,1.80,.354),.055,.02,light,'z',10)
            s.loft('hip_guard_skirt',(side*.21*w,1.05,.06),[(-.27,.15,.19,.18),(.10,.14,.17,.18)],armor,rotation=(0,0,side*.14))
            s.box('hip_guard_skirt_edge',(side*.25*w,.83,.253),(.21,.04,.024),trim,.16,(0,0,side*.14))
        s.box('thorax_crest',(0,1.75,.36),(.12,.14,.026),trim,.22)
    elif style=='overlord':
        for x,h in ((-.19,.16),(-.10,.25),(0,.32),(.10,.25),(.19,.16)):
            s.loft('helmet_crown_tine',(x,2.34+h/2,-.025),[(-h/2,.036,.07,.2),(h/2,.007,.035,.2)],trim,rotation=(0,0,-x*.7))
        for side in (-1,1):
            for n in (0,1):s.tube('shoulder_warlord_spike',[(side*(.58+.13*n)*w,1.96,-.03),(side*(.67+.14*n)*w,2.18+.07*n,-.05)],[.073,.008],trim,8)
            s.cylinder('reactor_power_stack',(side*.18*w,1.68,-.48),.13,.49,joint,'y')
            s.cylinder('reactor_power_cap',(side*.18*w,1.94,-.48),.102,.04,light,'y')
            s.box('shoulder_warlord_face',(side*.62*w,1.79,.31),(.33,.25,.10),dark,.28)
            s.box('shoulder_warlord_rune',(side*.62*w,1.80,.368),(.06,.14,.018),light,.2)
        s.loft('thorax_warlord_sternum',(0,1.63,.345),[(-.25,.06,.06,.2),(.20,.13,.07,.2)],trim)
    return s


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=ROOT/'assets'/'characters')
    parser.add_argument('--only',help='Build one character id')
    args=parser.parse_args();entries=[]
    for spec in CHARACTERS:
        if args.only and spec['id']!=args.only:continue
        scene=build(spec)
        rig={'bodyHeight':2.37,'hip':[.255*spec['width'],1.10,0],'shoulder':[.54*spec['width'],1.725,.02],'headPivot':[0,1.94,.08],'aimHead':[0,2.177,.236],'aimBody':[0,1.58,.045]}
        metadata={'characterId':spec['id'],'collection':'Vanguard','description':spec['description'],'rig':rig,'buildVersion':VERSION}
        result=scene.save(args.output/(spec['id']+'.glb'),metadata)
        # Portraits are captured from the actual WebGL renderer separately. Keep
        # their file metadata when rebuilding geometry without requiring Pillow.
        portrait=args.output/(spec['id']+'.webp')
        if portrait.exists():
            portrait_bytes=portrait.read_bytes()
            result['portrait']={'file':'characters/'+portrait.name,'mimeType':'image/webp','width':320,'height':400,'bytes':len(portrait_bytes),'sha256':hashlib.sha256(portrait_bytes).hexdigest(),'source':'Actual NeonModels WebGL render'}
        entries.append(result)
        print(f"{spec['id']}: {result['bytes']:,} bytes; {result['triangles']:,} triangles; {result['nodes']} parts")
    if not entries:parser.error('Unknown --only id')
    if not args.only:
        manifest={'schemaVersion':1,'collection':'Neon Breach Vanguard','buildVersion':VERSION,'format':'glTF 2.0 binary; embedded uncompressed indexed triangles and normals; opaque materials','source':'tools/build-characters.py','axes':'X right, Y up, +Z forward','license':'Original project assets; no third-party artwork or texture dependencies','models':entries}
        (args.output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')


if __name__=='__main__':main()
