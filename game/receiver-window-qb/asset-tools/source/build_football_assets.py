"""Receiver Window QB — detailed procedural football athlete authoring.
Run: blender --background --python build_football_assets.py -- --pack <directory>
Native authoring coordinates are right-handed, Z-up, front=-Y, metres.
All athlete surfaces are lofted polygon meshes; no primitive body objects.
Copyright 2026 Sean Pereksta. Generated original assets, no external dependencies.
"""
import bpy, math, json, os, sys, argparse, random, tempfile, struct
from mathutils import Vector, Quaternion, Euler, Matrix
from math import sin, cos, pi

argv = sys.argv[sys.argv.index('--')+1:] if '--' in sys.argv else []
ap=argparse.ArgumentParser();ap.add_argument('--pack',default=os.path.dirname(os.path.dirname(__file__)));ap.add_argument('--preview',default='');ap.add_argument('--only',default='');ap.add_argument('--shard',default='');ap.add_argument('--fix-path',default='');ap.add_argument('--lod',action='store_true');args=ap.parse_args(argv)
if args.preview:args.preview=os.path.abspath(args.preview)
PACK=os.path.abspath(args.pack); os.makedirs(PACK,exist_ok=True)
tempfile.tempdir=os.path.join(os.path.dirname(PACK),'blender_temp');os.makedirs(tempfile.tempdir,exist_ok=True)
for p in ['models/quarterbacks','models/receivers','models/defenders','models/other-positions','animations','equipment','textures','source']:os.makedirs(os.path.join(PACK,p),exist_ok=True)
random.seed(20261018)

ARCHETYPES=[
 ('qb_field_general','quarterbacks','Field General',1.93,103,0.99,0.99,0.75,12,'navy'),
 ('qb_mobile','quarterbacks','Mobile Quarterback',1.85,96,0.94,0.94,0.68,7,'crimson'),
 ('wr_speed','receivers','Speed Receiver',1.78,79,0.86,0.88,0.46,11,'ice'),
 ('wr_route_technician','receivers','Route Technician',1.83,87,0.91,0.92,0.57,18,'navy'),
 ('wr_possession','receivers','Possession Receiver',1.91,99,1.00,0.99,0.77,14,'crimson'),
 ('wr_jump_ball','receivers','Jump Ball Specialist',2.01,105,1.01,0.99,0.76,81,'ice'),
 ('wr_physical','receivers','Physical Receiver',1.94,109,1.08,1.06,0.93,88,'navy'),
 ('wr_slot','receivers','Slot Receiver',1.73,83,0.90,0.94,0.60,10,'crimson'),
 ('rb_elusive','other-positions','Elusive Running Back',1.75,94,0.99,1.04,0.81,22,'navy'),
 ('rb_power','other-positions','Power Running Back',1.83,112,1.13,1.12,0.99,28,'crimson'),
 ('te_receiving','other-positions','Receiving Tight End',1.98,116,1.10,1.07,0.88,86,'ice'),
 ('te_blocking','other-positions','Blocking Tight End',2.03,124,1.18,1.15,1.00,87,'navy'),
 ('cb_press','defenders','Press Cornerback',1.88,94,0.98,0.98,0.76,24,'crimson'),
 ('cb_ballhawk','defenders','Ball Hawk Cornerback',1.80,84,0.89,0.91,0.55,21,'ice'),
 ('safety_hybrid','defenders','Hybrid Safety',1.89,100,1.02,1.02,0.81,31,'navy'),
 ('linebacker_coverage','defenders','Coverage Linebacker',1.92,116,1.13,1.10,0.98,54,'crimson'),
]
PALETTES={'navy':((.014,.035,.09,1),(.06,.72,.78,1),(.96,.98,1,1)), 'crimson':((.42,.012,.037,1),(.94,.59,.12,1),(.98,.97,.90,1)), 'ice':((.77,.85,.91,1),(.08,.23,.47,1),(.96,.39,.10,1))}
MORPHS=['Muscularity','ShoulderWidth','ChestSize','ArmThickness','LegPower','BodyWeight','Wingspan','HandSize','Reach']
CLIPS={}
ALL_OBJECTS=[];PARTS=[];BONES={}; ACTIVE=None; LOD=False

def clean():
 bpy.ops.object.select_all(action='SELECT');bpy.ops.object.delete(use_global=False)
 bpy.context.scene.render.fps=30;bpy.context.scene.render.fps_base=1
 for dat in list(bpy.data.actions):bpy.data.actions.remove(dat)

def image_map(name,kind):
 path=os.path.join(PACK,'textures',name+'.png')
 if os.path.exists(path):return bpy.data.images.load(path,check_existing=True)
 n=512; im=bpy.data.images.new(name,width=n,height=n,alpha=False);px=[]
 for y in range(n):
  for x in range(n):
   weave=sin(x*pi/2)*sin(y*pi/2);fine=sin(x*.79+y*1.21)
   if kind=='normal': c=(.5+.025*sin(x*pi/2),.5+.025*sin(y*pi/2),.999,1)
   elif kind=='roughness':v=.73+.07*weave;c=(v,v,v,1)
   else:
    v=.84+.08*weave+.018*fine
    if kind=='grip':v=.22 if (x%18-9)**2+(y%18-9)**2<18 else .90
    c=(v,v,v,1)
   px.extend(c)
 im.pixels=px;im.filepath_raw=path;im.file_format='PNG';im.save();return im

def mat(name,color,rough=.6,metal=0,texture=False,alpha=1):
 m=bpy.data.materials.new(name);m.diffuse_color=(*color[:3],alpha);m.use_nodes=True
 bs=m.node_tree.nodes.get('Principled BSDF');bs.inputs['Base Color'].default_value=(*color[:3],alpha);bs.inputs['Roughness'].default_value=rough;bs.inputs['Metallic'].default_value=metal
 if alpha<1:bs.inputs['Alpha'].default_value=alpha
 if texture:
  nt=m.node_tree;tex=nt.nodes.new('ShaderNodeTexImage');tex.image=image_map('technical_fabric_albedo','albedo')
  mix=nt.nodes.new('ShaderNodeMixRGB');mix.blend_type='MULTIPLY';mix.inputs[0].default_value=1;mix.inputs[2].default_value=color;nt.links.new(tex.outputs['Color'],mix.inputs[1]);nt.links.new(mix.outputs[0],bs.inputs['Base Color'])
  nr=nt.nodes.new('ShaderNodeTexImage');nr.image=image_map('technical_fabric_normal','normal');nr.image.colorspace_settings.name='Non-Color';normal=nt.nodes.new('ShaderNodeNormalMap');normal.inputs['Strength'].default_value=.28;nt.links.new(nr.outputs[0],normal.inputs['Color']);nt.links.new(normal.outputs[0],bs.inputs['Normal'])
  rr=nt.nodes.new('ShaderNodeTexImage');rr.image=image_map('technical_fabric_roughness','roughness');rr.image.colorspace_settings.name='Non-Color';nt.links.new(rr.outputs[0],bs.inputs['Roughness'])
 return m

def coord(p):
 a=ACTIVE
 return Vector((p[0]*a['width'],p[1]*a['depth'],p[2]*a['height']/1.88))

def mesh(name,verts,faces,material,weight='chest',region='body',weights=None):
 me=bpy.data.meshes.new(name);me.from_pydata([coord(v) for v in verts],[],faces);me.materials.append(material);me.update()
 ob=bpy.data.objects.new(name,me);bpy.context.collection.objects.link(ob)
 for poly in me.polygons:poly.use_smooth=True
 uv=me.uv_layers.new(name='UVMap')
 for poly in me.polygons:
  for li in poly.loop_indices:
   p=verts[me.loops[li].vertex_index];uv.data[li].uv=((math.atan2(p[1],p[0])+pi)/(2*pi),p[2]*2)
 if weights is None:weights=[{weight:1} for v in verts]
 groups={}
 for i,ww in enumerate(weights):
  for key,val in ww.items():
   if key not in groups:groups[key]=ob.vertex_groups.new(name=key)
   if val>0:groups[key].add([i],val,'REPLACE')
 ob['surface_region']=region;PARTS.append(ob);return ob

def loft(name,rings,material,weight='chest',region='body',sides=None):
 # Rings contain (center xyz, radius horizontal, radius depth); anatomical,
 # non-uniform elliptical sections can include a rotation and muscle amplitude.
 n=sides or (10 if LOD else 18);vs=[];fs=[];ws=[]
 centers=[Vector(r[0]) for r in rings]
 for j,r in enumerate(rings):
  tangent=(centers[min(j+1,len(rings)-1)]-centers[max(j-1,0)]).normalized()
  u=Vector((0,1,0)) if abs(tangent.x)>.90 else Vector((1,0,0));u=(u-tangent*u.dot(tangent)).normalized();v=tangent.cross(u).normalized()
  for k in range(n):
   t=k*2*pi/n;muscle=(r[3] if len(r)>3 else .035)
   contour=1+muscle*cos(2*t)+.015*cos(3*t)
   p=centers[j]+u*(r[1]*cos(t)*contour)+v*(r[2]*sin(t)*(1+.02*cos(4*t)))
   vs.append(p)
   if callable(weight):ws.append(weight(j/(len(rings)-1),p))
   else:ws.append({weight:1})
 for j in range(len(rings)-1):
  for k in range(n):a=j*n+k;b=j*n+(k+1)%n;fs.append((a,b,b+n,a+n))
 fs.append(tuple(range(n-1,-1,-1)));fs.append(tuple((len(rings)-1)*n+k for k in range(n)))
 return mesh(name,vs,fs,material,region=region,weights=ws)

def tube(name,path,radius,material,bone='chest',sides=6,region='detail'):
 rings=[(tuple(v),radius,radius,0) for v in path];return loft(name,rings,material,bone,region,sides=sides)

def panel(name,vs,material,bone='chest',region='equipment'):
 return mesh(name,vs,[tuple(range(len(vs)))],material,bone,region)

def mixweights(t,parts):
 # normalized segmented weights with smooth transitions.
 for low,high,a,b in parts:
  if low<=t<=high:
   f=(t-low)/(high-low);return {a:1-f,b:f}
 return {parts[-1][-1]:1}

def body(m):
 mus=ACTIVE['muscle'];limb=0.91+.16*mus
 torso=[((0,.012,.94),.145,.104),((0,.004,1.02),.155,.105),((0,.0,1.10),.153,.101),((0,0,1.19),.160,.112),((0,.008,1.29),.197,.126),((0,.010,1.39),.230,.134),((0,.016,1.48),.242,.127),((0,.018,1.535),.198,.105),((0,.02,1.555),.095,.071)]
 def tw(t,p):return mixweights(t,[(0,.25,'pelvis','spine'),(.25,.68,'spine','chest'),(.68,1,'chest','neck')])
 loft('Sculpted performance jersey',torso,m['jersey'],tw,'torso')
 # Tailored abdomen panels and raised rib protection.
 for s in [-1,1]:
  tube('Tailored side seam',[(s*.153,-.031,1.04),(s*.158,-.055,1.15),(s*.19,-.055,1.28),(s*.218,-.045,1.41)],.0030,m['trim'])
  for z in [1.13,1.17,1.21]:tube('Rib stitch',[(s*.12,-.077,z),(s*.156,-.055,z+.005)],.0017,m['stitch'])
 loft('Neck', [((0,.012,1.51),.073,.067),((0,.008,1.56),.066,.062),((0,.005,1.62),.061,.063)],m['skin'],'neck','skin')
 # A sculpted head with jaw/chin, cheek and cranial sections; helmet opening exposes face.
 loft('Anatomical head',[((0,-.025,1.605),.036,.047),((0,-.026,1.625),.061,.064),((0,-.005,1.66),.077,.079),((0,.003,1.71),.086,.087),((0,.009,1.77),.087,.09),((0,.015,1.825),.071,.076),((0,.015,1.86),.042,.048),((0,.015,1.88),.003,.004)],m['skin'],'head','head')
 # Low profile nose, lips and brow, constructed from sculpted polygon patches.
 loft('Nose bridge',[((0,-.084,1.718),.010,.004),((0,-.097,1.698),.011,.007),((0,-.108,1.684),.018,.009),((0,-.094,1.674),.013,.004)],m['skin'],'head','head',sides=10)
 tube('Mouth', [(-.026,-.083,1.651),(0,-.090,1.648),(.026,-.083,1.651)],.002,m['skin_dark'],'head',region='head')
 for s in [-1,1]:
  tube('Brow',[(s*.017,-.083,1.724),(s*.041,-.085,1.728),(s*.064,-.074,1.721)],.0035,m['skin_dark'],'head',region='head')
  # shoulder pad has a tapered hard shell, quilted fabric sleeve covers its lower edge.
  sx=s*.237
  loft('Contoured shoulder pad '+str(s),[((sx,.014,1.523),.016,.020),((s*.250,.014,1.506),.060,.071),((s*.264,.013,1.475),.079,.082),((s*.282,.010,1.435),.068,.074),((s*.289,.007,1.405),.054,.061)],m['pad'],'upper_arm.'+('L' if s>0 else 'R'),'shoulder')
  b='L' if s>0 else 'R';ua='upper_arm.'+b;fa='forearm.'+b;hand='hand.'+b
  arm=[((s*.255,.014,1.475),.079*limb,.077*limb),((s*.28,.008,1.424),.081*limb,.080*limb),((s*.313,-.004,1.354),.071*limb,.076*limb),((s*.347,-.009,1.285),.060*limb,.063*limb),((s*.380,-.010,1.218),.049*limb,.052*limb),((s*.397,-.012,1.175),.047*limb,.049*limb)]
  loft('Deltoid and bicep '+b,arm,m['skin'],ua,'arm')
  sleeve=[((s*.237,.016,1.534),.014,.019),((s*.250,.016,1.517),.064*limb,.076*limb),((s*.265,.014,1.490),.088*limb,.090*limb),((s*.275,.010,1.455),.089*limb,.088*limb),((s*.287,.006,1.420),.085*limb,.084*limb),((s*.298,.002,1.39),.079*limb,.080*limb)]
  loft('Tailored sleeve '+b,sleeve,m['jersey'],ua,'shoulder')
  tube('Sleeve piping '+b,[(s*.298+.079*limb*cos(t),.002+.08*limb*sin(t),1.39) for t in [k*2*pi/24 for k in range(25)]],.0038,m['accent'],ua)
  fore=[((s*.385,-.012,1.206),.046*limb,.047*limb),((s*.410,-.011,1.161),.055*limb,.052*limb),((s*.435,-.013,1.098),.050*limb,.047*limb),((s*.459,-.015,1.033),.037*limb,.034*limb),((s*.478,-.017,.980),.028,.026)]
  loft('Forearm flexor '+b,fore,m['skin'],fa,'forearm')
  # Athletic compression elbow sleeve / wrist tape.
  loft('Elbow compression '+b,[((s*.373,-.011,1.236),.051*limb,.053*limb),((s*.394,-.012,1.189),.052*limb,.054*limb),((s*.413,-.012,1.148),.054*limb,.054*limb)],m['compression'],fa,'arm')
  loft('Wrist cuff '+b,[((s*.467,-.016,1.011),.034,.032),((s*.477,-.017,.985),.033,.030),((s*.482,-.018,.970),.031,.029)],m['white'],fa,'glove')
  create_hand(s,m)
 # fitted pants around pelvis and anatomically shaped legs.
 loft('Pants waist',[((0,.007,.942),.157,.110),((0,.007,1.015),.160,.109),((0,.007,1.039),.157,.105)],m['pants'],'pelvis','pelvis')
 for s in [-1,1]:
  b='L' if s>0 else 'R';th='thigh.'+b;sh='shin.'+b
  thigh=[((s*.095,.007,.995),.092*limb,.101*limb),((s*.112,.010,.927),.100*limb,.111*limb),((s*.120,-.003,.840),.096*limb,.112*limb),((s*.122,-.019,.741),.082*limb,.095*limb),((s*.121,-.029,.643),.067*limb,.075*limb),((s*.121,-.033,.560),.057*limb,.062*limb),((s*.121,-.028,.515),.054*limb,.058*limb)]
  loft('Quadriceps fitted pants '+b,thigh,m['pants'],th,'thigh')
  loft('Calf and Achilles '+b,[((s*.121,-.029,.538),.053*limb,.057*limb),((s*.120,-.015,.48),.057*limb,.061*limb),((s*.118,.003,.407),.063*limb,.071*limb),((s*.116,.015,.327),.056*limb,.064*limb),((s*.115,.020,.237),.039*limb,.042*limb),((s*.115,.019,.153),.029,.033),((s*.115,.014,.100),.031,.036)],m['sock'],sh,'shin')
  # Sewn knee pocket, lateral uniform stripe, compression sock ribs.
  loft('Knee protection pocket '+b,[((s*.121,-.064,.605),.052,.035),((s*.121,-.073,.554),.053,.038),((s*.121,-.063,.512),.046,.030)],m['pants'],th,'thigh',sides=12)
  tube('Pants outer piping '+b,[(s*.203,.008,.95),(s*.218,.003,.85),(s*.204,-.002,.73),(s*.179,-.014,.60)],.0052,m['accent'],th)
  for z in [.24,.28,.32,.36]:tube('Sock knit '+b,[(s*.115+.044*cos(t),.016+.047*sin(t),z) for t in [k*2*pi/20 for k in range(21)]],.0018,m['stitch'],sh)
  create_boot(s,m)
 # V-neck binding and belt.
 tube('V-neck binding',[(-.091,-.062,1.55),(-.063,-.089,1.521),(0,-.115,1.492),(.063,-.089,1.521),(.091,-.062,1.55)],.006,m['accent'])
 tube('Belt',[(.157*cos(t),.007+.111*sin(t),1.025) for t in [k*2*pi/40 for k in range(41)]],.008,m['compression'],'pelvis')
 panel('Belt buckle',[(-.017,-.110,1.014),(.017,-.110,1.014),(.017,-.111,1.037),(-.017,-.111,1.037)],m['metal'],'pelvis')
 # Small towel is cloth with a wavy silhouette.
 vs=[]
 for j in range(7):
  z=.98-j*.025
  for i in range(4):vs.append((.05+i*.025,-.112-.008*sin(j*.9+i),z))
 fs=[(j*4+i,j*4+i+1,(j+1)*4+i+1,(j+1)*4+i) for j in range(6) for i in range(3)]
 mesh('Woven waist towel',vs,fs,m['white'],'pelvis','accessory')

def create_hand(s,m):
 b='L' if s>0 else 'R';w=Vector((s*.480,-.018,.976));d=Vector((s*.32,0,-.948));a=Vector((s*.948,0,.32));hand='hand.'+b
 # A cupped, tapered palm with four metacarpal sections and padded back.
 rings=[]
 for dist,r1,r2 in [(0,.028,.024),(.025,.037,.027),(.055,.043,.026),(.085,.039,.022),(.098,.034,.019)]:rings.append((tuple(w+d*dist),r1,r2,.025))
 loft('Glove cupped palm '+b,rings,m['glove'],hand,'glove',sides=14 if not LOD else 10)
 finger_names=['index','middle','ring','pinky'];offsets=[-.028,-.009,.011,.030];lengths=[.080,.087,.081,.067]
 for idx,(fn,off,ln) in enumerate(zip(finger_names,offsets,lengths)):
  root=w+d*.087+a*off;fd=(d+a*(off*1.8)+Vector((0,-.06,0))).normalized();r=.0108 if idx<3 else .0092
  rings=[];ws=[]
  for frac,thick in [(0,1),(.16,1.05),(.34,.94),(.48,.97),(.66,.84),(.76,.82),(.91,.70),(1,.24)]:
   p=root+fd*(ln*frac)+Vector((0,-.012*frac*frac,0));rings.append((tuple(p),r*thick,r*.83*thick,0))
  def fw(t,p,fn=fn,b=b):
   if t<.38:return {fn+'_01.'+b:1}
   if t<.72:return {fn+'_02.'+b:1}
   return {fn+'_03.'+b:1}
  loft('Articulated '+fn+' glove '+b,rings,m['glove'],fw,'finger',sides=8 if LOD else 10)
  for k in [1,2]:
   p=root+fd*(ln*(.36 if k==1 else .70))+Vector((0,.008,0))
   tube('Finger knuckle seam '+fn+b+str(k),[tuple(p+a*(-r*.75)),tuple(p),tuple(p+a*(r*.75))],.0017,m['accent'],fn+('_01.' if k==1 else '_02.')+b,region='finger')
 # Thumb leaves the radial palm and bends inward for ball control.
 root=w+d*.028-a*.029;td=(d*.65-a*.76).normalized();tr=[]
 for frac,rr in [(0,.014),(.20,.0145),(.45,.0125),(.65,.012),(.85,.010),(1,.003)]:tr.append((tuple(root+td*(.072*frac)+Vector((0,-.015*frac,0))),rr,rr*.80,0))
 def thumbw(t,p):return {'thumb_01.'+b:1} if t<.50 else {'thumb_02.'+b:1}
 loft('Opposable thumb glove '+b,tr,m['glove'],thumbw,'finger',sides=10 if not LOD else 8)
 # Palm grip pads and wrist fastening strap.
 panel('Palm grip patch '+b,[tuple(w+d*.025+a*-.027+Vector((0,-.027,0))),tuple(w+d*.075+a*-.025+Vector((0,-.027,0))),tuple(w+d*.075+a*.025+Vector((0,-.027,0))),tuple(w+d*.025+a*.027+Vector((0,-.027,0)))],m['grip'],hand,'glove')
 tube('Glove back seam '+b,[tuple(w+d*.01+a*-.023+Vector((0,.026,0))),tuple(w+d*.057+a*-.029+Vector((0,.027,0))),tuple(w+d*.087+a*-.020+Vector((0,.023,0)))],.002,m['accent'],hand,region='glove')

def create_boot(s,m):
 b='L' if s>0 else 'R';bone='foot.'+b;x=s*.115
 # Foot-specific cross sections along heel-to-toe, no ellipsoid/capsule substitute.
 sections=[(.073,.031,.055,.114),(.045,.040,.034,.145),(-.006,.045,.027,.132),(-.066,.051,.026,.105),(-.135,.052,.026,.084),(-.185,.041,.028,.068),(-.203,.018,.035,.054)]
 n=12 if LOD else 18;vs=[];fs=[]
 for y,rx,zlo,zhi in sections:
  for k in range(n):
   t=2*pi*k/n;z=zlo+(zhi-zlo)*((sin(t)+1)/2)**.8;vs.append((x+rx*cos(t),y,z))
 for j in range(len(sections)-1):
  for k in range(n):fs.append((j*n+k,j*n+(k+1)%n,(j+1)*n+(k+1)%n,(j+1)*n+k))
 fs.extend([tuple(range(n-1,-1,-1)),tuple((len(sections)-1)*n+k for k in range(n))]);mesh('Sculpted cleat upper '+b,vs,fs,m['cleat'],bone,'foot')
 outline=[(x+.046*cos(t),-.06+.135*sin(t),.028) for t in [k*2*pi/36 for k in range(37)]]
 tube('Molded outsole '+b,outline,.010,m['compression'],bone,region='foot')
 for y in [-.025,-.050,-.075,-.10]:
  tube('Cleat laces '+b,[(x-.022,y,.114+y*.20),(x+.021,y-.013,.114+(y-.013)*.20)],.0024,m['white'],bone,region='foot')
 for side in [-1,1]:
  for y in [.045,-.025,-.10,-.16]:
   xx=x+side*(.027 if y==.045 else .034)
   loft('Traction stud '+b,[((xx,y,.031),.010,.009),((xx,y,.012),.007,.006),((xx,y,.007),.004,.004)],m['metal'],bone,'foot',sides=6)
 for ss in [-1,1]:tube('Cleat stitched panel '+b,[(x+ss*.035,.034,.104),(x+ss*.043,-.025,.102),(x+ss*.046,-.095,.075),(x+ss*.030,-.176,.062)],.0020,m['accent'],bone,region='foot')

def helmet(m):
 # Open helmet shell: a longitudinally shaped shell with a raised frontal face opening.
 rows=11 if LOD else 17;n=30 if LOD else 48;vs=[];fs=[]
 for j in range(rows):
  u=j/(rows-1)
  for k in range(n):
   a=k*2*pi/n;front=max(0,-sin(a));bottom=1.660+.100*front**5
   z=bottom+(1.940-bottom)*u
   q=(z-1.770)/.170;rr=math.sqrt(max(.0008,1-q*q))
   x=.130*cos(a)*rr*(1+.025*cos(2*a));y=.010+.147*sin(a)*rr
   vs.append((x,y,z))
 for j in range(rows-1):
  for k in range(n):fs.append((j*n+k,j*n+(k+1)%n,(j+1)*n+(k+1)%n,(j+1)*n+k))
 fs.append(tuple((rows-1)*n+k for k in range(n)))
 ob=mesh('Engineered open helmet shell',vs,fs,m['helmet'],'head','helmet')
 # Rim has dimensional thickness and facemask molded attachment lugs.
 rim=[vs[k] for k in range(n)]+[vs[0]];tube('Helmet rubber edge',rim,.0055,m['compression'],'head',region='helmet')
 for s in [-1,1]:
  tube('Raised helmet stripe '+str(s),[(s*.014,.010+.149*sin(t),1.770+.173*cos(t)) for t in [(-1.20+k*2.55/28) for k in range(29)]],.0048,m['accent'],'head',region='helmet')
  # Sculpted vent opening trim and ear cover panels.
  for yy,zz in [(-.038,1.908),(.045,1.910),(.091,1.85)]:
   tube('Recessed helmet vent '+str(s),[(s*.074,yy-.016,zz-.014),(s*.087,yy-.014,zz),(s*.088,yy+.016,zz+.005)],.004,m['compression'],'head',region='helmet')
  panel('Helmet ear plate '+str(s),[(s*.112,-.030,1.745),(s*.114,.044,1.737),(s*.106,.060,1.676),(s*.108,-.029,1.678)],m['helmet'],'head','helmet')
  tube('Chin strap '+str(s),[(s*.108,.016,1.70),(s*.080,-.071,1.625),(s*.026,-.086,1.613)],.004,m['white'],'head',region='helmet')
  # Facemask side rail and multiple individual bars with visibility openings.
  tube('Facemask side rail '+str(s),[(s*.103,-.046,1.753),(s*.133,-.092,1.727),(s*.121,-.163,1.686),(s*.080,-.188,1.670)],.0062,m['metal'],'head',region='helmet')
 for z,y in [(1.724,-.173),(1.670,-.185),(1.625,-.162)]:
  tube('Facemask horizontal cage',[(.117*cos(t),y-.020*sin(t),z+.010*cos(2*t)) for t in [k*pi/24 for k in range(25)]],.0057,m['metal'],'head',region='helmet')
 for x in [-.056,.056]:tube('Facemask vertical cage',[(x,-.176,1.730),(x,-.198,1.676),(x*.85,-.172,1.621)],.0055,m['metal'],'head',region='helmet')
 # Curved transparent tinted visor: shape follows the frontal opening.
 vs=[];fs=[]
 for row,z in enumerate([1.768,1.744,1.720]):
  for k in range(15):
   t=-1+2*k/14;vs.append((t*.093,-.120-.035*(1-t*t),z-.006*t*t))
 for j in range(2):
  for k in range(14):fs.append((j*15+k,j*15+k+1,(j+1)*15+k+1,(j+1)*15+k))
 mesh('Optical curved visor',vs,fs,m['visor'],'head','helmet')
 loft('Chin cup',[((0,-.087,1.606),.026,.012),((0,-.091,1.620),.036,.015),((0,-.090,1.632),.029,.010)],m['white'],'head','helmet',sides=12)

def jersey_text(text,loc,size,material,bone='chest',back=False):
 cu=bpy.data.curves.new('Athlete typography','FONT');cu.body=str(text);cu.align_x='CENTER';cu.align_y='CENTER';cu.size=size;cu.extrude=.0007;cu.resolution_u=4
 ob=bpy.data.objects.new('Jersey '+str(text),cu);bpy.context.collection.objects.link(ob);ob.location=coord(loc);ob.rotation_euler=(pi/2,0,pi if back else 0);ob.scale=(ACTIVE['width'],ACTIVE['height']/1.88,1);cu.materials.append(material)
 bpy.ops.object.select_all(action='DESELECT');ob.select_set(True);bpy.context.view_layer.objects.active=ob;bpy.ops.object.convert(target='MESH');ob=bpy.context.object
 vg=ob.vertex_groups.new(name=bone);vg.add(list(range(len(ob.data.vertices))),1,'REPLACE');ob['surface_region']='lettering';PARTS.append(ob)

def bone_add(name,head,tail,parent=None):BONES[name]=(head,tail,parent)
def rig():
 BONES.clear();bone_add('root',(0,0,0),(0,0,.18));bone_add('pelvis',(0,0,.97),(0,0,1.10),'root');bone_add('spine',(0,0,1.10),(0,.01,1.30),'pelvis');bone_add('chest',(0,.01,1.30),(0,.01,1.51),'spine');bone_add('neck',(0,.01,1.51),(0,.01,1.64),'chest');bone_add('head',(0,.01,1.64),(0,.01,1.88),'neck')
 for s in [-1,1]:
  b='L' if s>0 else 'R';bone_add('clavicle.'+b,(s*.04,.01,1.48),(s*.255,.014,1.475),'chest');bone_add('upper_arm.'+b,(s*.255,.014,1.475),(s*.385,-.012,1.206),'clavicle.'+b);bone_add('forearm.'+b,(s*.385,-.012,1.206),(s*.480,-.018,.976),'upper_arm.'+b);bone_add('hand.'+b,(s*.480,-.018,.976),(s*.510,-.018,.887),'forearm.'+b)
  bone_add('thigh.'+b,(s*.095,.007,.995),(s*.121,-.029,.538),'pelvis');bone_add('shin.'+b,(s*.121,-.029,.538),(s*.115,.014,.113),'thigh.'+b);bone_add('foot.'+b,(s*.115,.014,.113),(s*.115,-.155,.043),'shin.'+b);bone_add('toe.'+b,(s*.115,-.125,.045),(s*.115,-.203,.045),'foot.'+b)
  w=Vector((s*.480,-.018,.976));d=Vector((s*.32,0,-.948));a=Vector((s*.948,0,.32))
  for fn,off,ln in zip(['index','middle','ring','pinky'],[-.028,-.009,.011,.030],[.080,.087,.081,.067]):
   root=w+d*.087+a*off;fd=(d+a*(off*1.8)+Vector((0,-.06,0))).normalized()
   parent='hand.'+b
   for k,(lo,hi) in enumerate([(0,.38),(.38,.72),(.72,1)]):
    name=fn+'_'+str(k+1).zfill(2)+'.'+b;bone_add(name,tuple(root+fd*ln*lo),tuple(root+fd*ln*hi),parent);parent=name
  rt=w+d*.028-a*.029;td=(d*.65-a*.76).normalized()
  bone_add('thumb_01.'+b,tuple(rt),tuple(rt+td*.036),'hand.'+b);bone_add('thumb_02.'+b,tuple(rt+td*.036),tuple(rt+td*.072),'thumb_01.'+b)
 ar=bpy.data.armatures.new('Football athlete humanoid 50-bone');ob=bpy.data.objects.new('AthleteRig',ar);bpy.context.collection.objects.link(ob);bpy.context.view_layer.objects.active=ob;ob.select_set(True);bpy.ops.object.mode_set(mode='EDIT')
 for n,(h,t,p) in BONES.items():eb=ar.edit_bones.new(n);eb.head=coord(h);eb.tail=coord(t)
 for n,(h,t,p) in BONES.items():
  if p:ar.edit_bones[n].parent=ar.edit_bones[p]
 bpy.ops.object.mode_set(mode='OBJECT')
 for pb in ob.pose.bones:pb.rotation_mode='XYZ'
 return ob

def join_meshes(arm):
 bpy.ops.object.select_all(action='DESELECT')
 for ob in PARTS:ob.select_set(True)
 bpy.context.view_layer.objects.active=PARTS[0];bpy.ops.object.join();ob=bpy.context.object;ob.name=ACTIVE['id']+('_LOD1' if LOD else '_LOD0')
 bpy.ops.object.transform_apply(location=True,rotation=True,scale=True)
 # Region independent full surface deformation; equipment and lettering share it.
 basis=ob.shape_key_add(name='Basis');scale=ACTIVE['height']/1.88;w=ACTIVE['width'];dep=ACTIVE['depth']
 for name in MORPHS:
  sk=ob.shape_key_add(name=name);sk.slider_min=0;sk.slider_max=1
  for i,v in enumerate(basis.data):
   p=v.co.copy();x,y,z=p.x/w,p.y/dep,p.z/scale;s=1 if x>=0 else -1;zz=z
   if name=='Muscularity':
    torso=max(0,1-abs(z-1.34)/.28)*max(0,1-abs(x)/.32)
    if abs(x)<.26 and 1.05<z<1.54:p.x+=s*.016*torso*w;p.y+=(-1 if y<0 else 1)*.018*torso*dep
    elif .98<z<1.43 and abs(x)>.23:p.x+=s*.008*sin(pi*min(1,(z-.98)/.45))*w;p.y+=(-1 if y<-.01 else 1)*.012*sin(pi*min(1,(z-.98)/.45))*dep
    elif .23<z<.97:p.x+=s*.008*sin(pi*(z-.23)/.74)*w;p.y+=(-1 if y<0 else 1)*.013*sin(pi*(z-.23)/.74)*dep
   elif name=='ShoulderWidth':
    f=min(1,max(0,(z-1.04)/.36))*min(1,max(0,(1.60-z)/.08));p.x+=s*.030*f*min(1,abs(x)/.15)*w
   elif name=='ChestSize':
    f=max(0,1-abs(z-1.36)/.22)*max(0,1-abs(x)/.27);p.x+=x*.09*f*w;p.y+=(-1 if y<0 else 1)*.026*f*dep
   elif name=='ArmThickness':
    if .98<z<1.47 and abs(x)>.22:
     cx=.48-(z-.976)*.46;f=sin(pi*(z-.976)/.50);p.x+=(x-s*cx)*.23*f*w;p.y+=(y+.014)*.24*f*dep
   elif name=='LegPower':
    if .13<z<1.02:
     f=sin(pi*(z-.13)/.89);p.x+=(x-s*.116)*.22*f*w;p.y+=(y-.005)*.23*f*dep
   elif name=='BodyWeight':
    f=max(0,1-abs(z-1.14)/.40);p.x+=x*.12*f*w;p.y+=y*.23*f*dep
   elif name in ['Wingspan','Reach']:
    if abs(x)>.22 and z>.74:
     f=min(1,(abs(x)-.22)/.23);amount=.045 if name=='Wingspan' else .025;p.x+=s*amount*f*w;p.z-=amount*.55*f*scale
   elif name=='HandSize':
    if abs(x)>.465 and z<1.005:
     p.x+=(x-s*.480)*.20*w;p.y+=(y+.018)*.20*dep;p.z+=(z-.976)*.20*scale
   sk.data[i].co=p
  sk.value=0
 mod=ob.modifiers.new('Humanoid skin deformation','ARMATURE');mod.object=arm;ob.parent=arm
 ob['physique_morph_semantics']='Positive deltas from each archetype baseline. Runtime rest-bone adjustment required for ShoulderWidth, Wingspan, HandSize and Reach; see manifest.'
 return ob

def pose_key(arm,f,rot=None,root=(0,0,0),fingers=0):
 # Every key writes the complete skeleton. Euler values are local XYZ radians.
 rot=rot or {};bpy.context.scene.frame_set(f)
 for pb in arm.pose.bones:
  pb.rotation_euler=rot.get(pb.name,(0,0,0));pb.location=(0,0,0);pb.scale=(1,1,1)
  if any(pb.name.startswith(n) for n in ['index_','middle_','ring_','pinky_']):pb.rotation_euler.x=-fingers*(.9 if '_01' in pb.name else 1.15)
  if pb.name.startswith('thumb_'):pb.rotation_euler.x=-fingers*.55
  if pb.name=='root':pb.location=pb.bone.matrix_local.to_3x3().inverted() @ coord(root)
  pb.keyframe_insert('rotation_euler',frame=f,group=pb.name)
  if pb.name=='root':pb.keyframe_insert('location',frame=f,group=pb.name)

def action(arm,name,duration,keys,loop=False,contact=None,rootmotion='in_place'):
 ac=bpy.data.actions.new(name);arm.animation_data_create();arm.animation_data.action=ac
 for t,rot,root,fin in keys:pose_key(arm,1+round(t*30),rot,root,fin)
 # Correct authored feet-origin falls/dives against the actual deformed surface.
 # This also prevents a local/world root-axis mismatch from hiding below turf.
 # Sample first, then write corrections, so earlier fixes cannot bias later ones.
 athlete=next((o for o in bpy.context.scene.objects if o.type=='MESH' and o.parent==arm),None)
 if athlete:
  samples=[];inv=arm.data.bones['root'].matrix_local.to_3x3().inverted()
  for frame in range(1,2+round(duration*30)):
   bpy.context.scene.frame_set(frame);bpy.context.view_layer.update();ev=athlete.evaluated_get(bpy.context.evaluated_depsgraph_get());me=ev.to_mesh();low=min(v.co.z for v in me.vertices);ev.to_mesh_clear();p=arm.pose.bones['root'].location.copy()
   if low<.005:p+=inv@Vector((0,0,.005-low))
   samples.append((frame,p))
  for frame,p in samples:arm.pose.bones['root'].location=p;arm.pose.bones['root'].keyframe_insert('location',frame=frame,group='root')
 ac.use_fake_user=True
 # exporter samples bezier channels to preserve coherent clips.
 CLIPS[name]={'duration_seconds':duration,'loop':loop,'root_motion':rootmotion,'hand_contact_windows':contact or [],'fps':30}
 track=arm.animation_data.nla_tracks.new();track.name=name;strip=track.strips.new(name,1,ac);strip.action_frame_start=1;strip.action_frame_end=1+round(duration*30);strip.frame_end=1+round(duration*30);track.mute=True
 return ac

def animations(arm):
 def R(**kw):return kw
 def basebend(s=1):return {'spine':(.06,0,0),'chest':(.03,0,0),'upper_arm.L':(.1,0,-.12),'upper_arm.R':(.1,0,.12),'forearm.L':(-.45,0,0),'forearm.R':(-.45,0,0),'thigh.L':(.12,0,0),'thigh.R':(.12,0,0),'shin.L':(-.16,0,0),'shin.R':(-.16,0,0)}
 action(arm,'idle_ready',2,[(0,basebend(),(0,0,0),.18),(1,{**basebend(),'chest':(.048,0,0)},(0,0,.004),.22),(2,basebend(),(0,0,0),.18)],True)
 for name,speed,amp in [('walk',1.0,.42),('jog',.74,.68),('sprint',.62,1.03),('acceleration',1.20,1.04)]:
  keys=[]
  for k in range(9):
   t=k/8;phase=2*pi*t;pose=basebend();a=amp*(.45+.55*t) if name=='acceleration' else amp
   for side,sign in [('L',1),('R',-1)]:
    st=sin(phase)*sign;pose['thigh.'+side]=(a*st+.12,0,0);pose['shin.'+side]=(-.14-max(0,-st)*a*.9,0,0);pose['foot.'+side]=(max(0,st)*.2,0,0);pose['upper_arm.'+side]=(-st*a*.7,0,(-.12 if side=='L' else .12));pose['forearm.'+side]=(-.90-abs(st)*.12,0,0)
   pose['spine']=(.13 if name=='acceleration' else .07,0,0);keys.append((t*speed,pose,(0,0,.020*(1-cos(phase*2))),.25))
  action(arm,name,speed,keys,name!='acceleration',rootmotion='in_place_runtime_displacement')
 for name,sgn in [('plant_left',1),('plant_right',-1),('route_cut_left',1),('route_cut_right',-1)]:
  dur=.70 if name.startswith('plant') else .90;middle=basebend();middle.update({'root':(0,0,sgn*.18),'pelvis':(.15,sgn*.12,sgn*.18),'chest':(.20,-sgn*.12,-sgn*.22),'thigh.L':(.55,0,sgn*.25),'thigh.R':(.13,0,sgn*.24),'shin.L':(-.8,0,0),'shin.R':(-.30,0,0)})
  action(arm,name,dur,[(0,basebend(),(0,0,0),.20),(dur*.38,middle,(0,0,-.075),.26),(dur,{**basebend(),'pelvis':(0,0,-sgn*.32)},(0,0,0),.22)])
 # Upper arm Z swings lift A-pose hands to the overhead target; elbow flex shapes cupped catch.
 overhead={'upper_arm.L':(-.10,0,-2.83),'upper_arm.R':(-.10,0,2.83),'forearm.L':(-.22,0,0),'forearm.R':(-.22,0,0),'hand.L':(.16,0,-.12),'hand.R':(.16,0,.12),'chest':(-.08,0,0),'head':(-.18,0,0),'thigh.L':(.15,0,0),'thigh.R':(.18,0,0),'shin.L':(-.3,0,0),'shin.R':(-.34,0,0)}
 jumpkeys=[(0,basebend(),(0,0,0),.2),(.20,{**basebend(),'thigh.L':(.65,0,0),'thigh.R':(.65,0,0),'shin.L':(-1.0,0,0),'shin.R':(-1.0,0,0)},(0,0,-.12),.1),(.54,overhead,(0,0,.48),.12),(.72,overhead,(0,0,.58),.62),(1.05,basebend(),(0,0,.03),.70),(1.28,basebend(),(0,0,0),.35)]
 action(arm,'jump',1.28,jumpkeys)
 action(arm,'catch_high_point',1.28,jumpkeys,contact=[[.53,.77]])
 one={**overhead,'upper_arm.R':(.2,0,.35),'forearm.L':(-.10,0,0),'hand.L':(.06,0,-.26),'chest':(-.08,.12,-.16)}
 action(arm,'catch_one_hand',1.40,[(0,basebend(),(0,0,0),.1),(.3,one,(0,0,.23),.05),(.65,one,(0,0,.50),.12),(.79,{**one,'forearm.L':(-.30,0,0)},(0,0,.42),.82),(1.4,basebend(),(0,0,0),.75)],contact=[[.56,.79]])
 chestcatch={'upper_arm.L':(-1.1,0,-.5),'upper_arm.R':(-1.1,0,.5),'forearm.L':(-1.0,0,-.35),'forearm.R':(-1.0,0,.35),'hand.L':(.35,0,-.1),'hand.R':(.35,0,.1),'chest':(.07,0,0)}
 for name,turn in [('catch_two_hand',0),('catch_back_shoulder',.68),('catch_over_shoulder',-.65),('interception_snatch',.14)]:
  reach={**chestcatch,'pelvis':(0,0,turn*.40),'chest':(.04,0,turn),'head':(-.05,0,turn*.35)}
  action(arm,name,1.2,[(0,basebend(),(0,0,0),.1),(.38,reach,(0,0,.02),.05),(.57,reach,(0,0,.025),.75),(.83,{**reach,'forearm.L':(-1.40,0,-.35),'forearm.R':(-1.40,0,.35)},(0,0,0),.90),(1.2,basebend(),(0,0,0),.65)],contact=[[.38,.65]])
 dive={'root':(1.30,0,0),'spine':(0,0,0),'chest':(0,0,0),'upper_arm.L':(0,0,-2.83),'upper_arm.R':(0,0,2.83),'forearm.L':(0,0,0),'forearm.R':(0,0,0),'hand.L':(0,0,0),'hand.R':(0,0,0),'thigh.L':(0,0,0),'thigh.R':(0,0,0),'shin.L':(0,0,0),'shin.R':(0,0,0),'foot.L':(.90,0,0),'foot.R':(.90,0,0)}
 for name,offset in [('catch_dive',0),('defense_dive_pbu',.18),('interception_dive',-.12)]:
  dp={**dive,'chest':(-.08,offset,0)}
  action(arm,name,1.85,[(0,basebend(),(0,0,0),.15),(.28,{**basebend(),'spine':(.25,0,0)},(0,-.12,-.08),.05),(.64,dp,(0,-.65,.10),.08),(.85,dp,(0,-.90,.05),.75 if 'pbu' not in name else .10),(1.2,{**dp,'root':(1.48,0,0)},(0,-1.10,0),.85),(1.85,{**dp,'root':(1.48,0,0)},(0,-1.10,0),.65)],contact=[[.60,.90]],rootmotion='authored_local_displacement_commit_through_1.85s')
 toetap={**chestcatch,'pelvis':(.08,0,-.30),'chest':(-.12,0,.25),'thigh.L':(.1,0,0),'thigh.R':(.52,0,0),'shin.L':(-.06,0,0),'shin.R':(-.8,0,0),'foot.L':(.5,0,0),'foot.R':(.45,0,0)}
 action(arm,'catch_toe_tap',1.35,[(0,basebend(),(0,0,0),.1),(.35,toetap,(.08,0,.12),.1),(.57,toetap,(.10,0,.08),.8),(.8,{**toetap,'thigh.R':(.13,0,0),'shin.R':(-.12,0,0)},(.12,0,.02),.85),(1.35,basebend(),(.12,0,0),.65)],contact=[[.34,.65]])
 swat={**overhead,'upper_arm.R':(.10,0,.18),'upper_arm.L':(-.7,0,-2.10),'forearm.L':(-.10,0,0),'chest':(-.1,0,-.30)}
 for name,jump in [('defense_standing_swat',0),('defense_jump_swat',.47),('defense_trailing_breakup',.06),('defense_tip_drill',.29)]:
  action(arm,name,1.2,[(0,basebend(),(0,0,0),.15),(.30,swat,(0,0,jump*.6),.05),(.51,{**swat,'upper_arm.L':(-.9,0,-2.45)},(0,0,jump),0),(.73,{**swat,'upper_arm.L':(-1.0,0,-1.85)},(0,0,jump*.4),.1),(1.2,basebend(),(0,0,0),.15)],contact=[[.38,.68]])
 action(arm,'interception_undercut',1.35,[(0,basebend(),(0,0,0),.2),(.3,{**basebend(),'pelvis':(.1,0,.4),'chest':(.1,0,-.3)},(.18,-.10,-.05),.1),(.65,chestcatch,(.35,-.35,.05),.05),(.85,chestcatch,(.35,-.45,0),.85),(1.35,basebend(),(.35,-.6,0),.75)],contact=[[.60,.90]],rootmotion='authored_local_displacement')
 tackle={**basebend(),'spine':(.50,0,0),'upper_arm.L':(-1.1,0,-.55),'upper_arm.R':(-1.1,0,.55),'forearm.L':(-.90,0,-.3),'forearm.R':(-.90,0,.3),'thigh.L':(.65,0,0),'shin.L':(-.8,0,0)}
 action(arm,'tackle_wrap',1.55,[(0,basebend(),(0,0,0),.1),(.35,tackle,(0,-.2,-.12),.2),(.65,{**tackle,'forearm.L':(-1.4,0,-.5),'forearm.R':(-1.4,0,.5)},(0,-.5,-.18),.8),(1.10,{**tackle,'root':(.7,0,.15)},(0,-.75,-.35),.85),(1.55,{**dive,'root':(1.48,0,.08),'upper_arm.L':(0,0,-.15),'upper_arm.R':(0,0,.15),'forearm.L':(-.12,0,0),'forearm.R':(-.12,0,0)},(0,-.85,0),.7)],rootmotion='authored_local_displacement')
 fallen={'root':(1.50,0,.08),'spine':(0,0,0),'chest':(0,0,0),'upper_arm.L':(0,0,-.05),'upper_arm.R':(0,0,.05),'forearm.L':(0,0,0),'forearm.R':(0,0,0),'hand.L':(0,0,0),'hand.R':(0,0,0),'thigh.L':(0,0,0),'thigh.R':(0,0,0),'shin.L':(0,0,0),'shin.R':(0,0,0),'foot.L':(.90,0,0),'foot.R':(.90,0,0)}
 action(arm,'fall',1.2,[(0,basebend(),(0,0,0),.2),(.4,{**fallen,'root':(.70,0,.12)},(0,-.1,-.15),.4),(.85,fallen,(0,-.35,-.72),.5),(1.2,fallen,(0,-.4,-.72),.4)],rootmotion='authored_local_displacement')
 action(arm,'recovery_get_up',2.0,[(0,fallen,(0,-.4,-.72),.3),(.6,{**basebend(),'root':(.6,0,0),'thigh.L':(1.3,0,0),'shin.L':(-1.4,0,0),'upper_arm.L':(-1.2,0,-.3)},(0,-.2,-.45),.1),(1.3,{**basebend(),'thigh.L':(.8,0,0),'shin.L':(-1.2,0,0)},(0,-.05,-.17),.1),(2.0,basebend(),(0,0,0),.2)],rootmotion='authored_local_displacement')
 action(arm,'celebration_touchdown',2.4,[(0,basebend(),(0,0,0),.3),(.6,overhead,(0,0,.02),.8),(1.2,{**overhead,'chest':(-.12,0,-.18)},(0,0,.06),.9),(1.8,{**overhead,'chest':(-.12,0,.18)},(0,0,.02),.9),(2.4,basebend(),(0,0,0),.4)])
 # QB throw and defensive stance extend the mandatory library for integration.
 action(arm,'qb_release',1.1,[(0,{**chestcatch,'upper_arm.R':(-1.4,0,1.5),'forearm.R':(-1.4,0,0),'chest':(0,0,-.4)},(0,0,0),.7),(.40,{**chestcatch,'upper_arm.R':(-1.6,0,.4),'forearm.R':(-.10,0,0),'chest':(.12,0,.25)},(0,-.06,.02),.1),(1.1,basebend(),(0,-.10,0),.2)])
 action(arm,'defense_backpedal',.90,[(0,{**basebend(),'thigh.L':(.4,0,0),'thigh.R':(-.2,0,0),'shin.L':(-.6,0,0)},(0,0,0),.1),(.45,{**basebend(),'thigh.L':(-.2,0,0),'thigh.R':(.4,0,0),'shin.R':(-.6,0,0)},(0,0,.02),.1),(.90,{**basebend(),'thigh.L':(.4,0,0),'thigh.R':(-.2,0,0),'shin.L':(-.6,0,0)},(0,0,0),.1)],True,rootmotion='in_place_runtime_displacement')
 arm.animation_data.action=None
 for tr in arm.animation_data.nla_tracks:tr.mute=False
 bpy.context.scene.frame_set(1)
 return list(CLIPS)

def materials(palette,index):
 primary,accent,white=PALETTES[palette];skincolors=[(.34,.16,.09,1),(.68,.39,.23,1),(.17,.070,.035,1),(.51,.26,.14,1)]
 skin=skincolors[index%len(skincolors)]
 return {'jersey':mat('Jersey | primary',primary,texture=True),'accent':mat('Uniform | accent',accent,.5,texture=True),'trim':mat('Uniform | seams',accent,.65),'stitch':mat('Woven stitch',(.20,.23,.26,1),.8),'pants':mat('Pants | team',(.79,.83,.86,1) if palette!='ice' else primary,.75,texture=True),'skin':mat('Skin',skin,.53),'skin_dark':mat('Skin detail',tuple(v*.36 for v in skin[:3])+(1,),.6),'pad':mat('Shoulder pad | impact foam',(.085,.095,.11,1),.9),'compression':mat('Compression textile',(.018,.024,.033,1),.84,texture=True),'sock':mat('Compression sock',white,.8,texture=True),'white':mat('Uniform | number',white,.62,texture=True),'helmet':mat('Helmet | enamel',primary,.24,.20),'metal':mat('Equipment | titanium',(.16,.19,.23,1),.25,.72),'visor':mat('Optical visor',(.045,.15,.19,1),.12,.32,alpha=.50),'glove':mat('Glove | leather',accent,.53,texture=True),'grip':mat('Glove | silicone grip',(.029,.034,.041,1),.83,texture=True),'cleat':mat('Cleat | synthetic',primary,.40,.06)}

def correct_twist_axes(paths):
 """Convert intended torso turn channels to anatomical twist, rest-relative.
 Final GLB rotations are restQuaternion * localPoseQuaternion. Never swap
 components of the exported quaternion itself. Appends fresh accessors to
 avoid changing an accessor shared by another action or joint.
 """
 channels={'catch_back_shoulder':{'pelvis','chest','head'},'catch_over_shoulder':{'pelvis','chest','head'},'qb_release':{'chest'}}
 pocket_clips={'catch_two_hand','catch_back_shoulder','catch_over_shoulder','interception_snatch','catch_toe_tap'}
 for path in paths:
  data=open(path,'rb').read();jl=struct.unpack_from('<I',data,12)[0];doc=json.loads(data[20:20+jl]);offset=20+jl;blen,btype=struct.unpack_from('<II',data,offset);binary=bytearray(data[offset+8:offset+8+blen]);binary=binary[:doc['buffers'][0]['byteLength']]
  extras=doc.get('asset',{}).get('extras',{});twist_done=extras.get('rwqbTwistAxesCorrected',False);pocket_done=extras.get('rwqbHandPocketCorrected',False);pocket_v2=extras.get('rwqbHandPocketVersion',0)>=2
  if twist_done and pocket_v2:continue
  changed=0
  for animation in doc.get('animations',[]):
   animation_name=animation.get('name','');targets=channels.get(animation_name,set());pocket=animation_name in pocket_clips
   if not targets and not pocket:continue
   for channel in animation['channels']:
    target=channel['target'];node=doc['nodes'][target['node']]
    twist=(node.get('name') in targets and not twist_done);widen=(pocket and node.get('name') in ['upper_arm.L','upper_arm.R','forearm.L','forearm.R'] and not pocket_v2)
    if target['path']!='rotation' or not(twist or widen):continue
    sampler=animation['samplers'][channel['sampler']]
    if sampler.get('interpolation','LINEAR') not in ['LINEAR','STEP']:raise RuntimeError('Twist correction requires sampled quaternion channels')
    accessor=doc['accessors'][sampler['output']];view=doc['bufferViews'][accessor['bufferView']]
    if accessor['componentType']!=5126 or accessor['type']!='VEC4':raise RuntimeError('Expected float quaternion accessor')
    qv=node.get('rotation',[0,0,0,1]);rest=Quaternion((qv[3],qv[0],qv[1],qv[2]));rest.normalize();inverse=rest.conjugated();output=bytearray();previous=None
    start=view.get('byteOffset',0)+accessor.get('byteOffset',0);stride=view.get('byteStride',16)
    for i in range(accessor['count']):
     x,y,z,w=struct.unpack_from('<4f',binary,start+i*stride);q=Quaternion((w,x,y,z));q.normalize();relative=inverse@q;angles=relative.to_euler('XYZ')
     if twist:angles=Euler((angles.x,angles.z,angles.y),'XYZ')
     if widen:
      sign=1 if node.get('name').endswith('.L') else -1
      angles.z+=sign*((.35 if pocket_done else .445) if node.get('name').startswith('upper_arm.') else -.17)
     corrected=rest@angles.to_quaternion();corrected.normalize()
     if previous is not None and corrected.dot(previous)<0:corrected.negate()
     previous=corrected.copy();output.extend(struct.pack('<4f',corrected.x,corrected.y,corrected.z,corrected.w))
    binary.extend(b'\0'*((-len(binary))%4));start=len(binary);binary.extend(output);doc['bufferViews'].append({'buffer':0,'byteOffset':start,'byteLength':len(output)})
    doc['accessors'].append({'bufferView':len(doc['bufferViews'])-1,'componentType':5126,'count':accessor['count'],'type':'VEC4'});sampler['output']=len(doc['accessors'])-1;changed+=1
  doc.setdefault('asset',{}).setdefault('extras',{})['rwqbTwistAxesCorrected']=True;doc['asset']['extras']['rwqbHandPocketCorrected']=True;doc['asset']['extras']['rwqbHandPocketVersion']=2;doc['asset']['extras']['rwqbExportPoseChannelCorrections']=extras.get('rwqbExportPoseChannelCorrections',extras.get('rwqbTwistChannelCount',0))+changed
  doc['buffers'][0]['byteLength']=len(binary);binary.extend(b'\0'*((-len(binary))%4));js=json.dumps(doc,separators=(',',':')).encode('utf8');js+=b' '*((-len(js))%4)
  with open(path,'wb') as f:f.write(struct.pack('<III',0x46546c67,2,28+len(js)+len(binary)));f.write(struct.pack('<II',len(js),0x4e4f534a));f.write(js);f.write(struct.pack('<II',len(binary),0x004e4942));f.write(binary)
  print('TWIST_AXES_CORRECTED '+os.path.basename(path)+' channels='+str(changed),flush=True)

def export(path,objects=None,anims=True):
 bpy.ops.object.select_all(action='DESELECT')
 if objects:
  for ob in objects:ob.select_set(True)
 else:
  for ob in bpy.context.scene.objects:
   if ob.type in ['MESH','ARMATURE']:ob.select_set(True)
 bpy.ops.export_scene.gltf(filepath=path,export_format='GLB',use_selection=True,export_apply=False,export_texcoords=True,export_normals=True,export_tangents=True,export_materials='EXPORT',export_image_format='AUTO',export_cameras=False,export_lights=False,export_animations=anims,export_animation_mode='NLA_TRACKS',export_nla_strips=True,export_frame_range=False,export_force_sampling=True,export_anim_slide_to_zero=True,export_morph=True,export_morph_normal=True,export_skins=True,export_all_influences=False,export_rest_position_armature=True,export_extras=True)
 # Blender's glTF exporter omits the constant tint from a linked MixRGB fabric
 # shader. Serialize the explicit diffuse tint as standard glTF PBR factors so
 # the authored palette is identical in Blender and actual GLB consumers.
 data=open(path,'rb').read();jl=struct.unpack_from('<I',data,12)[0];doc=json.loads(data[20:20+jl]);tail=data[20+jl:]
 for material in doc.get('materials',[]):
  authored=bpy.data.materials.get(material.get('name',''))
  if authored:
   rgba=list(authored.diffuse_color);material.setdefault('pbrMetallicRoughness',{})['baseColorFactor']=rgba
   if rgba[3]<.999:material['alphaMode']='BLEND';material['doubleSided']=True
 js=json.dumps(doc,separators=(',',':')).encode('utf8');js+=b' '*((-len(js))%4)
 with open(path,'wb') as f:f.write(struct.pack('<III',0x46546c67,2,20+len(js)+len(tail)));f.write(struct.pack('<II',len(js),0x4e4f534a));f.write(js);f.write(tail)
 correct_twist_axes([path])

def build(archetype,index,lod=False):
 global ACTIVE,LOD,PARTS
 clean();PARTS=[];LOD=lod
 ident,folder,label,h,kg,wd,dp,mus,no,pal=archetype
 ACTIVE={'id':ident,'height':h,'weight':kg,'width':wd,'depth':dp,'muscle':mus}
 mats=materials(pal,index);body(mats);helmet(mats)
 jersey_text(no,(0,-.133,1.345),.185,mats['white']);jersey_text(no,(0,.143,1.355),.190,mats['white'],back=True);jersey_text('WINDOW',(0,-.140,1.471),.027,mats['white']);jersey_text(label.upper().replace('QUARTERBACK','QB')[:18],(0,.126,1.498),.025,mats['white'],back=True)
 for s in [-1,1]:jersey_text(no,(s*.122,-.110,.845),.053,mats['jersey'],'thigh.'+('L' if s>0 else 'R'))
 arm=rig();ob=join_meshes(arm);clips=animations(arm)
 ob['archetype']=label;ob['reference_height_m']=h;ob['reference_weight_kg']=kg;ob['forward_axis']='-Y in Blender; +Z after glTF Y-up conversion';ob['lod']=1 if lod else 0
 path=os.path.join(PACK,'models',folder,ident+('_lod1' if lod else '')+'.glb');export(path,[ob,arm])
 dims=[round(float(v),5) for v in ob.dimensions]
 def gltf(p):q=coord(p);return [round(q.x,6),round(q.z,6),round(-q.y,6)]
 shoulder=coord(BONES['upper_arm.L'][0]);elbow=coord(BONES['forearm.L'][0]);wrist=coord(BONES['hand.L'][0]);d=Vector((.32,0,-.948));a=Vector((.948,0,.32));tip=Vector((.480,-.018,.976))+d*.087+a*(-.009)+(d+a*(-.009*1.8)+Vector((0,-.06,0))).normalized()*.087
 armreach=(shoulder-elbow).length+(elbow-wrist).length+(coord(tip)-wrist).length
 anchors={}
 for s in [-1,1]:
  b='L' if s>0 else 'R';w=Vector((s*.480,-.018,.976));dd=Vector((s*.32,0,-.948));aa=Vector((s*.948,0,.32));anchors[b]={'shoulder':gltf(BONES['upper_arm.'+b][0]),'elbow':gltf(BONES['forearm.'+b][0]),'wrist':gltf(BONES['hand.'+b][0]),'palm_center':gltf(w+dd*.055+Vector((0,-.027,0))),'palm_contact_normal':[0,0,1],'fingertips':{}}
  for fn,off,ln in zip(['index','middle','ring','pinky'],[-.028,-.009,.011,.030],[.080,.087,.081,.067]):
   fd=(dd+aa*(off*1.8)+Vector((0,-.06,0))).normalized();anchors[b]['fingertips'][fn]=gltf(w+dd*.087+aa*off+fd*ln+Vector((0,-.012,0)))
  rt=w+dd*.028-aa*.029;td=(dd*.65-aa*.76).normalized();anchors[b]['fingertips']['thumb']=gltf(rt+td*.072+Vector((0,-.015,0)))
 minv=[min(v.co[j] for v in ob.data.vertices) for j in range(3)];maxv=[max(v.co[j] for v in ob.data.vertices) for j in range(3)]
 measurements={'height_m':h,'helmet_top_m':round(maxv[2],5),'weight_kg':kg,'shoulder_joint_width_m':round(2*shoulder.x,5),'wingspan_chain_extended_m':round(2*shoulder.x+2*armreach,5),'standing_reach_chain_extended_m':round(shoulder.z+armreach,5),'hand_wrist_to_middle_fingertip_m':round((coord(tip)-wrist).length,5),'mesh_bounds_xyz_m':dims,'mesh_bounds_gltf_min':[minv[0],minv[2],-maxv[1]],'mesh_bounds_gltf_max':[maxv[0],maxv[2],-minv[1]],'measurement_method':'Bounds from actual rest mesh vertices; shoulder span and fully extended reach from actual rig segment and glove fingertip lengths. Reach is skeletal chain extension before shoulder/scapula movement, not a gameplay catch radius.','contact_anchors_gltf':anchors,'authoring_width_scale':wd,'authoring_depth_scale':dp,'authoring_height_scale':h/1.88}
 print('ASSET_READY '+ident+(' LOD1' if lod else ' LOD0')+' vertices='+str(len(ob.data.vertices))+' triangles='+str(sum(len(p.vertices)-2 for p in ob.data.polygons))+' clips='+str(len(clips)),flush=True)
 return {'id':ident,'label':label,'position_group':folder,'model':'models/'+folder+'/'+ident+'.glb','lod1':'models/'+folder+'/'+ident+'_lod1.glb','uniform_number':no,'palette':pal,'measurements':measurements,'vertices':len(ob.data.vertices),'triangles':sum(len(p.vertices)-2 for p in ob.data.polygons),'bones':len(arm.data.bones),'morph_targets':MORPHS,'clips':clips}

def equipment_export():
 global ACTIVE,PARTS,LOD
 clean();PARTS=[];LOD=False;ACTIVE={'id':'equipment','height':1.88,'weight':100,'width':1,'depth':1,'muscle':.7};m=materials('navy',0)
 helmet(m);obs=list(PARTS);export(os.path.join(PACK,'equipment','helmet_cage_visor.glb'),obs,False)
 clean();PARTS=[];create_hand(1,m);arm=rig();ob=join_meshes(arm);export(os.path.join(PACK,'equipment','articulated_glove.glb'),[ob,arm],False)
 clean();PARTS=[];create_boot(1,m);export(os.path.join(PACK,'equipment','cleat.glb'),PARTS,False)
 clean();PARTS=[]
 # A pointed football with seam channels and eight crossed laces, explicit mesh.
 rings=[]
 for i in range(25):
  t=-pi/2+pi*i/24;rad=.087*cos(t)**1.12;rings.append(((0,.142*sin(t),.15),rad,rad,0))
 loft('Regulation football',rings,mat('Pebbled brown leather',(.23,.065,.020,1),.85,texture=True),'root','equipment',sides=32)
 for a in [0,pi/2,pi,3*pi/2]:
  path=[]
  for i in range(23):t=-1.48+2.96*i/22;r=.088*cos(t)**1.12;path.append((r*cos(a),.142*sin(t),.15+r*sin(a)))
  tube('Football seam',path,.0014,m['compression'],'root')
 for y in [-.041,-.029,-.017,-.005,.007,.019,.031,.043]:tube('Crossed football lace',[(-.015,y,.237),(0,y-.002,.240),(.015,y,.237)],.0022,m['white'],'root')
 tube('Lace spine',[(0,-.052,.237),(0,.052,.237)],.0018,m['white'],'root')
 export(os.path.join(PACK,'equipment','football.glb'),PARTS,False)

def preview(entries):
 # Import exported GLBs and render a studio lineup with one metre reference marks.
 clean();sc=bpy.context.scene;sc.render.engine='CYCLES';sc.cycles.samples=32;sc.render.resolution_x=2400;sc.render.resolution_y=1500;sc.render.resolution_percentage=100
 if hasattr(sc.cycles,'use_denoising'):sc.cycles.use_denoising=True
 sc.world.color=(.10,.10,.10)
 for i,en in enumerate(entries):
  before=set(bpy.data.objects);bpy.ops.import_scene.gltf(filepath=os.path.join(PACK,en['model']));new=set(bpy.data.objects)-before
  roots=[o for o in new if o.parent not in new]
  cols=min(8,len(entries));rows=math.ceil(len(entries)/cols);x=(i%cols-(cols-1)/2)*.98;y=0;zz=(rows-1-i//cols)*2.50
  for root in roots:root.location.x+=x;root.location.y+=y;root.location.z+=zz
  for ob in new:
   if ob.type=='ARMATURE':
    if ob.animation_data:ob.animation_data_clear()
    for pb in ob.pose.bones:pb.rotation_mode='XYZ';pb.rotation_euler=(0,0,0)
  cu=bpy.data.curves.new('Label','FONT');cu.body=en['id'].replace('_',' ').upper();cu.align_x='CENTER';cu.size=.065;cu.extrude=.001;ob=bpy.data.objects.new('Label',cu);sc.collection.objects.link(ob);ob.location=(x,y-.30,zz-.13 if len(entries)>1 else .006)
  if len(entries)>1:ob.rotation_euler=(pi/2,0,0)
  mm=mat('Label white',(.6,.75,.85,1),.5);cu.materials.append(mm)
 # plane geometry (environment only)
 me=bpy.data.meshes.new('Studio floor');me.from_pydata([(-9,-7,-.012),(9,-7,-.012),(9,8,-.012),(-9,8,-.012)],[],[(0,1,2,3)]);fl=bpy.data.objects.new('Studio floor',me);sc.collection.objects.link(fl);me.materials.append(mat('Floor',(.018,.027,.043,1),.65))
 def area(name,loc,power,size,color):
  dat=bpy.data.lights.new(name,'AREA');dat.energy=power;dat.shape='DISK';dat.size=size;dat.color=color;ob=bpy.data.objects.new(name,dat);sc.collection.objects.link(ob);ob.location=loc;ob.rotation_euler=(Vector((0,.4,1))-ob.location).to_track_quat('-Z','Y').to_euler()
 area('Softbox front',(-3,-4,6),1600,6,(.80,.90,1));area('Warm fill',(4,-1,4),1300,5,(1,.82,.68));area('Rim',(0,4,5),2100,5,(.3,.65,1))
 dat=bpy.data.cameras.new('Lineup camera');cam=bpy.data.objects.new('Lineup camera',dat);sc.collection.objects.link(cam);cam.location=(0,-8,3.3 if len(entries)>1 else 3.0);target=Vector((0,0,2.30 if len(entries)>1 else 1.0));cam.rotation_euler=(target-cam.location).to_track_quat('-Z','Y').to_euler();dat.type='ORTHO';dat.ortho_scale=8.6 if len(entries)>1 else 3.5;sc.camera=cam;sc.view_settings.view_transform='AgX';sc.render.image_settings.file_format='PNG';sc.render.filepath=args.preview or os.path.join(os.path.dirname(PACK),'football_lineup.png');bpy.ops.render.render(write_still=True)

def enrich_manifest(manifest):
 for collection in ['archetypes','models','assets']:
  for entry in manifest.get(collection,[]):
   measurement=entry.get('measurements',{});lo=measurement.get('mesh_bounds_gltf_min');hi=measurement.get('mesh_bounds_gltf_max')
   if lo and hi:
    size=[round(hi[j]-lo[j],6) for j in range(3)];measurement['mesh_bounds_xyz_m']=size;measurement['mesh_bounds_gltf_size_m']=size;measurement['bounds_axis_order']='glTF X width, Y height, Z depth; rest-pose raw surface vertices, excluding scene animation evaluation.'
 for name,clip in manifest['animations'].items():
  clip['duration_seconds']=round(clip['duration_seconds']*30)/30
  if name=='catch_high_point':clip['hand_contact_windows']=[[.53,.72]]
  attacking=bool(clip.get('hand_contact_windows'))
  clip['contact_hands']=(['L'] if any(s in name for s in ['one_hand','swat','trailing']) else ['L','R']) if attacking else []
  clip['contact_method']='Actual deformed glove/finger mesh or sampled hand surfaces; timing window is animation metadata, never an automatic catch.' if attacking else 'No ball contact event authored.'
  clip['commit_until_seconds']=clip['duration_seconds'] if any(s in name for s in ['dive','fall','tackle']) else round(clip['duration_seconds']*.65,3)
  clip['requires_recovery_clip']=any(s in name for s in ['dive','fall','tackle'])
  if clip['requires_recovery_clip']:clip['recovery_clip']='recovery_get_up'
  clip['root_motion_coordinates']='glTF +Y is vertical, +Z is athlete forward. Authored translations include surface-based grounding at 30fps.'
  if 'toe_tap' in name:clip['foot_contact_windows']={'L':[[.35,.85]],'R':[[.72,.98]]}
  if name=='qb_release':clip['ball_release_seconds']=.40
 manifest['height_customization']={'units':'metres','global_supported_preview_range':[1.55,2.20],'position_specific_builder_ranges':{'QB':[1.73,2.08],'WR':[1.60,2.13],'RB':[1.60,2.00],'TE':[1.83,2.18],'CB':[1.65,2.03],'S':[1.70,2.08],'LB':[1.73,2.13]},'adult_realistic_franchise_height_fixed':True,'optional_arcade_height_development_max_m':.05,'realistic_training':'Improve jumping, technique and reach mechanics without changing adult height.'}
 manifest['strength_preview_mapping']={'normalized':'n=clamp((strength-60)/35,0,1)','weights':{'Muscularity':'1.0*n','ShoulderWidth':'.40*n','ChestSize':'.65*n','ArmThickness':'1.0*n','LegPower':'.35*n','BodyWeight':'0'},'authoritative_runtime_helper':'preview/physique.js','meaning':'Supplied helper visible progression from 60 to 95 strength, with independent physique sliders applied separately. Runtime must recompute body bounds, reach samples, mass/contact balance and skeleton rest proportions from the final displayed athlete.'}
 return manifest

def write_manifest(entries,filename='manifest.json'):
 manifest={'schema_version':1,'asset_pack':'ReceiverWindowQB NextGen Football Models','asset_version':'1.0.0','created':'2026-10-18','license':'Original generated assets; included LICENSE.txt applies.','units':'metres','gltf_coordinate_system':{'up':'+Y','forward':'+Z','right':'+X','authoring':'Blender Z-up, front -Y; exporter converts to glTF Y-up'},'art_direction':'Detailed original parametric stylized athletes. Sculpted cross-section meshes; engineered equipment; no body primitive placeholders. Not scans or hand-sculpted photoreal assets.','archetypes':entries,'morphs':{n:{'range':[0,1],'baseline':0,'meaning':{'Muscularity':'Pectoral, deltoid, bicep, forearm and leg contour expansion','ShoulderWidth':'Shoulders and arm origins shift laterally by up to 0.03 m per side','ChestSize':'Pectoral and back depth and chest width','ArmThickness':'Upper/lower arm circumference','LegPower':'Quadriceps, hamstring and calf circumference','BodyWeight':'Waist, torso and hip volume','Wingspan':'Arm endpoint shift 0.045 m laterally and 0.02475 m downward per side','HandSize':'Palm and five fingers scale 1.0–1.2 about wrist','Reach':'Arm endpoint shift 0.025 m laterally and 0.01375 m downward per side'}[n]} for n in MORPHS},'physique_runtime':{'height':'uniform scale at model/armature parent = requested_height_m / archetype.measurements.height_m; scales visible meshes, bone translations, IK targets, bounds and gameplay contacts together. Adult height fixed unless optional arcade progression.','strength':'map strength rating to Muscularity, ShoulderWidth, ChestSize, ArmThickness and LegPower. Each baseline already includes an archetype-specific athletic build; additive morphs are deltas.','weight':'BodyWeight is visual volume; mass in kg remains an explicit gameplay value. Do not infer mass from mesh volume.','skeleton_adjustment':'For ShoulderWidth, Wingspan and Reach: apply the same analytic morph displacement to all relevant arm/hand bone rest head/tail locations, then update inverse bind matrices before playing clips. For HandSize, scale hand/finger rest lengths about wrist. Source morph function is authoritative. Recompute IK, glove contact samples and physical bounds from current skinned mesh/bones.','contact_contract':'Use final glove/finger bone transforms and mesh surfaces for catch reach; no invisible catch radius. Capsule collision can approximate body volume but cannot decide ball contacts.','combinations':'Morph targets are additive; clamp weights to [0,1]. Validate extreme combinations in engine, including animation posing, before production release.'},'rig':{'bone_count':len(BONES),'bone_names':list(BONES.keys()),'finger_chains':'index/middle/ring/pinky_01..03.L/.R; thumb_01..02.L/.R','hand_ik':'No runtime IK solver is embedded in glTF. Hand/forearm chains and finger rotations are authored; engine must apply two-bone arm IK and finger grip solving to track a live ball.','retargeting':'All archetypes share bone names and topology; rest proportions differ. Preserve scale and rebuild contact bounds when retargeting.'},'animations':CLIPS,'equipment':['equipment/helmet_cage_visor.glb','equipment/articulated_glove.glb','equipment/cleat.glb','equipment/football.glb'],'textures':['textures/technical_fabric_albedo.png','textures/technical_fabric_normal.png','textures/technical_fabric_roughness.png'],'lod':{'LOD0':'Full surface meshes, animated skin and nine physique targets.','LOD1':'Lower radial surface resolution with same rig, materials, morph names and animation library.','suggested_distances_m':{'LOD0':[0,16],'LOD1':[16,60]},'performance':'Actual device frame-rate benchmarking is deferred to game integration; no 60 FPS guarantee.'},'known_limits':['Animations are original authored keyframe clips, not motion capture.','Clips require engine blending, grounding and ball-aware IK; standalone clips do not determine catch success.','LOD meshes include individual fingers, accessories and complete animation data; mobile performance must be measured.','Skeletal proportions for extreme combined physique settings require runtime rest-pose adaptation as described above.']}
 manifest['schemaVersion']=1
 manifest['models']=[{'id':e['id'],'name':e['label'],'position':e['position_group'],'path':e['model'],'lodPath':e['lod1'],'heightM':e['measurements']['height_m'],'weightKg':e['measurements']['weight_kg'],'proportions':{'width':e['measurements']['authoring_width_scale'],'depth':e['measurements']['authoring_depth_scale']},'measurements':e['measurements']} for e in entries]
 manifest['assets']=[{'id':e['id'],'path':e['model'],'lods':{'high':e['model'],'low':e['lod1']},'heightM':e['measurements']['height_m'],'weightKg':e['measurements']['weight_kg'],'proportions':{'width':e['measurements']['authoring_width_scale'],'depth':e['measurements']['authoring_depth_scale']},'measurements':e['measurements']} for e in entries]
 manifest['morphTargets']=MORPHS
 enrich_manifest(manifest)
 with open(os.path.join(PACK,filename),'w',encoding='utf8') as f:json.dump(manifest,f,indent=2)
 with open(os.path.join(PACK,'animations','clip_library.json'),'w',encoding='utf8') as f:json.dump({'fps':30,'clips':CLIPS,'delivery':'All listed actions are embedded in every athlete GLB. Shared animation-only GLB uses the qb_field_general rest rig.'},f,indent=2)

if __name__=='__main__':
 if args.only=='preview':
  entries=json.load(open(os.path.join(PACK,'manifest.json')))['archetypes'];preview(entries)
 elif args.only=='merge':
  import glob
  docs=[json.load(open(p,encoding='utf8')) for p in sorted(glob.glob(os.path.join(PACK,'manifest.part*.json')))];doc=docs[0];order={a[0]:i for i,a in enumerate(ARCHETYPES)}
  for key in ['archetypes','models','assets']:doc[key]=sorted([e for d in docs for e in d[key]],key=lambda e:order[e['id']])
  enrich_manifest(doc)
  with open(os.path.join(PACK,'manifest.json'),'w',encoding='utf8') as f:json.dump(doc,f,indent=2)
  with open(os.path.join(PACK,'animations','clip_library.json'),'w',encoding='utf8') as f:json.dump({'fps':30,'clips':doc['animations'],'delivery':'All listed actions are embedded in every athlete GLB. Shared animation GLB includes the qb_field_general reference rig and skin.'},f,indent=2)
  print('MERGED_MANIFEST '+str(len(doc['archetypes'])),flush=True)
 elif args.only=='fix-turns':
  import glob
  correct_twist_axes([os.path.abspath(args.fix_path)] if args.fix_path else glob.glob(os.path.join(PACK,'**','*.glb'),recursive=True))
 else:
  entries=[]
  selected=[(i,a) for i,a in enumerate(ARCHETYPES) if not args.only or a[0]==args.only]
  if args.shard:
   shard,count=map(int,args.shard.split(':'));selected=[(i,a) for i,a in selected if i%count==shard]
  for i,a in selected:
   en=build(a,i,False);entries.append(en)
   if i==0:
    # A complete shared animation file retains a skinned reference athlete for
    # robust loaders; it is a true GLB, not an external clip stub.
    export(os.path.join(PACK,'animations','shared_humanoid_clips.glb'))
    bpy.ops.file.pack_all();bpy.ops.wm.save_as_mainfile(filepath=os.path.join(PACK,'source','football_authoring.blend'))
   build(a,i,True)
  if not args.shard or shard==0:equipment_export()
  write_manifest(entries,'manifest.part'+str(shard)+'.json' if args.shard else 'manifest.json')
  print('PACK_BUILD_COMPLETE '+str(len(entries)),flush=True)
