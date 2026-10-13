"""Original geometric modelling toolkit; glTF 2.0 binary export without a DCC dependency.
All shapes are real indexed triangle meshes with normals, PBR materials and named nodes.
X right, Y up, +Z forward. Units are metres. Euler rotations apply X then Y then Z.
"""
from pathlib import Path
import json, math, struct
import numpy as np
from scipy.spatial import ConvexHull

def material(name, color, metallic=.4, roughness=.5, emissive=0.0):
    color=color.lstrip('#');rgb=[int(color[i:i+2],16)/255 for i in (0,2,4)]
    return {'name':name,'pbrMetallicRoughness':{'baseColorFactor':rgb+[1.0], 'metallicFactor':float(metallic),'roughnessFactor':float(roughness)},'emissiveFactor':[v*float(emissive) for v in rgb], 'doubleSided':True,'alphaMode':'OPAQUE'}

def rotation_matrix(rotation=None):
    if rotation is None:return np.eye(3)
    if np.asarray(rotation).shape==(3,3):return np.asarray(rotation,float)
    x,y,z=rotation;cx,sx=math.cos(x),math.sin(x);cy,sy=math.cos(y),math.sin(y);cz,sz=math.cos(z),math.sin(z)
    return np.array([[cz,-sz,0],[sz,cz,0],[0,0,1]])@np.array([[cy,0,sy],[0,1,0],[-sy,0,cy]])@np.array([[1,0,0],[0,cx,-sx],[0,sx,cx]])

def normals(vertices,faces):
    a,b,c=vertices[faces[:,0]],vertices[faces[:,1]],vertices[faces[:,2]]
    face=np.cross(b-a,c-a);out=np.zeros_like(vertices)
    for i in range(3):np.add.at(out,faces[:,i],face)
    lengths=np.linalg.norm(out,axis=1);out/=np.maximum(lengths[:,None],1e-12)
    return out

class Scene:
    def __init__(self,title):self.title=title;self.parts=[];self.metadata={}
    def _add(self,name,vertices,faces,mat,center=(0,0,0),rotation=None,normals_in=None):
        vertices=np.asarray(vertices,float);faces=np.asarray(faces,np.int64).reshape(-1,3)
        rot=rotation_matrix(rotation);vertices=vertices@rot.T
        area=np.linalg.norm(np.cross(vertices[faces[:,1]]-vertices[faces[:,0]],vertices[faces[:,2]]-vertices[faces[:,0]]),axis=1)
        faces=faces[area>1e-12]
        if not len(faces):raise ValueError('Empty mesh '+name)
        # For closed primitive meshes the signed volume establishes outward winding.
        volume=np.sum(np.einsum('ij,ij->i',vertices[faces[:,0]],np.cross(vertices[faces[:,1]],vertices[faces[:,2]])))/6
        edges=np.concatenate([faces[:,[0,1]],faces[:,[1,2]],faces[:,[2,0]]]);edges.sort(axis=1)
        _,edge_counts=np.unique(edges,axis=0,return_counts=True)
        # Open bores/membranes may deliberately face inward; only closed solids
        # have a meaningful global outward-volume rule.
        if np.all(edge_counts==2) and volume < -1e-10:faces=faces[:,[0,2,1]]
        n=normals(vertices,faces) if normals_in is None else np.asarray(normals_in,float)@rot.T
        if not np.isfinite(vertices).all() or not np.isfinite(n).all():raise ValueError('Invalid geometry '+name)
        self.parts.append({'name':f'{name}_{len(self.parts)+1}','v':vertices.astype('<f4'),'f':faces,'n':n.astype('<f4'),'center':np.asarray(center,float),'mat':mat})
        return self
    def mesh(self,name,vertices,faces,mat,normals=None):
        vertices=np.asarray(vertices,float);center=(vertices.min(axis=0)+vertices.max(axis=0))/2
        return self._add(name,vertices-center,faces,mat,center,normals_in=normals)
    def box(self,name,center,size,mat,bevel=.08,rotation=None):
        half=np.asarray(size,float)/2;b=min(half)*max(0,min(.8,float(bevel)))
        if b<=1e-8:
            v=np.array([[x,y,z] for x in [-half[0],half[0]] for y in [-half[1],half[1]] for z in [-half[2],half[2]]])
        else:
            points=[]
            for axis in range(3):
                other=[a for a in range(3) if a!=axis]
                for side in [-1,1]:
                    for a in [-1,1]:
                        for c in [-1,1]:
                            p=np.zeros(3);p[axis]=side*half[axis];p[other[0]]=a*(half[other[0]]-b);p[other[1]]=c*(half[other[1]]-b);points.append(p)
            v=np.array(points)
        hull=ConvexHull(v);f=hull.simplices.copy()
        for i,face in enumerate(f):
            normal=np.cross(v[face[1]]-v[face[0]],v[face[2]]-v[face[0]])
            if np.dot(normal,hull.equations[i,:3])<0:f[i]=face[[0,2,1]]
        # Keep broad machined faces flat and their chamfers crisp rather than
        # averaging a plate's normal into neighbouring edge bevels.
        flat=v[f].reshape(-1,3);face_normals=np.cross(v[f[:,1]]-v[f[:,0]],v[f[:,2]]-v[f[:,0]])
        face_normals/=np.linalg.norm(face_normals,axis=1)[:,None]
        return self._add(name,flat,np.arange(len(flat)).reshape(-1,3),mat,center,rotation,np.repeat(face_normals,3,axis=0))
    def ellipsoid(self,name,center,radii,mat,segments=16,rings=8,rotation=None):
        segments=max(6,int(segments));rings=max(4,int(rings));r=np.asarray(radii,float)
        v=[[0,r[1],0]]
        for j in range(1,rings):
            t=math.pi*j/rings
            for i in range(segments):
                p=math.tau*i/segments;v.append([r[0]*math.sin(t)*math.cos(p),r[1]*math.cos(t),r[2]*math.sin(t)*math.sin(p)])
        bottom=len(v);v.append([0,-r[1],0]);f=[]
        for i in range(segments):f.append([0,1+(i+1)%segments,1+i])
        for j in range(rings-2):
            for i in range(segments):
                a=1+j*segments+i;b=1+j*segments+(i+1)%segments;c=b+segments;d=a+segments;f.extend([[a,b,c],[a,c,d]])
        for i in range(segments):f.append([bottom,1+(rings-2)*segments+i,1+(rings-2)*segments+(i+1)%segments])
        v=np.array(v);n=v/(r*r);n/=np.linalg.norm(n,axis=1)[:,None]
        return self._add(name,v,f,mat,center,rotation,n)
    def tube(self,name,points,radii,mat,sides=12,caps=True):
        points=np.asarray(points,float);sides=max(6,int(sides));count=len(points)
        if count<2:raise ValueError('Tube needs two points')
        radii=np.asarray(radii,float)
        if radii.ndim==0:radii=np.repeat(radii,count)
        if radii.ndim==1:radii=np.column_stack([radii,radii])
        if radii.shape!=(count,2):raise ValueError('Tube radius count mismatch '+name)
        center=(points.min(axis=0)+points.max(axis=0))/2;v=[];previous=None
        for i,p in enumerate(points):
            tangent=points[min(i+1,count-1)]-points[max(0,i-1)];tangent/=max(1e-12,np.linalg.norm(tangent))
            if previous is None:
                seed=np.eye(3)[np.argmin(np.abs(tangent))];u=np.cross(tangent,seed);u/=np.linalg.norm(u)
            else:
                u=previous-np.dot(previous,tangent)*tangent
                if np.linalg.norm(u)<1e-6:u=np.cross(tangent,np.eye(3)[np.argmin(np.abs(tangent))])
                u/=np.linalg.norm(u)
            previous=u;vv=np.cross(tangent,u)
            for j in range(sides):
                a=math.tau*j/sides;v.append(p+u*math.cos(a)*radii[i,0]+vv*math.sin(a)*radii[i,1]-center)
        f=[]
        for i in range(count-1):
            for j in range(sides):
                a=i*sides+j;b=i*sides+(j+1)%sides;c=b+sides;d=a+sides;f.extend([[a,b,c],[a,c,d]])
        if caps:
            for ring,p in [(0,points[0]),(count-1,points[-1])]:
                index=len(v);v.append(p-center)
                for j in range(sides):f.append([index,ring*sides+(j+1)%sides,ring*sides+j] if ring==0 else [index,ring*sides+j,ring*sides+(j+1)%sides])
        return self._add(name,v,f,mat,center)
    def cylinder(self,name,center,radius,length,mat,axis='z',sides=16):
        center=np.asarray(center,float);vector=np.eye(3)[{'x':0,'y':1,'z':2}[axis]]*length/2
        return self.tube(name,[center-vector,center+vector],[radius,radius],mat,sides)
    def torus(self,name,center,major,minor,mat,axis='z',segments=24,sides=8):
        v=[];f=[]
        for i in range(segments):
            a=math.tau*i/segments
            for j in range(sides):
                b=math.tau*j/sides;r=major+minor*math.cos(b);v.append([r*math.cos(a),r*math.sin(a),minor*math.sin(b)])
        for i in range(segments):
            for j in range(sides):
                a=i*sides+j;b=((i+1)%segments)*sides+j;c=((i+1)%segments)*sides+(j+1)%sides;d=i*sides+(j+1)%sides;f.extend([[a,b,c],[a,c,d]])
        rotation={'z':None,'y':(math.pi/2,0,0),'x':(0,math.pi/2,0)}[axis]
        return self._add(name,v,f,mat,center,rotation)
    def scale(self,factor):
        factor=np.asarray(factor,float)
        for p in self.parts:p['v']*=factor;p['center']*=factor
        return self
    def ground(self):
        low=min(float(np.min(p['v'][:,1])+p['center'][1]) for p in self.parts)
        for p in self.parts:p['center'][1]-=low
        return self
    def save(self,path):
        path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
        data=bytearray();views=[];accessors=[];meshes=[];nodes=[];mats=[];mat_lookup={}
        def accessor(array,kind,target):
            array=np.ascontiguousarray(array);offset=len(data);data.extend(array.tobytes());data.extend(b'\0'*((-len(data))%4))
            views.append({'buffer':0,'byteOffset':offset,'byteLength':array.nbytes,'target':target})
            a={'bufferView':len(views)-1,'byteOffset':0,'componentType':5126 if array.dtype.kind=='f' else (5123 if array.dtype.itemsize==2 else 5125),'count':len(array),'type':kind}
            if kind=='VEC3' and target==34962:a.update(min=array.min(axis=0).astype(float).tolist(),max=array.max(axis=0).astype(float).tolist())
            accessors.append(a);return len(accessors)-1
        lo=np.full(3,np.inf);hi=-lo
        for p in self.parts:
            key=json.dumps(p['mat'],sort_keys=True)
            if key not in mat_lookup:mat_lookup[key]=len(mats);mats.append(p['mat'])
            ind=p['f'].reshape(-1).astype('<u2' if len(p['v'])<65536 else '<u4')
            position=accessor(p['v'],'VEC3',34962);normal=accessor(p['n'],'VEC3',34962);indices=accessor(ind,'SCALAR',34963)
            meshes.append({'name':p['name']+'_mesh','primitives':[{'attributes':{'POSITION':position,'NORMAL':normal},'indices':indices,'material':mat_lookup[key],'mode':4}]})
            nodes.append({'name':p['name'],'mesh':len(meshes)-1,'translation':p['center'].astype(float).tolist()})
            lo=np.minimum(lo,p['v'].min(axis=0)+p['center']);hi=np.maximum(hi,p['v'].max(axis=0)+p['center'])
        gltf={'asset':{'version':'2.0','generator':'Neon Breach original detailed geometric asset builder v2'},'scene':0,'scenes':[{'name':self.title,'nodes':list(range(len(nodes)))}],'nodes':nodes,'meshes':meshes,'materials':mats,'buffers':[{'byteLength':len(data)}],'bufferViews':views,'accessors':accessors,'extras':{'title':self.title,'axes':'X right, Y up, +Z forward','units':'metres','staticNamedParts':True,**self.metadata}}
        js=json.dumps(gltf,separators=(',',':')).encode();js+=b' '*((-len(js))%4);length=12+8+len(js)+8+len(data)
        path.write_bytes(struct.pack('<III',0x46546c67,2,length)+struct.pack('<II',len(js),0x4e4f534a)+js+struct.pack('<II',len(data),0x004e4942)+data)
        return {'path':path.name,'bytes':length,'nodes':len(nodes),'triangles':sum(len(p['f']) for p in self.parts),'vertices':sum(len(p['v']) for p in self.parts),'bounds':{'min':lo.tolist(),'max':hi.tolist()},'materials':len(mats)}
