#!/usr/bin/env python3
"""Build small, indexed, UV-mapped survival scenery meshes. No runtime dependency.

Run this file with Python 3 to regenerate the JSON next to it. Mesh space is
centered on X/Z, Y begins at zero, and every mesh occupies a unit bounding box.
Collision x/z are centers; collision y is the box's bottom, never its center.
All passage arches intentionally collide only on their supporting columns.
"""

import json
import math
from pathlib import Path

TAU = math.tau


class Mesh:
    def __init__(self, name):
        self.name = name
        self.vertices = []
        self.uv = []
        self.faces = []
        self.collision = []
        self.compound = []

    def physical_bounds(self, points):
        """One close fitting hull per authored component, never a whole prop box."""
        lo = [min(p[i] for p in points) for i in range(3)]
        hi = [max(p[i] for p in points) for i in range(3)]
        self.compound.append(dict(x=(lo[0]+hi[0])/2, z=(lo[2]+hi[2])/2,
                                  w=max(.001,hi[0]-lo[0]), d=max(.001,hi[2]-lo[2]),
                                  y=lo[1], h=max(.001,hi[1]-lo[1])))

    def vertex(self, xyz, uv=(0, 0)):
        self.vertices.append(tuple(xyz))
        self.uv.append(tuple(uv))
        return len(self.vertices) - 1

    def triangle(self, a, b, c):
        self.faces.append((a, b, c))

    def quad(self, a, b, c, d):
        self.triangle(a, b, c)
        self.triangle(a, c, d)

    def surface(self, points, uv=None):
        """An isolated polygon has crisp edges and a shared indexed face fan."""
        uv = uv or [(0, 0), (1, 0), (1, 1), (0, 1)]
        ids = [self.vertex(p, uv[i % len(uv)]) for i, p in enumerate(points)]
        for i in range(1, len(ids) - 1):
            self.triangle(ids[0], ids[i], ids[i + 1])

    def box(self, cx, y, cz, w, h, d):
        a, b = cx - w / 2, cx + w / 2
        c, e = cz - d / 2, cz + d / 2
        t = y + h
        self.physical_bounds([(a,y,c),(b,t,e)])
        for face in [
            [(a, y, e), (b, y, e), (b, t, e), (a, t, e)],
            [(b, y, c), (a, y, c), (a, t, c), (b, t, c)],
            [(a, y, c), (a, y, e), (a, t, e), (a, t, c)],
            [(b, y, e), (b, y, c), (b, t, c), (b, t, e)],
            [(a, t, e), (b, t, e), (b, t, c), (a, t, c)],
            [(a, y, c), (b, y, c), (b, y, e), (a, y, e)],
        ]:
            self.surface(face)

    def cylinder(self, cx, y, cz, radius, height, sides=12, top_radius=None, inner=0):
        top_radius = radius if top_radius is None else top_radius
        if inner:
            # Closed shell segments preserve the genuinely hollow pipe interior.
            for i in range(sides):
                self.physical_bounds([(cx+r*math.cos(j*TAU/sides),level,cz+r*math.sin(j*TAU/sides))
                    for level,ro in [(y,radius),(y+height,top_radius)]
                    for r in [inner,ro] for j in [i,i+1]])
        else:
            # Four axial bands follow tapered crystals rather than their full bounds.
            for i in range(4):
                a,b=i/4,(i+1)/4
                r=max(radius+(top_radius-radius)*a,radius+(top_radius-radius)*b)
                self.physical_bounds([(cx-r,y+height*a,cz-r),(cx+r,y+height*b,cz+r)])
        outer = []
        for level, r in [(y, radius), (y + height, top_radius)]:
            outer.append([self.vertex((cx + r * math.cos(i * TAU / sides), level,
                                       cz + r * math.sin(i * TAU / sides)),
                                      (i / sides, (level - y) / height))
                          for i in range(sides + 1)])
        for i in range(sides):
            self.quad(outer[0][i], outer[1][i], outer[1][i + 1], outer[0][i + 1])
        if inner:
            inside = []
            for level in [y, y + height]:
                inside.append([self.vertex((cx + inner * math.cos(i * TAU / sides), level,
                                            cz + inner * math.sin(i * TAU / sides)),
                                           (i / sides, (level - y) / height))
                               for i in range(sides + 1)])
            for i in range(sides):
                self.quad(inside[0][i + 1], inside[1][i + 1], inside[1][i], inside[0][i])
                # Separate vertices preserve the sharp rim normals.
                for level, ro, ri, reverse in [(y, radius, inner, True),
                                               (y + height, top_radius, inner, False)]:
                    pts = [(cx + r * math.cos(j * TAU / sides), level,
                            cz + r * math.sin(j * TAU / sides))
                           for r, j in [(ri, i), (ri, i + 1), (ro, i + 1), (ro, i)]]
                    self.surface(pts[::-1] if reverse else pts)
        else:
            for level, r, reverse in [(y, radius, True), (y + height, top_radius, False)]:
                pts = [(cx + r * math.cos(i * TAU / sides), level,
                        cz + r * math.sin(i * TAU / sides)) for i in range(sides)]
                self.surface(pts if reverse else pts[::-1],
                             [(0.5 + 0.5 * math.cos(i * TAU / sides),
                               0.5 + 0.5 * math.sin(i * TAU / sides)) for i in range(sides)])

    def ellipsoid(self, center, radii, sides=12, rings=6):
        cx, cy, cz = center
        rx, ry, rz = radii
        for i in range(4):
            a,b=-1+i*.5,-1+(i+1)*.5
            near=0 if a<=0<=b else min(abs(a),abs(b))
            width=math.sqrt(max(0,1-near*near))
            self.physical_bounds([(cx-rx*width,cy+ry*a,cz-rz*width),
                                  (cx+rx*width,cy+ry*b,cz+rz*width)])
        rows = []
        for j in range(rings + 1):
            phi = j * math.pi / rings
            rows.append([self.vertex((cx + rx * math.sin(phi) * math.cos(i * TAU / sides),
                                      cy + ry * math.cos(phi),
                                      cz + rz * math.sin(phi) * math.sin(i * TAU / sides)),
                                     (i / sides, j / rings)) for i in range(sides + 1)])
        for j in range(rings):
            for i in range(sides):
                # At either pole skip the zero-area triangle.
                if j:
                    self.triangle(rows[j][i], rows[j][i + 1], rows[j + 1][i + 1])
                if j < rings - 1:
                    self.triangle(rows[j][i], rows[j + 1][i + 1], rows[j + 1][i])

    def segment(self, start, end, radius, sides=6, end_radius=None):
        """A cylindrical strut between arbitrary points, with closed flat caps."""
        end_radius = radius if end_radius is None else end_radius
        dx, dy, dz = [end[i] - start[i] for i in range(3)]
        length = math.sqrt(dx * dx + dy * dy + dz * dz)
        axis = (dx / length, dy / length, dz / length)
        ref = (0, 1, 0) if abs(axis[1]) < 0.95 else (1, 0, 0)
        u = (axis[1] * ref[2] - axis[2] * ref[1],
             axis[2] * ref[0] - axis[0] * ref[2],
             axis[0] * ref[1] - axis[1] * ref[0])
        ul = math.sqrt(sum(c * c for c in u))
        u = tuple(c / ul for c in u)
        v = (axis[1] * u[2] - axis[2] * u[1],
             axis[2] * u[0] - axis[0] * u[2],
             axis[0] * u[1] - axis[1] * u[0])
        pts, ids = [], []
        for center, radius, level in [(start, radius, 0), (end, end_radius, 1)]:
            row = [tuple(center[k] + radius * (u[k] * math.cos(i * TAU / sides) +
                                              v[k] * math.sin(i * TAU / sides))
                         for k in range(3)) for i in range(sides)]
            pts.append(row)
            ids.append([self.vertex(p, (i / sides, level)) for i, p in enumerate(row)])
        if abs(axis[2]) > .99:
            # Axial turbine/engine housings use six close circular bands;
            # the cylinder's empty square corners remain walkable.
            outer=max(radius,end_radius)
            for i in range(6):
                a,b=-outer+i*outer/3,-outer+(i+1)*outer/3
                near=0 if a<=0<=b else min(abs(a),abs(b))
                half=math.sqrt(max(0,outer*outer-near*near))
                self.physical_bounds([(start[0]+a,start[1]-half,min(start[2],end[2])),
                                      (start[0]+b,start[1]+half,max(start[2],end[2]))])
        else:
            self.physical_bounds(pts[0]+pts[1])
        for i in range(sides):
            j = (i + 1) % sides
            self.quad(ids[0][i], ids[0][j], ids[1][j], ids[1][i])
        self.surface(pts[0][::-1])
        self.surface(pts[1])

    def arch(self, center_y, outer_x, outer_y, inner_x, inner_y,
             depth, segments=12, rough=False):
        """An actual open arch shell; faces cover only the curved ring."""
        rows = []
        for z in [-depth / 2, depth / 2]:
            for inside in [False, True]:
                rx, ry = (inner_x, inner_y) if inside else (outer_x, outer_y)
                rows.append([self.vertex((rx * math.cos(i * math.pi / segments) *
                                          (1 + (0.035 * math.sin(i * 5.17) if rough and not inside else 0)),
                                          center_y + ry * math.sin(i * math.pi / segments), z),
                                         (i / segments, 1 if inside else 0))
                             for i in range(segments + 1)])
        front_out, front_in, back_out, back_in = rows
        for i in range(segments):
            j = i + 1
            # Every arc segment is solid, including curved shoulders and the crown.
            self.physical_bounds([self.vertices[row[k]] for row in rows for k in [i,j]])
            self.quad(front_out[j], front_out[i], front_in[i], front_in[j])
            self.quad(back_out[i], back_out[j], back_in[j], back_in[i])
            self.quad(front_out[i], front_out[j], back_out[j], back_out[i])
            self.quad(front_in[j], front_in[i], back_in[i], back_in[j])
        self.quad(front_out[0], back_out[0], back_in[0], front_in[0])
        self.quad(front_out[-1], front_in[-1], back_in[-1], back_out[-1])

    def collider(self, x, z, w, d, y, h):
        self.collision.append(dict(x=x, z=z, w=w, d=d, y=y, h=h))

    def write(self, destination=None):
        if self.name not in ['distant-planet','starfield','cargo-crate','train-carriage']:
            self.collision = self.compound
        # Standard placement coordinates, including collision volumes.
        low = [min(v[i] for v in self.vertices) for i in range(3)]
        high = [max(v[i] for v in self.vertices) for i in range(3)]
        span = [high[i] - low[i] for i in range(3)]
        self.vertices = [((v[0] - low[0]) / span[0] - 0.5,
                          (v[1] - low[1]) / span[1],
                          (v[2] - low[2]) / span[2] - 0.5) for v in self.vertices]
        for box in self.collision:
            box['x'] = (box['x'] - low[0]) / span[0] - 0.5
            box['z'] = (box['z'] - low[2]) / span[2] - 0.5
            box['y'] = (box['y'] - low[1]) / span[1]
            box['w'] /= span[0]
            box['h'] /= span[1]
            box['d'] /= span[2]
            if box['y'] < 0:
                box['h'] += box['y']
                box['y'] = 0
        normals = [[0.0, 0.0, 0.0] for _ in self.vertices]
        for a, b, c in self.faces:
            u = [self.vertices[b][i] - self.vertices[a][i] for i in range(3)]
            v = [self.vertices[c][i] - self.vertices[a][i] for i in range(3)]
            n = [u[1] * v[2] - u[2] * v[1],
                 u[2] * v[0] - u[0] * v[2],
                 u[0] * v[1] - u[1] * v[0]]
            for vertex in (a, b, c):
                for i in range(3):
                    normals[vertex][i] += n[i]
        for n in normals:
            length = math.sqrt(sum(c * c for c in n))
            if length < 1e-10:
                n[:] = [0, 1, 0]
            else:
                n[:] = [c / length for c in n]
        rounder = lambda seq: [round(c, 5) for c in seq]
        data = dict(version=1, id=self.name,
                    positions=rounder([c for vertex in self.vertices for c in vertex]),
                    uvs=rounder([c for uv in self.uv for c in uv]),
                    normals=rounder([c for normal in normals for c in normal]),
                    indices=[c for face in self.faces for c in face],
                    bounds=dict(min=[-0.5, 0, -0.5], max=[0.5, 1, 0.5]),
                    collision=[{key: round(value, 5) for key, value in box.items()}
                               for box in self.collision])
        directory=Path(destination) if destination is not None else Path(__file__).parent
        directory.mkdir(parents=True,exist_ok=True)
        path = directory / (self.name + '.json')
        path.write_text(json.dumps(data, separators=(',', ':')) + '\n')
        print(f'{self.name}: {len(self.vertices)} vertices, {len(self.faces)} triangles, {path.stat().st_size} bytes')


def build(destination=None):
    meshes = []
    m = Mesh('curved-pipe')
    m.cylinder(0, 0, 0, 0.46, 1, 16, inner=0.33)
    for y in [0.03, 0.85]:
        m.cylinder(0, y, 0, 0.5, 0.1, 16, inner=0.33)
    # Collision follows the pipe wall with four conservative boxes.
    for x, z, w, d in [(0.42, 0, 0.16, 0.55), (-0.42, 0, 0.16, 0.55),
                        (0, 0.42, 0.55, 0.16), (0, -0.42, 0.55, 0.16)]:
        m.collider(x, z, w, d, 0, 1)
    meshes.append(m)

    m = Mesh('tunnel-arch')
    m.arch(0.68, 0.5, 0.32, 0.35, 0.22, 1, 12)
    for x in [-0.425, 0.425]:
        m.box(x, 0, 0, 0.15, 0.68, 1)
        m.collider(x, 0, 0.15, 1, 0, 0.85)
    for z in [-0.51, 0.51]:
        m.box(-0.425, 0, z, 0.18, 0.68, 0.04)
        m.box(0.425, 0, z, 0.18, 0.68, 0.04)
    m.collider(0, 0, .36, 1, .9, .1)
    meshes.append(m)

    m = Mesh('rock-arch')
    m.arch(0.36, 0.5, 0.64, 0.32, 0.46, 0.56, 10, rough=True)
    for x in [-0.425, 0.425]:
        m.box(x, 0, 0, 0.18, 0.36, 0.56)
        m.ellipsoid((x, 0.17, 0), (0.11, 0.17, 0.32), 7, 4)
        m.collider(x, 0, 0.18, 0.58, 0, 0.72)
    m.collider(0, 0, .40, .56, .82, .18)
    meshes.append(m)

    m = Mesh('rib-arch')
    m.arch(0, 0.5, 1, 0.4, 0.87, 0.16, 14)
    for x in [-0.455, 0.455]:
        m.ellipsoid((x, 0.06, 0), (0.06, 0.06, 0.12), 8, 4)
        m.collider(x, 0, 0.09, 0.18, 0, 0.34)
    m.collider(0, 0, .32, .16, .87, .13)
    meshes.append(m)

    m = Mesh('spaceship-hull')
    # Streamlined hull with tapered nose/tail, cockpit, swept wings and thrusters.
    m.ellipsoid((0, 0.35, 0), (0.29, 0.27, 0.5), 12, 8)
    m.ellipsoid((0, 0.62, -0.08), (0.15, 0.3, 0.22), 10, 5)
    for sign in [-1, 1]:
        a = [(sign * 0.16, 0.32, -0.27), (sign * 0.5, 0.14, 0.18),
             (sign * 0.5, 0.14, 0.42), (sign * 0.13, 0.32, 0.24)]
        b = [(x, y - 0.06, z) for x, y, z in a]
        m.physical_bounds(a+b)
        m.surface(a if sign == -1 else a[::-1])
        m.surface(b[::-1] if sign == -1 else b)
        for i in range(4):
            j = (i + 1) % 4
            m.surface([a[i], a[j], b[j], b[i]])
        m.segment((sign * 0.23, 0.32, 0.26), (sign * 0.23, 0.32, 0.5), 0.08, 10)
    m.box(0, 0, 0.08, 0.34, 0.15, 0.5)
    m.collider(0, 0, 0.55, 0.9, 0, 0.82)
    m.collider(0, 0.25, 0.95, 0.36, 0.07, 0.25)
    meshes.append(m)

    m = Mesh('crystal-cluster')
    for x, z, r, h in [(0, 0, 0.18, 1), (-0.27, 0.2, 0.13, 0.63),
                         (0.29, 0.17, 0.14, 0.72), (-0.18, -0.26, 0.1, 0.49),
                         (0.19, -0.27, 0.12, 0.54)]:
        m.cylinder(x, 0, z, r, h * 0.72, 5, top_radius=r * 0.78)
        m.cylinder(x, h * 0.72, z, r * 0.78, h * 0.28, 5, top_radius=0.005)
    m.collider(0, 0, 0.78, 0.82, 0, 0.72)
    meshes.append(m)

    m = Mesh('ribcage')
    m.segment((0, 0.8, -0.5), (0, 0.8, 0.5), 0.045, 7)
    for z in [-0.42, -0.21, 0, 0.21, 0.42]:
        # Each rib curves down around a truly empty interior.
        for sign in [-1, 1]:
            points = [(sign * 0.42 * math.sin(i * math.pi / 6),
                       0.12 + 0.7 * math.cos(i * math.pi / 6), z)
                      for i in range(4)]
            for i in range(3):
                m.segment(points[i], points[i + 1], 0.03, 6)
    m.segment((0, 0.02, -0.5), (0, 0.02, 0.5), 0.035, 6)
    for x in [-0.38, 0.38]:
        m.collider(x, 0, 0.1, 1, 0, 0.48)
    meshes.append(m)

    m = Mesh('organic-heart')
    m.ellipsoid((0, 0.47, 0), (0.5, 0.43, 0.44), 14, 9)
    # Raised arteries make the spherical core recognizable at a distance.
    for sign in [-1, 1]:
        m.segment((sign * 0.19, 0.72, 0), (sign * 0.29, 0.96, -0.04), 0.075, 8, 0.055)
        m.segment((sign * 0.39, 0.3, -0.13), (sign * 0.24, 0.63, -0.36), 0.04, 7)
    m.cylinder(0, 0, 0, 0.22, 0.16, 10, top_radius=0.33)
    m.collider(0, 0, 0.8, 0.8, 0, 0.82)
    meshes.append(m)

    m = Mesh('egg-cluster')
    for x, z, y, s in [(-0.25, 0.19, 0, 0.72), (0.24, 0.21, 0, 0.8),
                        (0, -0.23, 0, 1), (0.05, 0.16, 0.52, 0.58)]:
        m.ellipsoid((x, y + s * 0.37, z), (s * 0.22, s * 0.37, s * 0.23), 10, 7)
    m.collider(0, 0, 0.78, 0.78, 0, 0.83)
    meshes.append(m)

    m = Mesh('turbine')
    m.box(0, 0, 0.1, 0.82, 0.16, 0.8)
    # Main cylindrical housing faces along Z. Radial blades are physical geometry.
    m.segment((0, 0.55, -0.4), (0, 0.55, 0.4), 0.45, 16)
    m.segment((0, 0.55, -0.45), (0, 0.55, -0.39), 0.47, 16)
    m.segment((0, 0.55, -0.47), (0, 0.55, -0.43), 0.13, 10)
    for i in range(8):
        a = i * TAU / 8
        b = a + 0.33
        front = [(0.12 * math.cos(a), 0.55 + 0.12 * math.sin(a), -0.48),
                 (0.41 * math.cos(b), 0.55 + 0.41 * math.sin(b), -0.48),
                 (0.41 * math.cos(b + 0.22), 0.55 + 0.41 * math.sin(b + 0.22), -0.48),
                 (0.12 * math.cos(a + 0.45), 0.55 + 0.12 * math.sin(a + 0.45), -0.48)]
        m.surface(front[::-1])
    m.collider(0, 0, 0.9, 0.84, 0, 0.96)
    meshes.append(m)

    m = Mesh('cryopod')
    m.box(0, 0, 0, 0.65, 0.12, 0.65)
    m.ellipsoid((0, 0.5, 0), (0.34, 0.48, 0.28), 12, 8)
    for x in [-0.31, 0.31]:
        m.box(x, 0.12, 0, 0.07, 0.73, 0.15)
    m.box(0, 0.85, 0, 0.52, 0.12, 0.36)
    m.box(0.3, 0.41, -0.16, 0.13, 0.21, 0.08)
    m.collider(0, 0, 0.7, 0.62, 0, 1)
    meshes.append(m)

    m = Mesh('antenna')
    m.box(0, 0, 0, 0.62, 0.16, 0.62)
    m.cylinder(0, 0.16, 0, 0.055, 0.84, 8, top_radius=0.025)
    for z, y in [(-0.12, 0.59), (0, 0.75), (0.1, 0.9)]:
        m.segment((-0.5, y, z), (0.5, y, z), 0.022, 6)
    for x, z in [(-0.22, -0.22), (0.22, -0.22), (0, 0.22)]:
        m.segment((x, 0.16, z), (0, 0.55, 0), 0.016, 5)
    m.collider(0, 0, 0.62, 0.62, 0, 0.2)
    m.collider(0, 0, 0.14, 0.14, 0.2, 0.8)
    meshes.append(m)

    m = Mesh('generator')
    m.box(0, 0, 0, 0.92, 0.14, 0.82)
    m.box(0, 0.14, 0, 0.72, 0.5, 0.72)
    for x in [-0.23, 0.23]:
        m.cylinder(x, 0.63, 0.05, 0.11, 0.28, 10)
        m.cylinder(x, 0.91, 0.05, 0.14, 0.07, 10)
    for i in range(5):
        m.box(0, 0.24 + i * 0.066, -0.375, 0.55, 0.025, 0.035)
    m.box(0.37, 0.27, 0, 0.12, 0.29, 0.25)
    m.collider(0, 0, 0.92, 0.82, 0, 0.98)
    meshes.append(m)

    m = Mesh('train-carriage')
    m.box(0, 0.13, 0, 0.88, 0.4, 1)
    # Rounded roof with separate high windows rather than a box-shaped carriage.
    m.arch(0.64, 0.44, 0.27, 0.39, 0.22, 1, 10)
    m.box(0, 0.53, 0, 0.88, 0.12, 1)
    for sign in [-1, 1]:
        for z in [-0.36, -0.12, 0.12, 0.36]:
            m.box(sign * 0.448, 0.43, z, 0.025, 0.22, 0.16)
        for z in [-0.32, 0.32]:
            m.segment((sign * 0.35, 0.12, z), (sign * 0.48, 0.12, z), 0.12, 10)
    m.box(0, 0.26, -0.51, 0.31, 0.38, 0.025)
    m.collider(0, 0, 0.9, 1, 0, 0.92)
    meshes.append(m)

    m = Mesh('cargo-crate')
    m.box(0, 0, 0, 0.9, 0.92, 0.9)
    for x in [-0.45, 0.45]:
        for z in [-0.45, 0.45]:
            m.box(x, 0, z, 0.1, 1, 0.1)
    for y in [0.04, 0.84]:
        m.box(0, y, 0, 1, 0.12, 1)
    for z in [-0.46, 0.46]:
        m.segment((-0.36, 0.2, z), (0.36, 0.76, z), 0.028, 5)
        m.segment((0.36, 0.2, z), (-0.36, 0.76, z), 0.028, 5)
    m.collider(0, 0, 1, 1, 0, 1)
    meshes.append(m)

    # Authored orbital scenery is an actual UV sphere and a seeded 3D starfield.
    m = Mesh('distant-planet')
    m.ellipsoid((0, .5, 0), (.5, .5, .5), 32, 16)
    meshes.append(m)
    m = Mesh('starfield')
    import random
    rng = random.Random(8315)
    for i in range(96):
        angle = rng.random() * TAU
        elevation = rng.uniform(.14, .88)
        cx, cz = math.cos(angle) * .49, math.sin(angle) * .49
        radius = rng.uniform(.0012, .0029)
        points = [(cx-radius,elevation,cz-radius),(cx+radius,elevation,cz-radius),(cx,elevation+radius*1.7,cz),(cx,elevation,cz+radius)]
        for a,b,c in [(0,1,2),(1,3,2),(3,0,2),(0,3,1)]:
            m.surface([points[a],points[b],points[c]],[(.5,.5)]*3)
    meshes.append(m)
    for mesh in meshes:
        mesh.write(destination)


if __name__ == '__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,help='Write to a separate folder for reproducibility checks')
    build(parser.parse_args().output)
