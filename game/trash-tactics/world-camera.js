import {terrainHeight} from './terrain.js';

export const ELEVATION = 116;
export const DEFAULT_YAW = 14;
export const DEFAULT_PITCH = 44;
const ANGLE = Math.PI / 180;
const orientations = new WeakMap();

function orientation(yaw,pitch) {
 const cosYaw = Math.cos(yaw * ANGLE);
 const sinYaw = Math.sin(yaw * ANGLE);
 const sinPitch = Math.sin(pitch * ANGLE);
 const cosPitch = Math.cos(pitch * ANGLE);
 const right = Object.freeze([cosYaw,-sinYaw,0]);
 const up = Object.freeze([-sinYaw*sinPitch,-cosYaw*sinPitch,cosPitch]);
 return { yaw,pitch,cosYaw,sinYaw,sinPitch,cosPitch,basis:Object.freeze({right,up}),
  rayX:sinYaw*cosPitch/sinPitch,rayY:cosYaw*cosPitch/sinPitch };
}

const defaultOrientation = orientation(DEFAULT_YAW,DEFAULT_PITCH);
export const BILLBOARD_RIGHT = defaultOrientation.basis.right;
export const BILLBOARD_UP = defaultOrientation.basis.up;

function viewOrientation(camera) {
 const yaw=Number.isFinite(camera.yaw)?camera.yaw:DEFAULT_YAW;
 const pitch=Number.isFinite(camera.pitch)?Math.max(28,Math.min(78,camera.pitch)):DEFAULT_PITCH;
 if(yaw===DEFAULT_YAW&&pitch===DEFAULT_PITCH)return defaultOrientation;
 let current=orientations.get(camera);
 if(current?.yaw===yaw&&current.pitch===pitch)return current;
 current=orientation(yaw,pitch);
 orientations.set(camera,current);
 return current;
}

export function billboardBasis(camera) {
 return viewOrientation(camera).basis;
}

export function projectHeading(camera, angle) {
 const {cosYaw,sinYaw,sinPitch}=viewOrientation(camera);
 const x=Math.cos(angle),y=Math.sin(angle);
 return Math.atan2((sinYaw*x+cosYaw*y)*sinPitch,cosYaw*x-sinYaw*y);
}

export function projectWorld(camera, width, height, x, y, z = 0) {
 const {cosYaw,sinYaw,sinPitch,cosPitch}=viewOrientation(camera);
 const dx = x - camera.x;
 const dy = y - camera.y;
 const across = cosYaw * dx - sinYaw * dy;
 const into = sinYaw * dx + cosYaw * dy;
 return {
  x: width / 2 + across * camera.zoom,
  y: height / 2 + (into * sinPitch - z * cosPitch) * camera.zoom,
  depth: into * cosPitch + z * sinPitch
 };
}

export function projectGround(map, camera, width, height, x, y) {
 return projectWorld(camera, width, height, x, y, ELEVATION * terrainHeight(map || {}, x, y));
}

// Inverse ray of the orthographic camera. First intersect the flat floor,
// then test each upper face and ramp for a closer intersection.
export function pickGround(map, camera, width, height, screenX, screenY) {
 const {cosYaw,sinYaw,sinPitch,rayX,rayY}=viewOrientation(camera);
 const across = (screenX - width / 2) / camera.zoom;
 const into = (screenY - height / 2) / (camera.zoom * sinPitch);
 const x = camera.x + cosYaw * across + sinYaw * into;
 const y = camera.y - sinYaw * across + cosYaw * into;
 const contains = (area, px, py) => px >= area.x && py >= area.y && px <= area.x + area.w && py <= area.y + area.h;
 const upperX = x + rayX * ELEVATION;
 const upperY = y + rayY * ELEVATION;
 if ((map?.plateaus || []).some(p => contains(p, upperX, upperY)) && terrainHeight(map, upperX, upperY) >= .99999) {
  return { x: upperX, y: upperY };
 }
 let closest = 0;
 for (const ramp of map?.ramps || []) {
  const gx = ramp.side === 'west' ? 1 / ramp.w : ramp.side === 'east' ? -1 / ramp.w : 0;
  const gy = ramp.side === 'north' ? 1 / ramp.h : ramp.side === 'south' ? -1 / ramp.h : 0;
  const base = (gx * (x - ramp.x) + gy * (y - ramp.y) + Number(gx < 0 || gy < 0)) * ELEVATION;
  const z = base / (1 - ELEVATION * (gx * rayX + gy * rayY));
  if (z > closest && z <= ELEVATION && contains(ramp, x + rayX * z, y + rayY * z)) closest = z;
 }
 return { x: x + rayX * closest, y: y + rayY * closest };
}

// Unproject a screen-space drag to world pan on the lower plane.
export function panVector(camera, dx, dy) {
 const {cosYaw,sinYaw,sinPitch}=viewOrientation(camera);
 const across = dx / camera.zoom;
 const into = dy / (camera.zoom * sinPitch);
 return { x: cosYaw * across + sinYaw * into, y: -sinYaw * across + cosYaw * into };
}

export function getViewMatrix(camera, width, height) {
 const {cosYaw,sinYaw,sinPitch,cosPitch}=viewOrientation(camera);
 const xScale = 2 * camera.zoom / width;
 const yScale = -2 * camera.zoom / height;
 const zScale = -1 / 16000;
 const ux = xScale * cosYaw;
 const uy = -xScale * sinYaw;
 const vx = yScale * sinPitch * sinYaw;
 const vy = yScale * sinPitch * cosYaw;
 const wx = zScale * cosPitch * sinYaw;
 const wy = zScale * cosPitch * cosYaw;
 return new Float32Array([
  ux, vx, wx, 0,
  uy, vy, wy, 0,
  0, -yScale * cosPitch, zScale * sinPitch, 0,
  -ux * camera.x - uy * camera.y,
  -vx * camera.x - vy * camera.y,
  -wx * camera.x - wy * camera.y,
  1
 ]);
}
