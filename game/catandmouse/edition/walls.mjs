export const WALL_DIRECTIONS = Object.freeze([
  Object.freeze({ x: -1, y: 0 }),
  Object.freeze({ x: 0, y: -1 }),
  Object.freeze({ x: 1, y: 0 }),
  Object.freeze({ x: 0, y: 1 }),
]);

export function isWall(type) {
  return type === 'wall' || type === 'barricade';
}

export function wallKey(x, y) {
  return `${x},${y}`;
}

export function wallConnections(wall, wallsByCell) {
  return WALL_DIRECTIONS.filter(({ x, y }) =>
    isWall(wallsByCell.get(wallKey(wall.x + x, wall.y + y))?.type),
  );
}
