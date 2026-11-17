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
  const connections = WALL_DIRECTIONS.filter(({ x, y }) =>
    isWall(wallsByCell.get(wallKey(wall.x + x, wall.y + y))?.type),
  );
  return connections.length ? connections : WALL_DIRECTIONS.filter(direction => direction[wall.axis === 'y' ? 'y' : 'x']);
}
