'use strict';

// All paths are static. Game names and other catalog content are escaped by the renderer.
const paths = {
  library: '<path d="M3 4h4v16H3zM10 4h3v16h-3zM15 5l3-1 4 15-3 1z"/>',
  lobbies: '<circle cx="9" cy="8" r="3"/><path d="M3 19v-2a5 5 0 0 1 5-5h2a5 5 0 0 1 5 5v2M15 5a3 3 0 0 1 0 6m3 2a5 5 0 0 1 3 5v1"/>',
  rankings: '<path d="M8 4h8v5a4 4 0 0 1-8 0zM8 6H4v2a4 4 0 0 0 4 4m8-6h4v2a4 4 0 0 1-4 4m-4 2v4m-4 2h8m-6-2h4"/>',
  downloads: '<path d="M12 3v11m-4-4 4 4 4-4M4 15v5h16v-5"/>',
  settings: '<path d="m9 3-.5 2-2 .9-2-.5L2.9 8l1.5 1.6-.1 2.5L3 14l1.5 2.8 2-.4 2.1 1 .5 2.2h4l.5-2.2 2-.9 2 .4L19.5 14 18 12l.1-2.4 1.4-1.7L18 5.3l-2 .5-2-1L13.4 3z"/><circle cx="11.3" cy="11.4" r="2.8"/>',
  chevrons: '<path d="m8 9 4-4 4 4m-8 6 4 4 4-4"/>',
  search: '<circle cx="10" cy="10" r="6"/><path d="m15 15 5 5"/>',
  star: '<path d="m12 3 2.8 5.8 6.4.9-4.6 4.5 1.1 6.3-5.7-3-5.7 3 1.1-6.3-4.6-4.5 6.4-.9z"/>',
  flag: '<path d="M5 21V4m0 1c4-4 8 4 14 0l-1 9c-6 4-9-4-13 0"/>',
  play: '<path fill="currentColor" stroke="none" d="M7 4.8a1 1 0 0 1 1.5-.85l12 7.2a1 1 0 0 1 0 1.7l-12 7.2a1 1 0 0 1-1.5-.85z"/>',
  left: '<path d="m14.5 5-7 7 7 7"/>', right: '<path d="m9.5 5 7 7-7 7"/>',
  back: '<path d="m10 5-7 7 7 7M3 12h17"/>', arrow: '<path d="M4 12h15m-7-7 7 7-7 7"/>',
  external: '<path d="M13 4h7v7m0-7L10 14m0-8H5a1 1 0 0 0-1 1v12a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-5"/>',
  clock: '<circle cx="12" cy="12" r="8.5"/><path d="M12 7v5l3.5 2"/>',
  info: '<circle cx="12" cy="12" r="8.5"/><path d="M12 11v5m0-9v.1"/>',
  gamepad: '<path d="M8 7h8c3 0 4 2 5 8 1 5-3 6-6 1H9c-3 5-7 4-6-1 1-6 2-8 5-8zM6 11h4m-2-2v4m7-2h.1m3 2h.1"/>',
  user: '<circle cx="12" cy="8" r="3.5"/><path d="M5 20a7 7 0 0 1 14 0"/>',
  box: '<path d="m3.5 7.3 8.5-4.5 8.5 4.5V17L12 21.5 3.5 17zM4 7.5l8 4.5 8-4.5M12 12v9"/>',
  globe: '<circle cx="12" cy="12" r="9"/><ellipse cx="12" cy="12" rx="4" ry="9"/><path d="M3 12h18"/>',
  check: '<path d="m4 12 5 5L20 6"/>', close: '<path d="m5 5 14 14m0-14L5 19"/>',
  refresh: '<path d="M20 11a8 8 0 1 0-2 6m2-12v6h-6"/>',
  image: '<rect x="3" y="4" width="18" height="16" rx="2"/><circle cx="9" cy="9" r="1.5"/><path d="m3 18 5-5 4 3 4-6 5 7"/>',
  link: '<path d="m10 14 4-4m-5 5-1 1a3.5 3.5 0 0 1-5-5l4-4a3.5 3.5 0 0 1 5 0m0 10a3.5 3.5 0 0 0 5 0l4-4a3.5 3.5 0 0 0-5-5l-1 1"/>',
  disc: '<circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="2"/><path d="M6.5 12a5.5 5.5 0 0 1 5.5-5.5"/>'
};
function icon(name) {
  return '<svg aria-hidden="true" focusable="false" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.65" stroke-linecap="round" stroke-linejoin="round">' + (paths[name] || '') + '</svg>';
}
