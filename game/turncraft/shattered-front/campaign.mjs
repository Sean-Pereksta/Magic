// Authored campaign content. Objectives are simulated by the director, not skirmish aliases.
export const CHARACTERS = [
  {
    id: 'elara_venn',
    name: 'Commander Elara Venn',
    role: 'The Strategist',
    glyph: '✧'
  },
  { id: 'kade_rusk', name: 'Kade Rusk', role: 'The Engineer', glyph: '⚙' },
  { id: 'nyx', name: 'Nyx', role: 'The Pathfinder', glyph: '◈' },
  {
    id: 'archivist_voss',
    name: 'Archivist Voss',
    role: 'The Historian',
    glyph: '⌘'
  },
  { id: 'conductor', name: 'The Conductor', role: 'The Fracture', glyph: '⟁' }
];
const palette = (accent, ground, rock, lane) => ({
  accent,
  background: '#08101b',
  ground,
  grass: ground,
  moss: ground,
  sand: ground,
  dust: ground,
  ruins: lane,
  lane,
  expansion: lane,
  rock,
  crystal: rock
});
export const REGIONS = [
  {
    id: 'amber-wastes',
    name: 'Amber Wastes',
    theme: 'desert',
    description:
      'Canyon settlements and a refinery at the edge of a gathering sandstorm.',
    weather: 'sandstorm',
    palette: palette('#edbd72', '#77603e', '#b68d55', '#a78a5b')
  },
  {
    id: 'ironwood-basin',
    name: 'Ironwood Basin',
    theme: 'jungle',
    description:
      'Forest corridors hide flanking armies and ancient command stations.',
    weather: 'canopy',
    palette: palette('#9cd18c', '#284e3c', '#1c3028', '#6d7850')
  },
  {
    id: 'sundered-coast',
    name: 'Sundered Coast',
    theme: 'coast',
    description:
      'Tidal islands, harbor causeways and exposed evacuation routes.',
    weather: 'rain',
    palette: palette('#7cd9e7', '#365866', '#102a43', '#91a6a2')
  },
  {
    id: 'ashglass-expanse',
    name: 'Ashglass Expanse',
    theme: 'volcanic',
    description:
      'Basalt bridges span energy fissures around the Fracture generators.',
    weather: 'eruption',
    palette: palette('#ff9c70', '#3d2930', '#8f4536', '#735951')
  },
  {
    id: 'frozen-rift',
    name: 'Frozen Rift',
    theme: 'ice',
    description:
      'A ring of frozen peaks shelters the research station and its secrets.',
    weather: 'blizzard',
    palette: palette('#afe9ff', '#416578', '#b2dae2', '#7099ac')
  },
  {
    id: 'heart-of-fracture',
    name: 'Heart of the Fracture',
    theme: 'fracture',
    description:
      'Mechanical platforms and unstable gates surround the Conductor.',
    weather: 'energy-storm',
    palette: palette('#d4a0ff', '#352c52', '#17112e', '#78688e')
  }
].map((r, index) => ({
  ...r,
  index,
  mapId: `sf-${r.id}`,
  assetId: r.id.replaceAll('-', '_')
}));
const protect = (id, label, x, y) => ({ id, type: 'protect', label, x, y });
const capture = (id, label, x, y, seconds = 10, optional = false) => ({
  id,
  type: 'capture',
  label,
  x,
  y,
  seconds,
  optional
});
const destroy = (id, label, x, y, optional = false) => ({
  id,
  type: 'destroy',
  label,
  x,
  y,
  optional
});
const escort = (id, label, x, y, to) => ({
  id,
  type: 'escort',
  label,
  x,
  y,
  to
});
const rescue = (id, label, x, y, optional = true) => ({
  id,
  type: 'rescue',
  label,
  x,
  y,
  optional
});
const scripts = [
  [
    'Dust Signal',
    'elara_venn',
    'The settlement requested a patrol. The patrol requested an army. You are the army.',
    [
      protect('settlement', 'Keep the settlement command base alive', 6, 15),
      capture('salvage', 'Recover the ruined mine stores', 17, 7, 8, true)
    ],
    4,
    'scout'
  ],
  [
    'Canyon Lifeline',
    'kade_rusk',
    'That crawler is carrying our entire repair budget. Please return it with most of its wheels.',
    [
      escort(
        'crawler',
        'Escort the supply crawler to the east settlement',
        9,
        15,
        { x: 26, y: 15 }
      ),
      rescue('miners', 'Rescue the stranded miners', 17, 24)
    ],
    5,
    'flank'
  ],
  [
    'The Broken Relay',
    'archivist_voss',
    'The relay predates every map in the archive. Naturally, I would like to switch it on.',
    [
      capture('relay', 'Reactivate the relay', 22, 14, 18),
      destroy('artillery', 'Silence the canyon artillery', 29, 6, true)
    ],
    5,
    'siege'
  ],
  [
    'Last Refinery',
    'elara_venn',
    'Hold the refinery. If it burns, Kade will have to invent fuel, and nobody wants that.',
    [
      protect('refinery', 'Defend the last refinery', 14, 15),
      capture('reserve', 'Secure the emergency fuel reserve', 22, 25, 12, true)
    ],
    7,
    'coordinated'
  ],
  [
    'Under the Canopy',
    'nyx',
    'The forest has three paths. They think we only noticed two.',
    [
      capture('watch', 'Take the hidden watchpoint', 19, 8, 12),
      rescue('scouts', 'Recover the lost scout team', 9, 24)
    ],
    5,
    'ambush'
  ],
  [
    'Thorn Road',
    'elara_venn',
    'Two roads, one supply column. Make the enemy regret choosing either.',
    [
      escort('column', 'Guide the supply column through Thorn Road', 8, 8, {
        x: 31,
        y: 23
      }),
      capture('grove', 'Claim the resource grove', 12, 22, 12, true)
    ],
    6,
    'flank'
  ],
  [
    'Roots of Iron',
    'kade_rusk',
    'The ancient machinery still works. I am choosing to call that encouraging.',
    [
      capture('engine', 'Activate the ancient defensive engine', 21, 15, 22),
      protect('outpost', 'Keep the forward outpost operational', 12, 12)
    ],
    6,
    'armor'
  ],
  [
    'Siege of Ironwood',
    'nyx',
    'Their command stations are talking to one another. Let us interrupt the conversation.',
    [
      destroy('west-command', 'Destroy the western command station', 24, 8),
      destroy('east-command', 'Destroy the eastern command station', 31, 23),
      rescue('pathfinders', 'Free the captured pathfinders', 11, 24)
    ],
    8,
    'coordinated'
  ],
  [
    'Tidebreak',
    'elara_venn',
    'The sea took the roads. The Fracture took the harbor. We will start with the harbor.',
    [
      protect('beacon', 'Protect the harbor beacon', 15, 7),
      capture('stores', 'Salvage the flooded depot', 20, 24, 12, true)
    ],
    6,
    'swarm'
  ],
  [
    'Between Two Shores',
    'nyx',
    'They are watching the bridge. Good thing we know where the second bridge is.',
    [
      protect('north-dock', 'Hold the north dock', 15, 7),
      protect('south-dock', 'Hold the south dock', 15, 23)
    ],
    6,
    'flank'
  ],
  [
    'Storm Lantern',
    'kade_rusk',
    'Repair the beacon while I repair the weather. That second part may take longer.',
    [
      capture('lantern', 'Power the storm lantern', 22, 15, 25),
      destroy('battery', 'Destroy the coastal battery', 32, 7, true)
    ],
    7,
    'siege'
  ],
  [
    'The Last Crossing',
    'elara_venn',
    'Every ship that leaves is a city that lives. Keep that causeway open.',
    [
      escort(
        'evacuation',
        'Escort the evacuation convoy to the harbor',
        7,
        15,
        { x: 30, y: 15 }
      ),
      protect('dock', 'Defend the evacuation dock', 26, 15)
    ],
    9,
    'coordinated'
  ],
  [
    'Black Horizon',
    'archivist_voss',
    'Those fissures are carrying instructions. The invasion is a network, Commander.',
    [
      capture('sampler', 'Recover a fissure sample', 20, 14, 18),
      rescue('crew', 'Rescue the survey crew', 10, 24)
    ],
    7,
    'armor'
  ],
  [
    'Furnace Choir',
    'kade_rusk',
    'If the support towers stop humming, the big machines should stop moving. Probably.',
    [
      destroy('support-a', 'Destroy the west support tower', 25, 8),
      destroy('support-b', 'Destroy the east support tower', 31, 23)
    ],
    8,
    'siege'
  ],
  [
    'Contested Fire',
    'nyx',
    'Their elites want the same mineral field. We arrived first. That matters to me.',
    [
      capture('field', 'Secure the contested resource field', 21, 18, 28),
      protect('collector', 'Keep the collector alive', 12, 12)
    ],
    8,
    'elite'
  ],
  [
    'Shatter the Grid',
    'conductor',
    'You have removed instruments. The composition remains.',
    [
      destroy('generator-a', 'Destroy generator Alpha', 25, 7),
      destroy('generator-b', 'Destroy generator Beta', 32, 15),
      destroy('generator-c', 'Destroy generator Gamma', 25, 24)
    ],
    10,
    'coordinated'
  ],
  [
    'White Silence',
    'nyx',
    'Fresh tracks. No engines. Something was dropped behind us.',
    [
      protect('station', 'Defend the mountain relay', 13, 15),
      rescue('researchers', 'Locate the missing researchers', 21, 6)
    ],
    8,
    'ambush'
  ],
  [
    'Cold Storage',
    'kade_rusk',
    'The reserve army is frozen solid. Warm the power cores, preferably before our army joins it.',
    [
      capture('core-a', 'Thaw the north power core', 20, 7, 20),
      capture('core-b', 'Thaw the south power core', 20, 23, 20)
    ],
    8,
    'armor'
  ],
  [
    'The Deliberate Spark',
    'archivist_voss',
    'Someone opened the Fracture on purpose. The evidence is here. So is the guard.',
    [
      capture('archive', 'Download the activation archive', 24, 15, 35),
      destroy('guard', 'Destroy the archive sentinel', 30, 15, true)
    ],
    9,
    'elite'
  ],
  [
    'Location: Conductor',
    'elara_venn',
    'Voss needs three more minutes. He also asked for quiet. One of those requests is achievable.',
    [
      protect('research', 'Hold the research station', 15, 15),
      capture('uplink', 'Keep the uplink connected', 22, 15, 40)
    ],
    11,
    'coordinated'
  ],
  [
    'Across the Threshold',
    'conductor',
    'Your arrival was included in the model. Your persistence remains inconvenient.',
    [
      capture('anchor', 'Establish the coalition anchor', 19, 15, 25),
      destroy('gate', 'Close the pursuit gateway', 31, 7, true)
    ],
    9,
    'flank'
  ],
  [
    'A Broken Measure',
    'nyx',
    'These routes change when the storm hits. Watch the gates, not the ground.',
    [
      destroy('pylon-a', 'Break the north phase pylon', 25, 7),
      destroy('pylon-b', 'Break the south phase pylon', 25, 23),
      rescue('squad', 'Extract the trapped assault squad', 12, 24)
    ],
    10,
    'coordinated'
  ],
  [
    'Silence the Orchestra',
    'kade_rusk',
    'That is the last stabilizer. Once it goes, this place becomes very unfriendly to its owner.',
    [
      destroy('stabilizer', 'Destroy the Fracture stabilizer', 30, 15),
      capture('weapon', 'Charge the coalition superweapon', 19, 15, 40)
    ],
    11,
    'elite'
  ],
  [
    'The Conductor’s Last Stand',
    'conductor',
    'I have learned every formation. Every retreat. Show me something I cannot predict.',
    [
      destroy('conductor', 'Destroy the Conductor core', 32, 15),
      destroy('gate-a', 'Collapse the north gateway', 27, 7),
      destroy('gate-b', 'Collapse the south gateway', 27, 23),
      protect('anchor', 'Protect the coalition anchor', 12, 15)
    ],
    15,
    'finale'
  ]
];
export const MISSIONS = scripts.map(
  ([title, speaker, text, objectives, waves, strategy], index) => {
    const region = REGIONS[Math.floor(index / 4)],
      number = (index % 4) + 1;
    return {
      id: `${region.id}-${number}`,
      index,
      number,
      title,
      name: title,
      regionId: region.id,
      mapId: region.mapId,
      waves,
      strategy,
      preparationSeconds: index === 0 ? 70 : 50,
      betweenWavesSeconds: index > 15 ? 30 : 40,
      objectives,
      briefing: { speaker, text },
      victory: {
        speaker: index === 23 ? 'elara_venn' : speaker,
        text:
          index === 23
            ? 'The signal is gone. For the first time, the world is quiet. Enjoy it, Commander. We have rebuilding to do.'
            : `${title} is secure. The coalition has a way forward.`
      },
      reward: {
        minerals: 120 + index * 20,
        badge: number === 4 ? `${region.id}-liberator` : null
      },
      events: [
        {
          wave: Math.ceil(waves / 2),
          speaker: 'nyx',
          text: 'A second force is moving. Watch the exposed route.'
        },
        {
          wave: waves,
          speaker: number === 4 ? 'conductor' : 'elara_venn',
          text:
            number === 4
              ? 'Adaptation complete. Commencing final movement.'
              : 'This is their last assault. Hold the line.'
        }
      ]
    };
  }
);
export const ARENAS = REGIONS.map((region, index) => ({
  id: region.id,
  regionId: region.id,
  mapId: region.mapId,
  name: [
    'Amber Wastes Outpost',
    'Ironwood Fortress',
    'Sundered Harbor',
    'Ashglass Citadel',
    'Frozen Bastion',
    'Fracture Nexus'
  ][index],
  description: region.description,
  unlockMission: index ? MISSIONS[index * 4 - 1].id : null
}));
export const CATALOG = {
  regions: REGIONS,
  missions: MISSIONS,
  arenas: ARENAS,
  characters: CHARACTERS
};
export function missionUnlocked(id, completed = []) {
  const m = MISSIONS.find((v) => v.id === id);
  return (
    !!m &&
    (m.index === 0 ||
      completed.includes(MISSIONS[m.index - 1].id) ||
      completed.includes(id))
  );
}

// Six separately authored topologies, with passable routes explicitly carved to every objective.
export function campaignLayout(regionId, seed = 1) {
  const region =
      REGIONS.find((r) => r.id === regionId || r.mapId === regionId) ||
      REGIONS[0],
    width = 42,
    height = 30;
  let randSeed = seed >>> 0;
  const random = () => {
    randSeed = (Math.imul(randSeed, 1664525) + 1013904223) >>> 0;
    return randSeed / 4294967296;
  };
  const starts = [
      { x: 6, y: 15, rotation: 0 },
      { x: 35, y: 15, rotation: Math.PI },
      { x: 21, y: 25, rotation: -Math.PI / 2 }
    ],
    expansions = [
      { x: 13, y: 15, naturalFor: 0 },
      { x: 29, y: 15, naturalFor: 1 },
      { x: 21, y: 23, naturalFor: 2 },
      { x: 11, y: 6 },
      { x: 11, y: 24 },
      { x: 22, y: 15, rich: true }
    ].map((s) => ({ rotation: 0, ...s }));
  const ground =
    region.theme === 'desert'
      ? 'sand'
      : region.theme === 'jungle'
        ? 'grass'
        : region.theme === 'ice'
          ? 'dust'
          : 'ruins';
  const terrain = Array.from({ length: width * height }, (_, i) => ({
    x: i % width,
    y: Math.floor(i / width),
    type: random() < 0.12 ? 'dust' : ground,
    blocked: region.index === 2 || region.index === 5
  }));
  const paint = (x, y, type, blocked = false) => {
    const t = terrain[y * width + x];
    if (x >= 0 && x < width && y >= 0 && y < height && t) {
      t.type = type;
      t.blocked = blocked;
    }
  };
  const rect = (x1, y1, x2, y2, type = 'lane', blocked = false) => {
    for (let y = y1; y <= y2; y++)
      for (let x = x1; x <= x2; x++) paint(x, y, type, blocked);
  };
  const circle = (cx, cy, r, type = 'ground') => {
    for (let y = Math.max(0, cy - r); y <= Math.min(height - 1, cy + r); y++)
      for (let x = Math.max(0, cx - r); x <= Math.min(width - 1, cx + r); x++)
        if (Math.hypot(x - cx, y - cy) <= r) paint(x, y, type);
  };
  const route = (a, b, w = 1) => {
    const steps = Math.max(Math.abs(a.x - b.x), Math.abs(a.y - b.y)) * 2;
    for (let n = 0; n <= steps; n++) {
      const f = steps ? n / steps : 0,
        x = Math.round(a.x + (b.x - a.x) * f),
        y = Math.round(a.y + (b.y - a.y) * f);
      rect(x - w, y - w, x + w, y + w);
    }
  };
  if (region.index === 0) {
    for (const y of [8, 22]) {
      rect(14, y - 1, 24, y + 1, 'rock', true);
      rect(28, y, 36, y + 1, 'rock', true);
    }
    rect(2, 1, 4, 7, 'rock', true);
    rect(37, 22, 40, 28, 'rock', true);
  }
  if (region.index === 1) {
    for (let y = 0; y < height; y++) {
      const x = Math.round(21 + Math.sin(y * 0.35) * 4);
      rect(x - 3, y, x + 3, y, 'rock', true);
    }
    for (const y of [7, 15, 23]) route({ x: 11, y }, { x: 34, y });
  }
  if (region.index === 2) {
    for (const [x, y, r] of [
      [6, 15, 6],
      [15, 7, 5],
      [15, 23, 5],
      [27, 15, 6],
      [35, 15, 5]
    ])
      circle(x, y, r);
    route(starts[0], { x: 15, y: 7 });
    route({ x: 15, y: 7 }, starts[1]);
    route(starts[0], { x: 15, y: 23 });
    route({ x: 15, y: 23 }, starts[1]);
    route(starts[0], starts[1], 1);
  }
  if (region.index === 3) {
    for (let y = 0; y < height; y++)
      rect(
        20 + Math.floor(y / 8),
        y,
        24 + Math.floor(y / 8),
        y,
        y % 4 ? 'rock' : 'crystal',
        true
      );
    for (const y of [7, 15, 24]) route({ x: 14, y }, { x: 33, y });
    rect(8, 22, 10, 26, 'crystal', true);
  }
  if (region.index === 4) {
    for (let y = 0; y < height; y++)
      for (let x = 0; x < width; x++) {
        const d = ((x - 22) / 11) ** 2 + ((y - 15) / 9) ** 2;
        if (d > 0.65 && d < 1.5) paint(x, y, 'crystal', true);
      }
    for (const y of [7, 15, 23]) route({ x: 12, y }, { x: 35, y });
    route({ x: 22, y: 4 }, { x: 22, y: 26 });
  }
  if (region.index === 5) {
    for (const [x, y, r] of [
      [6, 15, 6],
      [12, 7, 4],
      [12, 23, 4],
      [21, 15, 5],
      [28, 7, 4],
      [28, 23, 4],
      [35, 15, 5]
    ])
      circle(x, y, r, 'ruins');
    for (const y of [7, 23]) {
      route(starts[0], { x: 28, y });
      route({ x: 28, y }, starts[1]);
    }
    route(starts[0], starts[1]);
  }
  for (const point of [...starts, ...expansions])
    circle(
      point.x,
      point.y,
      point === starts[0] ? 5 : 3,
      point.naturalFor !== undefined ? 'expansion' : 'ground'
    );
  for (const e of expansions) route(e, starts[0]);
  for (const m of MISSIONS.filter((m) => m.regionId === region.id))
    for (const o of m.objectives) {
      circle(o.x, o.y, 2, 'ruins');
      route({ x: o.x, y: o.y }, starts[0]);
      if (o.to) route(o, o.to);
    }
  const spawnPoints = [
    { x: 36, y: 5 },
    { x: 36, y: 15 },
    { x: 36, y: 25 },
    { x: 21, y: 3 },
    { x: 21, y: 27 }
  ];
  for (const p of spawnPoints) {
    circle(p.x, p.y, 2);
    route(p, starts[1]);
  }
  for (const t of terrain)
    if (t.blocked)
      t.type =
        region.index === 2 ? 'rock' : region.index === 5 ? 'crystal' : t.type;
  return {
    id: region.mapId,
    width,
    height,
    terrain,
    starts,
    expansions,
    spawnPoints,
    conquestSites: [
      { x: 21, y: 15, label: 'Fracture Relay' },
      { x: 14, y: 7, label: 'North Approach' },
      { x: 14, y: 23, label: 'South Approach' }
    ],
    neutralSites: [],
    relic: { x: 21, y: 15 }
  };
}
