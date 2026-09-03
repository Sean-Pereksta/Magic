export const VIEW = { width: 960, height: 540, floorY: 468 };

export const PHYSICS = {
  gravity: 2700,
  runSpeed: 430,
  sandalsSpeed: 505,
  groundAcceleration: 3600,
  airAcceleration: 2450,
  groundFriction: 4100,
  airFriction: 580,
  jumpVelocity: 900,
  coyoteTime: 0.11,
  jumpBuffer: 0.13,
  maxFall: 1280,
  dropThroughTime: 0.18,
  fixedStep: 1 / 120,
  maxSteps: 8,
  playerWidth: 34,
  playerHeight: 62,
  requiredLandingWidth: 42,
};

export const COLORS = {
  galilee: { sky: "#ccecff", far: "#8bc9ca", ground: "#8a6d42", accent: "#f4d98b" },
  rome: { sky: "#f5d9bf", far: "#c89779", ground: "#725b50", accent: "#d6af5e" },
  jerusalem: { sky: "#f7e6bf", far: "#c6a36f", ground: "#8c704f", accent: "#ece0b7" },
  hell: { sky: "#29151d", far: "#541d27", ground: "#21191e", accent: "#ff704d" },
  heaven: { sky: "#dff4ff", far: "#9fc8ff", ground: "#f7fbff", accent: "#f2cf69" },
};

export const POWER_LABELS = {
  sandals: "Sandals",
  wings: "Angel Wings",
  shield: "Shield",
  bread: "Bread",
  holyLight: "Holy Light",
  fireRain: "Fire Rain",
};

export const FIREBASE_CONFIG = {
  apiKey: "AIzaSyB7twY7z31ucB6pGA8JC_HrVMZhA8lNaJA",
  authDomain: "bible-game-246c0.firebaseapp.com",
  databaseURL: "https://bible-game-246c0-default-rtdb.firebaseio.com",
  projectId: "bible-game-246c0",
  storageBucket: "bible-game-246c0.firebasestorage.app",
  messagingSenderId: "959619818996",
  appId: "1:959619818996:web:5a9fbf492e23c765e445a1",
  measurementId: "G-8PR6LVKSH3",
};

