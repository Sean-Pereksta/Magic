import assert from "node:assert/strict";
import test from "node:test";

import {
  installPlayerSeenMeow,
  playPlayerSeenMeow,
} from "./hearthmouse-player-seen-audio.mjs";

class FakeAudio {
  static instances = [];

  constructor(src) {
    this.src = src;
    this.currentTime = 0;
    this.preload = "";
    this.volume = 1;
    this.playCount = 0;
    FakeAudio.instances.push(this);
  }

  play() {
    this.playCount++;
    return Promise.resolve();
  }
}

test("plays meow.mp3 with a short duplicate-sighting cooldown", () => {
  const engine = { time: 1 };

  assert.equal(playPlayerSeenMeow(engine, FakeAudio), true);
  assert.equal(FakeAudio.instances.length, 1);
  assert.match(FakeAudio.instances[0].src, /\/audio\/meow\.mp3$/);
  assert.equal(FakeAudio.instances[0].playCount, 1);

  engine.time = 1.2;
  assert.equal(playPlayerSeenMeow(engine, FakeAudio), false);
  assert.equal(FakeAudio.instances[0].playCount, 1);

  engine.time = 1.8;
  assert.equal(playPlayerSeenMeow(engine, FakeAudio), true);
  assert.equal(FakeAudio.instances[0].playCount, 2);
});

test("cat vision meows only when the cat newly locks onto the player", () => {
  class Engine {}
  Engine.prototype.processCatVision = function baseVision(cat, target) {
    if (target?.id) {
      cat.state = "chase";
      cat.targetId = target.id;
    }
    return target?.id ?? null;
  };

  assert.equal(installPlayerSeenMeow({ Engine }), true);

  const previousAudio = globalThis.Audio;
  globalThis.Audio = FakeAudio;
  try {
    const engine = new Engine();
    engine.time = 5;
    const cat = { state: "relaxed", targetId: null };
    const startingInstances = FakeAudio.instances.length;

    engine.processCatVision(cat, { id: "ally-mouse" }, 0.1);
    assert.equal(FakeAudio.instances.length, startingInstances);

    cat.state = "search";
    cat.targetId = null;
    engine.time = 6;
    engine.processCatVision(cat, { id: "player" }, 0.1);
    assert.equal(FakeAudio.instances.length, startingInstances + 1);
    assert.equal(FakeAudio.instances.at(-1).playCount, 1);

    engine.time = 6.2;
    engine.processCatVision(cat, { id: "player" }, 0.1);
    assert.equal(FakeAudio.instances.at(-1).playCount, 1);
  } finally {
    globalThis.Audio = previousAudio;
  }
});
