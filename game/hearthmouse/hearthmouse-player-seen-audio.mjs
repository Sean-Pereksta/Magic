const PLAYER_ID = "player";
const PLAYER_SEEN_MEOW_COOLDOWN = 0.7;

export function playPlayerSeenMeow(engine, AudioCtor = globalThis.Audio) {
  if (!engine || typeof AudioCtor !== "function") return false;

  const currentTime = Number(engine.time) || 0;
  const previousTime = Number(engine.__playerSeenMeowTime);
  if (Number.isFinite(previousTime) && currentTime - previousTime < PLAYER_SEEN_MEOW_COOLDOWN) return false;

  let audio = engine.__playerSeenMeowAudio;
  if (!audio) {
    audio = new AudioCtor(new URL("./audio/meow.mp3", import.meta.url).href);
    audio.preload = "auto";
    audio.volume = 0.9;
    engine.__playerSeenMeowAudio = audio;
  }

  engine.__playerSeenMeowTime = currentTime;
  try {
    audio.currentTime = 0;
  } catch {
    // Some browser audio implementations do not allow seeking until metadata is ready.
  }
  audio.play?.().catch?.(() => void 0);
  return true;
}

export function installPlayerSeenMeow(I = globalThis.window?.HearthmouseInternals) {
  const proto = I?.Engine?.prototype;
  if (!proto || typeof proto.processCatVision !== "function") return false;
  if (proto.__playerSeenMeowInstalled) return true;

  const coreProcessCatVision = proto.processCatVision;
  Object.defineProperty(proto, "__playerSeenMeowInstalled", { value: true });

  proto.processCatVision = function meowWhenCatSeesPlayer(cat, target, interval) {
    const wasChasingPlayer = cat?.state === "chase" && cat?.targetId === PLAYER_ID;
    const result = coreProcessCatVision.call(this, cat, target, interval);
    const isChasingPlayer = cat?.state === "chase" && cat?.targetId === PLAYER_ID;

    if (!wasChasingPlayer && isChasingPlayer && target?.id === PLAYER_ID) {
      playPlayerSeenMeow(this);
    }
    return result;
  };

  return true;
}

function installWhenReady() {
  if (typeof window === "undefined") return;
  if (!installPlayerSeenMeow(window.HearthmouseInternals)) {
    window.setTimeout(installWhenReady, 16);
  }
}

if (typeof window !== "undefined") installWhenReady();
