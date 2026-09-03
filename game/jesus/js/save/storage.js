import { FIREBASE_CONFIG } from "../config.js";
import { createJourneySave } from "../world/journey.js";

const JOURNEY_KEY = "jesusRunJourneyV2";
const SCORE_KEY = "jesusRunHighscores";

function safeParse(raw, fallback) {
  try { return raw ? JSON.parse(raw) : fallback; }
  catch { return fallback; }
}

function normalizeScores(rows) {
  return (Array.isArray(rows) ? rows : [])
    .filter((row) => row && Number.isFinite(Number(row.score)))
    .map((row) => ({
      name: String(row.name || "Anonymous").slice(0, 20),
      score: Math.max(0, Math.floor(Number(row.score))),
      createdAt: Number(row.createdAt) || Date.now(),
    }))
    .sort((a, b) => (b.score - a.score) || (a.createdAt - b.createdAt))
    .slice(0, 10);
}

export class GameStorage {
  constructor() {
    this.db = null;
    this.cloudEnabled = true;
    try {
      if (window.firebase) {
        if (!window.firebase.apps.length) window.firebase.initializeApp(FIREBASE_CONFIG);
        this.db = window.firebase.firestore();
      }
    } catch {
      this.db = null;
    }
  }

  loadJourney() {
    const saved = safeParse(localStorage.getItem(JOURNEY_KEY), null);
    if (!saved || saved.version !== 2) return createJourneySave();
    return { ...createJourneySave(), ...saved };
  }

  saveJourney(save) {
    try {
      localStorage.setItem(JOURNEY_KEY, JSON.stringify({ ...save, updatedAt: Date.now() }));
      return true;
    } catch { return false; }
  }

  clearJourney() {
    localStorage.removeItem(JOURNEY_KEY);
    return createJourneySave();
  }

  localScores() {
    return normalizeScores(safeParse(localStorage.getItem(SCORE_KEY), []));
  }

  async scores() {
    const local = this.localScores();
    if (!this.db || !this.cloudEnabled) return local;
    try {
      const snapshot = await this.db.collection("highscores").orderBy("score", "desc").limit(10).get();
      return normalizeScores([...local, ...snapshot.docs.map((doc) => doc.data())]);
    } catch (error) {
      if (["permission-denied", "failed-precondition"].includes(error?.code)) this.cloudEnabled = false;
      return local;
    }
  }

  async saveScore(name, score) {
    const row = { name: String(name || "Anonymous").slice(0, 20), score: Math.floor(score), createdAt: Date.now() };
    const local = normalizeScores([row, ...this.localScores()]);
    try { localStorage.setItem(SCORE_KEY, JSON.stringify(local)); } catch { /* cloud can still succeed */ }
    if (!this.db || !this.cloudEnabled) return "browser";
    try {
      await this.db.collection("highscores").add(row);
      return "cloud";
    } catch (error) {
      if (["permission-denied", "failed-precondition"].includes(error?.code)) this.cloudEnabled = false;
      return "browser";
    }
  }
}

