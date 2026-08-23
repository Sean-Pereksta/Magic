const PLAY_LAUNCHER_ID = "warrealmsPlayLauncher";
const PLAY_LAUNCHER_STYLE_ID = "warrealmsPlayLauncherStyles";

const BOT_STRATEGIES = Object.freeze([
  Object.freeze({
    id: "vanguard",
    name: "Vanguard",
    icon: "⚔",
    color: "#67b64c",
    secondary: "#c2465d",
    factions: "Green + Red",
    deck: "Combat pressure · base breaking · lifelink",
    description: "Pushes combat early and keeps pressure on with units that can crack bases and recover authority."
  }),
  Object.freeze({
    id: "engine",
    name: "Engine",
    icon: "⚙",
    color: "#d6bb3d",
    secondary: "#c2465d",
    factions: "Yellow + Red",
    deck: "Trade · draw · deck thinning",
    description: "Builds a resource engine first, cycles rapidly, and trims weak cards so its stronger turns arrive more often."
  }),
  Object.freeze({
    id: "stronghold",
    name: "Stronghold",
    icon: "⬢",
    color: "#49a7e8",
    secondary: "#67b64c",
    factions: "Blue + Green",
    deck: "Bases · shields · base scaling",
    description: "Constructs a durable board of bases and attachments, then turns that fortified position into long-game value."
  }),
  Object.freeze({
    id: "cycle",
    name: "Cycle",
    icon: "↻",
    color: "#49a7e8",
    secondary: "#d6bb3d",
    factions: "Yellow + Blue",
    deck: "Draw · purge · fast rotation",
    description: "Draws aggressively and purges weak cards to see its best tools repeatedly and assemble cleaner turns."
  }),
  Object.freeze({
    id: "siege",
    name: "Siege",
    icon: "✹",
    color: "#67b64c",
    secondary: "#d6bb3d",
    factions: "Green + Yellow",
    deck: "Combat · stun · base destruction",
    description: "Specializes in breaking defensive boards, stunning key threats, and converting combat into structural damage."
  }),
  Object.freeze({
    id: "attrition",
    name: "Attrition",
    icon: "☠",
    color: "#c2465d",
    secondary: "#49a7e8",
    factions: "Red + Blue",
    deck: "Discard · sustain · deck thinning",
    description: "Wins slowly by shrinking options, improving its own deck, and using sustain effects to survive the grind."
  }),
  Object.freeze({
    id: "marketeer",
    name: "Marketeer",
    icon: "◆",
    color: "#d6bb3d",
    secondary: "#c2465d",
    factions: "Yellow + Red",
    deck: "Trade · market control · card flow",
    description: "Generates buying power, removes useful market cards, and keeps cards flowing to shape the shared market in its favor."
  })
]);

const BOT_DIFFICULTIES = Object.freeze([
  Object.freeze({ id: "easy", name: "Easy", hint: "Loose, intentionally random play." }),
  Object.freeze({ id: "medium", name: "Medium", hint: "Recognizable strategy with some variety." }),
  Object.freeze({ id: "hard", name: "Hard", hint: "Cohesive deck and efficient choices." }),
  Object.freeze({ id: "impossible", name: "Impossible", hint: "Optimized sequencing and adaptation." })
]);

let selectedMode = "single";
let selectedStrategy = "random";
let selectedDifficulty = "medium";
let lastFocusedElement = null;

function clearLegacySingleplayerAutoPrompt() {
  if (typeof window === "undefined" || !window.history?.replaceState) return;
  const url = new URL(window.location.href);
  const legacyModePrompt = url.searchParams.get("mode") === "singleplayer";
  const legacySingleplayerPrompt = url.searchParams.get("singleplayer") === "1";
  if (!legacyModePrompt && !legacySingleplayerPrompt) return;
  if (legacyModePrompt) url.searchParams.delete("mode");
  if (legacySingleplayerPrompt) url.searchParams.delete("singleplayer");
  window.history.replaceState(window.history.state, "", `${url.pathname}${url.search}${url.hash}`);
}

function injectStyles() {
  if (document.getElementById(PLAY_LAUNCHER_STYLE_ID)) return;
  const style = document.createElement("style");
  style.id = PLAY_LAUNCHER_STYLE_ID;
  style.textContent = `
    #${PLAY_LAUNCHER_ID}{position:fixed;inset:0;z-index:140;display:grid;place-items:center;padding:18px;background:rgba(2,5,9,.82);backdrop-filter:blur(10px)}
    #${PLAY_LAUNCHER_ID}[hidden]{display:none!important}
    #${PLAY_LAUNCHER_ID} *{box-sizing:border-box}
    .wrPlayPanel{width:min(1040px,100%);max-height:min(850px,calc(100dvh - 36px));overflow:auto;border:1px solid #405064;border-radius:20px;background:linear-gradient(155deg,#151e29,#090e14 72%);box-shadow:0 28px 90px rgba(0,0,0,.72);color:#f4f7fb}
    .wrPlayHead{position:sticky;top:0;z-index:3;display:flex;align-items:flex-start;gap:14px;padding:18px 20px 14px;background:linear-gradient(180deg,rgba(18,27,38,.99),rgba(12,18,26,.96));border-bottom:1px solid #324052}
    .wrPlayHeadCopy{min-width:0;flex:1}.wrPlayEyebrow{font-size:8px;font-weight:950;letter-spacing:.18em;text-transform:uppercase;color:#e3c64f}.wrPlayTitle{margin:4px 0 4px;font:800 clamp(20px,4vw,31px)/1.05 var(--displayFont,"Cinzel",serif)}
    .wrPlaySubtitle{margin:0;color:#9eabb9;font-size:10px;line-height:1.5}.wrPlayClose{width:34px;height:34px;flex:0 0 auto;border:1px solid #405064;border-radius:10px;background:#18232f;color:#dce3eb;font-size:18px;font-weight:800;cursor:pointer}.wrPlayClose:hover{background:#253442}
    .wrPlayBody{padding:18px 20px 20px}.wrModeTabs{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px;margin-bottom:18px}.wrModeTab{min-height:58px;border:1px solid #36475a;border-radius:13px;background:#111923;padding:10px 14px;text-align:left;cursor:pointer}.wrModeTab strong{display:block;font-size:12px}.wrModeTab span{display:block;margin-top:3px;color:#8696a7;font-size:8px;line-height:1.35}.wrModeTab.active{border-color:#d6bb3d;background:linear-gradient(145deg,rgba(214,187,61,.18),#131c26);box-shadow:inset 0 0 0 1px rgba(214,187,61,.22)}
    .wrPlaySectionTitle{display:flex;align-items:end;justify-content:space-between;gap:10px;margin:16px 0 8px}.wrPlaySectionTitle h3{margin:0;font-size:11px}.wrPlaySectionTitle span{color:#8493a3;font-size:8px;text-align:right}
    .wrDifficultyGrid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:7px}.wrDifficulty{min-height:66px;border:1px solid #334354;border-radius:10px;background:#101821;padding:9px;text-align:left;cursor:pointer}.wrDifficulty strong{display:block;font-size:9px}.wrDifficulty span{display:block;margin-top:4px;color:#8494a5;font-size:7px;line-height:1.35}.wrDifficulty.active{border-color:#e5cb58;background:#211f14;box-shadow:inset 0 0 0 1px rgba(229,203,88,.2)}.wrDifficulty:disabled{opacity:.35;cursor:not-allowed}
    .wrStrategyGrid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:9px}.wrStrategyCard{--wr-strategy:#8794a4;--wr-strategy2:#c6ced7;position:relative;min-height:166px;border:1px solid #354558;border-radius:13px;background:linear-gradient(155deg,color-mix(in srgb,var(--wr-strategy) 10%,#111923),#0d141c 70%);padding:11px;text-align:left;overflow:hidden;cursor:pointer}.wrStrategyCard::after{content:"";position:absolute;inset:auto -35px -50px auto;width:100px;height:100px;border-radius:50%;background:var(--wr-strategy);opacity:.07;pointer-events:none}.wrStrategyCard.active{border-color:var(--wr-strategy);box-shadow:inset 0 0 0 1px color-mix(in srgb,var(--wr-strategy) 65%,transparent),0 9px 28px rgba(0,0,0,.22)}
    .wrStrategyTop{display:flex;align-items:center;gap:9px}.wrStrategyIcon{width:38px;height:38px;flex:0 0 auto;display:grid;place-items:center;border:1px solid color-mix(in srgb,var(--wr-strategy) 72%,#fff);border-radius:10px;background:linear-gradient(145deg,var(--wr-strategy),var(--wr-strategy2));color:#071018;font-size:20px;font-weight:950;box-shadow:0 7px 20px color-mix(in srgb,var(--wr-strategy) 22%,transparent)}.wrStrategyNames{min-width:0}.wrStrategyNames strong{display:block;font-size:10px}.wrStrategyNames span{display:block;margin-top:2px;color:#a8b4c1;font-size:7px}.wrStrategyBox{margin-top:9px;border:1px solid #2d3a49;border-radius:9px;background:rgba(5,9,13,.5);padding:8px}.wrStrategyBox b{display:block;color:var(--wr-strategy);font-size:7px;letter-spacing:.08em;text-transform:uppercase}.wrStrategyBox span{display:block;margin-top:3px;color:#c3ccd6;font-size:7px;line-height:1.4}.wrStrategyDesc{margin:8px 0 0;color:#8999aa;font-size:7px;line-height:1.42}
    .wrRandomCard{--wr-strategy:#c98ef4;--wr-strategy2:#49a7e8}.wrRandomCard .wrStrategyIcon{background:conic-gradient(from 40deg,#d6bb3d,#67b64c,#49a7e8,#c2465d,#d6bb3d);color:#fff;text-shadow:0 2px 5px #000}
    .wrPlayFoot{display:flex;align-items:center;gap:10px;margin-top:17px;padding-top:14px;border-top:1px solid #2b3948}.wrSelectionSummary{min-width:0;flex:1;color:#9aa8b7;font-size:8px;line-height:1.45}.wrSelectionSummary strong{color:#fff}.wrStartBtn,.wrMultiplayerBtn,.wrResumeBtn{min-height:42px;border:1px solid #c35369;border-radius:10px;background:#983047;padding:9px 15px;color:#fff;font-size:10px;font-weight:950;cursor:pointer}.wrStartBtn:hover,.wrMultiplayerBtn:hover{background:#ad3850}.wrResumeBtn{border-color:#405164;background:#1b2632}.wrResumeBtn:hover{background:#243240}
    .wrMultiplayerPane{display:grid;grid-template-columns:minmax(0,1.15fr) minmax(250px,.85fr);gap:14px}.wrMultiplayerHero,.wrMultiplayerSteps{border:1px solid #354558;border-radius:14px;background:#101821;padding:16px}.wrMultiplayerHero h3,.wrMultiplayerSteps h3{margin:0 0 7px;font-size:14px}.wrMultiplayerHero p,.wrMultiplayerSteps p{margin:0;color:#95a4b3;font-size:9px;line-height:1.55}.wrMultiplayerHero .wrMultiplayerBtn{margin-top:15px}.wrMultiplayerSteps ol{margin:9px 0 0;padding-left:18px;color:#c4ced8;font-size:8px;line-height:1.8}
    .wrStrategyNote{margin:8px 0 0;color:#d7bd54;font-size:7px;line-height:1.4}
    @media(max-width:820px){.wrStrategyGrid{grid-template-columns:repeat(2,minmax(0,1fr))}.wrDifficultyGrid{grid-template-columns:repeat(2,minmax(0,1fr))}.wrMultiplayerPane{grid-template-columns:1fr}}
    @media(max-width:520px){#${PLAY_LAUNCHER_ID}{padding:7px}.wrPlayPanel{max-height:calc(100dvh - 14px);border-radius:14px}.wrPlayHead{padding:14px}.wrPlayBody{padding:13px}.wrModeTabs{grid-template-columns:1fr 1fr}.wrStrategyGrid{grid-template-columns:1fr}.wrPlayFoot{align-items:stretch;flex-direction:column}.wrStartBtn,.wrResumeBtn{width:100%}}
  `;
  document.head.appendChild(style);
}

function strategyById(id) {
  return BOT_STRATEGIES.find(strategy => strategy.id === id) || null;
}

function launcher() {
  let root = document.getElementById(PLAY_LAUNCHER_ID);
  if (root) return root;
  injectStyles();
  root = document.createElement("div");
  root.id = PLAY_LAUNCHER_ID;
  root.hidden = true;
  root.setAttribute("role", "dialog");
  root.setAttribute("aria-modal", "true");
  root.setAttribute("aria-labelledby", "warrealmsPlayLauncherTitle");
  root.addEventListener("click", event => {
    if (event.target === root || event.target.closest("[data-wr-play-close]")) {
      closePlayLauncher();
      return;
    }
    const modeButton = event.target.closest("[data-wr-play-mode]");
    if (modeButton) {
      selectedMode = modeButton.dataset.wrPlayMode;
      renderLauncher();
      return;
    }
    const difficultyButton = event.target.closest("[data-wr-difficulty]");
    if (difficultyButton && !difficultyButton.disabled) {
      selectedDifficulty = difficultyButton.dataset.wrDifficulty;
      renderLauncher();
      return;
    }
    const strategyButton = event.target.closest("[data-wr-strategy]");
    if (strategyButton) {
      selectedStrategy = strategyButton.dataset.wrStrategy;
      if (selectedStrategy !== "random" && selectedDifficulty === "easy") selectedDifficulty = "medium";
      renderLauncher();
      return;
    }
    if (event.target.closest("[data-wr-start-single]")) {
      startSelectedSinglePlayer();
      return;
    }
    if (event.target.closest("[data-wr-open-multiplayer]")) {
      openExistingMultiplayerLobby();
      return;
    }
    if (event.target.closest("[data-wr-resume]")) closePlayLauncher();
  });
  document.body.appendChild(root);
  return root;
}

function strategyCard(strategy, selected) {
  return `
    <button type="button" class="wrStrategyCard${selected ? " active" : ""}" data-wr-strategy="${strategy.id}" style="--wr-strategy:${strategy.color};--wr-strategy2:${strategy.secondary}">
      <span class="wrStrategyTop">
        <span class="wrStrategyIcon" aria-hidden="true">${strategy.icon}</span>
        <span class="wrStrategyNames"><strong>${strategy.name}</strong><span>${strategy.factions}</span></span>
      </span>
      <span class="wrStrategyBox"><b>Deck identity</b><span>${strategy.deck}</span></span>
      <span class="wrStrategyDesc">${strategy.description}</span>
    </button>`;
}

function renderSinglePlayer() {
  const strategy = strategyById(selectedStrategy);
  const strategyLocked = Boolean(strategy);
  return `
    <div class="wrPlaySectionTitle"><h3>Bot difficulty</h3><span>Difficulty changes how efficiently the bot pilots its deck.</span></div>
    <div class="wrDifficultyGrid">
      ${BOT_DIFFICULTIES.map(difficulty => {
        const disabled = strategyLocked && difficulty.id === "easy";
        return `<button type="button" class="wrDifficulty${selectedDifficulty === difficulty.id ? " active" : ""}" data-wr-difficulty="${difficulty.id}" ${disabled ? "disabled" : ""}><strong>${difficulty.name}</strong><span>${difficulty.hint}</span></button>`;
      }).join("")}
    </div>
    ${strategyLocked ? `<p class="wrStrategyNote">Easy intentionally ignores archetypes and makes random purchases, so a chosen strategy starts at Medium.</p>` : ""}

    <div class="wrPlaySectionTitle"><h3>Choose the bot's deck & strategy</h3><span>Or leave it completely unpredictable.</span></div>
    <div class="wrStrategyGrid">
      <button type="button" class="wrStrategyCard wrRandomCard${selectedStrategy === "random" ? " active" : ""}" data-wr-strategy="random">
        <span class="wrStrategyTop"><span class="wrStrategyIcon" aria-hidden="true">?</span><span class="wrStrategyNames"><strong>Random Bot</strong><span>Any faction pair + archetype</span></span></span>
        <span class="wrStrategyBox"><b>Deck identity</b><span>One of WarRealms' strategy decks is chosen when the battle begins.</span></span>
        <span class="wrStrategyDesc">Best when you want to scout against an unknown matchup and adapt after the first few turns.</span>
      </button>
      ${BOT_STRATEGIES.map(strategyOption => strategyCard(strategyOption, selectedStrategy === strategyOption.id)).join("")}
    </div>

    <div class="wrPlayFoot">
      <div class="wrSelectionSummary"><strong>${strategy ? strategy.name : "Random Bot"} · ${BOT_DIFFICULTIES.find(item => item.id === selectedDifficulty)?.name || "Medium"}</strong><br>${strategy ? `${strategy.factions} — ${strategy.deck}` : "The bot will roll a strategy and faction pairing when the match starts."}</div>
      ${document.body.classList.contains("battleMode") ? `<button type="button" class="wrResumeBtn" data-wr-resume>Resume Battle</button>` : ""}
      <button type="button" class="wrStartBtn" data-wr-start-single>Start Single Player</button>
    </div>`;
}

function renderMultiplayer() {
  return `
    <div class="wrMultiplayerPane">
      <section class="wrMultiplayerHero">
        <div class="wrPlayEyebrow">Player vs Player</div>
        <h3>Multiplayer Realm Warfare</h3>
        <p>Open the existing WarRealms multiplayer lobby to create a room, join a room, and bring your active command deck against another player.</p>
        <button type="button" class="wrMultiplayerBtn" data-wr-open-multiplayer>Open Multiplayer Lobby</button>
      </section>
      <section class="wrMultiplayerSteps">
        <h3>How it flows</h3>
        <p>Play now acts as the shared front door for both battle types.</p>
        <ol><li>Select Multiplayer here.</li><li>Create or join a room.</li><li>Use the existing synced PvP battle flow.</li></ol>
        ${document.body.classList.contains("battleMode") ? `<button type="button" class="wrResumeBtn" data-wr-resume>Resume Current Battle</button>` : ""}
      </section>
    </div>`;
}

function renderLauncher() {
  const root = launcher();
  root.innerHTML = `
    <section class="wrPlayPanel">
      <header class="wrPlayHead">
        <div class="wrPlayHeadCopy"><div class="wrPlayEyebrow">War Realms · Play</div><h2 class="wrPlayTitle" id="warrealmsPlayLauncherTitle">Choose your battle</h2><p class="wrPlaySubtitle">Start against a bot, control the matchup you want to practice, or move into multiplayer from the same Play menu.</p></div>
        <button type="button" class="wrPlayClose" data-wr-play-close aria-label="Close Play menu">×</button>
      </header>
      <div class="wrPlayBody">
        <div class="wrModeTabs" role="tablist" aria-label="Play mode">
          <button type="button" class="wrModeTab${selectedMode === "single" ? " active" : ""}" data-wr-play-mode="single"><strong>⚔ Single Player</strong><span>Random bot or choose its deck strategy.</span></button>
          <button type="button" class="wrModeTab${selectedMode === "multiplayer" ? " active" : ""}" data-wr-play-mode="multiplayer"><strong>◎ Multiplayer</strong><span>Create or join an online battle room.</span></button>
        </div>
        ${selectedMode === "single" ? renderSinglePlayer() : renderMultiplayer()}
      </div>
    </section>`;
}

function showPlayLauncher(mode = "single") {
  selectedMode = mode === "multiplayer" ? "multiplayer" : "single";
  lastFocusedElement = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  const root = launcher();
  root.hidden = false;
  renderLauncher();
  requestAnimationFrame(() => root.querySelector(".wrModeTab.active")?.focus({ preventScroll: true }));
}

function closePlayLauncher() {
  const root = document.getElementById(PLAY_LAUNCHER_ID);
  if (!root || root.hidden) return;
  root.hidden = true;
  lastFocusedElement?.focus?.({ preventScroll: true });
}

function forceNextStrategy(strategyId, run) {
  const index = BOT_STRATEGIES.findIndex(strategy => strategy.id === strategyId);
  if (index < 0) return run();
  const originalRandom = Math.random;
  const forcedValue = (index + 0.5) / BOT_STRATEGIES.length;
  let restored = false;
  const restore = () => {
    if (restored) return;
    restored = true;
    if (Math.random === forcedRandom) Math.random = originalRandom;
  };
  function forcedRandom() {
    restore();
    return forcedValue;
  }
  Math.random = forcedRandom;
  try {
    return run();
  } finally {
    queueMicrotask(restore);
  }
}

function startSelectedSinglePlayer() {
  if (selectedStrategy !== "random" && selectedDifficulty === "easy") selectedDifficulty = "medium";
  const nativeButton = document.getElementById("botChallengeBtn");
  if (!nativeButton) return;
  nativeButton.click();
  const nativeDifficulty = document.querySelector(`[data-act="start-bot"][data-difficulty="${selectedDifficulty}"]`);
  if (!(nativeDifficulty instanceof HTMLElement)) return;
  closePlayLauncher();
  if (selectedStrategy === "random") {
    nativeDifficulty.click();
    return;
  }
  forceNextStrategy(selectedStrategy, () => nativeDifficulty.click());
}

function openExistingMultiplayerLobby() {
  const multiplayerButton = document.querySelector('#moreMenu [data-primary-destination="multiplayer"]');
  if (!(multiplayerButton instanceof HTMLElement)) return;
  closePlayLauncher();
  multiplayerButton.click();
}

function relabelBattleAsPlay() {
  const playButton = document.querySelector('.primaryNavBtn[data-primary-destination="battle"]');
  if (!playButton) return;
  const label = playButton.querySelector("b");
  if (label) label.textContent = "Play";
  playButton.setAttribute("aria-label", "Play — choose single player or multiplayer");
  playButton.title = "Play — single player or multiplayer";
}

function installPlayLauncher() {
  relabelBattleAsPlay();
  injectStyles();
  document.addEventListener("click", event => {
    const playButton = event.target instanceof Element ? event.target.closest('.primaryNavBtn[data-primary-destination="battle"]') : null;
    if (!playButton) return;
    event.preventDefault();
    event.stopPropagation();
    event.stopImmediatePropagation();
    showPlayLauncher("single");
  }, true);
  document.addEventListener("keydown", event => {
    if (event.key === "Escape" && !document.getElementById(PLAY_LAUNCHER_ID)?.hidden) closePlayLauncher();
  });
}

if (typeof window !== "undefined") clearLegacySingleplayerAutoPrompt();

if (typeof document !== "undefined") {
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", installPlayLauncher, { once: true });
  else installPlayLauncher();
}

export { showPlayLauncher };