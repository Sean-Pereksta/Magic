import "./presentation-polish.js";
import { loadCampaignProfile } from "../campaign/campaign-state.js?v=5";
import { campaignNodeAt, campaignRegionScaling } from "../campaign/campaign-map.js?v=5";
import { campaignProgressSummary } from "../campaign/campaign.js?v=4";
import {
  commanderFactionLabel,
  commanderImagePath,
  commanderInitials,
  getCampaignCommander
} from "../campaign/campaign-commanders.js?v=1";

const PLAY_LAUNCHER_ID = "warrealmsPlayLauncher";
const PLAY_LAUNCHER_STYLE_ID = "warrealmsPlayLauncherStyles";

const BOT_STRATEGIES = Object.freeze([
  Object.freeze({ id: "vanguard", name: "Vanguard", icon: "⚔", color: "#67b64c", secondary: "#c2465d", factions: "Green + Red", deck: "Combat · base breaking · lifelink", description: "Fast pressure that cracks bases and recovers Authority." }),
  Object.freeze({ id: "engine", name: "Engine", icon: "⚙", color: "#d6bb3d", secondary: "#c2465d", factions: "Yellow + Red", deck: "Trade · draw · deck thinning", description: "Builds buying power, cycles quickly, and trims weak cards." }),
  Object.freeze({ id: "stronghold", name: "Stronghold", icon: "⬢", color: "#49a7e8", secondary: "#67b64c", factions: "Blue + Green", deck: "Bases · shields · scaling", description: "Fortifies the board and converts durable bases into value." }),
  Object.freeze({ id: "cycle", name: "Cycle", icon: "↻", color: "#49a7e8", secondary: "#d6bb3d", factions: "Yellow + Blue", deck: "Draw · purge · rotation", description: "Draws aggressively and purges weak cards to repeat its best turns." }),
  Object.freeze({ id: "siege", name: "Siege", icon: "✹", color: "#67b64c", secondary: "#d6bb3d", factions: "Green + Yellow", deck: "Combat · stun · raze", description: "Breaks defensive boards and converts combat into structural damage." }),
  Object.freeze({ id: "attrition", name: "Attrition", icon: "☠", color: "#c2465d", secondary: "#49a7e8", factions: "Red + Blue", deck: "Discard · sustain · thinning", description: "Shrinks opposing options while surviving long enough to take over." }),
  Object.freeze({ id: "marketeer", name: "Marketeer", icon: "◆", color: "#d6bb3d", secondary: "#c2465d", factions: "Yellow + Red", deck: "Trade · market control · flow", description: "Shapes the shared market while keeping cards and Trade flowing." }),
  Object.freeze({ id: "summoning", name: "Summoning", icon: "✦", color: "#67b64c", secondary: "#d6bb3d", factions: "Green + Yellow", deck: "Tokens · deck pulls · swarm", description: "Builds around token makers and cards that pull extra bodies from the draw pile." }),
  Object.freeze({ id: "ascendents", name: "Ascendents", icon: "◇", color: "#c2465d", secondary: "#67b64c", factions: "Red + Green", deck: "Evolution · transformation · scaling", description: "Prioritizes cards that evolve, ascend, hatch, or transform into stronger forms." }),
  Object.freeze({ id: "bastion", name: "Bastion", icon: "▣", color: "#49a7e8", secondary: "#67b64c", factions: "Blue + Green", deck: "Structures · healing · shields", description: "Commits heavily to Bases, repair, healing, Shield, and durable defensive engines." }),
  Object.freeze({ id: "fleet", name: "Fleet", icon: "✧", color: "#49a7e8", secondary: "#d6bb3d", factions: "Blue + Yellow", deck: "Drones · Interceptors · token synergy", description: "A focused summoning plan that hunts Drone and Interceptor producers and their payoffs." })
]);

const BOT_DIFFICULTIES = Object.freeze([
  Object.freeze({ id: "easy", name: "Easy", hint: "Loose, random play" }),
  Object.freeze({ id: "medium", name: "Medium", hint: "Clear strategy" }),
  Object.freeze({ id: "hard", name: "Hard", hint: "Efficient choices" }),
  Object.freeze({ id: "impossible", name: "Impossible", hint: "Optimized play" })
]);

let selectedMode = "single";
let selectedView = "main";
let selectedStrategy = "random";
let selectedDifficulty = "medium";
let showAllStrategies = false;
let lastFocusedElement = null;
let autoOpened = false;

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, character => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[character]);
}

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
    #${PLAY_LAUNCHER_ID}{position:fixed;inset:0;z-index:140;display:grid;place-items:center;padding:16px;background:rgba(2,5,9,.84);backdrop-filter:blur(10px)}
    #${PLAY_LAUNCHER_ID}[hidden]{display:none!important}#${PLAY_LAUNCHER_ID} *{box-sizing:border-box}
    .wrPlayPanel{width:min(760px,100%);max-height:min(760px,calc(100dvh - 32px));overflow:auto;border:1px solid #405064;border-radius:18px;background:linear-gradient(155deg,#151e29,#090e14 72%);box-shadow:0 28px 90px rgba(0,0,0,.72);color:#f4f7fb}
    .wrPlayHead{position:sticky;top:0;z-index:3;display:flex;align-items:flex-start;gap:12px;padding:16px 18px 13px;background:linear-gradient(180deg,rgba(18,27,38,.99),rgba(12,18,26,.96));border-bottom:1px solid #324052}
    .wrPlayHeadCopy{min-width:0;flex:1}.wrPlayEyebrow{font-size:8px;font-weight:950;letter-spacing:.18em;text-transform:uppercase;color:#e3c64f}.wrPlayTitle{margin:4px 0 3px;font:800 clamp(20px,4vw,29px)/1.05 var(--displayFont,"Cinzel",serif)}.wrPlaySubtitle{margin:0;color:#9eabb9;font-size:9px;line-height:1.45}
    .wrPlayClose{width:34px;height:34px;flex:0 0 auto;border:1px solid #405064;border-radius:10px;background:#18232f;color:#dce3eb;font-size:18px;font-weight:800;cursor:pointer}.wrPlayClose:hover{background:#253442}
    .wrPlayBody{padding:15px 18px 18px}.wrModeTabs{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin-bottom:14px}.wrModeTab{min-height:48px;border:1px solid #36475a;border-radius:11px;background:#111923;padding:9px 12px;text-align:left;cursor:pointer}.wrModeTab strong{display:block;font-size:11px}.wrModeTab span{display:block;margin-top:2px;color:#8696a7;font-size:7px}.wrModeTab.active{border-color:#d6bb3d;background:linear-gradient(145deg,rgba(214,187,61,.17),#131c26);box-shadow:inset 0 0 0 1px rgba(214,187,61,.2)}
    .wrPlaySectionTitle{display:flex;align-items:center;justify-content:space-between;gap:8px;margin:13px 0 7px}.wrPlaySectionTitle h3{margin:0;font-size:10px}.wrPlaySectionTitle span{color:#8493a3;font-size:7px;text-align:right}
    .wrDifficultyGrid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:6px}.wrDifficulty{min-height:48px;border:1px solid #334354;border-radius:9px;background:#101821;padding:8px;text-align:left;cursor:pointer}.wrDifficulty strong{display:block;font-size:9px}.wrDifficulty span{display:block;margin-top:2px;color:#8494a5;font-size:6px}.wrDifficulty.active{border-color:#e5cb58;background:#211f14;box-shadow:inset 0 0 0 1px rgba(229,203,88,.2)}.wrDifficulty:disabled{opacity:.35;cursor:not-allowed}
    .wrStrategyGrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:7px}.wrStrategyCard{--wr-strategy:#8794a4;--wr-strategy2:#c6ced7;position:relative;min-height:88px;border:1px solid #354558;border-radius:11px;background:linear-gradient(155deg,color-mix(in srgb,var(--wr-strategy) 9%,#111923),#0d141c 70%);padding:9px;text-align:left;overflow:hidden;cursor:pointer}.wrStrategyCard:hover{transform:translateY(-1px);border-color:color-mix(in srgb,var(--wr-strategy) 62%,#fff)}.wrStrategyCard.active{border-color:var(--wr-strategy);box-shadow:inset 0 0 0 1px color-mix(in srgb,var(--wr-strategy) 60%,transparent),0 8px 24px rgba(0,0,0,.2)}
    .wrStrategyTop{display:flex;align-items:center;gap:8px}.wrStrategyIcon{width:34px;height:34px;flex:0 0 auto;display:grid;place-items:center;border:1px solid color-mix(in srgb,var(--wr-strategy) 72%,#fff);border-radius:9px;background:linear-gradient(145deg,var(--wr-strategy),var(--wr-strategy2));color:#071018;font-size:18px;font-weight:950}.wrStrategyNames{min-width:0}.wrStrategyNames strong{display:block;font-size:10px}.wrStrategyNames span{display:block;margin-top:1px;color:#a8b4c1;font-size:7px}.wrStrategyDesc{display:block;margin:6px 0 0;color:#8999aa;font-size:7px;line-height:1.35}.wrStrategyDeck{display:block;margin-top:4px;color:var(--wr-strategy);font-size:6px;font-weight:900;letter-spacing:.04em;text-transform:uppercase}
    .wrRandomCard{--wr-strategy:#c98ef4;--wr-strategy2:#49a7e8}.wrRandomCard .wrStrategyIcon{background:conic-gradient(from 40deg,#d6bb3d,#67b64c,#49a7e8,#c2465d,#d6bb3d);color:#fff;text-shadow:0 2px 5px #000}.wrMoreStrategies{min-height:32px;margin-top:7px;border:1px solid #334354;border-radius:8px;background:#111923;color:#b9c4d0;padding:6px 10px;font-size:8px;font-weight:900;cursor:pointer}.wrMoreStrategies:hover{background:#18232e;color:#fff}
    .wrStrategyNote{margin:7px 0 0;color:#d7bd54;font-size:7px;line-height:1.35}
    .wrPlayFoot{display:flex;align-items:center;gap:9px;margin-top:14px;padding-top:12px;border-top:1px solid #2b3948}.wrSelectionSummary{min-width:0;flex:1;color:#9aa8b7;font-size:8px;line-height:1.4}.wrSelectionSummary strong{color:#fff}.wrStartBtn,.wrMultiplayerBtn,.wrResumeBtn{min-height:40px;border:1px solid #c35369;border-radius:9px;background:#983047;padding:8px 14px;color:#fff;font-size:9px;font-weight:950;cursor:pointer}.wrStartBtn:hover,.wrMultiplayerBtn:hover{background:#ad3850}.wrResumeBtn{border-color:#405164;background:#1b2632}.wrResumeBtn:hover{background:#243240}
    .wrMultiplayerPane{display:grid;grid-template-columns:minmax(0,1.1fr) minmax(220px,.9fr);gap:10px}.wrMultiplayerHero,.wrMultiplayerSteps{border:1px solid #354558;border-radius:12px;background:#101821;padding:14px}.wrMultiplayerHero h3,.wrMultiplayerSteps h3{margin:0 0 6px;font-size:13px}.wrMultiplayerHero p,.wrMultiplayerSteps p{margin:0;color:#95a4b3;font-size:8px;line-height:1.5}.wrMultiplayerHero .wrMultiplayerBtn{margin-top:12px}.wrMultiplayerSteps ol{margin:8px 0 0;padding-left:17px;color:#c4ced8;font-size:8px;line-height:1.7}
    .wrWarTablePanel{width:min(980px,100%);max-height:calc(100dvh - 32px);overflow:auto;border:1px solid #5e5131;border-radius:20px;background:radial-gradient(circle at 50% 32%,rgba(144,101,43,.18),transparent 34%),linear-gradient(145deg,#171c21,#080b0f 72%);box-shadow:0 32px 110px rgba(0,0,0,.82),inset 0 0 90px rgba(188,133,49,.06);color:#f4f7fb}
    .wrWarTableHead{position:sticky;top:0;z-index:4;display:flex;align-items:center;gap:14px;padding:18px 21px 15px;border-bottom:1px solid #4b422d;background:linear-gradient(180deg,rgba(18,22,26,.99),rgba(12,15,19,.96));backdrop-filter:blur(12px)}
    .wrWarTableCrest{width:54px;height:54px;display:grid;place-items:center;flex:0 0 auto;border:1px solid #8b7336;border-radius:50%;background:radial-gradient(circle,#dfc66d 0 6%,#55451f 7% 25%,#171c22 26% 100%);box-shadow:0 0 28px rgba(220,180,67,.22);color:#f2d675;font-size:24px}.wrWarTableHeadCopy{min-width:0;flex:1}.wrWarTableTitle{margin:4px 0 2px;font:850 clamp(23px,4.6vw,38px)/1 var(--displayFont,"Cinzel",serif);letter-spacing:.03em}.wrWarTableSubtitle{margin:0;color:#9da8b5;font-size:9px;line-height:1.5}
    .wrWarTableBody{padding:17px 21px 21px}.wrWarTableGrid{display:grid;grid-template-columns:minmax(0,1.35fr) minmax(290px,.65fr);gap:11px}.wrWarTablePrimary,.wrWarTableStack{display:grid;gap:10px}.wrWarTableStack{grid-template-columns:1fr 1fr;align-content:start}.wrWarAction{position:relative;min-height:110px;overflow:hidden;border:1px solid #394858;border-radius:14px;background:linear-gradient(145deg,rgba(27,37,47,.96),rgba(10,15,21,.98));padding:14px;text-align:left;color:#edf2f7;cursor:pointer;transition:transform .16s ease,border-color .16s ease,box-shadow .16s ease}.wrWarAction:hover{transform:translateY(-2px);border-color:#a68b45;box-shadow:0 12px 32px rgba(0,0,0,.32)}.wrWarAction strong{display:block;font:850 14px/1.1 var(--displayFont,"Cinzel",serif);letter-spacing:.04em}.wrWarAction>span{display:block;margin-top:5px;color:#92a0ae;font-size:8px;line-height:1.45}.wrWarAction em{display:block;margin-top:8px;color:#ddc45e;font-size:7px;font-style:normal;font-weight:950;letter-spacing:.1em;text-transform:uppercase}.wrWarActionIcon{position:absolute;right:12px;bottom:4px;color:rgba(229,202,85,.13);font-size:58px;font-weight:950;pointer-events:none}
    .wrContinueCampaign{min-height:244px;border-color:#8c7336;background:radial-gradient(circle at 78% 32%,color-mix(in srgb,var(--commanderPrimary,#4d8ec2) 26%,transparent),transparent 34%),linear-gradient(145deg,#282618,#11171e 68%);box-shadow:inset 0 0 0 1px rgba(225,198,82,.12),0 18px 45px rgba(0,0,0,.28)}.wrContinueCampaign:hover{border-color:#e4c85a;box-shadow:inset 0 0 0 1px rgba(240,211,97,.23),0 18px 50px rgba(0,0,0,.4)}.wrContinueLayout{display:grid;grid-template-columns:132px minmax(0,1fr);gap:16px;align-items:center;height:100%}.wrContinuePortrait{position:relative;width:132px;aspect-ratio:3/4;display:grid;place-items:center;overflow:hidden;border:1px solid color-mix(in srgb,var(--commanderPrimary,#558fc0) 72%,#fff);border-radius:14px;background:linear-gradient(150deg,var(--commanderPrimary,#315c7b),var(--commanderSecondary,#2d3a44));box-shadow:0 15px 34px rgba(0,0,0,.42)}.wrContinuePortrait img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover}.wrContinuePortrait b{font:900 34px/1 var(--displayFont,"Cinzel",serif);color:#fff;text-shadow:0 3px 12px #000}.wrContinueCopy{min-width:0}.wrContinueEyebrow{color:#e0c55a;font-size:8px;font-weight:950;letter-spacing:.16em;text-transform:uppercase}.wrContinueCopy h3{margin:7px 0 2px;font:900 clamp(19px,3vw,29px)/1.05 var(--displayFont,"Cinzel",serif)}.wrContinueCopy p{margin:0;color:#b0bcc8;font-size:9px}.wrCampaignFacts{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-top:13px}.wrCampaignFact{border:1px solid rgba(116,132,148,.28);border-radius:8px;background:rgba(7,11,15,.52);padding:7px}.wrCampaignFact span{display:block;color:#7f8e9d;font-size:6px;text-transform:uppercase;letter-spacing:.08em}.wrCampaignFact b{display:block;margin-top:2px;color:#eef3f8;font-size:9px}.wrContinueCta{display:inline-flex;margin-top:12px;border:1px solid #c7a942;border-radius:8px;background:#9e7b23;padding:8px 11px;color:#fff;font-size:8px;font-weight:950;letter-spacing:.08em}
    .wrNewCampaign{border-color:#6e5c31;background:linear-gradient(145deg,#242012,#10171e)}.wrQuickBattle{--actionAccent:#c45366}.wrMultiplayerAction{--actionAccent:#6d7ed7}.wrArmoryAction{--actionAccent:#d0b447}.wrWarAction:not(.wrContinueCampaign)::before{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--actionAccent,#9b8546)}
    .wrWarSecondary{display:flex;flex-wrap:wrap;gap:7px;margin-top:12px;padding-top:12px;border-top:1px solid #2e3944}.wrWarSecondary button,.wrWarBack{min-height:34px;border:1px solid #354555;border-radius:8px;background:#111a23;padding:7px 11px;color:#aebbc8;font-size:8px;font-weight:900;cursor:pointer}.wrWarSecondary button:hover,.wrWarBack:hover{background:#1a2733;color:#fff}.wrWarBack{margin-right:8px}
    .wrInfoPanel{display:grid;gap:10px}.wrInfoCard{border:1px solid #354555;border-radius:12px;background:#101821;padding:13px}.wrInfoCard h3{margin:0 0 7px;font-size:12px}.wrInfoCard p{margin:0;color:#91a0af;font-size:8px;line-height:1.55}.wrStatsGrid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}.wrStatTile{border:1px solid #354555;border-radius:10px;background:#0c131b;padding:11px;color:#8d9baa;font-size:7px;text-transform:uppercase;letter-spacing:.07em}.wrStatTile b{display:block;margin-top:4px;color:#fff;font-size:18px;letter-spacing:0}.wrSettingRow{display:flex;align-items:center;justify-content:space-between;gap:12px;border-top:1px solid #2e3b49;padding:11px 0}.wrSettingRow:first-child{border-top:0}.wrSettingRow span{color:#d5dde5;font-size:9px}.wrSettingRow small{display:block;margin-top:2px;color:#8291a0;font-size:7px}.wrSettingToggle{min-width:62px;border:1px solid #495b6d;border-radius:999px;background:#17222d;padding:6px 9px;color:#dfe7ee;font-size:7px;font-weight:950;cursor:pointer}.wrSettingToggle.active{border-color:#b79d48;background:#5b4a1b;color:#fff}
    html.wrReduceMotion *,html.wrReduceMotion *::before,html.wrReduceMotion *::after{animation-duration:.001ms!important;animation-iteration-count:1!important;transition-duration:.001ms!important;scroll-behavior:auto!important}
    @media(max-width:650px){.wrDifficultyGrid{grid-template-columns:1fr 1fr}.wrMultiplayerPane{grid-template-columns:1fr}.wrPlayFoot{align-items:stretch;flex-direction:column}.wrStartBtn,.wrResumeBtn{width:100%}}
    @media(max-width:760px){.wrWarTableGrid{grid-template-columns:1fr}.wrContinueCampaign{min-height:220px}.wrWarTableStack{grid-template-columns:1fr 1fr}.wrWarAction{min-height:96px}}
    @media(max-width:480px){#${PLAY_LAUNCHER_ID}{padding:7px}.wrPlayPanel,.wrWarTablePanel{max-height:calc(100dvh - 14px);border-radius:13px}.wrPlayHead,.wrWarTableHead{padding:13px}.wrPlayBody,.wrWarTableBody{padding:12px}.wrStrategyGrid{grid-template-columns:1fr}.wrModeTab span{display:none}.wrContinueLayout{grid-template-columns:88px minmax(0,1fr);gap:10px}.wrContinuePortrait{width:88px}.wrContinueCampaign{min-height:210px}.wrCampaignFacts{grid-template-columns:1fr}.wrCampaignFact:nth-child(n+3){display:none}.wrWarTableStack{grid-template-columns:1fr}.wrWarAction{min-height:82px}.wrStatsGrid{grid-template-columns:1fr 1fr}.wrWarTableCrest{width:42px;height:42px}}
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
    if (event.target === root || event.target.closest("[data-wr-play-close]")) return closePlayLauncher();
    const mainAction = event.target.closest("[data-wr-main-action]");
    if (mainAction) {
      handleWarTableAction(mainAction.dataset.wrMainAction);
      return;
    }
    if (event.target.closest("[data-wr-back-main]")) {
      selectedView = "main";
      renderLauncher();
      return;
    }
    const setting = event.target.closest("[data-wr-setting]");
    if (setting) {
      toggleWarTableSetting(setting.dataset.wrSetting);
      renderLauncher();
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
    if (event.target.closest("[data-wr-more-strategies]")) {
      showAllStrategies = !showAllStrategies;
      renderLauncher();
      return;
    }
    if (event.target.closest("[data-wr-start-single]")) return startSelectedSinglePlayer();
    if (event.target.closest("[data-wr-open-multiplayer]")) return openExistingMultiplayerLobby();
    if (event.target.closest("[data-wr-resume]")) closePlayLauncher();
  });
  document.body.appendChild(root);
  return root;
}

function strategyCard(strategy, selected) {
  return `
    <button type="button" class="wrStrategyCard${selected ? " active" : ""}" data-wr-strategy="${strategy.id}" style="--wr-strategy:${strategy.color};--wr-strategy2:${strategy.secondary}">
      <span class="wrStrategyTop"><span class="wrStrategyIcon" aria-hidden="true">${strategy.icon}</span><span class="wrStrategyNames"><strong>${strategy.name}</strong><span>${strategy.factions}</span></span></span>
      <span class="wrStrategyDeck">${strategy.deck}</span><span class="wrStrategyDesc">${strategy.description}</span>
    </button>`;
}

function visibleStrategies() {
  if (showAllStrategies) return BOT_STRATEGIES;
  const selected = strategyById(selectedStrategy);
  const first = BOT_STRATEGIES.slice(0, 4);
  if (!selected || first.some(item => item.id === selected.id)) return first;
  return [selected, ...first.slice(0, 3)];
}

function renderSinglePlayer() {
  const strategy = strategyById(selectedStrategy);
  const strategyLocked = Boolean(strategy);
  const difficultyName = BOT_DIFFICULTIES.find(item => item.id === selectedDifficulty)?.name || "Medium";
  return `
    <div class="wrPlaySectionTitle"><h3>Difficulty</h3><span>Choose how efficiently the bot pilots its deck.</span></div>
    <div class="wrDifficultyGrid">
      ${BOT_DIFFICULTIES.map(difficulty => {
        const disabled = strategyLocked && difficulty.id === "easy";
        return `<button type="button" class="wrDifficulty${selectedDifficulty === difficulty.id ? " active" : ""}" data-wr-difficulty="${difficulty.id}" ${disabled ? "disabled" : ""}><strong>${difficulty.name}</strong><span>${difficulty.hint}</span></button>`;
      }).join("")}
    </div>
    ${strategyLocked ? `<p class="wrStrategyNote">Easy intentionally uses random purchases, so named strategies begin at Medium.</p>` : ""}

    <div class="wrPlaySectionTitle"><h3>Opponent</h3><span>Pick a matchup or leave it unknown.</span></div>
    <div class="wrStrategyGrid">
      <button type="button" class="wrStrategyCard wrRandomCard${selectedStrategy === "random" ? " active" : ""}" data-wr-strategy="random">
        <span class="wrStrategyTop"><span class="wrStrategyIcon" aria-hidden="true">?</span><span class="wrStrategyNames"><strong>Random Bot</strong><span>Any faction pair</span></span></span>
        <span class="wrStrategyDeck">Random archetype</span><span class="wrStrategyDesc">Best for an unknown matchup and quick practice.</span>
      </button>
      ${visibleStrategies().map(option => strategyCard(option, selectedStrategy === option.id)).join("")}
    </div>
    <button type="button" class="wrMoreStrategies" data-wr-more-strategies>${showAllStrategies ? "Show fewer strategies" : `More strategies · ${BOT_STRATEGIES.length}`}</button>

    <div class="wrPlayFoot">
      <div class="wrSelectionSummary"><strong>${strategy ? strategy.name : "Random Bot"} · ${difficultyName}</strong><br>${strategy ? `${strategy.factions} — ${strategy.deck}` : "A strategy and faction pairing will be chosen when the match begins."}</div>
      ${document.body.classList.contains("battleMode") ? `<button type="button" class="wrResumeBtn" data-wr-resume>Resume Battle</button>` : ""}
      <button type="button" class="wrStartBtn" data-wr-start-single>Start Battle</button>
    </div>`;
}

function renderMultiplayer() {
  return `
    <div class="wrMultiplayerPane">
      <section class="wrMultiplayerHero"><div class="wrPlayEyebrow">Player vs Player</div><h3>Online Battle</h3><p>Create or join a room and bring your active command deck against another player.</p><button type="button" class="wrMultiplayerBtn" data-wr-open-multiplayer>Open Multiplayer Lobby</button></section>
      <section class="wrMultiplayerSteps"><h3>Quick flow</h3><p>Play is now the shared front door for both battle types.</p><ol><li>Create or join a room.</li><li>Confirm your active deck.</li><li>Start the synced PvP battle.</li></ol>${document.body.classList.contains("battleMode") ? `<button type="button" class="wrResumeBtn" data-wr-resume>Resume Current Battle</button>` : ""}</section>
    </div>`;
}

function commanderPortrait(commander) {
  const path = commanderImagePath(commander);
  return `<span class="wrContinuePortrait" aria-label="${escapeHtml(commander?.name || "Commander portrait")}"><b>${escapeHtml(commanderInitials(commander))}</b>${path ? `<img src="${escapeHtml(path)}" alt="${escapeHtml(commander?.name || "Commander")}" onerror="this.remove()">` : ""}</span>`;
}

function activeCampaignDetails() {
  const profile = loadCampaignProfile();
  if (!profile) return { profile: null, commander: null, node: null, summary: null, scaling: null };
  return {
    profile,
    commander: getCampaignCommander(profile.commanderId),
    node: campaignNodeAt(profile),
    summary: campaignProgressSummary(profile),
    scaling: campaignRegionScaling(profile.region)
  };
}

function warTableActionCard(id, title, description, meta, icon, className = "") {
  return `<button type="button" class="wrWarAction ${className}" data-wr-main-action="${escapeHtml(id)}"><strong>${escapeHtml(title)}</strong><span>${escapeHtml(description)}</span>${meta ? `<em>${escapeHtml(meta)}</em>` : ""}<i class="wrWarActionIcon" aria-hidden="true">${escapeHtml(icon)}</i></button>`;
}

function renderWarTableMain() {
  const { profile, commander, node, summary, scaling } = activeCampaignDetails();
  const active = profile?.status === "active";
  const primaryFaction = commander?.factions?.[0] || "blue";
  const secondaryFaction = commander?.factions?.[1] || commander?.factions?.[0] || "green";
  const continueCard = active ? `
    <button type="button" class="wrWarAction wrContinueCampaign" data-wr-main-action="continue" style="--commanderPrimary:var(--${escapeHtml(primaryFaction)});--commanderSecondary:var(--${escapeHtml(secondaryFaction)})">
      <span class="wrContinueLayout">${commanderPortrait(commander)}<span class="wrContinueCopy"><span class="wrContinueEyebrow">Continue Campaign · ${escapeHtml(scaling?.progressBand || "Realm Campaign")}</span><h3>${escapeHtml(commander?.name || "Commander")} · ${escapeHtml(commander?.title || "Field Commander")}</h3><p>${escapeHtml(commanderFactionLabel(commander))}</p><span class="wrCampaignFacts"><span class="wrCampaignFact"><span>Region</span><b>${summary?.region || 1} · ${escapeHtml(node?.label || "War Table")}</b></span><span class="wrCampaignFact"><span>Authority</span><b>${summary?.authority || 0} / ${summary?.maxAuthority || 60}</b></span><span class="wrCampaignFact"><span>Commander Level</span><b>${profile.commanderLevel || profile.level || 1}</b></span><span class="wrCampaignFact"><span>Battles Won</span><b>${summary?.battlesWon || 0}</b></span></span><span class="wrContinueCta">RETURN TO THE WAR TABLE →</span></span></span>
    </button>` : warTableActionCard("new", profile?.status === "defeated" ? "Begin a New Campaign" : "Choose Your Commander", profile?.status === "defeated" ? `The previous campaign ended after ${profile.battlesWon || 0} victories.` : "Select a multi-faction commander and begin a military journey across the realm.", "Commander selection · persistent deck · branching routes", "♛", "wrContinueCampaign wrNewCampaign");
  return `<div class="wrWarTableGrid"><div class="wrWarTablePrimary">${continueCard}${active ? warTableActionCard("new", "New Campaign", "Choose a different commander and replace the current run after confirmation.", "Six multi-faction commanders", "♛", "wrNewCampaign") : ""}</div><div class="wrWarTableStack">${warTableActionCard("quick", "Quick Battle", "Choose a bot archetype and difficulty for a standalone match.", "Solo practice", "⚔", "wrQuickBattle")}${warTableActionCard("multiplayer", "Multiplayer", "Create or join an online War Realms command room.", "Player vs player", "◎", "wrMultiplayerAction")}${warTableActionCard("armory", "Armory", "Manage command decks, browse cards, and inspect deck intelligence.", "Decks and collection", "◆", "wrArmoryAction")}</div></div><div class="wrWarSecondary"><button type="button" data-wr-main-action="help">? How to Play</button><button type="button" data-wr-main-action="statistics">▥ Statistics</button><button type="button" data-wr-main-action="settings">⚙ Settings</button>${document.body.classList.contains("battleMode") ? '<button type="button" data-wr-resume>Resume Battle</button>' : ""}</div>`;
}

function renderWarTableStatistics() {
  const { profile, commander, summary } = activeCampaignDetails();
  const mechanics = Object.values(profile?.stats || {}).reduce((sum, value) => sum + (Number(value) || 0), 0);
  return `<button type="button" class="wrWarBack" data-wr-back-main>← War Table</button><div class="wrInfoPanel"><section class="wrInfoCard"><h3>Campaign Record</h3><p>${profile ? `${escapeHtml(commander?.name || "Your commander")} currently leads the campaign.` : "Begin a campaign to build a persistent military record."}</p><div class="wrStatsGrid" style="margin-top:10px"><div class="wrStatTile">Battles Won<b>${summary?.battlesWon || 0}</b></div><div class="wrStatTile">Region<b>${summary?.region || 0}</b></div><div class="wrStatTile">Bosses<b>${summary?.bossesDefeated || 0}</b></div><div class="wrStatTile">Deck Size<b>${summary?.deckSize || 0}</b></div><div class="wrStatTile">Commander Level<b>${profile?.commanderLevel || 0}</b></div><div class="wrStatTile">Mechanic Events<b>${mechanics}</b></div></div></section></div>`;
}

function reduceMotionEnabled() {
  try { return localStorage.getItem("warRealms.reduceMotion") === "1"; } catch { return false; }
}

function applyStoredWarTableSettings() {
  document.documentElement.classList.toggle("wrReduceMotion", reduceMotionEnabled());
}

function toggleWarTableSetting(setting) {
  if (setting !== "reduce-motion") return;
  const enabled = !reduceMotionEnabled();
  try { localStorage.setItem("warRealms.reduceMotion", enabled ? "1" : "0"); } catch {}
  applyStoredWarTableSettings();
}

function renderWarTableSettings() {
  const reduced = reduceMotionEnabled();
  return `<button type="button" class="wrWarBack" data-wr-back-main>← War Table</button><div class="wrInfoPanel"><section class="wrInfoCard"><h3>Presentation Settings</h3><div class="wrSettingRow"><span>Reduced motion<small>Shortens battle and menu animation without removing information.</small></span><button type="button" class="wrSettingToggle ${reduced ? "active" : ""}" data-wr-setting="reduce-motion">${reduced ? "ON" : "OFF"}</button></div></section><section class="wrInfoCard"><h3>Commander Artwork</h3><p>Portraits load from <code>graphics/warrealmscommanders/</code>. Missing files automatically fall back to commander initials and faction colors.</p></section></div>`;
}

function openArmoryDestination(destination, followupAction = "") {
  const selector = `.primaryNavBtn[data-primary-destination="${destination}"] , #moreMenu [data-primary-destination="${destination}"]`;
  const button = document.querySelector(selector);
  if (!(button instanceof HTMLElement)) return;
  closePlayLauncher();
  button.click();
  if (followupAction) setTimeout(() => document.querySelector(`[data-act="${followupAction}"]`)?.click(), 0);
}

function handleWarTableAction(action) {
  if (action === "quick") {
    selectedView = "quick";
    selectedMode = "single";
    renderLauncher();
    return;
  }
  if (action === "continue") return openArmoryDestination("campaign", "continue-campaign");
  if (action === "new") return openArmoryDestination("campaign", "new-campaign");
  if (action === "multiplayer") return openExistingMultiplayerLobby();
  if (action === "armory") return openArmoryDestination("store");
  if (action === "help") {
    closePlayLauncher();
    document.getElementById("helpBtn")?.click();
    return;
  }
  if (action === "statistics" || action === "settings") {
    selectedView = action;
    renderLauncher();
  }
}

function renderLauncher() {
  const root = launcher();
  if (selectedView !== "quick") {
    const title = selectedView === "statistics" ? "Campaign statistics" : selectedView === "settings" ? "Command settings" : "The War Table";
    const subtitle = selectedView === "main" ? "Choose a campaign, battle, or command destination." : "Review the realm before returning to command.";
    const body = selectedView === "statistics" ? renderWarTableStatistics() : selectedView === "settings" ? renderWarTableSettings() : renderWarTableMain();
    root.innerHTML = `
      <section class="wrWarTablePanel">
        <header class="wrWarTableHead"><span class="wrWarTableCrest" aria-hidden="true">♛</span><div class="wrWarTableHeadCopy"><div class="wrPlayEyebrow">War Realms · Campaign Command</div><h2 class="wrWarTableTitle" id="warrealmsPlayLauncherTitle">${escapeHtml(title)}</h2><p class="wrWarTableSubtitle">${escapeHtml(subtitle)}</p></div><button type="button" class="wrPlayClose" data-wr-play-close aria-label="Close War Table">×</button></header>
        <div class="wrWarTableBody">${body}</div>
      </section>`;
    return;
  }
  root.innerHTML = `
    <section class="wrPlayPanel">
      <header class="wrPlayHead"><button type="button" class="wrWarBack" data-wr-back-main>← War Table</button><div class="wrPlayHeadCopy"><div class="wrPlayEyebrow">War Realms · Quick Battle</div><h2 class="wrPlayTitle" id="warrealmsPlayLauncherTitle">Choose your battle</h2><p class="wrPlaySubtitle">Solo practice, controlled matchups, and multiplayer.</p></div><button type="button" class="wrPlayClose" data-wr-play-close aria-label="Close Play menu">×</button></header>
      <div class="wrPlayBody"><div class="wrModeTabs" role="tablist" aria-label="Play mode"><button type="button" class="wrModeTab${selectedMode === "single" ? " active" : ""}" data-wr-play-mode="single"><strong>⚔ Solo</strong><span>Battle a bot</span></button><button type="button" class="wrModeTab${selectedMode === "multiplayer" ? " active" : ""}" data-wr-play-mode="multiplayer"><strong>◎ Multiplayer</strong><span>Create or join a room</span></button></div>${selectedMode === "single" ? renderSinglePlayer() : renderMultiplayer()}</div>
    </section>`;
}

function showPlayLauncher(mode = "single") {
  selectedView = "quick";
  selectedMode = mode === "multiplayer" ? "multiplayer" : "single";
  lastFocusedElement = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  const root = launcher();
  root.hidden = false;
  renderLauncher();
  requestAnimationFrame(() => root.querySelector(".wrModeTab.active")?.focus({ preventScroll: true }));
}

function showWarTable(options = {}) {
  if (options.auto && autoOpened) return;
  if (options.auto) autoOpened = true;
  selectedView = "main";
  lastFocusedElement = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  const root = launcher();
  root.hidden = false;
  renderLauncher();
  requestAnimationFrame(() => root.querySelector("[data-wr-main-action]")?.focus({ preventScroll: true }));
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
  if (selectedStrategy === "random") return nativeDifficulty.click();
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
  if (label) label.textContent = "War Table";
  playButton.setAttribute("aria-label", "War Table — campaign, quick battle, multiplayer, and armory");
  playButton.title = "Open the War Table";
}

function installPlayLauncher() {
  relabelBattleAsPlay();
  injectStyles();
  applyStoredWarTableSettings();
  document.addEventListener("click", event => {
    const playButton = event.target instanceof Element ? event.target.closest('.primaryNavBtn[data-primary-destination="battle"]') : null;
    if (!playButton) return;
    event.preventDefault();
    event.stopPropagation();
    event.stopImmediatePropagation();
    showWarTable();
  }, true);
  window.addEventListener("warrealms:open-war-table", event => showWarTable(event.detail || {}));
  document.addEventListener("keydown", event => {
    if (event.key === "Escape" && !document.getElementById(PLAY_LAUNCHER_ID)?.hidden) closePlayLauncher();
  });
}

if (typeof window !== "undefined") clearLegacySingleplayerAutoPrompt();
if (typeof document !== "undefined") {
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", installPlayLauncher, { once: true });
  else installPlayLauncher();
}

export { showPlayLauncher, showWarTable };
