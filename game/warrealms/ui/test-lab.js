import {
  TEST_LAB_DIFFICULTIES,
  TEST_LAB_PRIORITY_MODES,
  TEST_LAB_STRATEGIES,
  getTestLabCards
} from "./test-lab-simulator.js?v=2";
import { runTestLabWithHistory } from "./test-lab-runner.js?v=2";

const cards = getTestLabCards();
const cardNameById = new Map(cards.map(card => [card.id, card.name]));
const selectedCards = new Set();
const priorityCardsA = new Set();
const priorityCardsB = new Set();
const MAX_PRIORITY_CARDS = 8;
let activeController = null;
let latestRows = [];
let latestSummary = null;
let latestRecentGames = [];
let latestStrategyRows = [];
let latestStrategyCards = {};
let sortKey = "winRate";
let sortDirection = -1;

const $ = id => document.getElementById(id);
const escapeHtml = value => String(value ?? "").replace(/[&<>"']/g, character => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[character]);
const percent = value => `${Number(value || 0).toFixed(1)}%`;
const number = value => Number(value || 0).toLocaleString();

function strategyOptions(selected = "random") {
  return TEST_LAB_STRATEGIES.map(strategy => `<option value="${strategy.id}"${strategy.id === selected ? " selected" : ""}>${escapeHtml(strategy.name)}</option>`).join("");
}

function difficultyOptions(selected = "hard") {
  return TEST_LAB_DIFFICULTIES.map(difficulty => `<option value="${difficulty.id}"${difficulty.id === selected ? " selected" : ""}>${escapeHtml(difficulty.name)}</option>`).join("");
}

function priorityModeOptions(selected = "strong") {
  return TEST_LAB_PRIORITY_MODES.map(mode => `<option value="${mode.id}"${mode.id === selected ? " selected" : ""}>${escapeHtml(mode.name)}</option>`).join("");
}

function installSelectors() {
  $("strategyA").innerHTML = strategyOptions("random");
  $("strategyB").innerHTML = strategyOptions("random");
  $("difficultyA").innerHTML = difficultyOptions("hard");
  $("difficultyB").innerHTML = difficultyOptions("hard");
}

function installAdvancedPanels() {
  const runControls = document.querySelector(".runControls");
  const resultsPanel = document.querySelector(".resultsPanel");
  if (!runControls || !resultsPanel || $("advancedTestSettings")) return;

  const style = document.createElement("style");
  style.textContent = `
    .advancedSettings{margin-top:10px;border:1px solid #384553;border-radius:11px;background:#0a1118;overflow:hidden}
    .advancedSettings summary{cursor:pointer;list-style:none;padding:10px 11px;font:900 8px var(--display);color:#d8c264;text-transform:uppercase;letter-spacing:.08em}.advancedSettings summary::-webkit-details-marker{display:none}.advancedSettings summary:after{content:"＋";float:right;color:#8594a3}.advancedSettings[open] summary:after{content:"−"}
    .advancedBody{padding:0 10px 10px;display:grid;gap:9px}.toggleRow{display:flex;align-items:flex-start;gap:8px;border:1px solid #2e3b49;border-radius:9px;background:#0d151d;padding:8px}.toggleRow input{margin-top:2px}.toggleRow b{display:block;font-size:8px}.toggleRow small{display:block;margin-top:2px;color:#7c8b9a;font-size:6px;line-height:1.4}
    .analyticsSettings{display:grid;grid-template-columns:1fr 1fr;gap:7px}.compactField{display:grid;gap:4px}.compactField label{font-size:6px;font-weight:950;text-transform:uppercase;letter-spacing:.07em;color:#8594a3}.compactField input,.compactField select{height:32px;border:1px solid #354455;border-radius:7px;background:#0b1219;color:#fff;padding:5px 7px;font-size:8px}
    .priorityGrid{display:grid;grid-template-columns:1fr 1fr;gap:8px}.priorityBox{border:1px solid #344251;border-radius:9px;background:#0c141c;padding:8px;min-width:0}.priorityBox.a{border-color:#31546b}.priorityBox.b{border-color:#653541}.priorityHead{display:flex;align-items:center;gap:6px;margin-bottom:6px}.priorityHead b{font:850 8px var(--display)}.priorityHead span{margin-left:auto;color:#7f8e9d;font-size:6px}.priorityTools{display:grid;grid-template-columns:minmax(0,1fr) 112px;gap:5px}.priorityTools input,.priorityTools select{height:30px;border:1px solid #354455;border-radius:7px;background:#0b1219;color:#fff;padding:5px 7px;font-size:7px}.priorityPicker{height:126px;overflow:auto;margin-top:5px;border:1px solid #293744;border-radius:7px;background:#080e14;padding:3px}.priorityPick{display:grid;grid-template-columns:15px 8px minmax(0,1fr);gap:5px;align-items:center;padding:5px;border-radius:6px;font-size:7px;cursor:pointer}.priorityPick:hover{background:#121c25}.priorityPick.selected{background:#1c1c13}.priorityPick input{margin:0}.priorityHelp{margin-top:5px;color:#748393;font-size:6px;line-height:1.4}
    .strategyPanel .panelBody{padding:10px}.strategyLayout{display:grid;grid-template-columns:minmax(280px,.8fr) minmax(0,1.2fr);gap:10px}.strategyTableWrap{overflow:auto;max-height:390px;border:1px solid #2d3a48;border-radius:9px}.strategyTable{width:100%;border-collapse:collapse;font-size:7px}.strategyTable th{position:sticky;top:0;background:#111923;color:#8998a8;padding:7px;text-align:right;font-size:6px;text-transform:uppercase}.strategyTable th:first-child,.strategyTable td:first-child{text-align:left}.strategyTable td{padding:7px;border-top:1px solid #202c37;text-align:right;color:#b7c1cc;white-space:nowrap}.strategyTable tr.active td{background:#18212a}.strategyTable button{border:0;background:transparent;color:#eef3f7;font-weight:900;cursor:pointer;padding:0}.strategyCardTools{display:flex;align-items:center;gap:7px;margin-bottom:7px}.strategyCardTools select{height:32px;min-width:160px;border:1px solid #354455;border-radius:7px;background:#0c1219;color:#fff;padding:5px 7px;font-size:7px}.strategyCardTools span{margin-left:auto;color:#7d8c9b;font-size:6px}.strategyCardsWrap{overflow:auto;max-height:390px;border:1px solid #2d3a48;border-radius:9px}.priorityStar{color:#ecd05b;font-weight:950}.strategyDisabled{padding:20px;text-align:center;color:#788797;font-size:8px}
    @media(max-width:780px){.priorityGrid,.strategyLayout{grid-template-columns:1fr}.analyticsSettings{grid-template-columns:1fr}}
  `;
  document.head.appendChild(style);

  const advanced = document.createElement("details");
  advanced.className = "advancedSettings";
  advanced.id = "advancedTestSettings";
  advanced.innerHTML = `
    <summary>Advanced Test Settings</summary>
    <div class="advancedBody">
      <label class="toggleRow"><input id="strategyAnalyticsEnabled" type="checkbox" checked><span><b>Strategy Analytics</b><small>Rank each strategy by win rate and record which cards that strategy actually purchases.</small></span></label>
      <div class="analyticsSettings">
        <div class="compactField"><label for="strategyMinGames">Minimum strategy games shown</label><input id="strategyMinGames" type="number" min="1" max="50000" value="10"></div>
        <div class="compactField"><label for="strategyCardLimit">Cards shown per strategy</label><select id="strategyCardLimit"><option value="10">Top 10</option><option value="25">Top 25</option><option value="50" selected>Top 50</option></select></div>
      </div>
      <label class="toggleRow"><input id="injectPriorityCards" type="checkbox" checked><span><b>Ensure priority cards enter the market pool</b><small>Priority cards are added to the same experimental market injection so both bots can see them; only the assigned bot receives the purchase preference.</small></span></label>
      <div class="priorityGrid">
        <div class="priorityBox a">
          <div class="priorityHead"><input id="priorityEnabledA" type="checkbox"><b>Bot A Priority Cards</b><span id="priorityCountA">0 / ${MAX_PRIORITY_CARDS}</span></div>
          <div class="priorityTools"><input id="prioritySearchA" type="search" placeholder="Search cards…"><select id="priorityModeA">${priorityModeOptions("strong")}</select></div>
          <div class="priorityPicker" id="priorityPickerA"></div>
          <div class="priorityHelp">Select up to ${MAX_PRIORITY_CARDS}. “Force when affordable” makes Bot A buy a selected card first whenever it can pay for one.</div>
        </div>
        <div class="priorityBox b">
          <div class="priorityHead"><input id="priorityEnabledB" type="checkbox"><b>Bot B Priority Cards</b><span id="priorityCountB">0 / ${MAX_PRIORITY_CARDS}</span></div>
          <div class="priorityTools"><input id="prioritySearchB" type="search" placeholder="Search cards…"><select id="priorityModeB">${priorityModeOptions("strong")}</select></div>
          <div class="priorityPicker" id="priorityPickerB"></div>
          <div class="priorityHelp">Bot B has its own independent list and preference strength, so you can run targeted strategy/card experiments.</div>
        </div>
      </div>
    </div>`;
  runControls.parentElement.insertBefore(advanced, runControls);

  const panel = document.createElement("section");
  panel.className = "panel strategyPanel";
  panel.id = "strategyAnalyticsPanel";
  panel.innerHTML = `
    <div class="panelHead"><div><h3>Strategy Intelligence</h3><p>Win-rate ranking plus the most common cards purchased by each strategy.</p></div><div class="grow"></div><button class="miniBtn" id="exportStrategyCsv" type="button">Export Strategy CSV</button></div>
    <div class="panelBody"><div id="strategyAnalyticsBody" class="strategyDisabled">Run the Test Lab with Strategy Analytics enabled to populate strategy rankings and purchase profiles.</div></div>`;
  resultsPanel.parentElement.insertBefore(panel, resultsPanel);
}

function installRecentMatchPanel() {
  const resultsPanel = document.querySelector(".resultsPanel");
  if (!resultsPanel || $("recentMatchesPanel")) return;
  const style = document.createElement("style");
  style.textContent = `
    .recentMatchGrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:7px}
    .recentMatch{border:1px solid #2f3e4d;border-radius:10px;background:#0c131a;padding:9px;min-width:0}
    .recentMatchTop{display:flex;align-items:center;gap:7px;margin-bottom:7px}.recentMatchTop b{font:800 9px var(--display)}
    .recentWinner{margin-left:auto;border:1px solid #3d4c5d;border-radius:999px;padding:4px 7px;font-size:6px;font-weight:950;text-transform:uppercase;letter-spacing:.06em}
    .recentWinner.a{border-color:#3c789d;color:#85ccf5}.recentWinner.b{border-color:#884455;color:#f08a9c}.recentWinner.draw{color:#b0bac4}
    .recentMeta{color:#788797;font-size:6px;margin-bottom:7px}.recentPurchases{display:grid;gap:5px}.recentPurchaseLine{display:grid;grid-template-columns:42px minmax(0,1fr);gap:6px;font-size:6px;line-height:1.45}.recentPurchaseLine strong{color:#aeb9c5}.recentPurchaseLine span{color:#8795a5;overflow:hidden;text-overflow:ellipsis}
    @media(max-width:700px){.recentMatchGrid{grid-template-columns:1fr}}
  `;
  document.head.appendChild(style);
  const panel = document.createElement("section");
  panel.className = "panel";
  panel.id = "recentMatchesPanel";
  panel.innerHTML = `<div class="panelHead"><div><h3>Recent Simulated Matches</h3><p>See who won and exactly what each bot purchased in the latest games.</p></div></div><div class="panelBody"><div class="recentMatchGrid" id="recentMatchGrid"><div class="emptyState">Recent match results will appear while the simulation runs.</div></div></div>`;
  resultsPanel.parentElement.insertBefore(panel, resultsPanel);
}

function renderExperimentalCards() {
  const query = $("cardSearch").value.trim().toLowerCase();
  const faction = $("cardFaction").value;
  const filtered = cards.filter(card => {
    if (faction !== "all" && card.faction !== faction) return false;
    if (!query) return true;
    return `${card.name} ${card.id} ${card.type}`.toLowerCase().includes(query);
  }).slice(0, 180);
  $("cardPicker").innerHTML = filtered.map(card => `
    <label class="cardPick ${selectedCards.has(card.id) ? "selected" : ""}" data-card-id="${escapeHtml(card.id)}">
      <input type="checkbox" value="${escapeHtml(card.id)}" ${selectedCards.has(card.id) ? "checked" : ""}>
      <span class="factionDot ${escapeHtml(card.faction)}"></span>
      <span class="cardPickName"><b>${escapeHtml(card.name)}</b><small>${escapeHtml(card.faction)} · ${escapeHtml(card.type)} · Cost ${card.cost}</small></span>
    </label>
  `).join("") || `<div class="emptyState">No cards match this search.</div>`;
  $("selectedCount").textContent = `${selectedCards.size} experimental card${selectedCards.size === 1 ? "" : "s"}`;
}

function prioritySet(side) {
  return side === "A" ? priorityCardsA : priorityCardsB;
}

function renderPriorityPicker(side) {
  const set = prioritySet(side);
  const query = $(`prioritySearch${side}`)?.value.trim().toLowerCase() || "";
  const filtered = cards.filter(card => !query || `${card.name} ${card.id} ${card.type} ${card.faction}`.toLowerCase().includes(query)).slice(0, 100);
  const picker = $(`priorityPicker${side}`);
  if (!picker) return;
  picker.innerHTML = filtered.map(card => `
    <label class="priorityPick ${set.has(card.id) ? "selected" : ""}">
      <input type="checkbox" value="${escapeHtml(card.id)}" ${set.has(card.id) ? "checked" : ""} ${!set.has(card.id) && set.size >= MAX_PRIORITY_CARDS ? "disabled" : ""}>
      <span class="factionDot ${escapeHtml(card.faction)}"></span>
      <span title="${escapeHtml(card.name)}">${escapeHtml(card.name)} · ${card.cost}</span>
    </label>`).join("") || `<div class="emptyState">No cards match.</div>`;
  $(`priorityCount${side}`).textContent = `${set.size} / ${MAX_PRIORITY_CARDS}`;
}

function renderSummary(summary = {}) {
  $("gamesComplete").textContent = number(summary.games);
  $("botAWin").textContent = percent(summary.winRateA);
  $("botBWin").textContent = percent(summary.winRateB);
  $("avgTurns").textContent = Number(summary.averageTurns || 0).toFixed(1);
  $("simSpeed").textContent = `${number(summary.gamesPerSecond)} / sec`;
  $("drawCount").textContent = number(summary.draws);
}

function filteredRows() {
  const minimum = Math.max(0, Number($("minimumPurchases").value) || 0);
  const query = $("resultSearch").value.trim().toLowerCase();
  return latestRows.filter(row => row.purchases >= minimum && (!query || `${row.name} ${row.cardId}`.toLowerCase().includes(query)));
}

function sortedRows() {
  const rows = filteredRows();
  return rows.sort((a, b) => {
    if (sortKey === "name") return a.name.localeCompare(b.name) * sortDirection;
    const aValue = Number(a[sortKey] || 0);
    const bValue = Number(b[sortKey] || 0);
    if (aValue === bValue) return (b.purchases - a.purchases) || a.name.localeCompare(b.name);
    return (aValue - bValue) * sortDirection;
  });
}

function renderChart() {
  const minimum = Math.max(3, Number($("minimumPurchases").value) || 0);
  const rows = latestRows
    .filter(row => row.purchases >= minimum)
    .sort((a, b) => (b.winRate - a.winRate) || (b.purchases - a.purchases))
    .slice(0, 14);
  $("liveChart").innerHTML = rows.map((row, index) => `
    <div class="chartRow" style="--rank:${index}">
      <div class="chartLabel" title="${escapeHtml(row.name)}"><span class="factionDot ${escapeHtml(row.faction)}"></span>${escapeHtml(row.name)}</div>
      <div class="chartTrack"><i style="width:${Math.max(2, Math.min(100, row.winRate))}%"></i><b>${percent(row.winRate)}</b></div>
      <div class="chartPurchases">${number(row.purchases)} buys</div>
    </div>
  `).join("") || `<div class="emptyState">Run a test to watch card win rates move here.</div>`;
}

function renderTable() {
  const rows = sortedRows();
  $("resultBody").innerHTML = rows.slice(0, 500).map(row => `
    <tr>
      <td class="nameCell"><span class="factionDot ${escapeHtml(row.faction)}"></span><span><b>${escapeHtml(row.name)}</b><small>${escapeHtml(row.faction)} · Cost ${row.cost}</small></span></td>
      <td>${number(row.purchases)}</td>
      <td>${number(row.gamesPurchased)}</td>
      <td class="${row.winRate >= 55 ? "positive" : row.winRate <= 45 ? "negative" : ""}">${percent(row.winRate)}</td>
      <td>${Number(row.avgBuyTurn || 0).toFixed(2)}</td>
      <td>${row.avgWinningBuyTurn ? Number(row.avgWinningBuyTurn).toFixed(2) : "—"}</td>
      <td>${row.avgLosingBuyTurn ? Number(row.avgLosingBuyTurn).toFixed(2) : "—"}</td>
      <td>${number(row.wins)} / ${number(row.losses)}</td>
    </tr>
  `).join("") || `<tr><td colspan="8" class="emptyCell">No cards meet the current minimum-purchase filter yet.</td></tr>`;
  document.querySelectorAll("th[data-sort]").forEach(th => {
    th.classList.toggle("activeSort", th.dataset.sort === sortKey);
    th.dataset.direction = th.dataset.sort === sortKey ? (sortDirection > 0 ? "up" : "down") : "";
  });
}

function purchaseLine(game, playerId) {
  const purchases = (game.purchases || []).filter(purchase => purchase.playerId === playerId);
  if (!purchases.length) return "No purchases";
  const shown = purchases.slice(0, 9).map(purchase => `${purchase.priority ? "★ " : ""}${cardNameById.get(purchase.cardId) || purchase.cardId} (T${purchase.turn})`);
  if (purchases.length > shown.length) shown.push(`+${purchases.length - shown.length} more`);
  return shown.join(" · ");
}

function renderRecentGames(recentGames = []) {
  latestRecentGames = recentGames;
  const grid = $("recentMatchGrid");
  if (!grid) return;
  grid.innerHTML = recentGames.slice(0, 8).map(game => {
    const winnerClass = game.winnerId || "draw";
    const winnerLabel = game.winnerId === "a" ? "Bot A won" : game.winnerId === "b" ? "Bot B won" : "Turn-limit draw";
    return `<article class="recentMatch">
      <div class="recentMatchTop"><b>Game ${number(game.gameNumber)}</b><span class="recentWinner ${winnerClass}">${winnerLabel}</span></div>
      <div class="recentMeta">${escapeHtml(game.strategies?.a || "?")} vs ${escapeHtml(game.strategies?.b || "?")} · ${number(game.totalTurns)} total turns · Authority ${Math.round(game.authority?.a || 0)}–${Math.round(game.authority?.b || 0)}</div>
      <div class="recentPurchases">
        <div class="recentPurchaseLine"><strong>Bot A</strong><span>${escapeHtml(purchaseLine(game, "a"))}</span></div>
        <div class="recentPurchaseLine"><strong>Bot B</strong><span>${escapeHtml(purchaseLine(game, "b"))}</span></div>
      </div>
    </article>`;
  }).join("") || `<div class="emptyState">Recent match results will appear while the simulation runs.</div>`;
}

function strategyRowsShown() {
  const minimum = Math.max(1, Number($("strategyMinGames")?.value) || 1);
  return latestStrategyRows.filter(row => row.games >= minimum);
}

function renderStrategyAnalytics() {
  const body = $("strategyAnalyticsBody");
  if (!body) return;
  if (!$("strategyAnalyticsEnabled")?.checked) {
    body.innerHTML = `<div class="strategyDisabled">Strategy Analytics is turned off for this test configuration.</div>`;
    return;
  }
  const rows = strategyRowsShown();
  if (!rows.length) {
    body.innerHTML = `<div class="strategyDisabled">Run more games or lower the minimum strategy-game setting to populate this section.</div>`;
    return;
  }
  const previous = $("strategyProfileSelect")?.value;
  const selected = rows.some(row => row.strategyId === previous) ? previous : rows[0].strategyId;
  const profileRows = latestStrategyCards[selected] || [];
  body.innerHTML = `
    <div class="strategyLayout">
      <div>
        <div class="strategyTableWrap"><table class="strategyTable"><thead><tr><th>Strategy</th><th>Games</th><th>Win Rate</th><th>W-L-D</th><th>Buys/Game</th></tr></thead><tbody>
          ${rows.map((row, index) => `<tr class="${row.strategyId === selected ? "active" : ""}"><td><button type="button" data-strategy-pick="${escapeHtml(row.strategyId)}">#${index + 1} ${escapeHtml(row.name)}</button></td><td>${number(row.games)}</td><td class="${row.winRate >= 55 ? "positive" : row.winRate <= 45 ? "negative" : ""}">${percent(row.winRate)}</td><td>${number(row.wins)}-${number(row.losses)}-${number(row.draws)}</td><td>${Number(row.purchasesPerGame || 0).toFixed(2)}</td></tr>`).join("")}
        </tbody></table></div>
      </div>
      <div>
        <div class="strategyCardTools"><select id="strategyProfileSelect">${rows.map(row => `<option value="${escapeHtml(row.strategyId)}"${row.strategyId === selected ? " selected" : ""}>${escapeHtml(row.name)} — ${percent(row.winRate)}</option>`).join("")}</select><span>${profileRows.length} most common purchased cards</span></div>
        <div class="strategyCardsWrap"><table class="strategyTable"><thead><tr><th>Card</th><th>Purchases</th><th>Share</th><th>Win Rate</th><th>Avg Buy Turn</th></tr></thead><tbody id="strategyCardBody">
          ${strategyCardTableRows(profileRows)}
        </tbody></table></div>
      </div>
    </div>`;
  $("strategyProfileSelect").addEventListener("change", event => renderStrategyCardProfile(event.target.value));
  body.querySelectorAll("[data-strategy-pick]").forEach(button => button.addEventListener("click", () => renderStrategyCardProfile(button.dataset.strategyPick, true)));
}

function strategyCardTableRows(rows) {
  return rows.map(row => `<tr><td><span class="factionDot ${escapeHtml(row.faction)}"></span> ${escapeHtml(row.name)}</td><td>${number(row.purchases)}</td><td>${percent(row.purchaseShare)}</td><td class="${row.winRate >= 55 ? "positive" : row.winRate <= 45 ? "negative" : ""}">${percent(row.winRate)}</td><td>${Number(row.avgBuyTurn || 0).toFixed(2)}</td></tr>`).join("") || `<tr><td colspan="5" class="emptyCell">No purchases recorded for this strategy yet.</td></tr>`;
}

function renderStrategyCardProfile(strategyId, rerenderRanking = false) {
  const select = $("strategyProfileSelect");
  if (select) select.value = strategyId;
  const body = $("strategyCardBody");
  if (body) body.innerHTML = strategyCardTableRows(latestStrategyCards[strategyId] || []);
  if (rerenderRanking) {
    const row = strategyRowsShown().find(entry => entry.strategyId === strategyId);
    if (row) {
      document.querySelectorAll("[data-strategy-pick]").forEach(button => button.closest("tr")?.classList.toggle("active", button.dataset.strategyPick === strategyId));
      if (select) select.value = strategyId;
    }
  }
}

function renderProgress(payload) {
  latestRows = payload.rows || [];
  latestSummary = payload.summary || null;
  latestRecentGames = payload.recentGames || latestRecentGames;
  latestStrategyRows = payload.strategyRows || latestStrategyRows;
  latestStrategyCards = payload.strategyCards || latestStrategyCards;
  const ratio = payload.requested ? payload.completed / payload.requested : 0;
  $("progressFill").style.width = `${Math.min(100, ratio * 100)}%`;
  $("progressText").textContent = `${number(payload.completed)} / ${number(payload.requested)} games`;
  renderSummary(payload.summary);
  renderChart();
  renderTable();
  renderRecentGames(latestRecentGames);
  renderStrategyAnalytics();
}

function gameCount() {
  const preset = $("gameCount").value;
  if (preset === "custom") return Math.max(1, Math.min(50000, Number($("customGames").value) || 1000));
  return Number(preset) || 1000;
}

function buildOptions() {
  const experimental = new Set(selectedCards);
  if ($("injectPriorityCards")?.checked) {
    if ($("priorityEnabledA")?.checked) priorityCardsA.forEach(cardId => experimental.add(cardId));
    if ($("priorityEnabledB")?.checked) priorityCardsB.forEach(cardId => experimental.add(cardId));
  }
  return {
    games: gameCount(),
    strategyA: $("strategyA").value,
    strategyB: $("strategyB").value,
    difficultyA: $("difficultyA").value,
    difficultyB: $("difficultyB").value,
    experimentalCardIds: [...experimental],
    experimentalCopies: Number($("experimentalCopies").value) || 2,
    priorityAEnabled: $("priorityEnabledA")?.checked === true,
    priorityBEnabled: $("priorityEnabledB")?.checked === true,
    priorityCardsA: [...priorityCardsA],
    priorityCardsB: [...priorityCardsB],
    priorityModeA: $("priorityModeA")?.value || "strong",
    priorityModeB: $("priorityModeB")?.value || "strong",
    strategyAnalyticsEnabled: $("strategyAnalyticsEnabled")?.checked !== false,
    strategyCardLimit: Number($("strategyCardLimit")?.value) || 50,
    maxTurns: Number($("maxTurns").value) || 80,
    seed: Number($("seed").value) || 24681357,
    batchSize: 20
  };
}

async function startSimulation() {
  activeController?.abort();
  activeController = new AbortController();
  latestRows = [];
  latestSummary = null;
  latestRecentGames = [];
  latestStrategyRows = [];
  latestStrategyCards = {};
  renderSummary({});
  renderChart();
  renderTable();
  renderRecentGames([]);
  renderStrategyAnalytics();
  $("progressFill").style.width = "0%";
  $("startTest").disabled = true;
  $("stopTest").disabled = false;
  $("runState").textContent = "SIMULATING";
  $("runState").classList.add("running");
  const options = buildOptions();
  $("progressText").textContent = `0 / ${number(options.games)} games`;

  try {
    const result = await runTestLabWithHistory(options, {
      signal: activeController.signal,
      onProgress: renderProgress
    });
    latestRows = result.rows || latestRows;
    latestSummary = result.summary || latestSummary;
    latestRecentGames = result.recentGames || latestRecentGames;
    latestStrategyRows = result.strategyRows || latestStrategyRows;
    latestStrategyCards = result.strategyCards || latestStrategyCards;
    renderChart();
    renderTable();
    renderRecentGames(latestRecentGames);
    renderSummary(latestSummary || {});
    renderStrategyAnalytics();
    $("runState").textContent = activeController.signal.aborted ? "STOPPED" : "COMPLETE";
  } catch (error) {
    console.error("[WarRealms Test Lab] Simulation failed", error);
    $("runState").textContent = "ERROR";
    $("progressText").textContent = error?.message || "Simulation failed.";
  } finally {
    $("runState").classList.remove("running");
    $("startTest").disabled = false;
    $("stopTest").disabled = true;
  }
}

function stopSimulation() {
  activeController?.abort();
  $("runState").textContent = "STOPPING";
}

function downloadCsv(filename, rows) {
  const csv = rows.map(row => row.map(value => `"${String(value ?? "").replaceAll('"', '""')}"`).join(",")).join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}

function exportCsv() {
  if (!latestRows.length) return;
  const columns = ["Card", "Card ID", "Faction", "Cost", "Purchases", "Games Purchased", "Wins", "Losses", "Draws", "Win Rate", "Avg Buy Turn", "Avg Buy Turn Wins", "Avg Buy Turn Losses"];
  downloadCsv(`warrealms-test-lab-${new Date().toISOString().slice(0, 10)}.csv`, [columns, ...latestRows.map(row => [
    row.name, row.cardId, row.faction, row.cost, row.purchases, row.gamesPurchased, row.wins, row.losses, row.draws,
    row.winRate, row.avgBuyTurn, row.avgWinningBuyTurn, row.avgLosingBuyTurn
  ])]);
}

function exportStrategyCsv() {
  if (!latestStrategyRows.length) return;
  const rows = [["Strategy", "Games", "Wins", "Losses", "Draws", "Win Rate", "Avg Turns", "Purchases", "Purchases/Game"]];
  latestStrategyRows.forEach(row => rows.push([row.name, row.games, row.wins, row.losses, row.draws, row.winRate, row.averageTurns, row.purchases, row.purchasesPerGame]));
  rows.push([]);
  rows.push(["Strategy", "Card", "Card ID", "Faction", "Cost", "Purchases", "Games Purchased", "Purchase Share", "Win Rate When Bought", "Avg Buy Turn"]);
  Object.values(latestStrategyCards).flat().forEach(row => rows.push([row.strategyName, row.name, row.cardId, row.faction, row.cost, row.purchases, row.gamesPurchased, row.purchaseShare, row.winRate, row.avgBuyTurn]));
  downloadCsv(`warrealms-strategy-analytics-${new Date().toISOString().slice(0, 10)}.csv`, rows);
}

function wirePriorityEvents(side) {
  const set = prioritySet(side);
  $(`prioritySearch${side}`).addEventListener("input", () => renderPriorityPicker(side));
  $(`priorityPicker${side}`).addEventListener("change", event => {
    const input = event.target.closest('input[type="checkbox"]');
    if (!input) return;
    if (input.checked) {
      if (set.size >= MAX_PRIORITY_CARDS) {
        input.checked = false;
        return;
      }
      set.add(input.value);
    } else set.delete(input.value);
    renderPriorityPicker(side);
  });
}

function wireEvents() {
  $("cardSearch").addEventListener("input", renderExperimentalCards);
  $("cardFaction").addEventListener("change", renderExperimentalCards);
  $("cardPicker").addEventListener("change", event => {
    const input = event.target.closest('input[type="checkbox"]');
    if (!input) return;
    if (input.checked) selectedCards.add(input.value);
    else selectedCards.delete(input.value);
    renderExperimentalCards();
  });
  $("clearExperimental").addEventListener("click", () => {
    selectedCards.clear();
    renderExperimentalCards();
  });
  $("gameCount").addEventListener("change", () => {
    $("customGamesWrap").hidden = $("gameCount").value !== "custom";
  });
  $("startTest").addEventListener("click", startSimulation);
  $("stopTest").addEventListener("click", stopSimulation);
  $("minimumPurchases").addEventListener("input", () => { renderChart(); renderTable(); });
  $("resultSearch").addEventListener("input", renderTable);
  $("exportCsv").addEventListener("click", exportCsv);
  $("strategyAnalyticsEnabled").addEventListener("change", renderStrategyAnalytics);
  $("strategyMinGames").addEventListener("input", renderStrategyAnalytics);
  $("exportStrategyCsv").addEventListener("click", exportStrategyCsv);
  wirePriorityEvents("A");
  wirePriorityEvents("B");
  document.querySelectorAll("th[data-sort]").forEach(th => th.addEventListener("click", () => {
    const nextKey = th.dataset.sort;
    if (sortKey === nextKey) sortDirection *= -1;
    else {
      sortKey = nextKey;
      sortDirection = nextKey === "name" ? 1 : -1;
    }
    renderTable();
  }));
}

installSelectors();
installAdvancedPanels();
installRecentMatchPanel();
renderExperimentalCards();
renderPriorityPicker("A");
renderPriorityPicker("B");
renderSummary({});
renderChart();
renderTable();
renderRecentGames([]);
renderStrategyAnalytics();
wireEvents();
