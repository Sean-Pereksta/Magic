import {
  TEST_LAB_DIFFICULTIES,
  TEST_LAB_STRATEGIES,
  getTestLabCards,
  runTestLabSimulation
} from "./test-lab-simulator.js?v=1";

const cards = getTestLabCards();
const selectedCards = new Set();
let activeController = null;
let latestRows = [];
let latestSummary = null;
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

function installSelectors() {
  $("strategyA").innerHTML = strategyOptions("random");
  $("strategyB").innerHTML = strategyOptions("random");
  $("difficultyA").innerHTML = difficultyOptions("hard");
  $("difficultyB").innerHTML = difficultyOptions("hard");
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

function renderProgress(payload) {
  latestRows = payload.rows || [];
  latestSummary = payload.summary || null;
  const ratio = payload.requested ? payload.completed / payload.requested : 0;
  $("progressFill").style.width = `${Math.min(100, ratio * 100)}%`;
  $("progressText").textContent = `${number(payload.completed)} / ${number(payload.requested)} games`;
  renderSummary(payload.summary);
  renderChart();
  renderTable();
}

function gameCount() {
  const preset = $("gameCount").value;
  if (preset === "custom") return Math.max(1, Math.min(50000, Number($("customGames").value) || 1000));
  return Number(preset) || 1000;
}

function buildOptions() {
  return {
    games: gameCount(),
    strategyA: $("strategyA").value,
    strategyB: $("strategyB").value,
    difficultyA: $("difficultyA").value,
    difficultyB: $("difficultyB").value,
    experimentalCardIds: [...selectedCards],
    experimentalCopies: Number($("experimentalCopies").value) || 2,
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
  renderSummary({});
  renderChart();
  renderTable();
  $("progressFill").style.width = "0%";
  $("startTest").disabled = true;
  $("stopTest").disabled = false;
  $("runState").textContent = "SIMULATING";
  $("runState").classList.add("running");
  const options = buildOptions();
  $("progressText").textContent = `0 / ${number(options.games)} games`;

  try {
    const result = await runTestLabSimulation(options, {
      signal: activeController.signal,
      onProgress: renderProgress
    });
    latestRows = result.rows || latestRows;
    latestSummary = result.summary || latestSummary;
    renderChart();
    renderTable();
    renderSummary(latestSummary || {});
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

function exportCsv() {
  if (!latestRows.length) return;
  const columns = ["Card", "Card ID", "Faction", "Cost", "Purchases", "Games Purchased", "Wins", "Losses", "Draws", "Win Rate", "Avg Buy Turn", "Avg Buy Turn Wins", "Avg Buy Turn Losses"];
  const csvRows = [columns, ...latestRows.map(row => [
    row.name, row.cardId, row.faction, row.cost, row.purchases, row.gamesPurchased, row.wins, row.losses, row.draws,
    row.winRate, row.avgBuyTurn, row.avgWinningBuyTurn, row.avgLosingBuyTurn
  ])].map(row => row.map(value => `"${String(value ?? "").replaceAll('"', '""')}"`).join(","));
  const blob = new Blob([csvRows.join("\n")], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `warrealms-test-lab-${new Date().toISOString().slice(0, 10)}.csv`;
  anchor.click();
  URL.revokeObjectURL(url);
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
renderExperimentalCards();
renderSummary({});
renderChart();
renderTable();
wireEvents();
