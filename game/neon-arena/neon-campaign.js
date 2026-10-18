/* Offline campaign rules. The host supplies physics, guards and interactions;
 * this module owns serializable mission state and never reads a live enemy. */
(function (root) {
  'use strict';
  const VERSION = 1, clamp = (n, a, b) => Math.max(a, Math.min(b, n));
  const clone = value => JSON.parse(JSON.stringify(value));
  const finite = (n, fallback = 0) => Number.isFinite(n) ? n : fallback;
  const distance = (a, b) => Math.hypot(a.x - b.x, a.z - b.z);
  const difficulties = Object.freeze({ easy: { guards: 5, reinforcements: 2, defend: .8 }, normal: { guards: 7, reinforcements: 3, defend: 1 }, hard: { guards: 9, reinforcements: 4, defend: 1.2 }, nightmare: { guards: 11, reinforcements: 5, defend: 1.35 } });
  const objective = (type, anchor, title, duration = 0, extra = {}) => ({ type, anchor, title, duration, radius: type === 'defend' ? 9 : type === 'escort' ? 6 : 3.8, ...extra });
  const MISSIONS = [
    { id: 'mice_broken_convoy', faction: 'mice', chapter: 1, name: 'The Broken Convoy', mapId: 'cold_storage', style: 'AMBUSH & INTELLIGENCE',
      briefing: 'A Dominion supply convoy is stranded in the cold-storage yards. Resistance scouts need its manifest before the escort reaches the fortress. Slip around the guards or break through the loading bay, secure the evidence, and reach the service exit.',
      debrief: 'The manifest names Rail Yard Nine. The supplies are only cover for a military communications network.', reward: 'resistance_convoy_patch',
      objectives: [objective('reach', 'entry', 'Reach the convoy loading yard'), objective('intel', 'intel', 'Recover the convoy manifest', 2.5), objective('extract', 'extract', 'Extract through the service exit', 3)] },
    { id: 'mice_behind_enemy_lines', faction: 'mice', chapter: 2, name: 'Behind Enemy Lines', mapId: 'rail_yard_nine', style: 'STEALTH INFILTRATION',
      briefing: 'Rail Yard Nine relays Dominion troop movements. Avoid the platform patrols, cut the security relay, and copy the dispatch ledger from the communications room. The maintenance route offers a quieter way out.',
      debrief: 'The ledger reveals a surveillance hub below the city. Floodgate must go dark before the prison rescue can begin.', reward: 'resistance_night_visor',
      objectives: [objective('reach', 'entry', 'Enter the depot through a flank route'), objective('sabotage', 'security', 'Disable the depot security relay', 3, { disableAlarm: true }), objective('intel', 'vault', 'Copy the classified dispatch ledger', 4), objective('extract', 'extract', 'Escape along the maintenance route', 3)] },
    { id: 'mice_operation_floodgate', faction: 'mice', chapter: 3, name: 'Operation Floodgate', mapId: 'floodgate', style: 'SABOTAGE & ESCAPE',
      briefing: 'The pumping complex carries more than water: it powers the Dominion surveillance net. Shut down the cameras, plant a timed charge at the pump controller, and hold the control platform while the charge arms.',
      debrief: 'The surveillance grid is down. Prisoners in Whiskerhold now have a chance.', reward: 'resistance_engineer_tools',
      objectives: [objective('sabotage', 'security', 'Disable the surveillance network', 3, { disableAlarm: true }), objective('sabotage', 'sabotage', 'Plant the pump-controller charge', 4), objective('defend', 'defend', 'Defend the control platform while the charge arms', 24, { counterattack: true }), objective('extract', 'extract', 'Escape before the facility seals', 3)] },
    { id: 'mice_whiskerhold_rescue', faction: 'mice', chapter: 4, name: 'Whiskerhold Rescue', mapId: 'whiskerhold', style: 'RESCUE & ESCORT',
      briefing: 'Captured Resistance soldiers are being held inside Whiskerhold. Find the cell roster, release the prisoners, and keep their leader alive through the courtyard. The service gate is your extraction point.',
      debrief: 'The rescued commander carries the location of the Dominion command archive. One final operation can expose the war orders.', reward: 'resistance_rescuer_armor',
      objectives: [objective('intel', 'intel', 'Locate the prisoners on the cell roster', 2), objective('rescue', 'prison', 'Unlock the prisoner holding area', 3), objective('escort', 'escort', 'Escort the rescued commander to the service gate'), objective('extract', 'extract', 'Extract with the rescued commander', 4, { requiresEscort: true })] },
    { id: 'mice_dominions_end', faction: 'mice', chapter: 5, name: "Dominion's End", mapId: 'whiskerhold', style: 'COMMAND ASSAULT',
      briefing: 'Return to Whiskerhold at the head of the Resistance assault. Disable its command defenses, seize the archive, and hold the transmitter while the orders are broadcast. Extract with the truth, or the war will continue in darkness.',
      debrief: 'The broadcast reaches every district. The Dominion can no longer hide the orders that drove the war. Your squad leaves the fortress with the evidence and a path toward a ceasefire.', reward: 'resistance_liberator_mantle',
      objectives: [objective('sabotage', 'security', 'Disable the headquarters defenses', 4, { disableAlarm: true }), objective('intel', 'vault', 'Seize the command archive', 4), objective('defend', 'defend', 'Hold the transmitter through the final counterattack', 38, { counterattack: true }), objective('extract', 'extract', 'Extract the archive team', 4)] },
    { id: 'cats_silent_outpost', faction: 'cats', chapter: 1, name: 'The Silent Outpost', mapId: 'cold_storage', style: 'RECON & RECOVERY',
      briefing: 'An outpost has stopped answering command. Search the empty loading offices and recover its recorder. A surviving radio operator is trapped nearby; bring them to the evacuation team before the next ambush.',
      debrief: 'The recorder shows a reconnaissance team moving toward the Night Market. The attack was intended to conceal their route.', reward: 'dominion_outpost_ribbon',
      objectives: [objective('reach', 'entry', 'Investigate the silent outpost'), objective('intel', 'intel', 'Recover the outpost recorder', 2.5), objective('rescue', 'prison', 'Free the stranded radio operator', 2), objective('escort', 'escort', 'Escort the operator to evacuation'), objective('extract', 'extract', 'Secure the evacuation point', 3, { requiresEscort: true })] },
    { id: 'cats_night_market_hunt', faction: 'cats', chapter: 2, name: 'The Night Market Hunt', mapId: 'night_market', style: 'URBAN SEARCH',
      briefing: 'A Resistance scout crossed the ruined market with stolen route codes. Search the storefront cache, secure the rooftop relay, and retrieve the scout’s dispatch from the inner courtyard. Expect ambushes through the broken shops.',
      debrief: 'The route codes point beneath the city. The Resistance supply network is larger than command expected.', reward: 'dominion_tracker_visor',
      objectives: [objective('intel', 'intel', 'Search the storefront reconnaissance cache', 2.5), objective('defend', 'defend', 'Secure the rooftop relay', 18, { counterattack: true }), objective('intel', 'vault', 'Recover the scout’s dispatch', 3), objective('extract', 'extract', 'Leave through the ruined courtyard', 3)] },
    { id: 'cats_beneath_city', faction: 'cats', chapter: 3, name: 'Beneath the City', mapId: 'floodgate', style: 'COVERT RECONNAISSANCE',
      briefing: 'Follow the supply route through Floodgate. Shut down the local alarm before taking the tunnel ledger, then tag the ammunition junction. Quiet work will keep the entire garrison from converging on your squad.',
      debrief: 'The ledger identifies the Clockwork Foundry as the Resistance production center. Command orders an immediate offensive.', reward: 'dominion_shadow_armor',
      objectives: [objective('sabotage', 'security', 'Silence the tunnel alarm relay', 3, { disableAlarm: true }), objective('intel', 'intel', 'Obtain the underground supply ledger', 3), objective('sabotage', 'sabotage', 'Tag the ammunition junction for seizure', 3), objective('extract', 'extract', 'Return through the drainage route', 3)] },
    { id: 'cats_foundry_offensive', faction: 'cats', chapter: 4, name: 'The Foundry Offensive', mapId: 'clockwork', style: 'ASSAULT & DEFENSE',
      briefing: 'Push into the Clockwork Foundry and cut its production controls. The data vault contains the evacuation route to the hidden citadel. Hold the workshop against organized counterattacks while your squad copies it.',
      debrief: 'The archive contains a hidden tunnel map—and records of civilians sheltered below. The final operation must preserve the evidence.', reward: 'dominion_foundry_plate',
      objectives: [objective('sabotage', 'sabotage', 'Shut down the production controller', 4), objective('intel', 'vault', 'Secure the foundry data vault', 4), objective('defend', 'defend', 'Survive the workshop counterattacks', 32, { counterattack: true }), objective('extract', 'extract', 'Extract through the loading route', 4)] },
    { id: 'cats_underground_citadel', faction: 'cats', chapter: 5, name: 'The Underground Citadel', mapId: 'floodgate', style: 'FINAL INFILTRATION',
      briefing: 'Enter the Resistance citadel through the pumping tunnels. Disable its outer defenses, recover the command ledger, and protect the archive terminal until its evidence is transmitted. The war will be decided by what survives this operation.',
      debrief: 'The uncensored ledger reaches Dominion command and the districts above. Your squad secures the citadel, but the evidence makes a negotiated peace possible.', reward: 'dominion_peacemaker_crest',
      objectives: [objective('reach', 'entry', 'Enter the hidden citadel'), objective('sabotage', 'security', 'Disable the citadel defense relay', 4, { disableAlarm: true }), objective('intel', 'vault', 'Recover the uncensored command ledger', 4), objective('defend', 'defend', 'Protect the final evidence transmission', 40, { counterattack: true }), objective('extract', 'extract', 'Withdraw with the recovered evidence', 4)] }
  ].map(m => Object.freeze({ ...m, guardCount: 4 + m.chapter, guardTypes: ['rifler', 'skirmisher', ...(m.chapter > 1 ? ['marksman'] : []), ...(m.chapter > 2 ? ['heavy'] : []), ...(m.chapter > 3 ? ['rusher'] : [])], objectives: m.objectives.map((o, i) => Object.freeze({ ...o, id: m.id + '_' + i })), optional: Object.freeze({ id: 'silent_relay', type: 'sabotage', anchor: 'security', title: 'Optional: silence the alarm relay', duration: 3, radius: 3.8 }), thumbnail: 'assets/campaign/missions/' + m.id + '.svg' }));
  const byId = new Map(MISSIONS.map(m => [m.id, m]));
  const mission = id => byId.get(id) || null;
  const catalog = faction => MISSIONS.filter(m => !faction || m.faction === faction).map(clone);
  const currentObjective = run => mission(run?.missionId)?.objectives[run.index] || null;
  const objectivePoint = (run, anchors) => { const o = currentObjective(run); return o ? anchors?.[o.anchor] || null : null; };
  function createRun(id, options = {}) {
    const m = mission(id); if (!m) return null;
    const difficulty = difficulties[options.difficulty] ? options.difficulty : 'normal';
    const run = { version: VERSION, missionId: id, faction: m.faction, mapId: m.mapId, difficulty, index: 0, elapsed: 0, progress: 0, status: 'active', completed: [], optionalDone: [], optionalProgress: 0,
      alarm: { enabled: true, level: 0, heat: 0, lastEvidence: -100, lastKnown: null }, stats: { kills: 0, alarms: 0, detections: 0, damage: 0 }, guardCount: m.guardCount + difficulties[difficulty].guards - 7, reinforcementTimer: 20, reinforcementsPending: 0, counterattackPending: 0, wave: 0, escort: { rescued: false, delivered: false }, checkpointData: null, checkpointRevision: 0, eventSerial: 0, events: [] };
    if (options.checkpoint) return restoreCheckpoint(run, options.checkpoint) || run;
    return run;
  }
  function emit(run, type, details = {}) {
    const event = { type, time: run.elapsed, serial: ++run.eventSerial, ...details }; run.events.push(event);
    if (run.events.length > 24) run.events.shift(); return event;
  }
  function raiseAlarm(run, evidence = {}) {
    if (!run || run.status !== 'active' || !run.alarm.enabled || run.elapsed - run.alarm.lastEvidence < .85) return false;
    const was = run.alarm.level, point = evidence.position || evidence;
    run.alarm.heat = clamp(run.alarm.heat + (evidence.source === 'explosion' || evidence.kind === 'explosion' ? .9 : .55), 0, 3);
    run.alarm.level = Math.ceil(run.alarm.heat); run.alarm.lastEvidence = run.elapsed;
    if (Number.isFinite(point.x + point.y + point.z)) run.alarm.lastKnown = { x: point.x, y: point.y, z: point.z, time: run.elapsed, uncertainty: finite(evidence.uncertainty), source: evidence.kind || evidence.source || 'sight' };
    run.stats.detections++;
    if (was === 0) { run.stats.alarms++; emit(run, 'alarm', { level: run.alarm.level }); }
    else if (was !== run.alarm.level) emit(run, 'escalation', { level: run.alarm.level });
    return true;
  }
  function disableAlarm(run) {
    run.alarm.enabled = false; run.alarm.level = 0; run.alarm.heat = 0; run.reinforcementsPending = 0;
    if (!run.optionalDone.includes('silent_relay')) run.optionalDone.push('silent_relay');
    emit(run, 'alarm-disabled');
  }
  function checkpoint(run, player) {
    if (!run || !mission(run.missionId) || run.status !== 'active') return null;
    const data = clone({ ...run, checkpointData: null, events: [] });
    data.player = player && Number.isFinite(player.x + player.y + player.z) ? { x: player.x, y: player.y, z: player.z, yaw: finite(player.yaw), hp: Math.max(35, finite(player.hp, 100)), armor: Math.max(0, finite(player.armor)), weapon: typeof player.weapon === 'string' ? player.weapon.slice(0, 40) : null } : null;
    run.checkpointRevision++; data.checkpointRevision = run.checkpointRevision; run.checkpointData = data;
    return clone(data);
  }
  function sanitizeCheckpoint(raw, id) {
    const m = mission(id);
    if (!m || !raw || raw.missionId !== id || !Number.isInteger(raw.index) || raw.index < 0 || raw.index >= m.objectives.length || raw.status !== 'active') return null;
    const run = createRun(id, { difficulty: raw.difficulty });
    run.index = raw.index; run.elapsed = clamp(finite(raw.elapsed), 0, 1e6); run.completed = m.objectives.slice(0, raw.index).map(o => o.id);
    run.optionalDone = raw.optionalDone?.includes('silent_relay') ? ['silent_relay'] : [];
    if (run.optionalDone.length) { run.alarm.enabled = false; }
    run.escort = { rescued: !!raw.escort?.rescued, delivered: !!raw.escort?.delivered };
    for (const key of Object.keys(run.stats)) run.stats[key] = clamp(finite(raw.stats?.[key]), 0, 1e7);
    run.checkpointRevision = clamp(Math.floor(finite(raw.checkpointRevision)), 0, 10000);
    run.wave = clamp(Math.floor(finite(raw.wave)), 0, 10000);
    if (currentObjective(run).counterattack) run.counterattackPending = difficulties[run.difficulty].reinforcements + m.chapter;
    if (raw.player && Number.isFinite(raw.player.x + raw.player.y + raw.player.z) && Math.max(Math.abs(raw.player.x), Math.abs(raw.player.y), Math.abs(raw.player.z)) < 2000) run.player = { x: raw.player.x, y: raw.player.y, z: raw.player.z, yaw: finite(raw.player.yaw), hp: clamp(finite(raw.player.hp, 100), 35, 100), armor: clamp(finite(raw.player.armor), 0, 100), weapon: typeof raw.player.weapon === 'string' ? raw.player.weapon.slice(0, 40) : null };
    return run;
  }
  function restoreCheckpoint(run, data = run?.checkpointData) {
    const restored = sanitizeCheckpoint(data, run?.missionId); if (!restored) return null;
    const saved = clone(restored); restored.checkpointData = saved;
    // Reset active detection on a retry; the host reconstructs guards from the stage.
    return restored;
  }
  function consumeReinforcements(run) { const count = (run?.reinforcementsPending || 0) + (run?.counterattackPending || 0); if (run) { run.reinforcementsPending = 0; run.counterattackPending = 0; } return count; }
  function advance(run, player) {
    const previous = currentObjective(run); run.completed.push(previous.id); run.index++; run.progress = 0;
    emit(run, 'objective-complete', { objectiveId: previous.id, title: previous.title });
    if (previous.type === 'rescue') { run.escort.rescued = true; emit(run, 'rescue', { anchor: previous.anchor }); }
    if (previous.type === 'escort') run.escort.delivered = true;
    if (previous.disableAlarm) disableAlarm(run);
    if (!currentObjective(run)) { run.status = 'complete'; emit(run, 'mission-complete', { reward: mission(run.missionId).reward }); return; }
    if (currentObjective(run).counterattack) { run.counterattackPending += difficulties[run.difficulty].reinforcements + mission(run.missionId).chapter; emit(run, 'counterattack'); }
    checkpoint(run, player);
  }
  function step(run, dt, frame = {}) {
    if (!run || run.status !== 'active') return { events: [], advanced: false, completed: run?.status === 'complete', failed: run?.status === 'failed' };
    dt = clamp(finite(dt), 0, .25); const before = run.index, sequence = run.eventSerial, previousCheckpoint = run.checkpointRevision;
    run.elapsed += dt;
    if (frame.player?.dead || frame.player?.hp <= 0 || run.escort.rescued && frame.escort && (frame.escort.dead || frame.escort.hp <= 0)) {
      run.status = 'failed'; emit(run, 'mission-failed', { reason: frame.player?.dead || frame.player?.hp <= 0 ? 'Your squad leader was lost.' : 'The rescued soldier was lost.' });
    }
    if (run.status === 'active') {
      run.stats.kills += Math.max(0, Math.floor(finite(frame.kills))); run.stats.damage += Math.max(0, finite(frame.damage));
      if (frame.detected) raiseAlarm(run, frame.evidence || {});
      if (run.elapsed - run.alarm.lastEvidence > 12 && run.alarm.heat > 0) { run.alarm.heat = Math.max(0, run.alarm.heat - dt * .07); run.alarm.level = Math.ceil(run.alarm.heat); }
      if (run.alarm.level > 0 && run.alarm.enabled) {
        run.reinforcementTimer -= dt;
        if (run.reinforcementTimer <= 0) { run.wave++; run.reinforcementsPending = Math.min(12, run.reinforcementsPending + difficulties[run.difficulty].reinforcements + run.alarm.level - 1); run.reinforcementTimer = Math.max(12, 24 - mission(run.missionId).chapter - run.alarm.level * 2); emit(run, 'reinforcements', { count: run.reinforcementsPending }); }
      }
      const o = currentObjective(run), point = frame.objectivePosition || frame.anchors?.[o.anchor], player = frame.player;
      const near = player && point && distance(player, point) <= o.radius && Math.abs(player.y - point.y) < 3;
      const duration = o.duration * (o.type === 'defend' ? difficulties[run.difficulty].defend : 1);
      let eligible = false;
      if (o.type === 'reach') eligible = near;
      else if (o.type === 'defend') eligible = near && (frame.enemyCount || 0) === 0;
      else if (o.type === 'escort') eligible = near && run.escort.rescued && frame.escort && !frame.escort.dead && distance(frame.escort, point) <= o.radius && Math.abs(frame.escort.y - point.y) < 3;
      else if (o.type === 'extract') eligible = near && (frame.enemyCount || 0) === 0 && (!o.requiresEscort || run.escort.rescued && frame.escort && !frame.escort.dead && distance(frame.escort, point) <= o.radius + 2);
      else eligible = near && !!frame.interact && frame.canInteract !== false;
      if (eligible) { run.progress += dt; if (run.progress + 1e-8 >= duration) advance(run, player); }
      else if (o.type !== 'defend') run.progress = Math.max(0, run.progress - dt * 1.5);
      if (run.status === 'active' && run.alarm.enabled && frame.optionalInteract && frame.anchors?.security && player && distance(player, frame.anchors.security) < 3.8 && Math.abs(player.y - frame.anchors.security.y) < 3 && frame.canOptionalInteract !== false) {
        run.optionalProgress += dt; if (run.optionalProgress >= 3) disableAlarm(run);
      } else run.optionalProgress = Math.max(0, run.optionalProgress - dt);
    }
    const events = run.events.filter(event => event.serial > sequence);
    return { events, advanced: run.index !== before, completed: run.status === 'complete', failed: run.status === 'failed', needsCheckpoint: run.checkpointRevision !== previousCheckpoint, objective: currentObjective(run), progress: run.progress, alarm: run.alarm.level };
  }
  function freshProgress() { return { version: VERSION, updatedAt: 0, completed: {}, rewards: [], checkpoints: {}, selectedFaction: 'mice', selectedMission: 'mice_broken_convoy' }; }
  function sanitizeProgress(raw) {
    const value = freshProgress(); if (!raw || typeof raw !== 'object') return value;
    value.updatedAt = Math.max(0, finite(raw.updatedAt)); value.selectedFaction = raw.selectedFaction === 'cats' ? 'cats' : 'mice';
    if (mission(raw.selectedMission)) value.selectedMission = raw.selectedMission;
    else value.selectedMission = catalog(value.selectedFaction)[0].id;
    for (const m of MISSIONS) {
      const record = raw.completed?.[m.id];
      if (record && typeof record === 'object' && record.stars > 0) value.completed[m.id] = { stars: clamp(Math.floor(finite(record.stars)), 1, 3), bestTime: clamp(finite(record.bestTime, 1e6), 0, 1e6), completions: clamp(Math.floor(finite(record.completions, 1)), 1, 1e6), difficulty: difficulties[record.difficulty] ? record.difficulty : 'normal' };
      const saved = sanitizeCheckpoint(raw.checkpoints?.[m.id], m.id); if (saved) value.checkpoints[m.id] = saved;
    }
    value.rewards = MISSIONS.filter(m => value.completed[m.id]).map(m => m.reward);
    return value;
  }
  function isUnlocked(raw, id) { const m = mission(id); if (!m) return false; const previous = MISSIONS.find(p => p.faction === m.faction && p.chapter === m.chapter - 1); return !previous || !!raw?.completed?.[previous.id]; }
  function saveCheckpointProgress(raw, run, player) {
    const value = sanitizeProgress(raw), saved = checkpoint(run, player); if (saved) value.checkpoints[run.missionId] = saved;
    value.selectedFaction = run.faction; value.selectedMission = run.missionId; return value;
  }
  function completeProgress(raw, run) {
    const value = sanitizeProgress(raw), m = mission(run?.missionId); if (!m || run.status !== 'complete') return value;
    const stars = 1 + (run.stats.alarms === 0 ? 1 : 0) + (run.optionalDone.includes('silent_relay') ? 1 : 0), previous = value.completed[m.id];
    value.completed[m.id] = { stars: Math.max(stars, previous?.stars || 0), bestTime: Math.min(run.elapsed, previous?.bestTime ?? Infinity), completions: (previous?.completions || 0) + 1, difficulty: run.difficulty };
    if (!value.rewards.includes(m.reward)) value.rewards.push(m.reward); delete value.checkpoints[m.id];
    value.selectedFaction = m.faction; value.selectedMission = MISSIONS.find(next => next.faction === m.faction && next.chapter === m.chapter + 1)?.id || m.id;
    return value;
  }
  function mergeProgress(left, right) {
    const a = sanitizeProgress(left), b = sanitizeProgress(right), value = sanitizeProgress(a.updatedAt >= b.updatedAt ? a : b);
    for (const m of MISSIONS) {
      const x = a.completed[m.id], y = b.completed[m.id];
      if (x || y) value.completed[m.id] = !x ? y : !y ? x : { ...x, stars: Math.max(x.stars, y.stars), bestTime: Math.min(x.bestTime, y.bestTime), completions: Math.max(x.completions, y.completions) };
      if (value.completed[m.id]) delete value.checkpoints[m.id];
    }
    value.rewards = MISSIONS.filter(m => value.completed[m.id]).map(m => m.reward); return value;
  }
  const api = Object.freeze({ VERSION, difficulties, catalog, mission, currentObjective, objectivePoint, createRun, step, raiseAlarm, disableAlarm, consumeReinforcements, checkpoint, restoreCheckpoint, sanitizeCheckpoint, freshProgress, sanitizeProgress, isUnlocked, saveCheckpointProgress, completeProgress, mergeProgress });
  root.NeonCampaign = api; if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
