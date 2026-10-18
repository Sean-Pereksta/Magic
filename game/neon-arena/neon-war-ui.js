(function (root) {
  'use strict';
  const $ = id => document.getElementById(id), key = 'neonBreachWarSelectionV1';
  const stylesheet = new URL('./neon-war-menu.css', document.currentScript?.src || document.baseURI).href;
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const presets = {
    skirmish: { budget: 8, base: 3, perFlag: 1 },
    balanced: { budget: 12, base: 4, perFlag: 1 },
    large: { budget: 16, base: 4, perFlag: 1 }
  };
  const weaponTips = {
    whisker: 'A versatile rifle for controlled bursts and medium-range fights.',
    clawbreaker: 'A close-range shotgun. Eight pellets spread out quickly at distance.',
    purr90: 'Fast fire and a generous magazine for mobile, close-range fights.',
    burrower: 'A quiet carbine. Its suppressor reduces how far guards hear shots.',
    squeakshot: 'Precise, deliberate fire. Stop moving before taking a long shot.',
    thunderpaw: 'Sustained fire with a large magazine. Plan around its slower reload.'
  };
  let saved = {};
  try { const value = JSON.parse(localStorage.getItem(key) || '{}'); if (value && typeof value === 'object') saved = value; } catch {}
  let profile = () => root.NeonSurvivalProgress?.store?.getCampaign?.() || root.NeonCampaign?.freshProgress();
  const factionChoices = {};
  let previousFaction;

  function selection() {
    return {
      faction: $('warFaction')?.value || 'mice', character: $('warCharacter')?.value || 'mouse_rifleman',
      weapon: $('warWeapon')?.value || 'burrower', mission: $('warMission')?.value,
      rule: $('warVictory')?.value || 'tickets', budget: Number($('warBudget')?.value || 16),
      base: Number($('warBase')?.value || 4), perFlag: Number($('warPerFlag')?.value ?? 1),
      resume: !!$('warResume')?.checked,
      livery: !!$('warLivery')?.checked && Object.keys(profile()?.completed || {}).length > 0
    };
  }

  function setOption(id, value) {
    const select = $(id);
    if ([...select.options].some(o => o.value === String(value) && !o.disabled)) select.value = String(value);
  }

  function characters(wanted) {
    const faction = $('warFaction').value;
    const previous = wanted || $('warCharacter').value || saved.character;
    $('warCharacter').innerHTML = root.NeonFactions.characters.filter(c => c.faction === faction)
      .map(c => '<option value="' + c.id + '">' + escape(c.name) + '</option>').join('');
    setOption('warCharacter', previous);
  }

  function recommendedMission(list, progress) {
    const available = list.filter(m => root.NeonCampaign.isUnlocked(progress, m.id));
    return available.find(m => progress?.checkpoints?.[m.id] && !progress?.completed?.[m.id])
      || available.find(m => !progress?.completed?.[m.id]) || available.at(-1);
  }

  function missions(wanted) {
    if (!$('warMission')) return;
    const list = root.NeonCampaign?.catalog($('warFaction').value) || [], progress = profile();
    const previous = wanted || $('warMission').value || saved.mission || recommendedMission(list, progress)?.id;
    $('warMission').innerHTML = list.map((m, i) => {
      const unlocked = root.NeonCampaign.isUnlocked(progress, m.id), record = progress?.completed?.[m.id];
      return '<option value="' + m.id + '"' + (unlocked ? '' : ' disabled') + '>' + String(i + 1).padStart(2, '0')
        + ' · ' + escape(m.name) + (record ? ' · ' + '★'.repeat(record.stars) : unlocked ? '' : ' · Locked') + '</option>';
    }).join('');
    setOption('warMission', previous);
  }

  function persist() {
    const value = selection();
    factionChoices[value.faction] = { character: value.character, mission: value.mission };
    try { localStorage.setItem(key, JSON.stringify(value)); } catch {}
    document.dispatchEvent(new CustomEvent('neon-war-selection', { detail: value }));
  }

  function updateLoadout(value) {
    const character = root.NeonFactions.characters.find(c => c.id === value.character);
    const weapon = root.NeonFactions.weapons.find(w => w.id === value.weapon);
    if (character) {
      $('warPortrait').src = './assets/' + character.portrait;
      $('warPortrait').alt = character.name;
      $('warCharacterRole').textContent = character.role;
      $('warCharacterDescription').textContent = character.description;
    }
    if (weapon) {
      $('warWeaponPreview').src = './assets/' + weapon.preview;
      $('warWeaponPreview').alt = weapon.label || weapon.name;
      $('warWeaponType').textContent = weapon.type;
      $('warWeaponDescription').textContent = weaponTips[weapon.id] || '';
      $('warWeaponMagazine').textContent = weapon.mag;
      $('warWeaponReload').textContent = weapon.reload + 's';
      $('warWeaponNoise').textContent = weapon.suppressed ? 'Suppressed' : 'Unsuppressed';
    }
  }

  function updateCampaign(value, progress) {
    const list = root.NeonCampaign?.catalog(value.faction) || [], mission = root.NeonCampaign?.mission(value.mission);
    if (!mission) return;
    const completed = list.filter(m => progress?.completed?.[m.id]);
    const stars = completed.reduce((n, m) => n + progress.completed[m.id].stars, 0);
    $('warCampaignProgress').textContent = completed.length + ' / ' + list.length + ' missions complete · ' + stars + ' / ' + list.length * 3 + ' stars';
    $('warCampaignMeter').max = list.length;
    $('warCampaignMeter').value = completed.length;
    // Keep the focused chapter button in place during refreshes and profile updates.
    for (const button of $('warChapters').querySelectorAll('[data-chapter]')) {
      const item = list[Number(button.dataset.chapter)], unlocked = !!item && root.NeonCampaign.isUnlocked(progress, item.id);
      const record = item && progress?.completed?.[item.id];
      button.disabled = !unlocked;
      button.dataset.mission = item?.id || '';
      button.dataset.status = record ? 'complete' : unlocked ? 'ready' : 'locked';
      button.setAttribute('aria-pressed', String(item?.id === mission.id));
      button.setAttribute('aria-label', item ? 'Chapter ' + item.chapter + ': ' + item.name + (record ? ', ' + record.stars + ' stars' : unlocked ? ', available' : ', locked') : 'Unavailable chapter');
      button.title = item?.name || '';
      button.querySelector('small').textContent = record ? '★'.repeat(record.stars) : unlocked ? 'READY' : 'LOCKED';
    }
    const next = recommendedMission(list, progress);
    $('warContinue').disabled = !next || next.id === mission.id;
    $('warContinue').dataset.mission = next?.id || '';
    $('warContinue').textContent = next && !progress?.completed?.[next.id] ? 'Continue: ' + next.name : 'All missions complete';
    $('warContinue').hidden = !next || next.id === mission.id || !!progress?.completed?.[next.id];
    $('warMissionTitle').textContent = mission.name;
    $('warMissionStyle').textContent = mission.style;
    $('warMissionMap').textContent = root.NeonWarMaps?.get(mission.mapId)?.name || mission.mapId.replaceAll('_', ' ');
    $('warMissionPreview').src = './' + mission.thumbnail;
    $('warMissionPreview').alt = mission.name + ' objective route';
    $('warBriefing').textContent = mission.briefing || mission.description || '';
    const checkpoint = progress?.checkpoints?.[mission.id], record = progress?.completed?.[mission.id];
    const checkpointIndex = Math.max(0, Math.min(mission.objectives.length - 1, checkpoint?.index || 0));
    const resuming = !!checkpoint && value.resume;
    $('warCheckpointStatus').textContent = checkpoint
      ? (value.resume ? 'Resume at objective ' + (checkpointIndex + 1) + ': ' + mission.objectives[checkpointIndex].title : 'Fresh start selected. Your saved checkpoint remains available.')
      : record ? 'Replay available · best result ' + '★'.repeat(record.stars) + ' · ' + formatTime(record.bestTime) : 'Fresh operation · checkpoints save as objectives advance.';
    $('warCheckpointStatus').dataset.saved = String(!!checkpoint);
    $('warResume').disabled = !checkpoint;
    $('warResumeLabel').textContent = checkpoint ? 'Resume saved checkpoint' : 'Resume checkpoints when available';
    $('warObjectives').innerHTML = mission.objectives.map((o, i) => '<li' + (resuming && i < checkpointIndex ? ' class="complete"' : '')
      + '><span>' + (resuming && i < checkpointIndex ? '✓' : String(i + 1).padStart(2, '0')) + '</span>' + escape(o.title) + '</li>').join('');
    $('warObjectiveCount').textContent = mission.objectives.length + ' objectives · view route';
    $('warOptionalObjective').textContent = mission.optional?.title || '';
    $('warOptionalObjective').hidden = !mission.optional;
  }

  function formatTime(seconds) {
    if (!Number.isFinite(seconds)) return 'time unrecorded';
    const total = Math.max(0, Math.round(seconds));
    return Math.floor(total / 60) + ':' + String(total % 60).padStart(2, '0');
  }

  function updateConquest(value) {
    const chosen = Object.keys(presets).find(id => Object.keys(presets[id]).every(k => value[k] === presets[id][k]));
    for (const button of $('warPresets').querySelectorAll('[data-preset]')) button.setAttribute('aria-pressed', String(button.dataset.preset === chosen));
    $('warPresetStatus').textContent = chosen ? 'Preset applied' : 'Custom roster';
    $('warRosterBase').textContent = value.base;
    $('warRosterFull').textContent = value.base + 5 * value.perFlag;
    $('warRosterBudget').textContent = value.budget;
    $('warRuleHelp').textContent = value.rule === 'score'
      ? 'Each held sector earns 1 point per second. First faction to 300 points wins.'
      : 'Each faction starts with 250 tickets. Casualties cost tickets; holding at least 3 of 5 sectors drains the enemy’s reserve.';
    $('warRosterHelp').textContent = value.perFlag
      ? 'Each held flag adds ' + value.perFlag + (value.perFlag === 1 ? ' place' : ' places') + ' to that team’s roster. Human players count toward the team roster; the bot limit is shared by both factions.'
      : 'Team rosters stay fixed as flags change hands. Human players count toward the team roster; the bot limit is shared by both factions.';
  }

  function refresh() {
    if (!$('warSetup')) return;
    const mode = $('mode').value, war = ['tdm', 'conquest', 'campaign'].includes(mode), campaign = mode === 'campaign';
    const value = selection(), progress = profile();
    $('warSetup').hidden = !war;
    $('warSetup').dataset.faction = value.faction;
    $('campaignSetup').hidden = !campaign;
    $('conquestSetup').hidden = mode !== 'conquest';
    $('characterLocker').hidden = war;
    $('warModeTag').textContent = campaign ? 'SOLO CAMPAIGN' : mode === 'conquest' ? 'TERRITORY WAR' : 'TEAM DEATHMATCH';
    for (const button of $('warFactionCards').querySelectorAll('[data-faction]')) button.setAttribute('aria-pressed', String(button.dataset.faction === value.faction));
    $('warStory').textContent = value.faction === 'cats'
      ? 'Fight for the Cat Dominion. Armored formations, contested territory, and orders from a distant command.'
      : 'Join the Mouse Resistance. Infiltrate occupied districts and fight for a path home.';
    updateLoadout(value);
    if (campaign) {
      updateCampaign(value, progress);
      const mission = root.NeonCampaign.mission(value.mission);
      if (mission) $('map').value = mission.mapId;
    }
    if (mode === 'conquest') updateConquest(value);
    for (const card of $('arenaCards').querySelectorAll('[data-map]')) card.hidden = (mode === 'conquest' && !root.NeonWarMaps.get(card.dataset.map)) || (campaign && card.dataset.map !== $('map').value);
    $('secondPlayer').disabled = campaign;
    if (campaign) $('secondPlayer').value = 'off';
    const earned = Object.keys(progress?.completed || {}).length;
    $('warLivery').disabled = !earned;
    $('warMedals').textContent = earned ? earned + (earned === 1 ? ' mission medal earned' : ' mission medals earned') + ' · weapon finish unlocked' : 'Complete your first mission to unlock the faction weapon finish.';
  }

  function changed(event) {
    if (event.target.id === 'warFaction') {
      if (previousFaction) factionChoices[previousFaction] = { character: $('warCharacter').value, mission: $('warMission').value };
      const remembered = factionChoices[$('warFaction').value];
      characters(remembered?.character);
      missions(remembered?.mission || recommendedMission(root.NeonCampaign.catalog($('warFaction').value), profile())?.id);
      previousFaction = $('warFaction').value;
    }
    refresh();
    persist();
    $('map').dispatchEvent(new Event('change', { bubbles: true }));
  }

  function choose(id, value) {
    setOption(id, value);
    $(id).dispatchEvent(new Event('change', { bubbles: true }));
  }

  function mount() {
    if ($('warSetup')) return;
    if (!$('neonWarMenuStyles')) {
      const link = document.createElement('link');
      link.id = 'neonWarMenuStyles'; link.rel = 'stylesheet'; link.href = stylesheet;
      document.head.appendChild(link);
    }
    const section = document.createElement('section');
    section.id = 'warSetup'; section.className = 'warSetup'; section.hidden = true;
    section.setAttribute('aria-labelledby', 'warSetupHeading');
    section.innerHTML = `
      <div class="warHeading"><div><span class="warEyebrow">CATS × MICE</span><h2 id="warSetupHeading">Choose your side.</h2></div><span id="warModeTag" class="warTag"></span></div>
      <div id="warFactionCards" class="warFactionCards" role="group" aria-label="Choose your faction">
        <button type="button" data-faction="mice" aria-pressed="false"><img src="./assets/factions/mice/icon.svg" alt=""><span><strong>Mouse Resistance</strong><small>INGENUITY · FREEDOM</small></span><b aria-hidden="true">✓</b></button>
        <button type="button" data-faction="cats" aria-pressed="false"><img src="./assets/factions/cats/icon.svg" alt=""><span><strong>Cat Dominion</strong><small>DISCIPLINE · ORDER</small></span><b aria-hidden="true">✓</b></button>
      </div>
      <label for="warFaction" class="warSrOnly">Faction</label><select id="warFaction" hidden aria-hidden="true" tabindex="-1"><option value="mice">Mouse Resistance</option><option value="cats">Cat Dominion</option></select>
      <p id="warStory"></p>
      <div class="warLoadout">
        <article class="warOperatorCard"><div class="warPortraitFrame"><img id="warPortrait" alt="Selected tactical soldier"><span id="warCharacterRole" class="warTag"></span></div><div class="warOperatorInfo"><label for="warCharacter">Your soldier</label><select id="warCharacter"></select><p id="warCharacterDescription"></p><small class="warMuted">Appearance only · choose your weapon separately</small></div></article>
        <article class="warWeaponCard"><div class="warWeaponArt"><img id="warWeaponPreview" alt="Selected weapon"><span id="warWeaponType"></span></div><label for="warWeapon">Starting weapon</label><select id="warWeapon"></select><p id="warWeaponDescription"></p><dl class="warWeaponStats"><div><dt>Magazine</dt><dd id="warWeaponMagazine"></dd></div><div><dt>Reload</dt><dd id="warWeaponReload"></dd></div><div><dt>Gunfire</dt><dd id="warWeaponNoise"></dd></div></dl></article>
      </div>
      <div id="campaignSetup" class="warCampaign">
        <div class="warSubheading"><h3>Your campaign</h3><span id="warCampaignProgress"></span></div>
        <progress id="warCampaignMeter" max="5" value="0" aria-label="Campaign missions completed"></progress>
        <div id="warChapters" class="warChapters" role="group" aria-label="Campaign chapters">${[1, 2, 3, 4, 5].map((n, i) => '<button type="button" data-chapter="' + i + '" aria-pressed="false"><strong>0' + n + '</strong><small>LOCKED</small></button>').join('')}</div>
        <button id="warContinue" type="button" class="warTextButton" hidden></button>
        <label for="warMission">Operation</label><select id="warMission"></select>
        <div class="warMissionBrief"><div class="warMissionArt"><img id="warMissionPreview" alt="Selected mission objective route"><span id="warMissionMap"></span></div><div><span id="warMissionStyle" class="warEyebrow"></span><h3 id="warMissionTitle"></h3><p id="warBriefing"></p></div></div>
        <div class="warCheckpoint"><p id="warCheckpointStatus" role="status"></p><label class="warCheck"><input id="warResume" type="checkbox" checked><span id="warResumeLabel">Resume checkpoints when available</span></label></div>
        <details class="warRoute"><summary id="warObjectiveCount">View objective route</summary><ol id="warObjectives"></ol><p id="warOptionalObjective"></p><p class="warMuted">Earn stars by completing the mission, avoiding alarms, and silencing the alarm relay.</p></details>
        <details class="warControls"><summary>Field controls & touch safety</summary><div><p><kbd>F</kbd> / Pickup: interact near objectives. <kbd>C</kbd>: crouch. <kbd>G</kbd>: aim down sights.</p><p>Touch: hold Slide to crouch; hold Reload to aim. Your campaign weapon starts <strong>SAFE</strong> — tap the safety control to enable auto-fire. Keep rescued soldiers close until extraction.</p></div></details>
      </div>
      <div id="conquestSetup" class="warConquestOptions">
        <div class="warSubheading"><h3>Shape the battle</h3><span id="warPresetStatus"></span></div>
        <div id="warPresets" class="warPresets" role="group" aria-label="Conquest roster presets"><button type="button" data-preset="skirmish" aria-pressed="false"><strong>Skirmish</strong><small>8 bots · lighter load</small></button><button type="button" data-preset="balanced" aria-pressed="false"><strong>Balanced</strong><small>12 bots · steady front</small></button><button type="button" data-preset="large" aria-pressed="false"><strong>Full battle</strong><small>16 bots · more reserves</small></button></div>
        <div class="warConquestFields"><label for="warVictory">Victory rule<select id="warVictory"><option value="tickets">250 reinforcement tickets</option><option value="score">300 territory score</option></select></label><label for="warBudget">Shared active bot limit<select id="warBudget"><option value="8">8 · mobile</option><option value="12">12 · balanced</option><option value="16" selected>16 · large battle</option></select></label><label for="warBase">Starting soldiers per team<select id="warBase"><option>3</option><option selected>4</option><option>5</option><option>6</option></select></label><label for="warPerFlag">Extra roster places per flag<select id="warPerFlag"><option>0</option><option selected>1</option><option>2</option></select></label></div>
        <p id="warRuleHelp" class="warHelp"></p>
        <div class="warRoster"><div><strong id="warRosterBase"></strong><small>per team at HQ</small></div><span aria-hidden="true">→</span><div><strong id="warRosterFull"></strong><small>with all 5 flags</small></div><div><strong id="warRosterBudget"></strong><small>active bots maximum</small></div></div>
        <p id="warRosterHelp" class="warHelp"></p><p class="warConquestNote">Capture connected sectors to open the next attack. Reinforcements arrive every <strong>15 seconds</strong>. Lost flags stop replacements; surviving troops stay in battle.</p>
      </div>
      <div class="warRewards"><label class="warCheck"><input id="warLivery" type="checkbox"><span>Equip faction weapon finish <small>Cosmetic reward</small></span></label><p id="warMedals" class="warHelp"></p></div>`;
    $('advancedOptions').before(section);
    $('warWeapon').innerHTML = root.NeonFactions.weapons.map(w => '<option value="' + w.id + '">' + escape(w.name) + '</option>').join('');
    for (const [id, value] of [['warFaction', saved.faction || 'mice'], ['warWeapon', saved.weapon || 'burrower'], ['warVictory', saved.rule || 'tickets'], ['warBudget', saved.budget || 16], ['warBase', saved.base || 4], ['warPerFlag', saved.perFlag ?? 1]]) setOption(id, value);
    $('warLivery').checked = !!saved.livery;
    $('warResume').checked = saved.resume !== false;
    characters(); missions(); previousFaction = $('warFaction').value;
    section.addEventListener('change', changed);
    section.addEventListener('click', event => {
      const faction = event.target.closest('[data-faction]');
      if (faction && faction !== section) { choose('warFaction', faction.dataset.faction); return; }
      const mission = event.target.closest('[data-mission]');
      if (mission && !mission.disabled) { choose('warMission', mission.dataset.mission); return; }
      const preset = event.target.closest('[data-preset]');
      if (preset && presets[preset.dataset.preset]) {
        const value = presets[preset.dataset.preset];
        setOption('warBudget', value.budget); setOption('warBase', value.base); setOption('warPerFlag', value.perFlag);
        $('warBudget').dispatchEvent(new Event('change', { bubbles: true }));
      }
    });
    refresh();
  }

  function refreshProgress() { missions(); refresh(); }
  root.NeonWarUI = { mount, selection, refresh, refreshProgress, setProfile: fn => { profile = fn; refreshProgress(); } };
})(window);
