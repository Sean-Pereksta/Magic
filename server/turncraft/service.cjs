'use strict';
const { randomUUID, randomBytes, createHash } = require('node:crypto');
const D = require('./domain.cjs');
const { createAuthority } = require('./authority.cjs');
const clone = (value) => JSON.parse(JSON.stringify(value));
const safeName = (value) => D.text(value).replace(/[\u0000-\u001f]/g, '');
const seatKey = (value) =>
  String(value)
    .replace(/[^a-zA-Z0-9_@.-]/g, '_')
    .slice(0, 100) || 'player';
const REGIONS = Object.freeze([
  'amber-wastes',
  'ironwood-basin',
  'sundered-coast',
  'ashglass-expanse',
  'frozen-rift',
  'heart-of-fracture'
]);
const MISSIONS = REGIONS.flatMap((region) =>
  [1, 2, 3, 4].map((n) => `${region}-${n}`)
);
function createTurncraftService({ db, auth, now = Date.now }) {
  const authority = createAuthority({ db, now });
  const profileRef = (uid) => db.doc(`turncraftProfiles/${uid}`),
    lobbyRef = (id) => db.doc(`lobbies/${D.id(id)}`);
  const capabilities = () => ({
    custom: true,
    ranked: authority.available(),
    globalSurvival: false,
    reason: authority.available()
      ? 'Ranked 1v1 runs on the authoritative TurnCraft server. Survival records are personal until a survival adjudicator is deployed.'
      : 'The authoritative ranked engine is unavailable. Custom matches and personal cloud saves remain available.'
  });
  async function identity(token) {
    D.fail(
      typeof token === 'string' && token.length > 0,
      'UNAUTHENTICATED',
      'Sign in with your CatnMice browser identity.',
      401
    );
    const decoded = await auth.verifyIdToken(token, true);
    D.fail(decoded.uid, 'UNAUTHENTICATED', 'Sign in again.', 401);
    const account = await db.doc(`factionForgeAccounts/${decoded.uid}`).get();
    let username = null,
      linked = false;
    if (account.exists && account.data().verified === true) {
      const claimed = account.data().username;
      D.fail(
        typeof claimed === 'string' &&
          claimed.length <= 80 &&
          !claimed.includes('/'),
        'ACCOUNT_NOT_LINKED',
        'The account binding is invalid.',
        403
      );
      const name = await db.doc(`factionForgeNames/${claimed}`).get();
      if (name.exists && name.data().uid === decoded.uid) {
        username = claimed;
        linked = true;
      }
    }
    return {
      uid: decoded.uid,
      username,
      displayName: username || safeName(decoded.name) || 'Commander',
      avatar:
        typeof decoded.picture === 'string' &&
        decoded.picture.startsWith('https://')
          ? decoded.picture
          : null,
      linked
    };
  }
  async function profile(who) {
    return db.runTransaction(async (tx) => {
      const ref = profileRef(who.uid),
        snap = await tx.get(ref);
      if (snap.exists) {
        const existing = snap.data();
        if (who.linked && existing.username !== who.username) {
          existing.username = who.username;
          tx.set(ref, existing);
          tx.set(
            db.doc(`users/${who.username}`),
            { gameStats: { turncraft: existing } },
            { merge: true }
          );
        }
        return {
          identity: who,
          profile: existing,
          capabilities: capabilities()
        };
      }
      const created = D.newProfile(who, now());
      tx.create(ref, created);
      if (who.linked)
        tx.set(
          db.doc(`users/${who.username}`),
          { gameStats: { turncraft: created } },
          { merge: true }
        );
      return { identity: who, profile: created, capabilities: capabilities() };
    });
  }
  async function saveProgress(who, kind, input) {
    D.exact(
      input,
      kind === 'campaign'
        ? ['missionId', 'stars', 'optionalObjectives', 'regionId', 'difficulty']
        : [
            'arenaId',
            'wave',
            'durationSeconds',
            'enemiesDefeated',
            'factionId',
            'difficulty',
            'date'
          ]
    );
    return db.runTransaction(async (tx) => {
      const ref = profileRef(who.uid),
        snap = await tx.get(ref),
        p = snap.exists ? snap.data() : D.newProfile(who, now()),
        stamp = now();
      if (kind === 'campaign') {
        // A personal cloud save is not a verified competitive result. This API
        // validates shape/order and never grants rating or global records.
        const missionIndex = MISSIONS.indexOf(input.missionId);
        D.fail(
          missionIndex >= 0 &&
            input.regionId === REGIONS[Math.floor(missionIndex / 4)],
          'INVALID_ARGUMENT',
          'Choose a valid campaign mission.'
        );
        D.integer(input.stars, 1, 3);
        D.fail(
          missionIndex === 0 ||
            p.campaign.completedMissions[MISSIONS[missionIndex - 1]],
          'MISSION_LOCKED',
          'Complete the previous campaign mission first.',
          409
        );
        const old = p.campaign.completedMissions[input.missionId];
        D.fail(
          Object.keys(p.campaign.completedMissions).length < 24 || old,
          'INVALID_ARGUMENT',
          'This campaign save already contains every mission.'
        );
        const difficulty = input.difficulty || 'medium';
        D.fail(
          ['easy', 'medium', 'hard', 'pro'].includes(difficulty),
          'INVALID_ARGUMENT',
          'Choose an existing difficulty.'
        );
        const optional = Array.isArray(input.optionalObjectives)
          ? [
              ...new Set(
                input.optionalObjectives.filter(
                  (v) => typeof v === 'string' && v.length <= 60
                )
              )
            ].slice(0, 12)
          : [];
        p.campaign.completedMissions[input.missionId] = {
          stars: Math.max(input.stars, old?.stars || 0),
          ratingsByDifficulty: {
            ...(old?.ratingsByDifficulty || {}),
            [difficulty]: Math.max(
              input.stars,
              old?.ratingsByDifficulty?.[difficulty] || 0
            )
          },
          optionalObjectives: [
            ...new Set([...(old?.optionalObjectives || []), ...optional])
          ],
          completedAt: old?.completedAt || stamp
        };
        p.campaign.highestCompletedMission = Math.max(
          p.campaign.highestCompletedMission,
          missionIndex + 1
        );
        p.campaign.unlockedRegions = REGIONS.filter(
          (_, index) =>
            index === 0 || p.campaign.completedMissions[MISSIONS[index * 4 - 1]]
        );
        if ((missionIndex + 1) % 4 === 0)
          p.achievements[`region-${input.regionId}`] = {
            earnedAt: stamp,
            personal: true
          };
        if (p.campaign.highestCompletedMission === 24)
          p.achievements['shattered-front-complete'] = {
            earnedAt: stamp,
            personal: true
          };
      } else {
        D.fail(
          REGIONS.includes(input.arenaId) &&
            D.FACTIONS.includes(input.factionId) &&
            ['easy', 'medium', 'hard', 'pro'].includes(input.difficulty),
          'INVALID_ARGUMENT',
          'Choose a supported survival arena, faction and difficulty.'
        );
        D.integer(input.wave, 0, 100000);
        D.integer(input.durationSeconds, 0, 604800);
        D.integer(input.enemiesDefeated, 0, 10000000);
        const key = `${input.arenaId}_${input.difficulty}`,
          old = p.survival.recordsByMap[key],
          record = {
            arenaId: input.arenaId,
            wave: input.wave,
            durationSeconds: input.durationSeconds,
            enemiesDefeated: input.enemiesDefeated,
            factionId: input.factionId,
            difficulty: input.difficulty,
            date: new Date(stamp).toISOString(),
            personal: true,
            verified: false
          };
        D.fail(
          Object.keys(p.survival.recordsByMap).length < 24 || old,
          'INVALID_ARGUMENT',
          'Too many survival arenas in this save.'
        );
        if (
          !old ||
          record.wave > old.wave ||
          (record.wave === old.wave &&
            record.durationSeconds > old.durationSeconds)
        )
          p.survival.recordsByMap[key] = record;
        p.survival.highestWave = Math.max(p.survival.highestWave, input.wave);
        for (const milestone of [5, 10, 15, 20, 30, 50, 75, 100])
          if (
            input.wave >= milestone &&
            !p.achievements[`survival-${milestone}`]
          )
            p.achievements[`survival-${milestone}`] = {
              earnedAt: stamp,
              personal: true
            };
      }
      p.updatedAt = stamp;
      if (who.linked) p.username = who.username;
      tx.set(ref, p);
      if (who.linked)
        tx.set(
          db.doc(`users/${who.username}`),
          { gameStats: { turncraft: p } },
          { merge: true }
        );
      return { identity: who, profile: p, capabilities: capabilities() };
    });
  }
  async function createLobby(who, input) {
    D.exact(input, [
      'name',
      'visibility',
      'settings',
      'displayName',
      'factionId',
      'requestId'
    ]);
    const name = safeName(input.name) || 'Shattered Front',
      displayName =
        who.username || safeName(input.displayName) || who.displayName;
    const visibility = input.visibility || 'public';
    D.fail(
      ['public', 'private'].includes(visibility),
      'INVALID_ARGUMENT',
      'Choose public or private.'
    );
    const settings = D.settings(input.settings || {}),
      factionId = input.factionId || 'frontier';
    D.fail(
      D.FACTIONS.includes(factionId),
      'INVALID_ARGUMENT',
      'Choose an existing faction.'
    );
    const requestId = input.requestId ? D.id(input.requestId) : randomUUID(),
      id = createHash('sha256')
        .update(who.uid + ':' + requestId)
        .digest('hex')
        .slice(0, 32),
      code = randomBytes(6).toString('hex').toUpperCase(),
      p = (await profile(who)).profile,
      stamp = now();
    const row = {
      name,
      gameType: 'turncraft',
      turncraftVersion: 1,
      status: 'waiting',
      visibility,
      host: displayName,
      hostUid: who.uid,
      members: { [who.uid]: displayName },
      players: [displayName],
      turncraftSeats: [who.uid],
      turncraftPlayers: {
        [who.uid]: {
          name: displayName,
          factionId,
          team: 'ffa',
          ready: false,
          mmr: p.mmr,
          rankedMatches: p.rankedMatches,
          avatar: who.avatar || null
        }
      },
      settings,
      ranked: false,
      createdAt: stamp,
      updatedAt: stamp
    };
    return db.runTransaction(async (tx) => {
      const existing = await tx.get(lobbyRef(id));
      if (existing.exists) {
        D.fail(
          existing.data().hostUid === who.uid,
          'NOT_A_MEMBER',
          'This room belongs to another commander.',
          403
        );
        const secret = await tx.get(db.doc(`turncraftLobbySecrets/${id}`));
        return { id, ...existing.data(), ...(secret.data() || {}) };
      }
      tx.create(lobbyRef(id), row);
      tx.create(db.doc(`turncraftInvites/${code}`), {
        lobbyId: id,
        createdAt: stamp,
        expiresAt: stamp + 86400000
      });
      tx.create(db.doc(`turncraftLobbySecrets/${id}`), { inviteCode: code });
      return { id, ...row, inviteCode: code };
    });
  }
  async function getLobby(who, id) {
    const snap = await lobbyRef(id).get();
    D.fail(
      snap.exists && snap.data().gameType === 'turncraft',
      'NOT_FOUND',
      'This TurnCraft lobby has closed.',
      404
    );
    const row = snap.data();
    D.fail(
      row.visibility === 'public' || row.members?.[who.uid],
      'INVITE_REQUIRED',
      'Join this private lobby with its invite code.',
      403
    );
    const secret = row.members?.[who.uid]
      ? await db.doc(`turncraftLobbySecrets/${id}`).get()
      : null;
    return { id, ...row, ...(secret?.exists ? secret.data() : {}) };
  }
  async function browse(who) {
    const rows = await db
      .collection('lobbies')
      .where('gameType', '==', 'turncraft')
      .get();
    return {
      lobbies: rows.docs
        .map((s) => ({ id: s.id, ...s.data() }))
        .filter(
          (row) =>
            row.turncraftVersion === 1 &&
            row.visibility === 'public' &&
            row.status === 'waiting' &&
            row.players.length < row.settings.maxPlayers
        )
        .slice(0, 50),
      capabilities: capabilities()
    };
  }
  async function join(who, input) {
    D.exact(input, ['lobbyId', 'code', 'displayName']);
    let id = input.lobbyId,
      invited = false;
    if (input.code) {
      const code = D.text(input.code, 12).toUpperCase();
      D.fail(
        /^[A-F0-9]{12}$/.test(code),
        'INVALID_ARGUMENT',
        'Enter the 12-character invite code.'
      );
      const secret = await db.doc(`turncraftInvites/${code}`).get();
      D.fail(
        secret.exists && secret.data().expiresAt > now(),
        'NOT_FOUND',
        'This invite has expired.',
        404
      );
      id = secret.data().lobbyId;
      invited = true;
    }
    await db.runTransaction(async (tx) => {
      const ref = lobbyRef(id),
        snap = await tx.get(ref);
      D.fail(
        snap.exists &&
          snap.data().gameType === 'turncraft' &&
          snap.data().turncraftVersion === 1,
        'NOT_FOUND',
        'This lobby has closed.',
        404
      );
      const row = snap.data();
      if (row.members[who.uid]) return;
      D.fail(
        row.status === 'waiting',
        'MATCH_STARTED',
        'This match has already started.',
        409
      );
      D.fail(
        row.visibility === 'public' || invited,
        'INVITE_REQUIRED',
        'Use the private invite code.',
        403
      );
      D.fail(
        row.players.length < row.settings.maxPlayers,
        'LOBBY_FULL',
        'This lobby is full.',
        409
      );
      const displayName =
        who.username || safeName(input.displayName) || who.displayName;
      D.fail(
        !row.players.some((name) => seatKey(name) === seatKey(displayName)),
        'NAME_IN_USE',
        'Another commander in this room uses that display name. Change your Game Hub display name.',
        409
      );
      const pSnap = await tx.get(profileRef(who.uid)),
        p = pSnap.exists ? pSnap.data() : D.newProfile(who, now());
      tx.update(ref, {
        members: { ...row.members, [who.uid]: displayName },
        players: [...row.players, displayName],
        turncraftSeats: [...row.turncraftSeats, who.uid],
        turncraftPlayers: {
          ...row.turncraftPlayers,
          [who.uid]: {
            name: displayName,
            factionId: D.FACTIONS[row.players.length % D.FACTIONS.length],
            team: 'ffa',
            ready: false,
            mmr: p.mmr,
            rankedMatches: p.rankedMatches,
            avatar: who.avatar || null
          }
        },
        updatedAt: now()
      });
    });
    return getLobby(who, id);
  }
  async function mutate(who, id, action, input = {}) {
    D.exact(
      input,
      action === 'configure'
        ? ['settings', 'visibility', 'name']
        : action === 'player'
          ? ['factionId', 'ready']
          : []
    );
    return db.runTransaction(async (tx) => {
      const ref = lobbyRef(id),
        snap = await tx.get(ref);
      D.fail(
        snap.exists &&
          snap.data().gameType === 'turncraft' &&
          snap.data().turncraftVersion === 1,
        'NOT_FOUND',
        'This lobby has closed.',
        404
      );
      const row = clone(snap.data());
      D.fail(
        row.members[who.uid],
        'NOT_A_MEMBER',
        'Join this lobby first.',
        403
      );
      if (action === 'leave') {
        D.fail(
          ['waiting', 'finished'].includes(row.status),
          'MATCH_STARTED',
          'Return to the game to finish or leave your active match.',
          409
        );
        delete row.members[who.uid];
        delete row.turncraftPlayers[who.uid];
        row.turncraftSeats = row.turncraftSeats.filter(
          (uid) => uid !== who.uid
        );
        row.players = row.turncraftSeats.map((uid) => row.members[uid]);
        if (!row.players.length) {
          row.status = 'closed';
        } else if (row.hostUid === who.uid) {
          row.hostUid = row.turncraftSeats[0];
          row.host = row.members[row.hostUid];
        }
      } else {
        D.fail(
          row.status === 'waiting',
          'MATCH_STARTED',
          'Match settings are locked after launch.',
          409
        );
        if (action === 'player') {
          const member = row.turncraftPlayers[who.uid];
          if (input.factionId !== undefined) {
            D.fail(
              D.FACTIONS.includes(input.factionId),
              'INVALID_ARGUMENT',
              'Choose an existing faction.'
            );
            member.factionId = input.factionId;
            member.ready = false;
          }
          if (input.ready !== undefined) {
            D.fail(
              typeof input.ready === 'boolean',
              'INVALID_ARGUMENT',
              'Choose a ready state.'
            );
            member.ready = input.ready;
          }
        }
        if (action === 'configure') {
          D.fail(
            row.hostUid === who.uid,
            'HOST_ONLY',
            'Only the host can configure this lobby.',
            403
          );
          if (input.settings) row.settings = D.settings(input.settings);
          D.fail(
            row.settings.maxPlayers >= row.players.length,
            'INVALID_ARGUMENT',
            'The room already has more commanders than that setting.',
            409
          );
          if (input.visibility !== undefined) {
            D.fail(
              ['public', 'private'].includes(input.visibility),
              'INVALID_ARGUMENT',
              'Choose a lobby visibility.'
            );
            row.visibility = input.visibility;
          }
          if (input.name) row.name = safeName(input.name);
          for (const p of Object.values(row.turncraftPlayers)) p.ready = false;
        }
        if (action === 'start') {
          D.fail(
            row.hostUid === who.uid,
            'HOST_ONLY',
            'Only the host can start this match.',
            403
          );
          D.fail(
            row.players.length >= 2 &&
              row.players.length <= row.settings.maxPlayers &&
              row.turncraftSeats.every(
                (uid) => row.turncraftPlayers[uid].ready
              ),
            'NOT_READY',
            'At least two commanders must be present and every commander must be ready.',
            409
          );
          row.status = 'started';
          row.startedAt = now();
          row.matchId = id;
          row.chessStarfrontReady = Object.fromEntries(
            row.turncraftSeats.map((uid) => {
              const p = row.turncraftPlayers[uid];
              return [
                seatKey(p.name),
                {
                  username: p.name,
                  factionId: p.factionId,
                  objectiveVote: row.settings.gameMode,
                  mapVote: row.settings.mapId,
                  ready: true,
                  readyAtMs: row.startedAt
                }
              ];
            })
          );
        }
      }
      row.updatedAt = now();
      tx.set(ref, row);
      return { id, ...row };
    });
  }
  async function complete(who, id, input = {}) {
    D.exact(input, []);
    return db.runTransaction(async (tx) => {
      const ref = lobbyRef(id),
        snap = await tx.get(ref);
      D.fail(
        snap.exists &&
          snap.data().gameType === 'turncraft' &&
          snap.data().turncraftVersion === 1,
        'NOT_FOUND',
        'This lobby has closed.',
        404
      );
      const row = snap.data();
      D.fail(
        row.members[who.uid] && row.ranked === false,
        'NOT_A_MEMBER',
        'Only custom-match participants can finish this lobby.',
        403
      );
      if (row.status === 'finished') return { id, ...row };
      D.fail(
        row.status === 'started',
        'MATCH_NOT_STARTED',
        'Start this match before completing it.',
        409
      );
      D.fail(
        who.uid === row.hostUid ||
          who.uid === row.chessStarfrontController?.ownerUid,
        'HOST_ONLY',
        'The host or active match controller must confirm the final snapshot.',
        403
      );
      const state = await tx.get(db.doc(`lobbies/${id}/chess_starfront/state`)),
        value = state.data();
      D.fail(
        state.exists &&
          value.gameKey === 'chess_starfront' &&
          value.gameOver === true &&
          value.controllerUid === row.chessStarfrontController?.ownerUid &&
          value.controllerEpoch === row.chessStarfrontController?.epoch &&
          JSON.stringify(value.players?.map((p) => p.name)) ===
            JSON.stringify(row.players) &&
          value.matchResult &&
          (value.matchResult.winnerIdx === null ||
            (Number.isInteger(value.matchResult.winnerIdx) &&
              value.matchResult.winnerIdx >= 0 &&
              value.matchResult.winnerIdx < row.players.length)),
        'RESULT_NOT_READY',
        'The final shared battlefield snapshot has not arrived yet.',
        409
      );
      const patch = {
        status: 'finished',
        finishedAt: now(),
        turncraftResult: {
          winnerIdx: value.matchResult.winnerIdx,
          reason: D.text(value.matchResult.reason, 200),
          players: value.players.map((p) => ({
            name: p.name,
            factionId: p.factionId
          })),
          durationSeconds: Math.max(
            0,
            Math.floor(
              (Number(value.matchResult.finishedAt) -
                Number(value.matchStartedAt || row.startedAt)) /
                1000
            )
          )
        },
        updatedAt: now()
      };
      tx.update(ref, patch);
      return { id, ...row, ...patch };
    });
  }
  async function rematch(who, id, input = {}) {
    D.exact(input, []);
    const nextId = randomUUID(),
      code = randomBytes(6).toString('hex').toUpperCase();
    return db.runTransaction(async (tx) => {
      const ref = lobbyRef(id),
        snap = await tx.get(ref);
      D.fail(
        snap.exists &&
          snap.data().gameType === 'turncraft' &&
          snap.data().turncraftVersion === 1,
        'NOT_FOUND',
        'This lobby has closed.',
        404
      );
      const row = snap.data();
      D.fail(
        row.hostUid === who.uid,
        'HOST_ONLY',
        'Only the host can create a rematch.',
        403
      );
      D.fail(
        row.ranked === false && row.status === 'finished',
        'MATCH_NOT_FINISHED',
        'Finish this custom match before requesting a rematch.',
        409
      );
      if (row.nextLobbyId) {
        const existing = await tx.get(lobbyRef(row.nextLobbyId)),
          secret = await tx.get(
            db.doc(`turncraftLobbySecrets/${row.nextLobbyId}`)
          );
        D.fail(
          existing.exists,
          'NOT_FOUND',
          'The rematch room has closed.',
          404
        );
        return {
          id: row.nextLobbyId,
          ...existing.data(),
          ...(secret.data() || {})
        };
      }
      const fresh = {
        name: row.name,
        gameType: 'turncraft',
        turncraftVersion: 1,
        status: 'waiting',
        visibility: row.visibility,
        ranked: false,
        host: row.host,
        hostUid: row.hostUid,
        members: row.members,
        players: row.players,
        turncraftSeats: row.turncraftSeats,
        turncraftPlayers: Object.fromEntries(
          Object.entries(row.turncraftPlayers).map(([uid, p]) => [
            uid,
            { ...p, ready: false }
          ])
        ),
        settings: row.settings,
        rematchOf: id,
        createdAt: now(),
        updatedAt: now()
      };
      tx.create(lobbyRef(nextId), fresh);
      tx.create(db.doc(`turncraftLobbySecrets/${nextId}`), {
        inviteCode: code
      });
      tx.create(db.doc(`turncraftInvites/${code}`), {
        lobbyId: nextId,
        createdAt: now(),
        expiresAt: now() + 86400000
      });
      tx.update(ref, { nextLobbyId: nextId, updatedAt: now() });
      return { id: nextId, ...fresh, inviteCode: code };
    });
  }
  async function leaderboard() {
    const snap = await db
      .collection('turncraftRankings')
      .orderBy('mmr', 'desc')
      .limit(50)
      .get();
    return {
      players: snap.docs.map((s) => s.data()),
      capabilities: capabilities()
    };
  }
  return {
    identity,
    profile,
    capabilities,
    saveCampaign: (who, input) => saveProgress(who, 'campaign', input),
    saveSurvival: (who, input) => saveProgress(who, 'survival', input),
    createLobby,
    getLobby,
    browse,
    join,
    configure: (who, id, input) => mutate(who, id, 'configure', input),
    player: (who, id, input) => mutate(who, id, 'player', input),
    start: (who, id) => mutate(who, id, 'start'),
    leave: (who, id) => mutate(who, id, 'leave'),
    complete,
    rematch,
    leaderboard,
    authority
  };
}
module.exports = { createTurncraftService };
