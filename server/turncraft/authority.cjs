'use strict';
const { randomUUID, randomBytes } = require('node:crypto');
const D = require('./domain.cjs');
const { createSimulator, validateCommand } = require('./simulator.cjs');
const { createRankedProcessor, pairId } = require('./ranked.cjs');
const safeName = (value) =>
  String(value)
    .replace(/[^a-zA-Z0-9_@.-]/g, '_')
    .slice(0, 100) || 'player';
function createAuthority({
  db,
  now = Date.now,
  simulatorFactory = createSimulator
}) {
  let healthy = null;
  function available() {
    if (healthy !== null) return healthy;
    try {
      simulatorFactory({ clock: now(), seed: 1 });
      healthy = true;
    } catch {
      healthy = false;
    }
    return healthy;
  }
  const processor = createRankedProcessor({
    db,
    now,
    verifyEvidence: async ({ matchId, session }) => {
      const engine = simulatorFactory({ checkpoint: session.checkpoint }),
        state = engine.snapshot();
      return {
        verified:
          session.terminal === true &&
          state.gameOver === true &&
          state.matchResult?.finishedAt === session.finishedAt,
        matchId,
        revision: session.revision,
        outcome:
          state.matchResult?.winnerIdx === null
            ? 'draw'
            : `p${state.matchResult?.winnerIdx}`,
        finishedAt: session.finishedAt,
        source:
          session.finishCause === 'forfeit'
            ? 'authenticated-forfeit'
            : 'authoritative-engine'
      };
    }
  });
  async function rankedIdentity(who) {
    D.fail(
      available(),
      'RANKED_UNAVAILABLE',
      'The authoritative TurnCraft engine is unavailable.',
      503
    );
    D.fail(
      who.linked && who.username,
      'ACCOUNT_NOT_LINKED',
      'Ranked play requires a verified CatnMice account linked to your Firebase sign-in.',
      403
    );
  }
  async function queue(who, input = {}) {
    D.exact(input, ['factionId']);
    await rankedIdentity(who);
    const factionId = input.factionId || 'frontier';
    D.fail(
      D.FACTIONS.includes(factionId),
      'INVALID_ARGUMENT',
      'Choose an existing faction.'
    );
    const profileRef = db.doc(`turncraftProfiles/${who.uid}`),
      profileSnap = await profileRef.get();
    D.fail(
      profileSnap.exists,
      'PROFILE_REQUIRED',
      'Open your TurnCraft profile first.',
      409
    );
    const profile = profileSnap.data();
    if (profile.activeRankedMatch) {
      const session = await db
        .doc(`turncraftMatches/${profile.activeRankedMatch}`)
        .get();
      if (session.exists && session.data().status !== 'complete')
        return {
          status: 'matched',
          lobbyId: session.data().lobbyId,
          matchId: session.id
        };
    }
    const stamp = now(),
      candidates = await db
        .collection('turncraftRankedQueue')
        .where('expiresAt', '>', stamp)
        .limit(100)
        .get(),
      eligible = candidates.docs
        .map((s) => ({ uid: s.id, ...s.data() }))
        .filter(
          (p) =>
            p.uid !== who.uid &&
            p.expiresAt > stamp &&
            Math.abs(p.mmr - profile.mmr) <=
              Math.min(800, 200 + Math.floor((stamp - p.joinedAt) / 1000) * 10)
        )
        .sort(
          (a, b) =>
            Math.abs(a.mmr - profile.mmr) - Math.abs(b.mmr - profile.mmr) ||
            a.joinedAt - b.joinedAt
        ),
      pairCounts = await Promise.all(
        eligible.map((p) =>
          db
            .doc(`turncraftRatingPairs/${pairId([p.uid, who.uid], stamp)}`)
            .get()
        )
      ),
      opponent = eligible.find(
        (p, index) => (pairCounts[index].data()?.count || 0) < 3
      );
    const queueRef = db.doc(`turncraftRankedQueue/${who.uid}`);
    if (!opponent) {
      return db.runTransaction(async (tx) => {
        const p = await tx.get(profileRef);
        if (p.data().activeRankedMatch)
          return {
            status: 'matched',
            lobbyId: p.data().activeRankedMatch,
            matchId: p.data().activeRankedMatch
          };
        const old = await tx.get(queueRef);
        tx.set(queueRef, {
          uid: who.uid,
          username: who.username,
          factionId,
          mmr: p.data().mmr,
          joinedAt: old.data()?.joinedAt || stamp,
          expiresAt: stamp + 60000
        });
        return {
          status: 'waiting',
          message: 'Searching for a similarly rated human commander…'
        };
      });
    }
    const id = randomUUID(),
      players = [
        { name: opponent.username, factionId: opponent.factionId },
        { name: who.username, factionId }
      ],
      engine = simulatorFactory({
        clock: stamp,
        seed: randomBytes(4).readUInt32LE()
      });
    engine.start({ players, mapId: 'classic', gameMode: 'annihilation' });
    const checkpoint = engine.save();
    return db.runTransaction(async (tx) => {
      const mine = await tx.get(profileRef),
        otherRef = db.doc(`turncraftProfiles/${opponent.uid}`),
        other = await tx.get(otherRef),
        candidateRef = db.doc(`turncraftRankedQueue/${opponent.uid}`),
        candidate = await tx.get(candidateRef);
      if (mine.data().activeRankedMatch)
        return {
          status: 'matched',
          lobbyId: mine.data().activeRankedMatch,
          matchId: mine.data().activeRankedMatch
        };
      if (
        !candidate.exists ||
        candidate.data().expiresAt <= stamp ||
        candidate.data().factionId !== opponent.factionId ||
        !other.exists ||
        other.data().activeRankedMatch
      ) {
        tx.set(queueRef, {
          uid: who.uid,
          username: who.username,
          factionId,
          mmr: mine.data().mmr,
          joinedAt: stamp,
          expiresAt: stamp + 60000
        });
        return {
          status: 'waiting',
          message:
            'An opponent was claimed by another match. Continuing the search…'
        };
      }
      const account = await tx.get(
          db.doc(`factionForgeAccounts/${opponent.uid}`)
        ),
        name = await tx.get(db.doc(`factionForgeNames/${opponent.username}`));
      D.fail(
        account.exists &&
          account.data().verified === true &&
          account.data().username === opponent.username &&
          name.exists &&
          name.data().uid === opponent.uid,
        'ACCOUNT_NOT_LINKED',
        'Your opponent’s account ownership could not be verified.',
        403
      );
      const pair = await tx.get(
        db.doc(`turncraftRatingPairs/${pairId([opponent.uid, who.uid], stamp)}`)
      );
      D.fail(
        (pair.data()?.count || 0) < 3,
        'PAIR_LIMIT',
        'You have played three rated matches against this opponent today. Try another commander tomorrow.',
        409
      );
      const uids = [opponent.uid, who.uid],
        members = Object.fromEntries(
          uids.map((uid, i) => [uid, players[i].name])
        ),
        lobby = {
          name: 'Ranked • Shattered Front',
          gameType: 'turncraft',
          turncraftVersion: 1,
          status: 'started',
          visibility: 'public',
          ranked: true,
          host: players[0].name,
          hostUid: uids[0],
          members,
          players: players.map((p) => p.name),
          turncraftSeats: uids,
          turncraftPlayers: Object.fromEntries(
            uids.map((uid, i) => [
              uid,
              {
                ...players[i],
                ready: true,
                team: `p${i}`,
                mmr: [other.data().mmr, mine.data().mmr][i],
                rankedMatches: [
                  other.data().rankedMatches,
                  mine.data().rankedMatches
                ][i]
              }
            ])
          ),
          settings: {
            mapId: 'classic',
            gameMode: 'annihilation',
            maxPlayers: 2,
            startingResources: 'standard'
          },
          matchId: id,
          startedAt: stamp,
          createdAt: stamp,
          updatedAt: stamp,
          chessStarfrontReady: Object.fromEntries(
            players.map((p) => [
              safeName(p.name),
              {
                username: p.name,
                factionId: p.factionId,
                objectiveVote: 'annihilation',
                mapVote: 'classic',
                ready: true,
                readyAtMs: stamp
              }
            ])
          )
        };
      const session = {
        id,
        lobbyId: id,
        status: 'active',
        ranked: true,
        visibility: 'public',
        mode: '1v1',
        hasBots: false,
        settingsVersion: 1,
        participants: uids,
        authenticatedUids: uids,
        players,
        revision: 0,
        checkpoint,
        startedAt: stamp,
        lastTickAt: stamp,
        lastSeen: Object.fromEntries(uids.map((uid) => [uid, stamp])),
        requests: {},
        terminal: false
      };
      tx.create(db.doc(`turncraftMatches/${id}`), session);
      tx.create(db.doc(`lobbies/${id}`), lobby);
      tx.update(profileRef, { activeRankedMatch: id });
      tx.update(otherRef, { activeRankedMatch: id });
      tx.delete(queueRef);
      tx.delete(candidateRef);
      return { status: 'matched', lobbyId: id, matchId: id };
    });
  }
  async function cancel(who) {
    return db.runTransaction(async (tx) => {
      const profile = await tx.get(db.doc(`turncraftProfiles/${who.uid}`));
      if (profile.data()?.activeRankedMatch)
        return {
          status: 'matched',
          lobbyId: profile.data().activeRankedMatch,
          matchId: profile.data().activeRankedMatch
        };
      tx.delete(db.doc(`turncraftRankedQueue/${who.uid}`));
      return { status: 'cancelled' };
    });
  }
  async function advance(
    who,
    id,
    { command, requestId, forfeit = false } = {}
  ) {
    D.id(id);
    if (command) {
      validateCommand(command);
      D.id(requestId);
    }
    const ref = db.doc(`turncraftMatches/${id}`),
      stamp = now();
    let session = await db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      D.fail(
        snap.exists,
        'NOT_FOUND',
        'This ranked match no longer exists.',
        404
      );
      const row = snap.data(),
        slot = row.participants.indexOf(who.uid);
      D.fail(
        slot >= 0,
        'NOT_A_MEMBER',
        'You are not a participant in this match.',
        403
      );
      if (row.status === 'complete' || row.terminal) return row;
      const engine = simulatorFactory({ checkpoint: row.checkpoint }),
        lastSeen = { ...row.lastSeen, [who.uid]: stamp };
      let cause = null;
      engine.advance(stamp);
      if (forfeit) {
        engine.finish(
          1 - slot,
          `${row.players[slot].name} forfeited the match.`,
          stamp
        );
        cause = 'forfeit';
      } else if (stamp - row.startedAt >= 120000) {
        const stale = row.participants.map(
          (uid) => stamp - (row.lastSeen[uid] || row.startedAt) > 120000
        );
        if (stale.every(Boolean)) {
          engine.finish(
            null,
            'Both commanders disconnected for more than two minutes.',
            stamp
          );
          cause = 'disconnect';
        } else if (stale[1 - slot]) {
          engine.finish(
            slot,
            `${row.players[1 - slot].name} disconnected for more than two minutes.`,
            stamp
          );
          cause = 'disconnect';
        }
      }
      if (!engine.snapshot().gameOver && stamp - row.startedAt >= 3600000) {
        engine.finish(
          null,
          'The one-hour ranked time limit has been reached.',
          stamp
        );
        cause = 'time-limit';
      }
      const requestKey = requestId ? `${who.uid}:${requestId}` : null,
        requests = { ...row.requests };
      if (command && !requests[requestKey] && !engine.snapshot().gameOver) {
        engine.command(slot, command);
        requests[requestKey] = stamp;
        for (const key of Object.keys(requests).slice(
          0,
          Math.max(0, Object.keys(requests).length - 512)
        ))
          delete requests[key];
      }
      const state = engine.snapshot(),
        next = {
          ...row,
          revision: row.revision + 1,
          checkpoint: engine.save(),
          lastTickAt: engine.clock,
          lastSeen,
          requests,
          terminal: state.gameOver,
          updatedAt: stamp,
          ...(state.gameOver
            ? {
                finishedAt: state.matchResult.finishedAt,
                finishCause: cause || 'combat'
              }
            : {})
        };
      tx.set(ref, next);
      return next;
    });
    if (session.terminal && !session.ratingReceipt) {
      await processor.recordTrustedResult(id);
      session = (await ref.get()).data();
    }
    const engine = simulatorFactory({ checkpoint: session.checkpoint });
    return {
      state: engine.publicSnapshot(session.participants.indexOf(who.uid)),
      match: {
        id,
        status: session.status,
        participants: session.participants,
        players: session.players,
        startedAt: session.startedAt,
        ranked: true
      },
      ratingReceipt: session.ratingReceipt || null
    };
  }
  return {
    available,
    queue,
    cancel,
    state: (who, id) => advance(who, id),
    command: async (who, id, input) => {
      D.exact(input, ['command', 'requestId']);
      return advance(who, id, input);
    },
    forfeit: (who, id) => advance(who, id, { forfeit: true })
  };
}
module.exports = { createAuthority };
