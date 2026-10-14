'use strict';
const D = require('./domain.cjs');
const { createHash } = require('node:crypto');
const pairId = (uids, stamp) =>
  createHash('sha256')
    .update(JSON.stringify([...uids].sort()))
    .digest('hex') +
  '_' +
  new Date(stamp).toISOString().slice(0, 10);
// Internal API only. The browser never supplies an outcome or an MMR delta.
// A future authoritative TurnCraft runtime must supply verifyEvidence. Agreement
// between clients, a host snapshot, and a signed client report are not evidence.
function createRankedProcessor({ db, verifyEvidence, now = Date.now }) {
  async function recordTrustedResult(matchId, evidence) {
    D.fail(
      typeof verifyEvidence === 'function',
      'RANKED_UNAVAILABLE',
      'Ranked adjudication is not deployed.',
      503
    );
    D.id(matchId);
    const ref = db.doc(`turncraftMatches/${matchId}`),
      initial = await ref.get();
    D.fail(initial.exists, 'NOT_FOUND', 'Match not found.', 404);
    const session = initial.data();
    if (session.ratingReceipt) return session.ratingReceipt;
    D.fail(
      session.ranked === true &&
        session.visibility === 'public' &&
        session.mode === '1v1' &&
        session.hasBots === false &&
        session.settingsVersion === 1 &&
        session.participants?.length === 2 &&
        new Set(session.participants).size === 2 &&
        session.authenticatedUids?.length === 2 &&
        session.participants.every((uid) =>
          session.authenticatedUids.includes(uid)
        ),
      'INELIGIBLE_MATCH',
      'This match cannot award rating.',
      409
    );
    const verified = await verifyEvidence({ matchId, session, evidence });
    D.fail(
      verified?.verified === true &&
        verified.matchId === matchId &&
        verified.revision === session.revision &&
        ['p0', 'p1', 'draw'].includes(verified.outcome) &&
        Number.isFinite(verified.finishedAt) &&
        verified.finishedAt >=
          session.startedAt +
            (verified.source === 'authenticated-forfeit' ? 0 : 20000) &&
        verified.finishedAt <= now() + 5000,
      'UNVERIFIED_RESULT',
      'The authoritative match result could not be verified.',
      409
    );
    return db.runTransaction(async (tx) => {
      const current = await tx.get(ref);
      D.fail(current.exists, 'NOT_FOUND', 'Match not found.', 404);
      const live = current.data();
      if (live.ratingReceipt) return live.ratingReceipt;
      D.fail(
        live.status === 'active' &&
          live.revision === session.revision &&
          JSON.stringify(live.participants) ===
            JSON.stringify(session.participants),
        'STALE_RESULT',
        'Match state has changed; verify it again.',
        409
      );
      const refs = session.participants.map((uid) =>
          db.doc(`turncraftProfiles/${uid}`)
        ),
        snaps = await Promise.all(refs.map((p) => tx.get(p)));
      const accounts = await Promise.all(
        session.participants.map((uid) =>
          tx.get(db.doc(`factionForgeAccounts/${uid}`))
        )
      );
      D.fail(
        snaps.every((s) => s.exists) &&
          accounts.every((a) => a.exists && a.data().verified === true),
        'ACCOUNT_NOT_LINKED',
        'Both commanders need verified CatnMice accounts.',
        403
      );
      const bindings = accounts.map((a) => a.data()),
        names = await Promise.all(
          bindings.map((a) => tx.get(db.doc(`factionForgeNames/${a.username}`)))
        );
      D.fail(
        names.every(
          (n, i) => n.exists && n.data().uid === session.participants[i]
        ),
        'ACCOUNT_NOT_LINKED',
        'Account ownership could not be verified.',
        403
      );
      const pairRef = db.doc(
          `turncraftRatingPairs/${pairId(session.participants, session.startedAt)}`
        ),
        pair = await tx.get(pairRef),
        count = pair.data()?.count || 0;
      const lobbyRef = db.doc(`lobbies/${session.lobbyId || matchId}`),
        lobby = await tx.get(lobbyRef);
      D.fail(
        count < 3,
        'PAIR_LIMIT',
        'This opponent pairing has reached today’s rated match limit.',
        409
      );
      const profiles = D.elo(
          snaps[0].data(),
          snaps[1].data(),
          verified.outcome,
          verified.finishedAt
        ).map((p) => ({ ...p, activeRankedMatch: null })),
        receipt = {
          matchId,
          outcome: verified.outcome,
          finishedAt: verified.finishedAt,
          mmr: profiles.map((p) => p.mmr),
          deltas: profiles.map((p) => p.lastDelta),
          ranks: profiles.map((p) => p.rank),
          changes: Object.fromEntries(
            profiles.map((p) => [
              p.uid,
              { delta: p.lastDelta, after: p.mmr, mmr: p.mmr, rank: p.rank }
            ])
          ),
          verified: true
        };
      for (let i = 0; i < 2; i++) {
        const profile = profiles[i];
        tx.set(refs[i], profile);
        tx.set(db.doc(`turncraftRankings/${profile.uid}`), {
          uid: profile.uid,
          username: bindings[i].username,
          mmr: profile.mmr,
          rank: profile.rank,
          rankedMatches: profile.rankedMatches,
          rankedWins: profile.rankedWins,
          rankedLosses: profile.rankedLosses,
          rankedDraws: profile.rankedDraws,
          updatedAt: profile.updatedAt
        });
        tx.set(
          db.doc(`users/${bindings[i].username}`),
          { gameStats: { turncraft: profile } },
          { merge: true }
        );
        tx.set(
          db.doc(`turncraftProfiles/${profile.uid}/history/${matchId}`),
          receipt
        );
      }
      tx.set(pairRef, { count: count + 1, updatedAt: verified.finishedAt });
      tx.update(ref, {
        status: 'complete',
        ratingReceipt: receipt,
        finishedAt: verified.finishedAt
      });
      if (lobby.exists)
        tx.update(lobbyRef, {
          status: 'finished',
          finishedAt: verified.finishedAt,
          ratingReceipt: receipt,
          turncraftResult: {
            winnerIdx:
              verified.outcome === 'draw'
                ? null
                : Number(verified.outcome.slice(1)),
            reason: 'Server-verified ranked result',
            players: session.players,
            durationSeconds: Math.max(
              0,
              Math.floor((verified.finishedAt - session.startedAt) / 1000)
            )
          },
          updatedAt: verified.finishedAt
        });
      return receipt;
    });
  }
  return { recordTrustedResult };
}
module.exports = { createRankedProcessor, pairId };
