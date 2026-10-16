const count = value => Math.max(0, Math.floor(Number(value) || 0));
const ranks = [['Master',1750],['Diamond',1500],['Platinum',1300],['Gold',1100],['Silver',900],['Bronze',0]];
const check = (ok, message) => { if (!ok) throw new Error(message); };

// The immutable finished lobby is the source of truth. The history document is
// also the receipt, so reconnects and concurrent tabs cannot count a match twice.
export async function saveMatchResult({db, auth, api, isCurrentAccount = () => true}, lobbyId, username) {
  const uid = auth.currentUser?.uid;
  check(uid && username && !username.includes('/') && /^[A-Za-z0-9_-]{1,100}$/.test(lobbyId), 'Choose your Game Hub profile before saving this match.');
  const profileRef = api.doc(db, 'users', username);
  const historyRef = api.doc(db, 'users', username, 'turncraftHistory', lobbyId);
  const operation = async tx => {
    const [roomSnap, profileSnap, receipt] = await Promise.all([
      tx.get(api.doc(db, 'lobbies', lobbyId)), tx.get(profileRef), tx.get(historyRef)
    ]);
    check(auth.currentUser?.uid === uid && isCurrentAccount(), 'Your selected profile changed before this match synced.');
    if (receipt.exists()) return {history: receipt.data(), wrote: false};
    check(roomSnap.exists() && profileSnap.exists(), 'Your match or Game Hub profile could not be found.');
    const room = roomSnap.data(), account = profileSnap.data(), seats = room.turncraftSeats || [];
    const seatUid = seats.find(memberUid => room.members?.[memberUid] === username && room.turncraftPlayers?.[memberUid]?.name === username);
    const slot = seats.indexOf(seatUid), result = room.turncraftResult;
    check(room.gameType === 'turncraft' && room.turncraftVersion === 1 && room.status === 'finished' && result, 'The shared match result is still syncing.');
    check(slot >= 0, 'This result belongs to the profile that entered the lobby.');
    check(result.winnerIdx === null || Number.isInteger(result.winnerIdx) && result.winnerIdx >= 0 && result.winnerIdx < result.players.length, 'The match winner is invalid.');
    const ranked = room.ranked === true;
    check(!ranked || seats.length === 2 && result.players.length === 2 && room.settings.maxPlayers === 2 && room.settings.fillAi === false, 'Ranked matches require two human commanders.');
    const outcome = result.winnerIdx === null ? 'draw' : result.winnerIdx === slot ? 'win' : 'loss';
    const previous = account.gameStats?.turncraft || {}, next = {...previous};
    next.multiplayerMatches = count(previous.multiplayerMatches) + 1;
    for (const [key, value] of [['Wins','win'],['Losses','loss'],['Draws','draw']]) next[`multiplayer${key}`] = count(previous[`multiplayer${key}`]) + Number(outcome === value);
    next.lastMatchId = lobbyId;
    const before = Number.isFinite(previous.mmr) ? count(previous.mmr) : 1000;
    const after = ranked ? Math.max(0, before + (outcome === 'win' ? 25 : outcome === 'loss' ? -25 : 0)) : before;
    if (ranked) {
      next.mmr = after;
      next.rankedMatches = count(previous.rankedMatches) + 1;
      for (const [key, value] of [['Wins','win'],['Losses','loss'],['Draws','draw']]) next[`ranked${key}`] = count(previous[`ranked${key}`]) + Number(outcome === value);
      next.highestMMR = Math.max(Number.isFinite(previous.highestMMR) ? count(previous.highestMMR) : 1000, before, after);
      next.provisionalMatchesRemaining = Math.max(0, 10 - next.rankedMatches);
      next.rank = ranks.find(([, minimum]) => after >= minimum)[0];
    }
    const history = {
      id: lobbyId, lobbyId, uid: seatUid, username, mode: ranked ? 'ranked' : 'custom', outcome,
      winnerIdx: result.winnerIdx, factionId: room.turncraftPlayers[seatUid].factionId,
      opponents: result.players.filter((_, index) => index !== slot).map(player => player.name),
      mapId: room.settings.mapId, gameMode: room.settings.gameMode,
      durationSeconds: count(result.durationSeconds), completedAt: count(room.finishedAt),
      mmrBefore: before, mmrAfter: after, mmrDelta: after - before
    };
    // Update only this game's fields. Other hub games and profile data survive.
    const patch = Object.fromEntries(Object.entries(next).filter(([key, value]) => previous[key] !== value).map(([key, value]) => [`gameStats.turncraft.${key}`, value]));
    tx.update(profileRef, patch);
    tx.set(historyRef, history);
    return {history, profile: next, wrote: true};
  };
  for (let attempt = 0; attempt < 3; attempt++) {
    try { return await api.runTransaction(db, operation); }
    catch (error) {
      if (!['permission-denied', 'aborted', 'firestore/permission-denied', 'firestore/aborted'].includes(error.code) || !api.getDoc || !isCurrentAccount() || auth.currentUser?.uid !== uid) throw error;
      // Rules can reject a stale atomic rating update before Firestore retries
      // contention. A committed receipt recovers the successful concurrent tab.
      const receipt = await api.getDoc(historyRef);
      if (receipt.exists() && receipt.data().username === username && receipt.data().lobbyId === lobbyId) return {history: receipt.data(), wrote: false};
      if (attempt === 2) throw error;
    }
  }
}
