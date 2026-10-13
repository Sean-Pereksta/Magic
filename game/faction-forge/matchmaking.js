export const AI_NAMES = Object.freeze(['IronWarden','AshenWolf','FrostRanger','CrimsonVale','SteelHawk']);

export function publicForgeLobby(data, username) {
  const players = Array.isArray(data?.players) ? [...new Set(data.players)] : [];
  return data?.gameType === 'crowncouncil' && data.status === 'waiting'
    && (data.visibility === 'public' || !data.visibility)
    && !data.password && !data.inviteOnly && !players.includes(username)
    && players.length === 1 && typeof data.host === 'string' && players.includes(data.host);
}

export function estimatedOpponent(mmr = 1000, difficulty = .5, random = Math.random) {
  return { name: AI_NAMES[Math.floor(random() * AI_NAMES.length)], ai: true,
    estimatedMmr: Math.max(400, Math.round(Number(mmr) + (difficulty - .5) * 240 + (random() - .5) * 120)) };
}

// The same /lobbies collection and crowncouncil game key used by Game Hub.
// A transaction rechecks visibility, status and both seats before adding a member.
export function createMatchmaking({db,auth,api,username,onRows,onStatus,onFallback,onJoined,timeoutMs=30000}) {
  let unsubscribe = null, timeout = null, generation = 0, joining = false;
  const stop = () => { generation++; unsubscribe?.(); unsubscribe=null; clearTimeout(timeout); timeout=null; };
  const join = async id => {
    if (joining) return false;
    if (!auth.currentUser || !username || username === 'Guest') throw new Error('Sign in to your Game Hub profile to join an online lobby.');
    joining = true;
    const uid = auth.currentUser.uid;
    try {
      await api.runTransaction(db, async tx => {
        const ref = api.doc(db,'lobbies',id), snap = await tx.get(ref);
        if (!snap.exists()) throw new Error('This lobby has closed.');
        const data = snap.data();
        if (data.players?.includes(username)) {
          if (data.members?.[uid] === username || !data.members) return;
          throw new Error('That profile is already occupying a seat.');
        }
        if (!publicForgeLobby(data,username)) throw new Error('This lobby is no longer available.');
        tx.update(ref,{players:[...data.players,username],...(data.hostUid?{members:{...(data.members||{}),[uid]:username}}:{}),updatedAt:Date.now()});
      });
      stop();
      onJoined?.(id);
      return true;
    } finally { joining = false; }
  };
  const search = () => {
    stop(); const current = generation;
    onStatus?.('Searching public Faction Forge lobbies…');
    const q = api.query(api.collection(db,'lobbies'),api.where('gameType','==','crowncouncil'));
    unsubscribe = api.onSnapshot(q,async snapshot => {
      const lobbies = snapshot.docs.map(s=>({id:s.id,...s.data()})).filter(d=>publicForgeLobby(d,username));
      const rows = await Promise.all(lobbies.map(async row=>{
        let mmr=1000;
        try { const user = await api.getDoc(api.doc(db,'users',row.host));
          const value=Number(user.data()?.crowncouncilElo); if (Number.isFinite(value)&&value>0) mmr=Math.round(value);
        } catch { /* Rating is optional; lobby remains joinable. */ }
        return {...row,mmr};
      }));
      if (current !== generation) return;
      onRows?.(rows,join);
      onStatus?.(rows.length ? 'Choose a real open lobby below.' : 'Waiting for a human opponent…');
    },error=>{ if (current===generation) onStatus?.('Lobby search unavailable: '+error.message); });
    timeout = setTimeout(()=>{ if (current!==generation)return;stop();onFallback?.(); },timeoutMs);
    return stop;
  };
  return {search,join,stop};
}
