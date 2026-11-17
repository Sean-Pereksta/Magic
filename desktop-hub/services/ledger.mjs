import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';

const games = new Set(['bannerlands', 'catandmouse']);
const required = (value, message) => { if (!value) throw Object.assign(new Error(message), { status: 400 }); };
export const seasonAt = date => `${date.getUTCFullYear()}-Q${1 + Math.floor(date.getUTCMonth() / 3)}`;
export function tier(rating, gamesPlayed) {
  if (gamesPlayed < 5) return 'Placement';
  return rating >= 1600 ? 'Crown' : rating >= 1400 ? 'Diamond' : rating >= 1250 ? 'Platinum' : rating >= 1100 ? 'Gold' : rating >= 950 ? 'Silver' : 'Bronze';
}
export function guestKey(token) {
  required(typeof token === 'string' && /^[a-zA-Z0-9_-]{43}$/.test(token), 'Invalid guest profile.');
  return 'g:' + createHash('sha256').update(token).digest('hex');
}
export function accountKey(identity) {
  required(typeof identity?.uid === 'string' && identity.uid && typeof identity.username === 'string' && identity.username, 'A verified Catnmice account is required.');
  return 'a:' + identity.username;
}
export function survivalWinner(scores) {
  required(Array.isArray(scores) && scores.length === 2 && scores.every(s => Number.isSafeInteger(s.waves) && s.waves >= 0 && Number.isSafeInteger(s.timeMs) && s.timeMs >= 0), 'Invalid simulation scores.');
  const difference = scores[0].waves - scores[1].waves || scores[0].timeMs - scores[1].timeMs;
  return difference > 0 ? 0 : difference < 0 ? 1 : null;
}
export function createLedger(path, catalogIds, clock = () => new Date()) {
  const catalog = new Set(catalogIds);
  const db = new DatabaseSync(path);
  db.exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;
    CREATE TABLE IF NOT EXISTS guest_links(guest TEXT PRIMARY KEY, account TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS plays(player TEXT NOT NULL, game TEXT NOT NULL, PRIMARY KEY(player,game));
    CREATE INDEX IF NOT EXISTS plays_by_game ON plays(game,player);
    CREATE TABLE IF NOT EXISTS scores(season TEXT NOT NULL,game TEXT NOT NULL,username TEXT NOT NULL,rating INTEGER NOT NULL,games INTEGER NOT NULL,change INTEGER NOT NULL,PRIMARY KEY(season,game,username));
    CREATE TABLE IF NOT EXISTS results(id TEXT PRIMARY KEY,input TEXT NOT NULL,result TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS history(username TEXT NOT NULL,id TEXT NOT NULL REFERENCES results(id),completedAt TEXT NOT NULL,PRIMARY KEY(username,id));`);
  function transaction(task) {
    db.exec('BEGIN IMMEDIATE');
    try { const result = task(); db.exec('COMMIT'); return result; }
    catch (error) { db.exec('ROLLBACK'); throw error; }
  }
  const canonical = actor => db.prepare('SELECT account FROM guest_links WHERE guest=?').get(actor)?.account ?? actor;
  function activity(actor, ids) {
    required(Array.isArray(ids) && ids.length > 0 && ids.length <= 180 && ids.every(id => catalog.has(id)), 'Unrecognized game activity.');
    transaction(() => {
      const player = canonical(actor);
      for (const game of new Set(ids)) db.prepare('INSERT OR IGNORE INTO plays VALUES(?,?)').run(player, game);
    });
  }
  function linkGuest(guest, account) {
    transaction(() => {
      const existing = canonical(guest);
      if (existing === account) return;
      if (existing !== guest) throw Object.assign(new Error('This guest already belongs to a different account.'), {status:409});
      db.prepare('INSERT INTO guest_links VALUES(?,?)').run(guest,account);
      db.prepare('INSERT OR IGNORE INTO plays SELECT ?,game FROM plays WHERE player=?').run(account,guest);
      db.prepare('DELETE FROM plays WHERE player=?').run(guest);
    });
  }
  function counts() {
    const columns = "count(DISTINCT CASE WHEN substr(player,1,2)='a:' THEN player END) AS accounts, count(DISTINCT CASE WHEN substr(player,1,2)='g:' THEN player END) AS guests, count(DISTINCT player) AS total";
    const rows = db.prepare(`SELECT game,${columns} FROM plays GROUP BY game`).all();
    const byGame = Object.fromEntries([...catalog].map(id => [id, {accounts:0,guests:0,total:0}]));
    for (const {game,...count} of rows) byGame[game]=count;
    return {asOf:clock().toISOString(), hub:db.prepare(`SELECT ${columns} FROM plays`).get(), games:byGame};
  }
  const format = row => ({...row, placementsRemaining:Math.max(0,5-row.games),tier:tier(row.rating,row.games)});
  function leaderboard(game, season = seasonAt(clock())) {
    required(games.has(game) && /^\d{4}-Q[1-4]$/.test(season), 'Unknown season or queue.');
    return {game,season,players:db.prepare('SELECT username,rating,games,change FROM scores WHERE season=? AND game=? ORDER BY rating DESC,games DESC,username LIMIT 100').all(season,game).map(format)};
  }
  function record({id,season,game,players,winner,scores=null}) {
    required(typeof id==='string' && /^[a-zA-Z0-9_-]{1,100}$/.test(id) && games.has(game) && /^\d{4}-Q[1-4]$/.test(season), 'Invalid match.');
    required(Array.isArray(players) && players.length===2 && accountKey(players[0])!==accountKey(players[1]) && players[0].uid!==players[1].uid,'Two different verified accounts are required.');
    if (game==='catandmouse') winner=survivalWinner(scores);
    required([null,0,1].includes(winner),'Invalid winner.');
    const input=JSON.stringify([season,game,players.map(({uid,username})=>[uid,username]),winner,scores]);
    return transaction(() => {
      const saved=db.prepare('SELECT input,result FROM results WHERE id=?').get(id);
      if (saved) {
        if (saved.input!==input) throw Object.assign(new Error('Conflicting match receipt.'),{status:409});
        return JSON.parse(saved.result);
      }
      const before=players.map(({username})=>db.prepare('SELECT rating,games FROM scores WHERE season=? AND game=? AND username=?').get(season,game,username)??{rating:1000,games:0});
      const expected=1/(1+10**((before[1].rating-before[0].rating)/400));
      const adjustment=Math.min(before[1].rating,Math.max(-before[0].rating,Math.round(32*((winner===null?.5:Number(winner===0))-expected))));
      const completedAt=clock().toISOString();
      const results=players.map(({username},i)=>{
        const change=i===0?adjustment:-adjustment;
        const next=format({username,rating:before[i].rating+change,games:before[i].games+1,change});
        db.prepare('INSERT OR REPLACE INTO scores VALUES(?,?,?,?,?,?)').run(season,game,username,next.rating,next.games,change);
        db.prepare('INSERT OR IGNORE INTO plays VALUES(?,?)').run(accountKey(players[i]),game==='catandmouse'&&!catalog.has(game)&&catalog.has('cat')?'cat':game);
        return {...next,before:before[i].rating};
      });
      const receipt={id,season,game,players:results,winner,scores,completedAt};
      db.prepare('INSERT INTO results VALUES(?,?,?)').run(id,input,JSON.stringify(receipt));
      for (const {username} of players) db.prepare('INSERT INTO history VALUES(?,?,?)').run(username,id,completedAt);
      return receipt;
    });
  }
  function history(username) {
    return db.prepare('SELECT r.result FROM history h JOIN results r ON r.id=h.id WHERE h.username=? ORDER BY h.completedAt DESC LIMIT 50').all(username).map(row=>JSON.parse(row.result));
  }
  return {activity,linkGuest,counts,leaderboard,record,history,close:()=>db.close()};
}
