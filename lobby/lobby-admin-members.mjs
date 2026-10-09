// Keep dashboard state limited to activity fields, never account credentials.
export function activityTime(value) {
  let milliseconds;
  if (typeof value?.toMillis === "function") milliseconds = value.toMillis();
  else if (typeof value === "number") milliseconds = value;
  else if (typeof value === "string") milliseconds = Date.parse(value);
  else if (typeof value?.seconds === "number") milliseconds = value.seconds * 1000;
  return Number.isFinite(milliseconds) && milliseconds > 0 && milliseconds <= 8640000000000000 ? milliseconds : null;
}

export function memberActivity(id, data) {
  const counts = data.gamePlayCounts && typeof data.gamePlayCounts === "object" && !Array.isArray(data.gamePlayCounts) ? data.gamePlayCounts : {};
  const games = Object.entries(counts).flatMap(([key, count]) => {
    const plays = Number(count);
    return Number.isSafeInteger(plays) && plays > 0 ? [{key, plays}] : [];
  }).sort((a, b) => b.plays - a.plays || a.key.localeCompare(b.key));
  const recent = Array.isArray(data.recentlyPlayed) ? data.recentlyPlayed : [];
  return {
    username: id,
    displayName: String(data.displayName || id),
    lastLogin: activityTime(data.lastLogin),
    createdAt: activityTime(data.createdAt),
    wins: Number.isSafeInteger(data.wins) && data.wins > 0 ? data.wins : 0,
    totalPlays: games.reduce((sum, game) => sum + game.plays, 0),
    hasPlayCounts: Object.keys(counts).length > 0,
    games,
    recent: recent.flatMap(item => {
      const key = typeof item === "string" ? item : item?.key;
      return typeof key === "string" && key ? [{key, playedAt: activityTime(item?.playedAt)}] : [];
    })
  };
}

export function filterMembers(members, search = "", sort = "lastLogin") {
  const term = search.trim().toLowerCase();
  return members.filter(member => `${member.username} ${member.displayName}`.toLowerCase().includes(term)).sort((a, b) => {
    if (sort === "name") return a.username.localeCompare(b.username);
    return (sort === "plays" ? b.totalPlays - a.totalPlays : (b.lastLogin || 0) - (a.lastLogin || 0)) || a.username.localeCompare(b.username);
  });
}
