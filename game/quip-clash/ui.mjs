/** Quip Clash's DOM layer. Game state and Firestore authority live in app/network. */
let currentView = {};
let currentActions = {};
let currentSignature = '';
let mountedRoot = null;
let toastTimeout;

const escape = (value = '') => String(value ?? '').replace(/[&<>"']/g, character => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[character]);
const format = value => Math.max(0, Math.round(Number(value) || 0)).toLocaleString('en-US');
const initials = name => String(name || '?').trim().split(/\s+/u).slice(0, 2).map(word => Array.from(word)[0] || '').join('').toUpperCase();
const playerName = player => player?.displayName || player?.username || 'Player';
const roomPlayers = view => view.room?.players || [];
const roundNumber = view => Number(view.room?.round) || 1;
const roundName = view => roundNumber(view) === 3 ? 'The final clash' : `Round ${roundNumber(view)}`;
const disabled = condition => condition ? ' disabled' : '';
const ownUid = view => view.identity?.uid;
const currentQuestion = view => view.room?.questions?.[Number(view.room?.questionIndex) || 0] || view.question || { text: '' };
const avatar = (name, index = 0) => `<span class="avatar" data-color="${index % 5}" aria-hidden="true">${escape(initials(name))}</span>`;

function brand() {
  return '<a class="brand" href="./quip-clash.html" aria-label="Quip Clash home"><span class="brand-mark" aria-hidden="true">Q!</span><span class="brand-name">quip<span>clash</span></span></a>';
}

function header(view) {
  return `<header class="topbar">${brand()}<div class="topbar-right">${view.identity ? `<div class="identity" title="Playing as ${escape(playerName(view.identity))}">${avatar(playerName(view.identity))}<span class="identity-name">${escape(playerName(view.identity))}</span></div>` : ''}<a class="back-link" href="/lobby/lobby.html">Game hub ↗</a></div></header>`;
}

function roomStrip(view) {
  if (!view.room) return '';
  return `<div class="room-strip"><div class="room-strip-name">${escape(view.room.name || 'The funny room')}</div><div class="room-strip-right"><span class="connection${view.connected === false ? ' offline' : ''}">${view.connected === false ? 'Reconnecting' : 'Live'}</span><span class="room-code" aria-label="Room code ${escape(view.room.id)}">${escape(view.room.id)}</span><button class="button compact secondary" type="button" data-action="copyCode" aria-label="Copy room invitation">Copy invite</button></div></div>`;
}

function notice(view) {
  const message = view.error || view.message;
  if (!message) return '<div data-notice></div>';
  return `<div data-notice><div class="notice${view.error ? ' error' : ''}" role="${view.error ? 'alert' : 'status'}"><span class="notice-icon" aria-hidden="true">${view.error ? '!' : '✦'}</span><span class="notice-text">${escape(message)}</span>${view.error ? '<button class="icon-button" type="button" data-action="clearError" aria-label="Dismiss error">×</button>' : ''}</div></div>`;
}

function connectionNotice(view) {
  return view.connected === false && view.room ? '<div class="notice error connection-banner" role="status"><span class="notice-icon" aria-hidden="true">↻</span><span class="notice-text">Reconnecting to the room. Your drafts stay here while we catch up.</span></div>' : '';
}

function timer(seconds = 0, label = 'seconds left') {
  const value = Math.max(0, Math.ceil(Number(seconds) || 0));
  return `<div class="timer${value <= 10 ? ' urgent' : ''}" data-timer role="timer" aria-label="${value} ${escape(label)}"><span class="timer-icon" aria-hidden="true"></span><div><div class="timer-value" data-seconds>${value}</div><span class="timer-label" data-timer-label>${value === 0 ? 'wrapping up' : escape(label)}</span></div></div>`;
}

function stageTop(view, title, subtitle) {
  return `<div class="stage-top"><div><p class="eyebrow">${escape(roundName(view))} <span aria-hidden="true">/</span> ${roundNumber(view)}× points</p><h1>${escape(title)}</h1><p>${escape(subtitle)}</p></div>${timer(view.secondsLeft)}</div>`;
}

function progress(view, label) {
  const done = Math.max(0, Number(view.progress?.done) || 0);
  const total = Math.max(0, Number(view.progress?.total) || 0);
  const percent = total ? Math.min(100, done / total * 100) : 0;
  return `<div class="progress-row"><span data-progress-label>${escape(label)}</span><strong data-progress-text>${done} / ${total}</strong></div><div class="progress-track" role="progressbar" aria-label="${escape(label)}" aria-valuenow="${done}" aria-valuemin="0" aria-valuemax="${total}" data-progress><div class="progress-fill" style="width:${percent}%" data-progress-fill></div></div>`;
}

function home(view) {
  return `<section class="home-hero"><div class="hero-copy"><p class="eyebrow">Your friends. No filter. Big laughs.</p><h1>Good prompts.<br><em>Questionable</em><br>answers.</h1><p>Two answers go head to head. The room picks its favorite. Bring your best worst ideas.</p><div class="badge-row"><span class="pill lime">4–12 players</span><span class="pill">3 rounds</span><span class="pill">Play on your phone</span></div></div><img class="hero-art" src="./quip-clash/assets/quip-clash.svg" width="720" height="360" alt="Two mischievous speech bubbles trading jokes"></section><div class="home-forms"><section class="panel form-panel"><div class="form-heading"><span class="number-badge" aria-hidden="true">+</span><h2>Make a room</h2></div><p>You host. Your friends bring the chaos.</p><form data-form="create"><label class="field-label" for="room-name">Room name</label><input class="text-input" id="room-name" name="name" maxlength="48" placeholder="The questionable ideas club" autocomplete="off"><button class="button coral full" type="submit">Create a room <span aria-hidden="true">→</span></button></form></section><section class="panel form-panel"><div class="form-heading"><span class="number-badge lime" aria-hidden="true">↗</span><h2>Join your friends</h2></div><p>Already got an invite? You're in the right place.</p><form data-form="join"><label class="field-label" for="room-code">Room code or invite link</label><input class="text-input code-input" id="room-code" name="code" maxlength="500" placeholder="ABCDEF or paste invite link" autocomplete="off" autocapitalize="none" spellcheck="false" required><button class="button full" type="submit">Join the room <span aria-hidden="true">→</span></button></form></section></div><section class="round-guide" aria-label="How Quip Clash works"><div><span class="round-number" aria-hidden="true">01</span><div><strong>Write something ridiculous</strong><p>90 seconds. Two prompts. Make them count.</p></div></div><div><span class="round-number" aria-hidden="true">02</span><div><strong>Pick your favorite</strong><p>20 seconds to vote. No voting for yourself.</p></div></div><div><span class="round-number" aria-hidden="true">03</span><div><strong>Climb the scoreboard</strong><p>Double points in round 2. A triple-point finale.</p></div></div></section><p class="footer-note">Playing as <strong>${escape(playerName(view.identity))}</strong> · Your website account is your ticket in.</p>`;
}

function waiting(view) {
  const players = roomPlayers(view);
  const missing = Math.max(0, 4 - players.length);
  return `<div class="stage-top"><div><p class="eyebrow">The party starts here</p><h1>Assemble your funny people.</h1><p>Share the code. Everyone joins from their own account and phone.</p></div></div><div class="waiting-layout"><section class="panel waiting-card"><div class="player-heading"><h2>The lineup</h2><span class="player-count">${players.length} / 12 players</span></div><ul class="player-list">${players.map((player, index) => `<li class="player-item">${avatar(playerName(player), index)}<div class="player-info"><div class="player-name">${escape(playerName(player))}${player.uid === ownUid(view) ? ' <span class="you-tag">YOU</span>' : ''}</div><div class="player-meta">${player.uid === view.room.hostUid ? 'Host · ready to start the show' : 'Ready for the ridiculous'}</div></div></li>`).join('')}${Array.from({ length: missing }, () => '<li class="player-empty"><span class="empty-avatar" aria-hidden="true">+</span>Waiting for a friend…</li>').join('')}</ul><div class="waiting-actions"><p>${missing ? `${missing} more ${missing === 1 ? 'player' : 'players'} needed to start.` : 'Everyone here? Let the bad ideas begin.'}</p>${view.isHost ? `<button class="button" type="button" data-action="start"${disabled(players.length < 4 || players.length > 12)}>Start the clash <span aria-hidden="true">→</span></button>` : '<span class="pill lime">Waiting for the host to start</span>'}</div></section><aside class="panel invite-card"><p class="invite-label">Your room code</p><div class="invite-code">${escape(view.room.id)}</div><button class="button coral" type="button" data-action="copyCode">Copy invite <span aria-hidden="true">↗</span></button><p>Send this to your friends. They can enter it on the Quip Clash home screen.</p></aside></div><section class="panel rules-card"><h3>A very serious briefing</h3><div class="rule-list"><p><strong>Write fast.</strong> Each player gets two prompts in rounds 1 and 2, then one shared prompt in the finale.</p><p><strong>Vote your heart out.</strong> Responses face off in pairs. An odd player count adds two three-way clashes.</p><p><strong>Every vote matters.</strong> Your share of votes becomes points. Round 2 doubles them; the finale triples them.</p></div></section><div class="stage-actions"><button class="button secondary compact" type="button" data-action="leave">Leave room</button></div>`;
}

function writing(view) {
  const assignments = view.assignments || [];
  const submitted = Boolean(view.submitted);
  return `${stageTop(view, roundNumber(view) === 3 ? 'One prompt. All the glory.' : 'Make the room lose it.', roundNumber(view) === 3 ? 'Everyone gets this prompt. One answer. Triple the points.' : 'Write one answer for each prompt. Your opponents are doing the same.')}<form data-form="answers"><div class="writing-grid${assignments.length === 1 ? ' single' : ''}">${assignments.map((question, index) => {
    const answer = view.draft?.[question.id] ?? question.answer ?? '';
    return `<section class="panel question-card"><label for="answer-${index}"><span class="question-number">${roundNumber(view) === 3 ? 'The final prompt' : `Prompt ${index + 1} of ${assignments.length}`}</span><h2>${escape(question.text)}</h2></label><textarea class="answer-input" id="answer-${index}" name="${escape(question.id)}" data-question="${escape(question.id)}" maxlength="1000" placeholder="Your brilliant (or brilliantly terrible) answer…" aria-describedby="answer-help-${index}"${submitted ? ' readonly' : ''}>${escape(answer)}</textarea><div class="answer-meta" id="answer-help-${index}"><span>${submitted ? '✓ Answer locked in' : 'Room for a whole ridiculous idea.'}</span><span class="answer-count" data-count-for="${escape(question.id)}">${String(answer).length} / 1,000</span></div></section>`;
  }).join('')}</div><div class="panel submit-bar"><p class="submit-hint" data-submit-hint>${submitted ? '<strong class="submitted-title">✓ Your answers are locked in.</strong>Now sit back. The voting starts when everyone is done.' : `<strong>Happy with the chaos?</strong>${assignments.length === 1 ? 'Lock in your answer' : 'Lock in both answers'}, or keep writing until time runs out.`}</p><button class="button" type="submit" data-submit-answers data-timed-control${disabled(submitted || Boolean(view.busy) || Number(view.secondsLeft) <= 0 || !assignments.length || assignments.some(question => !String(view.draft?.[question.id] ?? question.answer ?? '').trim()))}>${submitted ? 'Answers locked ✓' : assignments.length === 1 ? 'Lock in my answer →' : 'Lock in my answers →'}</button></div></form>${progress(view, 'Players finished writing')}<p class="writing-help">${submitted ? 'Your answers will appear without your name until the votes are in.' : 'Your saved text goes into the vote when time runs out. Lock in to confirm your answers.'}</p>`;
}

function voting(view) {
  const answers = view.answers || [];
  const question = currentQuestion(view);
  const count = view.room?.questions?.length || 1;
  const index = (Number(view.room?.questionIndex) || 0) + 1;
  return `${stageTop(view, view.voted ? 'Excellent taste. Vote locked.' : 'Which answer wins?', view.voted ? 'The votes are coming in. The reveal is almost here.' : 'Pick the answer that deserves the applause. You get one vote.')}<section class="vote-question"><p class="eyebrow">${roundNumber(view) === 3 ? 'The final prompt' : `Clash ${index} of ${count}`}</p><h2>${escape(question.text)}</h2></section><div class="vote-grid${roundNumber(view) === 3 ? ' final' : answers.length === 3 ? ' three' : ''}" aria-label="Answers to vote for">${answers.map((answer, answerIndex) => {
    const own = answer.uid === ownUid(view);
    return `<button class="answer-card${own ? ' own' : ''}" type="button" data-action="vote" data-answer="${escape(answer.id)}" data-own="${own}" data-timed-control${disabled(own || view.voted || Boolean(view.busy) || view.eligible === false || Number(view.secondsLeft) <= 0)}><span class="answer-label">Answer ${answerIndex + 1}${own ? '<span class="pill">Yours</span>' : ''}</span><span class="answer-text${String(answer.text).length > 160 ? ' long' : ''}">${escape(answer.text || 'No answer submitted.')}</span><span class="answer-cta"><span data-answer-cta>${own ? 'Your answer · admire it proudly' : view.voted ? 'Your vote is locked in' : 'Tap to vote for this answer'}</span><span class="answer-arrow" aria-hidden="true">${own ? '✦' : view.voted ? '✓' : '↗'}</span></span></button>`;
  }).join('')}</div><p class="vote-instruction${view.eligible === false ? ' ineligible' : ''}" data-vote-instruction role="status">${votingInstruction(view)}</p>${progress(view, 'Votes locked in')}<p class="sync-note">Answers stay anonymous until the reveal. Points follow the share of the vote.</p>`;
}

function votingInstruction(view) {
  if (view.voted) return '<strong>✓ Vote received.</strong> Waiting for the rest of the room.';
  if (view.eligible === false) return 'You’re sitting this vote out. The room is choosing its favorite.';
  if (Number(view.secondsLeft) <= 0) return 'Voting has closed. Getting the results ready…';
  return 'One vote. Go with your gut. <strong>You can’t vote for yourself.</strong>';
}

function reveal(view) {
  const answers = view.answers || [];
  const question = currentQuestion(view);
  const maxVotes = Math.max(0, ...answers.map(answer => Number(answer.votes) || 0));
  const totalVotes = answers.reduce((sum, answer) => sum + (Number(answer.votes) || 0), 0);
  const count = view.room?.questions?.length || 1;
  const last = (Number(view.room?.questionIndex) || 0) >= count - 1;
  return `<div class="stage-top"><div><p class="eyebrow">${escape(roundName(view))} / ${roundNumber(view)}× points</p><h1>The room has spoken.</h1><p>${maxVotes > 0 ? 'Applause, please. Here’s where every vote landed.' : 'No votes this time. This clash awards no points.'}</p></div></div><section class="vote-question"><p class="eyebrow">${roundNumber(view) === 3 ? 'The final prompt' : `Clash ${(Number(view.room?.questionIndex) || 0) + 1} of ${count}`}</p><h2>${escape(question.text)}</h2></section><div class="vote-grid${roundNumber(view) === 3 ? ' final' : answers.length === 3 ? ' three' : ''}">${answers.map((answer, index) => {
    const percent = Math.max(0, Math.min(100, Number(answer.percent) || (totalVotes ? (Number(answer.votes) || 0) / totalVotes * 100 : 0)));
    const winner = maxVotes > 0 && Number(answer.votes) === maxVotes;
    const name = answer.displayName || playerName(roomPlayers(view).find(player => player.uid === answer.uid));
    return `<article class="answer-card result${winner ? ' winner' : ''}"><div class="answer-label">Answer ${index + 1}${winner ? '<span class="pill lime">Fan favorite ✦</span>' : ''}</div><p class="answer-text${String(answer.text).length > 160 ? ' long' : ''}">${escape(answer.text || 'No answer submitted.')}</p><div class="answer-author">${avatar(name, roomPlayers(view).findIndex(player => player.uid === answer.uid))}<strong>${escape(name)}${answer.uid === ownUid(view) ? ' <span class="you-tag">YOU</span>' : ''}</strong></div><div class="result-meta"><div class="result-points">+${format(answer.points)} <small>points</small></div><div class="result-votes">${format(answer.votes)} ${Number(answer.votes) === 1 ? 'vote' : 'votes'} · ${Math.round(percent)}%</div></div><div class="vote-meter" aria-hidden="true"><span style="width:${percent}%"></span></div></article>`;
  }).join('')}</div><p class="reveal-summary"><strong>${format(totalVotes)} ${totalVotes === 1 ? 'vote' : 'votes'} counted.</strong> Points awarded from each answer’s share of the vote.</p><div class="stage-actions">${view.isHost ? `<button class="button" type="button" data-action="next">${last ? 'See the scoreboard' : 'Next clash'} <span aria-hidden="true">→</span></button>` : `<p>${last ? 'The scoreboard' : 'The next clash'} arrives in <strong data-auto-seconds>${Math.max(0, Math.ceil(Number(view.secondsLeft) || 0))}</strong>s…</p>`}</div>`;
}

function sortedLeaderboard(view) {
  return [...(view.leaderboard?.length ? view.leaderboard : roomPlayers(view))].sort((a, b) => (Number(b.score) || 0) - (Number(a.score) || 0) || playerName(a).localeCompare(playerName(b)));
}

function scoreboard(view) {
  const players = sortedLeaderboard(view);
  let rank = 0;
  let previousScore;
  return `<section class="panel score-table" aria-label="Scoreboard">${players.map((player, index) => {
    const score = Number(player.score) || 0;
    if (score !== previousScore) rank = index + 1;
    previousScore = score;
    return `<div class="score-row${rank === 1 ? ' leading' : ''}"><span class="score-rank" aria-label="Rank ${rank}">${rank === 1 ? '✦' : rank}</span><div class="score-person">${avatar(playerName(player), index)}<strong>${escape(playerName(player))}${player.uid === ownUid(view) ? ' <span class="you-tag">YOU</span>' : ''}</strong></div><div class="score-numbers"><div class="score-total">${format(score)}</div><div class="score-delta">${Number(player.delta) > 0 ? `+${format(player.delta)} this round` : ''}</div></div></div>`;
  }).join('')}</section>`;
}

function scores(view) {
  const round = roundNumber(view);
  const next = Math.min(3, round + 1);
  return `<div class="score-layout"><div class="score-header"><p class="eyebrow">${escape(roundName(view))} complete</p><h1>A little friendly competition.</h1><p>Every vote counted. Here’s how the room stacks up.</p></div>${scoreboard(view)}${round < 3 ? `<aside class="panel round-up-next"><span class="next-multiplier">${next}×</span><div><strong>${next === 3 ? 'Up next: the final clash.' : 'Round 2. The stakes just doubled.'}</strong><p>${next === 3 ? 'One shared prompt. Everyone answers. Triple points.' : 'Two fresh prompts each. Every vote is worth double.'}</p></div></aside>` : ''}<div class="stage-actions">${view.isHost ? `<button class="button" type="button" data-action="next">${round >= 3 ? 'Crown the winner' : next === 3 ? 'Start the final clash' : 'Start round 2'} <span aria-hidden="true">→</span></button>` : `<p>${round >= 3 ? 'The winner reveal' : next === 3 ? 'The final clash' : 'Round 2'} starts in <strong data-auto-seconds>${Math.max(0, Math.ceil(Number(view.secondsLeft) || 0))}</strong>s…</p>`}</div></div>`;
}

function finished(view) {
  const players = sortedLeaderboard(view);
  const top = Number(players[0]?.score) || 0;
  const winners = players.filter(player => (Number(player.score) || 0) === top);
  const names = winners.map(playerName);
  const winnerText = names.length === 1 ? `${names[0]} takes the crown!` : names.length === 2 ? `${names[0]} & ${names[1]} share the crown!` : 'A shared crown. A legendary tie.';
  return `<div class="score-layout"><div class="winner-banner"><div class="trophy" aria-hidden="true">♛</div><p class="eyebrow">${winners.length > 1 ? 'We have a tie at the top' : 'All hail the quip champion'}</p><h1>${escape(winnerText)}</h1><p>${winners.length > 2 ? `${escape(names.join(', '))} · ` : ''}${format(top)} points. Three rounds. Many questionable ideas.</p></div>${scoreboard(view)}<div class="end-actions">${view.isHost ? '<button class="button" type="button" data-action="replay">Run it back <span aria-hidden="true">↻</span></button>' : '<p class="muted small">The host can start another game with this room.</p>'}<button class="button secondary" type="button" data-action="leave">Leave the room</button></div><p class="footer-note">Same friends. Fresh prompts. Entirely new opportunities to embarrass yourself.</p></div>`;
}

function account() {
  return '<section class="panel center-card"><div class="center-icon" aria-hidden="true">Q!</div><p class="eyebrow">Your invite to the ridiculous</p><h1>Bring your name to the game.</h1><p>Sign in to your website account in the game hub, then come back here. Your display name follows you into the room.</p><a class="button" href="/lobby/lobby.html">Sign in at the game hub <span aria-hidden="true">↗</span></a><button class="button secondary" type="button" data-action="refresh">I’m signed in · refresh</button></section>';
}

function errorScreen(view) {
  return `<section class="panel center-card"><div class="center-icon" aria-hidden="true">!</div><p class="eyebrow">A small interruption</p><h1>The party hit a snag.</h1><p>${escape(view.error || view.message || 'We couldn’t load the room. Refresh to reconnect, or return to the game hub.')}</p><button class="button" type="button" data-action="refresh">Try again <span aria-hidden="true">↻</span></button><a class="button secondary" href="/lobby/lobby.html">Back to the game hub</a></section>`;
}

const screens = { home, waiting, writing, voting, reveal, scores, finished, account, error: errorScreen };

function signature(view) {
  if (view.screen === 'home') return JSON.stringify([view.screen, ownUid(view)]);
  if (view.screen === 'writing') return JSON.stringify([view.screen, view.room?.id, roundNumber(view), ownUid(view), (view.assignments || []).map(question => [question.id, question.text])]);
  if (view.screen === 'voting') return JSON.stringify([view.screen, view.room?.id, roundNumber(view), view.room?.questionIndex, currentQuestion(view).text, (view.answers || []).map(answer => [answer.id, answer.uid, answer.text])]);
  return '';
}

/** Render a view without discarding text input/focus when live state changes. */
export function render(view, actions = {}) {
  const root = document.getElementById('app');
  if (!root) return;
  currentView = { ...(view || {}) };
  currentActions = actions;
  bindRoot(root);
  const nextSignature = signature(currentView);
  if (nextSignature && nextSignature === currentSignature && root.querySelector('[data-live-screen]')) {
    patchLiveView(currentView);
    return;
  }
  currentSignature = nextSignature;
  if (currentView.screen === 'loading' || !currentView.screen) {
    root.innerHTML = '<main class="boot-screen" aria-busy="true"><span class="loading-dot" aria-hidden="true"></span><h1>Quip Clash</h1><p>Getting the party ready…</p></main>';
    return;
  }
  const screen = screens[currentView.screen] || errorScreen;
  root.innerHTML = `<div class="shell">${header(currentView)}${roomStrip(currentView)}<main data-live-screen="${escape(currentView.screen)}">${currentView.screen === 'error' ? '' : notice(currentView)}${connectionNotice(currentView)}${screen(currentView)}</main></div>`;
  root.querySelectorAll('textarea[data-question]').forEach(autoSize);
  if (currentView.screen === 'home' && currentView.createName) root.querySelector('[name="name"]').value = currentView.createName;
  updateTimer(currentView.secondsLeft);
}

function patchLiveView(view) {
  const root = mountedRoot;
  const noticeNode = root.querySelector('[data-notice]');
  if (noticeNode) {
    const temporary = document.createElement('div');
    temporary.innerHTML = notice(view);
    if (noticeNode.innerHTML !== temporary.firstElementChild.innerHTML) noticeNode.innerHTML = temporary.firstElementChild.innerHTML;
  }
  const connection = root.querySelector('.connection');
  if (connection) {
    connection.classList.toggle('offline', view.connected === false);
    connection.textContent = view.connected === false ? 'Reconnecting' : 'Live';
  }
  const existingBanner = root.querySelector('.connection-banner');
  if (view.connected === false && !existingBanner) {
    const node = document.createElement('div');
    node.innerHTML = connectionNotice(view);
    root.querySelector('[data-live-screen]')?.prepend(node.firstElementChild);
  } else if (view.connected !== false) existingBanner?.remove();
  if (view.screen === 'home') root.querySelectorAll('form button[type="submit"]').forEach(button => { button.disabled = Boolean(view.busy); });
  if (view.screen === 'writing') {
    const submitted = Boolean(view.submitted);
    root.querySelectorAll('textarea[data-question]').forEach(textarea => {
      textarea.readOnly = submitted;
      const description = textarea.parentElement.querySelector('.answer-meta > span');
      if (description) description.textContent = submitted ? '✓ Answer locked in' : 'Room for a whole ridiculous idea.';
    });
    const hint = root.querySelector('[data-submit-hint]');
    if (hint) hint.innerHTML = submitted ? '<strong class="submitted-title">✓ Your answers are locked in.</strong>Now sit back. The voting starts when everyone is done.' : `<strong>Happy with the chaos?</strong>${(view.assignments || []).length === 1 ? 'Lock in your answer' : 'Lock in both answers'}, or keep writing until time runs out.`;
    const help = root.querySelector('.writing-help');
    if (help) help.textContent = submitted ? 'Your answers will appear without your name until the votes are in.' : 'Your saved text goes into the vote when time runs out. Lock in to confirm your answers.';
    updateSubmitButton();
  }
  if (view.screen === 'voting') {
    const title = root.querySelector('.stage-top h1');
    if (title) title.textContent = view.voted ? 'Excellent taste. Vote locked.' : 'Which answer wins?';
    const subtitle = root.querySelector('.stage-top > div > p:last-child');
    if (subtitle) subtitle.textContent = view.voted ? 'The votes are coming in. The reveal is almost here.' : 'Pick the answer that deserves the applause. You get one vote.';
    root.querySelectorAll('[data-action="vote"]').forEach(button => {
      const own = button.dataset.own === 'true';
      button.disabled = own || Boolean(view.voted) || Boolean(view.busy) || view.eligible === false || Number(view.secondsLeft) <= 0;
      const cta = button.querySelector('[data-answer-cta]');
      if (cta) cta.textContent = own ? 'Your answer · admire it proudly' : view.voted ? 'Your vote is locked in' : 'Tap to vote for this answer';
      const arrow = button.querySelector('.answer-arrow');
      if (arrow) arrow.textContent = own ? '✦' : view.voted ? '✓' : '↗';
    });
    const instruction = root.querySelector('[data-vote-instruction]');
    if (instruction) instruction.innerHTML = votingInstruction(view);
  }
  const done = Math.max(0, Number(view.progress?.done) || 0);
  const total = Math.max(0, Number(view.progress?.total) || 0);
  const text = root.querySelector('[data-progress-text]');
  if (text) text.textContent = `${done} / ${total}`;
  const bar = root.querySelector('[data-progress]');
  if (bar) { bar.setAttribute('aria-valuenow', done); bar.setAttribute('aria-valuemax', total); }
  const fill = root.querySelector('[data-progress-fill]');
  if (fill) fill.style.width = `${total ? Math.min(100, done / total * 100) : 0}%`;
  updateTimer(view.secondsLeft);
}

/** Update the clock in place; safe to call every second while someone is typing. */
export function updateTimer(seconds) {
  const value = Math.max(0, Math.ceil(Number(seconds) || 0));
  currentView.secondsLeft = value;
  document.querySelectorAll('[data-timer]').forEach(clock => {
    clock.classList.toggle('urgent', value <= 10);
    clock.setAttribute('aria-label', `${value} seconds left`);
    const number = clock.querySelector('[data-seconds]');
    if (number) number.textContent = value;
    const label = clock.querySelector('[data-timer-label]');
    if (label) label.textContent = value === 0 ? 'wrapping up' : 'seconds left';
  });
  document.querySelectorAll('[data-auto-seconds]').forEach(number => { number.textContent = value; });
  if (value === 0) document.querySelectorAll('[data-timed-control]').forEach(control => { control.disabled = true; });
}

function autoSize(textarea) {
  const scrollTop = textarea.scrollTop;
  textarea.style.height = 'auto';
  textarea.style.height = `${Math.min(textarea.scrollHeight + 2, window.innerWidth <= 560 ? 320 : 352)}px`;
  textarea.scrollTop = scrollTop;
}

function updateSubmitButton() {
  const button = mountedRoot?.querySelector('[data-submit-answers]');
  if (!button) return;
  const inputs = [...mountedRoot.querySelectorAll('textarea[data-question]')];
  button.disabled = Boolean(currentView.submitted) || Boolean(currentView.busy) || Number(currentView.secondsLeft) <= 0 || !inputs.length || inputs.some(textarea => !textarea.value.trim());
  button.textContent = currentView.submitted ? 'Answers locked ✓' : inputs.length === 1 ? 'Lock in my answer →' : 'Lock in my answers →';
}

function showToast(message) {
  document.querySelector('.copy-toast')?.remove();
  clearTimeout(toastTimeout);
  const toast = document.createElement('div');
  toast.className = 'copy-toast';
  toast.setAttribute('role', 'status');
  toast.textContent = message;
  document.body.append(toast);
  toastTimeout = setTimeout(() => toast.remove(), 3000);
}

async function runAction(name, args, control) {
  const action = currentActions[name];
  if (typeof action !== 'function') return;
  const alreadyDisabled = control?.disabled;
  if (control) { control.disabled = true; control.setAttribute('aria-busy', 'true'); }
  try {
    await action(...args);
    if (name === 'copyCode') showToast('Room invitation ready to share');
  } catch (error) {
    showToast(error?.message || 'That didn’t go through. Please try again.');
  } finally {
    if (control?.isConnected) {
      control.removeAttribute('aria-busy');
      control.disabled = Boolean(alreadyDisabled);
      if (currentView.screen === 'writing') updateSubmitButton();
      if (currentView.screen === 'voting') patchLiveView(currentView);
    }
  }
}

function bindRoot(root) {
  if (mountedRoot === root) return;
  mountedRoot = root;
  root.addEventListener('click', event => {
    const control = event.target.closest('[data-action]');
    if (!control || !root.contains(control) || control.disabled) return;
    const name = control.dataset.action;
    if (name === 'refresh') { window.location.reload(); return; }
    if (name === 'vote' && (currentView.voted || currentView.eligible === false || control.dataset.own === 'true' || Number(currentView.secondsLeft) <= 0)) return;
    void runAction(name, name === 'vote' ? [control.dataset.answer] : [], control);
  });
  root.addEventListener('submit', event => {
    const form = event.target.closest('form[data-form]');
    if (!form) return;
    event.preventDefault();
    const button = form.querySelector('button[type="submit"]');
    if (button?.disabled) return;
    if (form.dataset.form === 'create') void runAction('create', [String(new FormData(form).get('name') || '').trim()], button);
    if (form.dataset.form === 'join') void runAction('join', [String(new FormData(form).get('code') || '').trim()], button);
    if (form.dataset.form === 'answers') {
      if (currentView.submitted || Number(currentView.secondsLeft) <= 0) return;
      const answers = Object.fromEntries([...form.querySelectorAll('textarea[data-question]')].map(textarea => [textarea.dataset.question, textarea.value]));
      void runAction('submit', [answers], button);
    }
  });
  root.addEventListener('input', event => {
    const textarea = event.target.closest('textarea[data-question]');
    if (!textarea || textarea.readOnly) return;
    autoSize(textarea);
    const count = textarea.parentElement.querySelector('[data-count-for]');
    if (count) { count.textContent = `${textarea.value.length} / 1,000`; count.classList.toggle('near-limit', textarea.value.length >= 900); }
    updateSubmitButton();
    if (typeof currentActions.draft === 'function') currentActions.draft(textarea.dataset.question, textarea.value);
  });
  window.addEventListener('resize', () => mountedRoot?.querySelectorAll('textarea[data-question]').forEach(autoSize), { passive: true });
}
