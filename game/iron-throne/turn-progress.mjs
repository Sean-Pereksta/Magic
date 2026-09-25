export function resolveTurnInWorker(state, onProgress) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL('./turn-worker.mjs', import.meta.url), {type:'module'});
    const fail = message => { worker.terminate(); reject(new Error(message)); };
    worker.onerror = event => { event.preventDefault(); fail('Turn processing could not start or finish.'); };
    worker.onmessageerror = () => fail('Turn results could not be read.');
    worker.onmessage = ({data}) => {
      if (data.type === 'progress') {
        // Complete only after the authoritative result has arrived and rendered.
        if (data.progress.phase !== 'complete') onProgress(data.progress);
      } else if (data.type === 'complete') {
        worker.terminate(); resolve(data.state);
      } else if (data.type === 'error') fail(data.message);
    };
    try { worker.postMessage(state); } catch (error) { fail(error.message); }
  });
}

export function showTurnProgress(progress) {
  const container = document.getElementById('turn-progress');
  const label = document.getElementById('turn-progress-label');
  const bar = document.getElementById('turn-progress-bar');
  container.hidden = !progress;
  if (!progress) return;
  const {phase,completed,total,ended} = progress;
  label.textContent = phase === 'ai' ? `AI turns: ${completed} / ${total}` :
    phase === 'complete' ? (ended ? 'Campaign complete' : 'Your turn') :
    phase === 'resolving' ? 'Resolving round…' : 'Preparing AI turns…';
  // This measures completed Houses, not a misleading estimate of seconds left.
  if (phase === 'ai') { bar.max = Math.max(1,total); bar.value = completed; }
  else if (phase === 'complete') { bar.max = 1; bar.value = 1; }
  else bar.removeAttribute('value');
}
