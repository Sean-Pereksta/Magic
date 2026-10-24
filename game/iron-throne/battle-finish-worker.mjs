import { finishAutomatically, validateBattleCheckpoint } from './battle-sim.mjs';

// Automatic completion can contain thousands of simulation steps. Keep the
// paused interface responsive and retain the persisted checkpoint on failure.
self.onmessage = ({data}) => {
  try {
    if(!validateBattleCheckpoint(data.checkpoint,data.encounter))throw new Error('The saved battle checkpoint is invalid.');
    const result=finishAutomatically(data.checkpoint);
    self.postMessage({result});
  } catch(error) { self.postMessage({error:error.message}); }
};
