import { finishAutomatically, validateBattleCheckpoint } from './battle-sim.mjs';

// postMessage gives the worker a private copy of the starting battlefield.
self.onmessage = ({data}) => {
  try {
    const battle=data.battle??data.checkpoint; // Accept requests from an already-open battle tab.
    if(!validateBattleCheckpoint(battle,data.encounter))throw new Error('The battlefield could not be resolved from its current position.');
    const result=finishAutomatically(battle);
    self.postMessage({result});
  } catch(error) { self.postMessage({error:error.message}); }
};
