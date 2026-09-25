import { endTurn } from './diplomacy.mjs';

// Work on a structured clone: failure never commits a partial round to the UI.
self.onmessage = ({data: state}) => {
  try {
    endTurn(state, progress => self.postMessage({type:'progress',progress}));
    self.postMessage({type:'complete',state});
  } catch (error) {
    self.postMessage({type:'error',message:error.message});
  }
};
