import {PHYSICS} from '../config.js';
import {PHYSICS_PROOF_SIGNATURE} from '../physics.js';

function fingerprint(text){let hash=2166136261;for(let i=0;i<text.length;i++){hash^=text.charCodeAt(i);hash=Math.imul(hash,16777619);}return(hash>>>0).toString(16);}
// Normalize source whitespace so a CRLF checkout and an LF static deployment
// share proofs. Changes to controller code, constants or room geometry expire them.
const physicsKey=fingerprint(JSON.stringify(PHYSICS)+PHYSICS_PROOF_SIGNATURE);
export function routeProofKey(room){return `${room.id}:${physicsKey}:${fingerprint(JSON.stringify([room.platforms,room.movers,room.hazards,room.switches,room.validationWaypoints]))}`;}
