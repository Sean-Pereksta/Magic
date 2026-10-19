/* One display/contact scale for the imported athletes and lightweight fallback. */
(function(root){
  'use strict';
  const actorScale=1.4;
  const api=Object.freeze({
    actorScale,
    referenceHeightM:1.88,
    referenceModelHeight:2.27,
    worldUnitsPerMetre:2.27/1.88*actorScale,
    eyeHeight:2.25*actorScale,
  });
  if(typeof module!=='undefined')module.exports=api;
  root.QBPlayerScale=api;
})(typeof globalThis!=='undefined'?globalThis:this);
