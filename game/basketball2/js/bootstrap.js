buildMenu();
init();
riseState=RiseToGlory.load();
buildRiseUI();
bootstrapIntegrations();
if(typeof loadPlayerModels==="function") loadPlayerModels();
animate();
