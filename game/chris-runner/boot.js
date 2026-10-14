/* No world, preview, or game loop is created until the complete pack is ready. */
(function (global) {
  'use strict';
  let initialized=false,launching=null,lastStatus=null,paintQueued=false;
  const get=id=>document.getElementById(id);
  const mb=bytes=>(bytes/1000000).toFixed(1)+' MB';
  function paint(status){
    const progress=get('runnerLoadProgress'),title=get('runnerLoadStatus'),detail=get('runnerLoadDetail');
    const ratio=status.totalBytes?status.loadedBytes/status.totalBytes:0;
    progress.value=status.phase==='ready'?100:Math.min(99,Math.round(ratio*100));
    if(status.phase==='error'){title.textContent='The chase needs a moment';detail.textContent=status.error;return;}
    title.textContent=status.phase==='manifest'?'Checking the starting line':status.phase==='ready'?'Ready to run!':'Warming up the chase';
    detail.textContent=status.totalBytes?`${status.loaded} / ${status.total} models · ${mb(status.loadedBytes)} / ${mb(status.totalBytes)}`:'Getting your runners, worlds, and obstacles ready.';
  }
  function update(status){lastStatus=status;if(!paintQueued){paintQueued=true;requestAnimationFrame(()=>{paintQueued=false;paint(lastStatus);});}}
  function enableMenu(enabled){document.querySelectorAll('#menuButtons button, #characterGrid button').forEach(button=>button.disabled=!enabled);get('menuCard').setAttribute('aria-busy',String(!enabled));get('menuOverlay').inert=!enabled;}
  function launch(retry=false){
    if(launching||initialized)return launching||Promise.resolve();
    const overlay=get('runnerLoadOverlay'),retryButton=get('runnerLoadRetry');
    overlay.hidden=false;enableMenu(false);retryButton.hidden=true;get('runnerLoadStatus').textContent='Warming up the chase';get('runnerLoadDetail').textContent='Getting your runners, worlds, and obstacles ready.';
    launching=Promise.resolve().then(()=>{
      if(!global.RunnerGraphics)throw Error('The graphics loader could not start. Reload this page to try again.');
      return (retry?global.RunnerGraphics.retry:global.RunnerGraphics.start)(update);
    }).then(()=>{
      init();animate();initialized=true;enableMenu(true);overlay.hidden=true;
      if(document.getElementById('menuOverlay').style.display!=='none')document.querySelector('#menuButtons button')?.focus({preventScroll:true});
      document.dispatchEvent(new CustomEvent('runner:ready'));
    }).catch(error=>{
      update({...global.RunnerGraphics?.status,phase:'error',error:error.message});
      get('runnerLoadStatus').textContent='The chase needs a moment';
      get('runnerLoadDetail').textContent=error.message;
      retryButton.hidden=false;retryButton.focus({preventScroll:true});
      console.error('Chris Runner startup:',error);
    }).finally(()=>{launching=null;});
    return launching;
  }
  global.bootRunnerPage=function(){get('runnerLoadRetry').addEventListener('click',()=>launch(true));return launch();};
})(window);
