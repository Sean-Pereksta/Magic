import path from 'node:path';
import {pathToFileURL} from 'node:url';

export async function start({packRoot,signal,onFailure}) {
  signal.throwIfAborted();
  const entry=path.join(packRoot,'game','trash-tactics','server.mjs');
  const {createTrashTacticsServer}=await import(pathToFileURL(entry).href);
  const game=createTrashTacticsServer({community:true,managed:true});
  const {server}=game;
  let closing;
  function close() {
    closing ||= game.close();
    return closing;
  }
  try {
    await new Promise((resolve,reject)=>{
      server.once('error',reject);
      server.listen(0,'127.0.0.1',()=>{
        server.off('error',reject);
        resolve();
      });
    });
    signal.throwIfAborted();
    server.on('error',error=>{if (!closing) onFailure?.(error);});
    server.once('close',()=>{if (!closing) onFailure?.(new Error('The Trash Tactics match server stopped.'));});
    return {
      origin:'http://127.0.0.1:'+server.address().port,
      getStatus:game.getStatus,
      joinPath:()=> '/?managed=1',
      close
    };
  } catch (error) {
    await close().catch(()=>{});
    throw error;
  }
}
