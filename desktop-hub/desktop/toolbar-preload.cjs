const {contextBridge,ipcRenderer} = require('electron');
contextBridge.exposeInMainWorld('gameWindow', {
  control: action => ipcRenderer.send('game-control',action),
  onStatus: handler => ipcRenderer.on('game-status',(_,text)=>handler(text))
});
