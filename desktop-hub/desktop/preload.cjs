const { contextBridge, ipcRenderer } = require('electron');
contextBridge.exposeInMainWorld('hub', {
  invoke: (name, data = {}) => ipcRenderer.invoke('hub', name, data),
  onChange: handler => {
    const notify = () => handler();
    ipcRenderer.on('changed', notify);
    return () => ipcRenderer.removeListener('changed', notify);
  }
});
