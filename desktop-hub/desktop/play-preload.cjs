const {ipcRenderer} = require('electron');
const surfaces='canvas,[data-gameplay-surface],#board,#gameBoard,#game-board,#battlefield,#hand,#playerHand,#playArea,.game-board,.chessboard,.game-grid,.game-cell,.hand-card,.battle-grid,.board-cell,.tile-grid';
const signal=()=>ipcRenderer.send('played-input',document.querySelector('script[data-catnmice-game]')?.dataset.catnmiceGame);
// Only trusted input on a rendered play surface is activity. Opening a room or reading a menu is not a session.
const send = event => {
  if (!event.isTrusted || (event.type === 'keydown' && !/^(Arrow(Up|Down|Left|Right)|Key[WASD]|Space)$/.test(event.code))) return;
  if (event.target?.closest?.('button,input,select,textarea,[role="dialog"],a')) return;
  const surface = event.type === 'keydown' ? [...document.querySelectorAll('canvas')].find(c => {const r=c.getBoundingClientRect();return r.width>=160 && r.height>=100 && document.elementFromPoint(r.x+r.width/2,r.y+r.height/2)===c;}) : event.target?.closest?.(surfaces);
  if (surface && !/preview|avatar|portrait|thumbnail/i.test(surface.id+' '+surface.className))signal();
};
window.addEventListener('catnmice-game-started',()=>{if(document.querySelector('script[data-confirmed="engine"]'))signal();});
window.addEventListener('pointerdown',send,true);
window.addEventListener('keydown',send,true);
