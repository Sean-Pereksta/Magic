const params = new URLSearchParams(location.search);
document.getElementById('title').textContent = params.get('title') || 'Catnmice';
for (const id of ['back','reload','full','local-edition']) document.getElementById(id).onclick = () => window.gameWindow.control(id);
window.gameWindow.onStatus(({msg,showLocalEdition}) => {
  document.getElementById('status').textContent = msg;
  document.getElementById('local-edition').hidden = !showLocalEdition;
});
