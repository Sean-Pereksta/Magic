const params = new URLSearchParams(location.search);
document.getElementById('title').textContent = params.get('title') || 'Catnmice';
for (const id of ['back','reload','full']) document.getElementById(id).onclick = () => window.gameWindow.control(id);
window.gameWindow.onStatus(value => document.getElementById('status').textContent = value);
