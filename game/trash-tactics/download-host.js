const repository = 'https://github.com/Sean-Pereksta/Magic';
const filename = 'Trash-Tactics-Host-Windows-x64.zip';

async function linkLatestHost() {
  try {
    const response = await fetch('https://api.github.com/repos/Sean-Pereksta/Magic/releases?per_page=50', {
      signal: AbortSignal.timeout(5000),
    });
    if (!response.ok) return;
    const releases = await response.json();
    const release = releases.find(row => /^trash-tactics-host-v[\d]+\.[\d]+\.[\d]+(?:-[\w.-]+)?$/.test(row.tag_name) &&
      row.assets?.some(asset => asset.name === filename));
    if (!release) return;

    const url = repository + '/releases/download/' + release.tag_name + '/' + filename;
    const download = document.querySelector('#host-download');
    download.href = url;
    download.textContent = 'Download for Windows';
    document.querySelector('#host-version').textContent = 'Windows x64 · Portable ZIP · ' +
      release.tag_name.replace('trash-tactics-host-', '');
    if (release.assets.some(asset => asset.name === filename + '.sha256')) {
      const checksum = document.querySelector('#host-checksum');
      checksum.href = url + '.sha256';
      checksum.hidden = false;
    }
  } catch {
    // Keep the GitHub releases link available when the API is unreachable.
  }
}

linkLatestHost();
