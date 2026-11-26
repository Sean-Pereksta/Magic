# Catnmice Game Hub

The Windows x64 launcher uses Magic’s published game catalog, which includes games from Truth. Bannerlands, Cat & Mouse and Trash Tactics include their Windows Local Editions with a match server and Cloudflare runtime. Last Signal includes its native Windows game and hosting runtime. Play and Host Lobby for these titles start their installed packs. Other online games, official web sign-in and ranked account gates use the persistent Catnmice browser session. The game window has a Library button and F11 fullscreen. Alt+Left returns from fullscreen to the library. Each local player has a separate save directory.

## Profiles and cloud saves

Under Player profiles, Log In and Create Profile open the website's account form. Players with an existing ranked binding can choose **Use verified browser profile**, enter the launcher's code on the official website, and approve the device. This carries the verified identity into its game session. The profile remains selectable for offline games.

When connected, the detail pages for Bramblebound, Dwarf World and Space Tyrants offer **Sync game saves**. Choose the game's existing cloud Save or Reopen control, slot and password. These games manage their own encrypted slots and conflicts.

## Build on Windows

Use Windows x64 Node 24.19.0, Python 3.12 and full checkouts of Magic and Truth. The build verifies the pinned cloudflared 2026.11.0 Windows x64 executable before packaging it. The two source revisions are recorded in `content/catalog.json`. From this directory:

```powershell
npm ci
npm ci --prefix desktop/provenance --ignore-scripts
node node_modules/electron/install.js
node content/build-catalog.mjs .. C:/src/Truth
$lastSignal = Join-Path $env:TEMP 'last-signal-build'
& C:/src/Truth/games/last-signal/Build-Windows.ps1 -Destination $lastSignal
& C:/src/Truth/games/last-signal/tests/Install-SoftwareGraphics.ps1 -GameDirectory $lastSignal
$cloudflared = Join-Path $env:TEMP 'catnmice-cloudflared.exe'
Invoke-WebRequest -Uri 'https://github.com/cloudflare/cloudflared/releases/download/2026.11.0/cloudflared-windows-amd64.exe' -OutFile $cloudflared
python content/build-packs.py .. C:/src/Truth --last-signal-build "$lastSignal" --cloudflared "$cloudflared"
node content/build-catalog.mjs .. C:/src/Truth
npm test
npm run build
```

Setup is written to `dist/Catnmice-Game-Hub-Setup.exe`. Bannerlands, Cat & Mouse, Trash Tactics and Last Signal are included by default and required in Setup. The component selector shows disk space and lets players choose additional offline games. Rerun Setup to repair games. Windows Installed Apps includes an uninstall entry. Uninstall offers to keep local data. The original setup file can add a bundled pack later without Internet access.

## Release and website integration

Magic hosts the Windows download page at `/hub/download.html`, the featured download panel in the lobby, the desktop sources and the official match-service code. The paired Truth changes add browser gameplay reporting and a ranked Bannerlands entry. The download page checks the stable GitHub release for the installer before displaying its download button.

Publish your game changes to the main branch of Magic or Truth. Changes to the relevant Magic files start the Windows workflow. Every six hours, Magic also checks both repositories for changes since the last published build. An existing Truth dispatch can bring a check forward. The scheduled check works without a cross-repository token. Each build records the exact commit from both repositories so they can be inspected together.

After the Windows tests pass, GitHub publishes the full installer and the component packages. GitHub signs the release manifest using the identity of Magic's release workflow. This works on the public repository without a paid certificate, a personal signing key, or an account for each player. Windows may show **Unknown publisher** when a player opens Setup. A maintainer can set `CATNMICE_AUTO_PUBLISH` to `false` to pause automatic publication.

Players who installed the early preview must run the new Setup once. With **Automatic updates** enabled, the hub then checks GitHub at startup and every four hours. **Check for updates** runs the same check immediately. The hub verifies the workflow signature and each downloaded package, updates changed installed games, and waits for an active game to close before switching files. Versioned game files and saves support recovery after a failed update. An update to the bundled Chromium runtime requires the latest full Setup.

## Verification

Run `node build/check-online.cjs <installed-exe> <fresh-profile-directory>` against the published website. Run `node build/smoke.cjs <installed-exe> <another-fresh-profile-directory> --offline-all` after selecting all packs in Setup. The source validation record in `content/release-coverage.json` lists each title, its offline eligibility, and screenshot coverage. `services/README.md` covers the shared ranking, ownership and lifetime-count service.

Before approving a public release, record clean-machine offline saves and installation checks, and complete an official match in each flagship from an installed desktop and a browser on separate networks. Compare both clients’ account, invite, Elo and match receipt, and repeat the offline and update-recovery checks during an outage.

## Server Manager

Open **Server Manager** from the Windows launcher and find Acorn Rally, Bannerlands, Trash Tactics, Cat & Mouse or Last Signal. Install the hosting pack if prompted, enter a server name, choose **Public listing** or **Four-digit join code**, and select **Start server**. Players can browse named public servers or use **Live Join** in the game's multiplayer menu. For a private session, share the four-digit code shown in Server Manager.

Each game can run its own server on the same laptop. Other laptops can host additional servers for the same game. Stop an individual server from its panel. Closing the Windows launcher stops all of its managed servers.

The laptop needs to stay on and awake. Run the standalone lobby launcher from Acorn Rally’s PC edition with `--no-open` for a Windows Task Scheduler setup that can run outside the Catnmice launcher. Set the task's execution time limit to indefinite.

Build the Windows packs from matching Magic and Truth revisions with the manager adapters and use the freshly compiled Last Signal executable. The signed launcher, catalog and game pack are required for download-channel installation.

### Server diagnostics

While a server is running, select **Server diagnostics** to see player occupancy,
capacity, match phase, queue length, public tunnel round-trip time, uptime, and
the status of the local host, tunnel, and lobby directory. Acorn Rally also
reports measured player ping, connection type, reconnecting seats, jitter,
snapshot delivery, FPS, and track loading. Tunnel round-trip time measures a
request from the host through its public Cloudflare address. A dash marks an
unavailable measurement.

The panel refreshes every three seconds and marks samples older than 12 seconds
as delayed. If the host has sent no samples after 15 seconds, the panel prompts
you to update its hosting pack.

Install both the signed launcher and updated hosting packs from **Downloads →
Check for updates**. Stop running hosts before updating their packs and start
the hosts again to publish to the new directory.

### Magic Servers Firestore permissions

Magic Servers (`magic-servers-1ca0c`) stores the five games' Server Manager
listings. In that project's Firebase Authentication settings, enable the
**Anonymous** sign-in provider. Publish the [Magic Servers rules](../game/server-manager/firebase/firestore.rules)
from the Firebase console or from the Magic repository root.

```powershell
firebase deploy --only firestore:rules --project magic-servers-1ca0c --config game/server-manager/firebase/firebase.json
```

Keep the existing rules and sign-in configuration in `bible-game-246c0` for
Acorn Rally's durable queue, official service, and the other Catnmice services.
If diagnostics show a ready public tunnel and an unavailable directory, check
Magic Servers' sign-in provider and active rules. Once the directory shows
**Listed**, check the same game's **Live Join** on a device outside the host's
network. Test a public listing and then a private four-digit code.
