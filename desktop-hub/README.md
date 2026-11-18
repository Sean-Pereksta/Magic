# Catnmice Game Hub

The Windows x64 launcher uses Magic’s published `lobby/lobby.html` and its generated game cards to open online games. The bundled catalog also includes the Truth-hosted games. Web sign-in, invitations, favorites and game-specific account gates run in a persistent Catnmice browser session. The game window has a Library button and F11 fullscreen. Alt+Left returns from fullscreen to the library. Offline games run on a per-title loopback origin with external requests blocked. Each local player has a separate save directory.

## Profiles and cloud saves

Under Player profiles, Log In and Create Profile open the website's account form. Players with an existing ranked binding can choose **Use verified browser profile**, enter the launcher's code on the official website, and approve the device. This carries the verified identity into its game session. The profile remains selectable for offline games.

When connected, the detail pages for Bramblebound, Dwarf World and Space Tyrants offer **Sync game saves**. Choose the game's existing cloud Save or Reopen control, slot and password. These games manage their own encrypted slots and conflicts.

## Build on Windows

Use 64-bit Node 24, Python 3.12 and full checkouts of Magic and Truth. For the reviewed preview, the two source revisions are recorded in `content/catalog.json`. From this directory:

```powershell
npm ci
npm ci --prefix desktop/provenance --ignore-scripts
node node_modules/electron/install.js
node content/build-catalog.mjs .. C:/src/Truth
python content/build-packs.py .. C:/src/Truth
node content/build-catalog.mjs .. C:/src/Truth
npm test
npm run build
```

Setup is written to `dist/Catnmice-Game-Hub-Setup.exe`. It includes the Chromium runtime, artwork and selectable offline packs. The component selector shows disk space before installation. Rerun Setup to add or repair games. Windows Installed Apps includes an uninstall entry. Uninstall offers to keep local data. The original setup file can add a bundled pack later without Internet access.

## Release and website integration

Magic hosts the Windows download page at `/hub/download.html`, the featured download panel in the lobby, the desktop sources and the official match-service code. The paired Truth changes add browser gameplay reporting and a ranked Bannerlands entry. The download page checks the stable GitHub release for the installer before displaying its download button.

Publish your game changes to the main branch of Magic or Truth. Changes to the relevant Magic files start the Windows workflow. Every six hours, Magic also checks both repositories for changes since the last published build. An existing Truth dispatch can bring a check forward. The scheduled check works without a cross-repository token. Each build records the exact commit from both repositories so they can be inspected together.

After the Windows tests pass, GitHub publishes the full installer and the component packages. GitHub signs the release manifest using the identity of Magic's release workflow. This works on the public repository without a paid certificate, a personal signing key, or an account for each player. Windows may show **Unknown publisher** when a player opens Setup. A maintainer can set `CATNMICE_AUTO_PUBLISH` to `false` to pause automatic publication.

Players who installed the early preview must run the new Setup once. With **Automatic updates** enabled, the hub then checks GitHub at startup and every four hours. **Check for updates** runs the same check immediately. The hub verifies the workflow signature and each downloaded package, updates changed installed games, and waits for an active game to close before switching files. Versioned game files and saves support recovery after a failed update. An update to the bundled Chromium runtime requires the latest full Setup.

## Verification

Run `node build/check-online.cjs <installed-exe> <fresh-profile-directory>` against the published website. Run `node build/smoke.cjs <installed-exe> <another-fresh-profile-directory> --offline-all` after selecting all packs in Setup. The source validation record in `content/release-coverage.json` lists each title, its offline eligibility, and screenshot coverage. `services/README.md` covers the shared ranking, ownership and lifetime-count service.

Before approving a public release, record clean-machine offline saves and installation checks, and complete an official match in each flagship from an installed desktop and a browser on separate networks. Compare both clients’ account, invite, Elo and match receipt, and repeat the offline and update-recovery checks during an outage.
