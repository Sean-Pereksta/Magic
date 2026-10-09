# Lobby profiles and member dashboard

`lobby/lobby.html` loads `lobby/lobby-core.html`, where the optional profile prompt and dashboard live.

First-time guests see the Create Profile tab with “No email required!” and a warning to use a unique password. Closing with the X, Continue as guest, Escape, or the backdrop leaves the lobby available. Local storage remembers the prompt; session storage is a fallback. Existing players and active invitation previews take priority. The account dialog uses the visual viewport height, a fixed header with a 44px close button, and a scrolling body so the close control remains visible on small screens and when the keyboard reduces the viewport.

The exact account username `Sean` can unlock a read-only member activity dashboard from its own Profile page. A successful manual Sean login unlocks it for that page session; a remembered username alone does not. Unlocking otherwise requires checking Sean's password again. Lock and logout clear loaded rows, and stale requests cannot restore them.

The dashboard reads all registered accounts, including those missing `lastLogin`, without the community dropdown's 50-member cap. It supports name search, sorting, last login, join date, wins, recorded launch totals, per-game launch counts, and stored recent activity. Missing historical counts are labeled “Not recorded.” Totals are based on `gamePlayCounts`; they are not completed matches and do not include untracked launches outside the lobby. Only explicit activity fields are retained in dashboard state; passwords and authentication identifiers are not rendered.

## Access limitation

This feature is a convenience dashboard, not a new server-enforced administrator role. The existing Firestore rules allow authenticated anonymous browser sessions to read and write `/users` documents, and the legacy account system checks passwords in the browser. A client-side password check cannot create an exclusive security boundary. Secure privileged operations or confidential reports require trusted account authentication and server-side authorization. This change adds no member-editing, deletion, role-granting, or other privileged operation and does not broaden database rules.

## Validation

Run `node --experimental-vm-modules --test lobby/tests/*.test.cjs lobby/tests/*.test.mjs` from the repository root.
