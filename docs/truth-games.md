# Truth-hosted games

`lobby/lobby.html` registers six browser games hosted by the Truth repository at `https://sean-pereksta.github.io/Truth/games/`. Keep those game files in Truth and their catalog entries and tile images in Magic.

| Catalog key | Truth folder | Tile source |
| --- | --- | --- |
| `bannerlands` | `bannerlands` | Screenshot of the supplied current title screen |
| `monstermallexpanded` | `monster-mall-expanded` | Screenshot of the supplied current title screen |
| `pacmanblackout` | `pacman` | Screenshot of the supplied current title screen |
| `pirates` | `pirates` | `Pirates/Source/dist/assets/cover.png` |
| `academystandards` | `academy-standards` | `Academy Standards/menu-art.png` |
| `operationwhiskerfall` | `operation-whiskerfall` | `Operation-Whiskerfall/assets/ui/meowquest-cover.png` |

Artwork is reused from the supplied games and resized to 640×360 tiles and 240×135 thumbnails. Both `game-artwork.json` and `game-artwork.js` contain the same entries; increment the artwork script version in `lobby-core.html` when updating them.

All six launch with the shared library's guest-accessible Play action. Operation Whiskerfall supports a second player on the same device; it does not use the website's online lobbies. The supplied Monster Mall Expanded Edition build displays “After Hours” inside the game. Pac-Man: Blackout is a strategy game with base building and fog of war.
