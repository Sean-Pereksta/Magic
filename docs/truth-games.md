# Truth-hosted games

`lobby/lobby.html` registers browser games hosted by the Truth repository at `https://sean-pereksta.github.io/Truth/games/`. Keep those game files in Truth and their catalog entries and tile images in Magic.

| Catalog key | Truth folder | Original tile scene |
| --- | --- | --- |
| `acornrally` | `acorn-rally` | Nib driving his orange number 07 kart through a mossy forest |
| `bannerlands` | `bannerlands` | Armored standard bearer leading armies toward a castle |
| `monstermallexpanded` | `monster-mall-expanded` | Stalker emerging beside a deserted mall fountain |
| `pacmanblackout` | `pacman` | Pac-Man commander and guardians in a haunted stone maze |
| `pirates` | `pirates` | Pirate galleon charging through tropical waves |
| `academystandards` | `academy-standards` | Four student adventurers overlooking their academy |
| `operationwhiskerfall` | `operation-whiskerfall` | Two feline resistance heroes in an alpine kingdom |

The artworks are original scenes created with built-in image generation. Screenshots of the supplied games served only as visual references for their worlds, characters and mood; menus, UI, titles and lettering were excluded. The complete prompts are recorded in `lobby/assets/truth-game-art-prompts.json`.

Versioned `-v2.webp` files provide 640×360 tiles, 240×135 thumbnails and 1280×720 heroes. The temporary JPEG screenshot/cover tiles were replaced. Both `game-artwork.json` and `game-artwork.js` contain the same entries and file hashes; increment the artwork script version in `lobby-core.html` when updating them.

The shared library launches each game for guests. Acorn Rally also opens its private-room screen and offers the Windows host download. Operation Whiskerfall supports a second player on the same device; it does not use the website's online lobbies. The supplied Monster Mall Expanded Edition build displays “After Hours” inside the game. Pac-Man: Blackout is a strategy game with base building and fog of war.
