'use strict';

/* Focused gameplay, input, equipment and menu suites retain their original
 * twelve-clip GLB fixtures and synchronous game-start contract. Disable only
 * the optional v2 adapter in those fixtures; do not change game logic or any
 * assertions. browser-smoke.cjs and graphics-v2-*.cjs cover the enabled adapter,
 * asynchronous startup, current assets and loading/disposal lifecycle.
 * Install after broad page routes. The route also survives page reloads.
 */
const installed=new WeakSet();
async function installLegacyGraphicsFixture(page){
  if(installed.has(page))return;
  await page.route(/\/game\/basketball2\/assets\/graphics-v2-config\.js(?:\?|$)/,route=>route.fulfill({
    contentType:'text/javascript',
    body:'window.BASKETBALL2_GRAPHICS = { enabled: false };'
  }));
  installed.add(page);
}
module.exports={installLegacyGraphicsFixture};
