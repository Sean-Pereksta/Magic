/* Log damage art follows the existing frontal bullet shield and its real durability. */
(() => {
  'use strict';
  ATSRenderer.prototype.drawLogShield = function (c, ape) {
    const image = window.ATSVisualAssets?.get('equipment-logs');
    if (!image || !(ape.shield?.hp > 0)) return false;
    const ratio = ape.shield.hp / ape.shield.maxHp;
    const stage = ratio < .3 ? 2 : ratio < .65 ? 1 : 0;
    const frame = window.ATSVisualAssets.manifest.equipment.frames[stage];
    const sockets = this.characterAttachment?.(ape, false);
    const mirror = sockets ? sockets.mirror : Math.cos(ape.dir || 0) < 0;
    const scale = sockets?.scale || 1;
    const x = sockets ? (sockets.left.x + sockets.right.x) / 2 + 10 : 10;
    const y = sockets ? (sockets.left.y + sockets.right.y) / 2 - 9 : -24;
    const sway = this.reducedMotion || !ape.moving ? 0 : Math.sin(this.time * 7 + (ape.phase || 0));
    c.save();
    c.scale((mirror ? -1 : 1) * scale, scale);
    c.translate(x, y + sway * 1.2);
    c.rotate(sway * .025);
    c.drawImage(image, frame.x, frame.y, frame.w, frame.h, -27, -28, 54, 54);
    c.restore();
    return true;
  };
})();
