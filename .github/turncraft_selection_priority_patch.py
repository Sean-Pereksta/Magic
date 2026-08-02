from pathlib import Path

path = Path("game/turncraft.html")
text = path.read_text(encoding="utf-8")


def replace_once(old: str, new: str, label: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"Expected exactly one {label} target, found {count}")
    text = text.replace(old, new, 1)


friendly_old = "const list=selectedEntities(),e=list[0],group=list.length>1,clicked=selectableEntityAt(t.x,t.y,true);if(interactionMode==='orbitalScan'&&e){"
friendly_new = "const list=selectedEntities(),e=list[0],group=list.length>1,clicked=selectableEntityAt(t.x,t.y,true),friendlyClicked=clicked&&clicked.faction===PLAYER,abilityTargeting=interactionMode==='ability'||interactionMode==='orbitalScan';if(friendlyClicked&&!abilityTargeting){selectEntity(clicked);interactionMode=null;interactionPayload=null;legalTiles=[];return}if(interactionMode==='orbitalScan'&&e){"
if friendly_new not in text:
    replace_once(friendly_old, friendly_new, "friendly selection priority")

hold_original = "function tryStrategicHold(sx,sy){if(!gameStarted||paused||interactionMode)return false;const t=screenToTile(sx,sy);if(!inBounds(t.x,t.y))return false;const list=selectedEntities().filter(ownEntity)"
hold_cycle = "function tryStrategicHold(sx,sy){if(!gameStarted||paused||interactionMode)return false;const t=screenToTile(sx,sy);if(!inBounds(t.x,t.y))return false;const clicked=selectableEntityAt(t.x,t.y,true);if(clicked&&clicked.faction===PLAYER)return false;const list=selectedEntities().filter(ownEntity)"
hold_new = "function tryStrategicHold(sx,sy){if(!gameStarted||paused||interactionMode)return false;const t=screenToTile(sx,sy);if(!inBounds(t.x,t.y))return false;const clicked=selectableEntityAt(t.x,t.y,false);if(clicked&&clicked.faction===PLAYER)return false;const list=selectedEntities().filter(ownEntity)"
if hold_new not in text:
    if hold_cycle in text:
        replace_once(hold_cycle, hold_new, "non-cycling strategic hold guard")
    else:
        replace_once(hold_original, hold_new, "strategic hold friendly guard")

path.write_text(text, encoding="utf-8")
print(f"Patched {path} ({len(text):,} characters)")
