from pathlib import Path

path = Path("game/turncraft.html")
text = path.read_text(encoding="utf-8")


def replace_once(old: str, new: str, label: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"Expected exactly one {label} target, found {count}")
    text = text.replace(old, new, 1)


replace_once(
    "const list=selectedEntities(),e=list[0],group=list.length>1,clicked=selectableEntityAt(t.x,t.y,true);if(interactionMode==='orbitalScan'&&e){",
    "const list=selectedEntities(),e=list[0],group=list.length>1,clicked=selectableEntityAt(t.x,t.y,true),friendlyClicked=clicked&&clicked.faction===PLAYER,abilityTargeting=interactionMode==='ability'||interactionMode==='orbitalScan';if(friendlyClicked&&!abilityTargeting){selectEntity(clicked);interactionMode=null;interactionPayload=null;legalTiles=[];return}if(interactionMode==='orbitalScan'&&e){",
    "friendly selection priority",
)

replace_once(
    "function tryStrategicHold(sx,sy){if(!gameStarted||paused||interactionMode)return false;const t=screenToTile(sx,sy);if(!inBounds(t.x,t.y))return false;const list=selectedEntities().filter(ownEntity)",
    "function tryStrategicHold(sx,sy){if(!gameStarted||paused||interactionMode)return false;const t=screenToTile(sx,sy);if(!inBounds(t.x,t.y))return false;const clicked=selectableEntityAt(t.x,t.y,true);if(clicked&&clicked.faction===PLAYER)return false;const list=selectedEntities().filter(ownEntity)",
    "strategic hold friendly guard",
)

path.write_text(text, encoding="utf-8")
print(f"Patched {path} ({len(text):,} characters)")
