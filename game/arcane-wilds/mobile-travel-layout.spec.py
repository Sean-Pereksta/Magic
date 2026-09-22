"""Chromium layout fixture, not a full-game or physical-device playtest.
Run: python mobile-travel-layout.spec.py (requires Python Playwright and Chromium).
Exercises final mobile CSS against the legacy campaign rules it must override.
"""
import itertools
import json
import os
from pathlib import Path
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent
LEGACY_CSS = '''
*{box-sizing:border-box}html,body{margin:0;width:100%;height:100%;overflow:hidden;background:#0d1b23;color:white;font:12px system-ui}
button{color:inherit;font:inherit}#hud{position:fixed;inset:0;pointer-events:none;z-index:10}
.hud-top{position:absolute;left:12px;right:12px;top:10px;display:flex;justify-content:space-between;align-items:flex-start;gap:10px}
.status-card,.room-card,.spell-bar,.mini-map,.gold-pill{background:#08101be8;border:1px solid #a9997340;box-shadow:0 8px 22px #0008}
.status-card{width:min(370px,48vw);padding:10px 12px;border-radius:16px}.status-line{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:5px;font-size:12px}.bar{height:12px;border-radius:999px;overflow:hidden;background:#111827}.bar>i{display:block;height:100%;width:70%;background:#ee5166}.xp>i{background:#6886ff}.status-bottom{display:flex;flex-wrap:wrap;gap:6px;margin-top:6px}.chip{padding:4px 7px;font-size:10px}
.room-card{min-width:220px;max-width:44vw;padding:9px 12px;border-radius:16px;text-align:right}.room-name{font-size:14px}.room-sub{font-size:10px;margin-top:3px}.room-state{display:inline-block;font-size:9px;padding:4px 8px;margin-top:6px}
#spells{position:absolute;left:50%;bottom:14px;transform:translateX(-50%);display:flex;gap:8px;padding:8px;pointer-events:auto}.spell-slot{width:78px;height:64px;border:1px solid #8192a044;position:relative;background:#14202a;overflow:hidden;padding:5px;display:flex;flex-direction:column;justify-content:space-between;align-items:flex-start}.spell-icon{display:block;height:37px;font-size:28px;margin-top:6px}.spell-name{font-size:8px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#gold{position:absolute;left:12px;bottom:18px;padding:7px 10px;font-size:12px}.round-btn{width:44px;height:44px;border-radius:50%;background:#14202a;display:grid;place-items:center}.hidden{display:none!important}#buttons{position:absolute;right:12px;bottom:18px;display:flex;gap:7px;pointer-events:auto}
#minimap{position:absolute;right:12px;top:104px;width:112px;height:112px;padding:8px}#bossHud{position:absolute;left:50%;top:82px;transform:translateX(-50%);width:70vw}.boss-name{text-align:center;margin-bottom:5px;font-size:12px}.bossbar{height:12px;background:#663499}
#mobileControls{position:fixed;inset:0;z-index:20;pointer-events:none}.stick-zone{position:absolute;bottom:18px;width:140px;height:140px;pointer-events:auto;touch-action:none}.stick-base{position:absolute;width:106px;height:106px;left:17px;top:17px;border-radius:50%;border:1px solid #eee4;background:#eeeeee15}.stick-knob{position:absolute;width:44px;height:44px;top:50%;left:50%;transform:translate(-50%,-50%);border-radius:50%;background:#eee4}#moveZone{left:12px}#aimZone{right:12px}#mobileControls button{position:absolute;pointer-events:auto;background:#14202a;border:1px solid #eee4}#mobileDodge{min-width:58px;min-height:58px}#mobileInteract{width:88px;height:48px;padding:0 10px;white-space:nowrap;font-size:10px}
@media(pointer:coarse),(max-width:760px){
 body[data-aw-continent] #moveZone,body[data-aw-continent] #aimZone{width:104px!important;height:104px!important;bottom:max(18px,env(safe-area-inset-bottom))!important;top:auto!important}
 body[data-aw-continent] #mobileDodge,body[data-aw-continent] #mobileInteract{right:130px!important;left:auto!important;width:48px!important;height:48px!important;font-size:10px!important}body[data-aw-continent] #mobileDodge{bottom:18px!important}body[data-aw-continent] #mobileInteract{bottom:78px!important}
 body[data-aw-continent] #spells,body[data-aw-continent] #spells.aw-expanded-spells{display:grid!important;grid-template-columns:repeat(3,50px);gap:6px!important;left:auto!important;right:12px!important;bottom:150px!important;transform:none!important;padding:4px!important;max-width:none!important}
 body[data-aw-continent] #spells .spell-slot{width:50px!important;height:55px!important;min-width:0;padding:3px!important}body[data-aw-continent] #spells[data-spell-slots="3"]{grid-template-columns:repeat(3,54px)}body[data-aw-continent] #spells[data-spell-slots="3"] .spell-slot{width:54px!important;height:60px!important}
 body[data-aw-continent] #buttons{bottom:auto!important;top:105px;display:grid;grid-template-columns:repeat(3,40px);gap:5px}body[data-aw-continent] #minimap{display:block!important;width:145px!important;min-height:44px}
 body[data-aw-continent] .stick-base{width:84px!important;height:84px!important;top:10px!important;left:10px!important}
}
@media(pointer:coarse) and (orientation:landscape) and (max-height:540px){body[data-aw-continent] #spells,body[data-aw-continent] #spells.aw-expanded-spells{grid-template-columns:repeat(5,48px);bottom:130px!important}body[data-aw-continent] #spells .spell-slot{width:48px!important;height:50px!important}}
'''
MARKUP = '''<div id="hud"><div class="hud-top"><div class="status-card"><div class="status-line"><span id="heroLabel">Wanderer • Lv 18</span><span id="hpText">180 / 220</span></div><div class="bar hp"><i></i><span id="hpBarLabel">180 / 220</span></div><div class="bar xp" style="margin-top:5px;height:8px"><i></i></div><div class="status-bottom"><span class="chip">Weapon</span><span class="chip">Armor</span></div></div><div class="room-card"><div id="roomName" class="room-name">Thornheart Sanctum</div><div id="roomSub">Threat 9</div><div id="roomState">Wave 2 / 4</div></div></div><div id="minimap">World Map</div><div id="bossHud"><div class="boss-name">Thornheart Sovereign</div><div class="bossbar"></div></div><div id="spells" class="spell-bar aw-expanded-spells"></div><div id="gold" class="gold-pill">🪙 1250</div><div id="buttons"><button class="round-btn">🎒</button><button class="round-btn">⏸</button><button class="round-btn">🗺</button><button class="round-btn">📖</button><button class="round-btn">🐎</button><button class="round-btn">⚙</button></div></div><div id="mobileControls"><div class="stick-zone" id="moveZone"><div class="stick-base"><div class="stick-knob"></div></div></div><div class="stick-zone" id="aimZone"><div class="stick-base"><div class="stick-knob"></div></div></div><button id="mobileDodge">DODGE</button><button id="mobileInteract" style="width:88px;height:48px;padding:0 10px;white-space:nowrap;font-size:10px">💬 TALK</button></div>'''


def overlap(a, b):
    return a['x'] < b['right'] - .5 and a['right'] > b['x'] + .5 and a['y'] < b['bottom'] - .5 and a['bottom'] > b['y'] + .5


def run():
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(executable_path=os.environ.get('CHROMIUM', '/usr/bin/chromium'), headless=True, args=['--no-sandbox'])
        for (width, height), count, inset in itertools.product([(320,568),(390,844),(568,320),(844,390),(1024,768)], [3,4,5], [0,20]):
            context = browser.new_context(viewport={'width': width, 'height': height}, has_touch=True)
            page = context.new_page()
            page.set_content('<!doctype html><html><head><style>' + LEGACY_CSS + '</style><style>' + (ROOT / 'mobile-travel.css').read_text() + '</style></head><body data-aw-continent="verdant">' + MARKUP + '</body></html>')
            page.evaluate('''count => { const spells=document.getElementById('spells'); spells.dataset.spellSlots=count; for(let i=0;i<count;i++){ const b=document.createElement('button');b.className='spell-slot';b.innerHTML='<span class="spell-icon">✦</span><span class="spell-name">Spell '+(i+1)+'</span>';spells.appendChild(b);}window.isTouch=true;window.AWCampaignData={mounts:{horse:{speed:1.42}}};window.AWPresentation={camera:{zoom:1,tick(){}}}; }''', count)
            page.add_script_tag(content=(ROOT / 'mobile-travel.js').read_text())
            if inset:
                page.evaluate('''() => {for(const edge of ['left','right','top','bottom'])document.documentElement.style.setProperty('--aw-mobile-'+edge,'24px')}''')
            state = page.evaluate('''() => { const ids=['spells','moveZone','aimZone','mobileDodge','mobileInteract','awMobileMenuToggle']; const out={};for(const id of ids)out[id]=document.getElementById(id).getBoundingClientRect().toJSON();out.slots=[...document.querySelectorAll('.spell-slot')].map(b=>({rect:b.getBoundingClientRect().toJSON(),topHit:document.elementFromPoint(b.getBoundingClientRect().x+27,b.getBoundingClientRect().y+30).closest('.spell-slot')===b}));out.room=document.querySelector('.room-card').getBoundingClientRect().toJSON();out.hp=document.querySelector('.status-card').getBoundingClientRect().toJSON();out.tray=getComputedStyle(document.getElementById('buttons')).display;return out;}''')
            assert abs(state['spells']['right'] - (width - 4 - inset)) < .5, state
            assert abs(state['spells']['bottom'] - (height - 4 - inset)) < .5, state
            assert state['tray'] == 'none'
            assert state['room']['width'] <= 104.5 and state['room']['height'] <= 33
            assert state['hp']['width'] <= 132.5 and state['hp']['height'] <= 40
            for slot in state['slots']:
                assert slot['rect']['width'] == 54 and slot['rect']['height'] == 60
                assert slot['topHit'], ('occluded spell', width,height,count,inset,slot)
            keys = ['spells','moveZone','aimZone','mobileDodge','mobileInteract','awMobileMenuToggle']
            for key in keys:
                box = state[key]
                assert box['x'] >= 0 and box['y'] >= 0 and box['right'] <= width and box['bottom'] <= height, (key, box)
            for a,b in itertools.combinations(keys, 2):
                assert not overlap(state[a],state[b]), (a,b,width,height,count,inset,state)
            page.click('#awMobileMenuToggle')
            tray = page.locator('#buttons').bounding_box()
            assert tray and tray['x'] >= 0 and tray['y'] >= 0
            assert page.evaluate('''() => [...document.querySelectorAll('#buttons button')].every(b=>{const r=b.getBoundingClientRect();return document.elementFromPoint(r.x+r.width/2,r.y+r.height/2).closest('button')===b;})'''), ('occluded menu tool',width,height,count,inset)
            page.locator('#buttons button').first.click()
            assert page.locator('#awMobileMenuToggle').get_attribute('aria-expanded') == 'false'
            # Live loadout changes update the utility-control offset without a per-frame layout loop.
            page.evaluate('''() => {const b=document.createElement('button');b.className='spell-slot';document.getElementById('spells').appendChild(b);}''')
            page.wait_for_function("document.documentElement.style.getPropertyValue('--aw-mobile-spell-rows') === '2'" if count >= 3 else 'true')
            if width == 390 and count == 5 and inset == 0:
                page.evaluate("document.querySelector('#spells').lastChild.remove()")
                page.screenshot(path=str(ROOT / 'mobile-layout-portrait.png'))
            results.append({'viewport':f'{width}x{height}','slots':count,'safeInset':inset,'pass':True})
            context.close()
        # Wide desktop keeps the original tray, camera and spell sizes.
        context = browser.new_context(viewport={'width':1280,'height':720},has_touch=False)
        page = context.new_page()
        page.set_content('<style>'+LEGACY_CSS+'</style><style>'+(ROOT/'mobile-travel.css').read_text()+'</style>'+MARKUP)
        page.evaluate('window.isTouch=false;window.AWPresentation={camera:{zoom:1,tick(){}}};')
        page.add_script_tag(content=(ROOT/'mobile-travel.js').read_text())
        assert page.evaluate('AWPresentation.camera.zoom') == 1
        assert page.locator('#awMobileMenuToggle').is_hidden()
        assert page.locator('#buttons').is_visible()
        context.close()
        browser.close()
    print(json.dumps({'mobile_layout_cases_passed':len(results),'desktop_case_passed':True,'cases':results},indent=2))

if __name__ == '__main__':
    run()
