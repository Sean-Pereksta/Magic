"""Isolated UI regression fixture; no Gemini, Firebase, or deal-engine calls.
Run: python game/iron-throne/tests/correspondence-browser.py
Requires playwright and a Chromium browser. Uses the repository's index/CSS.
"""
from pathlib import Path
import json
import os
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
houses = [
    dict(id=i, name='House '+i.title(), ruler=r, color=c, sigil=s)
    for i,r,c,s in [
        ('ashen','The Crown Regent','#dfa94f','♛'),('wintermere','Queen Ysella','#90bfd7','❄'),
        ('thornwall','Lord Cassian','#89b778','♜'),('sunspire','Prince Dorian','#dd9571','☀'),
        ('vesper','Duchess Nyra','#b29cc9','☾'),('redharbor','King Oren','#d67878','⚑'),
        ('stormholt','Lady Maera','#79b6ae','ϟ'),('goldmere','Duke Lucan','#d1c071','♢'),
        ('ravenfell','Queen Sera','#a09eb8','♠'),('oakwarden','Lord Edric','#b0bb80','♧'),
        ('dawnreach','Princess Alia','#e6ac9e','✦'),('saltwynd','King Torren','#87a6bd','≋')
    ]
]
html = ROOT.joinpath('index.html').read_text()
court = html[html.index('  <dialog id="diplomacy"'):html.index('  <dialog id="chronicle-dialog"')]
bootstrap = r'''
const $ = id => document.getElementById(id), houses = HOUSES;
const esc = s => String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
window.actions = {ratified:0, modified:0, counter:0, human:0};
window.entries = [['ruler','Your armies near our border concern my council.'],['ruler','What assurance can you offer?'],['player','They are there to defend against Redharbor.'],['council','An alliance was ratified on turn 4.']];
window.actor='vesper'; window.ruler='wintermere';
window.terms=[{title:'ALLIANCE · ACCEPT',amount:40,type:'ALLIANCE'},{title:'TRADE · COUNTER',amount:60,type:'EXCHANGE',counter:80},{title:'PROPOSED PROMISE · ACCEPT',amount:20,type:'PROMISE'}];
$('offer-type').innerHTML=['ALLIANCE','EXCHANGE','DEFEND','PROMISE'].map(v=>`<option>${v}</option>`).join('');
window.renderTerms = () => {$('proposals').innerHTML=terms.map((t,i)=>`<div class="proposal"><h4>${t.title}</h4><p>${t.amount} gold · ${t.type} · 10 turns</p><p>These are the current exact terms.</p>${t.counter?`<div class="counter-terms"><strong>Counteroffer</strong><p>${t.counter} gold · EXCHANGE · 10 turns</p><button data-counter="${i}">Review counteroffer</button></div>`:`<button data-ratify="${i}">Accept &amp; Ratify</button>`}<button data-modify="${i}">Modify Offer</button></div>`).join('');};
window.renderLegacy=()=>{
 const k=houses.find(h=>h.id===ruler),a=houses.find(h=>h.id===actor);
 $('ruler-mark').textContent=k.sigil;$('ruler-house').textContent=k.name;$('ruler-name').textContent=k.ruler;
 $('ruler-relation').textContent='Opinion 25 · Trust 42 · At peace';$('ruler-motto').textContent='“An oath outlasts the winter.”';
 $('message-allowance').textContent='Shared dispatches: 3/4 this turn';$('diplomacy').style.setProperty('--house',k.color);
 $('messages').innerHTML=entries.map(([role,text])=>`<div class="message ${role}"><small>${role==='player'?`YOU · ${a.name.toUpperCase()}`:role==='council'?'COUNCIL RULING':k.ruler.toUpperCase()}</small>${esc(text)}</div>`).join('');
 $('messages').scrollTop=$('messages').scrollHeight;renderTerms();
};
const setMode=compact=>{const d=$('diplomacy');if(d.open)d.close();d.classList.toggle('compact',compact);compact?d.show():d.showModal();};
$('quick-offer').onclick=()=>{setMode(false);$('offer-type').value='EXCHANGE';};
$('quick-request').onclick=()=>{setMode(false);$('offer-type').value='DEFEND';};
$('quick-promises').onclick=()=>{setMode(false);$('council-records').open=true;};
$('expand-council').onclick=()=>{setMode(!$('diplomacy').classList.contains('compact'));renderLegacy();};
$('proposals').addEventListener('click',e=>{
 const b=e.target.closest('button');if(!b)return;
 if(b.dataset.ratify!==undefined)actions.ratified++;
 if(b.dataset.humanAccept!==undefined)actions.human++;
 if(b.dataset.counter!==undefined){const t=terms[Number(b.dataset.counter)];t.amount=t.counter;delete t.counter;t.title='TRADE · ACCEPT';actions.counter++;renderTerms();}
 if(b.dataset.modify!==undefined){const t=terms[Number(b.dataset.modify)];setMode(false);$('give-amount').value=t.amount;$('offer-type').value=t.type;actions.modified++;}
});
$('offer-form').onsubmit=e=>e.preventDefault();$('chat-form').onsubmit=e=>e.preventDefault();
document.querySelector('[data-close="diplomacy"]').onclick=()=>$('diplomacy').close();
renderLegacy();
window.ui=installCorrespondence(document,{houses,portraits:Object.fromEntries(houses.map(h=>[h.id,'data:image/png;base64,invalid'+h.id]))});
$('diplomacy').showModal();
'''.replace('HOUSES',json.dumps(houses))
fixture = '<!doctype html><meta charset="utf-8"><style>' + ROOT.joinpath('style.css').read_text() + '\n' + ROOT.joinpath('correspondence.css').read_text() + '</style>' + court + '<script type="module">' + ROOT.joinpath('correspondence-ui.mjs').read_text() + '\n' + bootstrap + '</script>'
checks=[]
def check(condition,label):
    assert condition,label
    checks.append(label)
if __name__ == "__main__":
    with sync_playwright() as p:
        executable=os.environ.get('CHROMIUM')
        if not executable and Path('/usr/bin/chromium').exists(): executable='/usr/bin/chromium'
        browser=p.chromium.launch(executable_path=executable,headless=True,args=['--no-sandbox'])
        page=browser.new_page(viewport={'width':1440,'height':1000})
        errors=[];page.on('pageerror',lambda e: errors.append(str(e)))
        page.set_content(fixture)
        page.wait_for_selector('[data-correspondence-ready]');page.wait_for_timeout(250)
        check(page.locator('#treaty-drawer').get_attribute('aria-hidden')=='true','drawer closed by default')
        full=page.locator('.conversation').bounding_box()['width'];grid=page.locator('.diplomacy-grid').bounding_box()['width']
        check(full/grid>.98,'closed chat uses entire desktop grid')
        check(page.locator('.player').get_attribute('data-house-id')=='vesper','non-Ashen player portrait resolves from rendered House')
        check(page.locator('.ruler').first.get_attribute('data-house-id')=='wintermere','foreign ruler portrait resolves')
        check(page.locator('.ruler').nth(1).evaluate("e=>e.classList.contains('message-continuation')"),'consecutive speaker grouping')
        check(page.locator('.council .leader-portrait').count()==0,'system messages have no portrait')
        check(page.locator('.leader-portrait img:not([hidden])').count()==0,'missing portraits fall back without blocking')
        check(page.locator('.correspondence-proposal').count()==3,'all live proposals have conversation cards')
        check(page.locator('.correspondence-promise').count()==1,'promise card distinguished')
        page.locator('#quick-offer').click();page.wait_for_timeout(250)
        opened=page.locator('.conversation').bounding_box()['width']
        check(opened<full and opened/full>.6,'desktop drawer shrinks instead of covering chat')
        page.locator('#give-amount').fill('123')
        page.locator('.treaty-drawer-close').click();page.wait_for_timeout(250)
        check(abs(page.locator('.conversation').bounding_box()['width']-full)<2,'closing restores full chat width')
        page.locator('#quick-offer').click();page.wait_for_timeout(200)
        check(page.locator('#give-amount').input_value()=='123','drawer close/reopen preserves draft fields')
        page.locator('.treaty-drawer-close').click();page.wait_for_timeout(200)
        page.locator('.correspondence-proposal').first.get_by_text('Review Terms',exact=True).click();page.wait_for_timeout(250)
        check(page.locator('#give-amount').input_value()=='40','card loads exact original offer through existing Modify handler')
        check(page.evaluate('actions.ratified')==0,'review never ratifies')
        page.locator('.treaty-drawer-close').click();page.wait_for_timeout(200)
        page.locator('.correspondence-proposal').nth(1).get_by_text('Review counteroffer',exact=True).click();page.wait_for_timeout(250)
        check(page.locator('#give-amount').input_value()=='80' and page.evaluate('actions.counter')==1,'counteroffer loads exact counter through existing handler')
        check(page.evaluate('actions.ratified')==0,'counter review never ratifies')
        page.locator('.treaty-drawer-close').click();page.wait_for_timeout(200)
        page.locator('#quick-promises').click();page.wait_for_timeout(200)
        check(page.locator('#council-records').evaluate('e=>e.open') and page.locator('#council-records').evaluate("e=>e.closest('#treaty-drawer')!==null"),'Promises opens original records in drawer')
        page.keyboard.press('Escape');page.wait_for_timeout(200)
        check(page.locator('#diplomacy').evaluate('e=>e.open') and page.locator('#treaty-drawer').get_attribute('aria-hidden')=='true','Escape closes only drawer')
        check(page.locator('#gemini-diagnostics').is_hidden(),'diagnostics visibility unchanged')
        for house in houses:
            page.evaluate('(id)=>{actor=id;renderLegacy();}',house['id']);page.wait_for_timeout(20)
            check(page.locator('.player').get_attribute('data-house-id')==house['id'],'player House '+house['id'])
        page.evaluate("entries=Array.from({length:50},(_,i)=>[i%2?'player':'ruler','Correspondence entry '+i+' — The border deserves a careful discussion.']);renderLegacy();")
        page.wait_for_timeout(100)
        page.locator('.correspondence-scroll').evaluate('e=>e.scrollTop=150');page.wait_for_timeout(100)
        before=page.locator('.correspondence-scroll').evaluate('e=>e.scrollTop')
        page.evaluate("entries.push(['ruler','A new envoy has arrived.']);renderLegacy();");page.wait_for_timeout(100)
        after=page.locator('.correspondence-scroll').evaluate('e=>e.scrollTop')
        check(abs(before-after)<2 and after<500,'new message preserves history scroll position')
        check(page.locator('.correspondence-latest').is_visible(),'new-message jump control visible while reading history')
        page.locator('.correspondence-latest').click();page.wait_for_timeout(100)
        check(page.locator('.correspondence-scroll').evaluate('e=>e.scrollHeight-e.clientHeight-e.scrollTop')<3,'Latest jumps to bottom')
        page.evaluate("window.stale=document.querySelector('.correspondence-proposal button');terms=[{title:'NEW · ACCEPT',amount:999,type:'EXCHANGE'}];renderTerms();stale.click();")
        check(page.evaluate('actions.ratified')==0 and page.locator('#give-amount').input_value()!='999','stale card cannot resolve to a replacement by index')
        page.wait_for_timeout(100)
        page.evaluate("entries=[['ruler','Our borders have seen too many armies. What assurance can you offer?'],['player','A ten-turn agreement, and forty gold to provision your watch.'],['ruler','Let us have the terms in writing.']];actor='vesper';renderLegacy();")
        page.wait_for_timeout(150)
        output=ROOT/'test-output';output.mkdir(exist_ok=True)
        page.screenshot(path=str(output/'desktop-chat.png'))
        page.locator('#quick-offer').click();page.wait_for_timeout(200)
        page.screenshot(path=str(output/'desktop-drawer.png'))
        page.locator('.treaty-drawer-close').click();page.wait_for_timeout(200)
        page.set_viewport_size({'width':390,'height':844});page.wait_for_timeout(200)
        check(page.locator('#diplomacy').evaluate('e=>e.scrollWidth<=e.clientWidth+1'),'no mobile dialog horizontal overflow')
        check(page.locator('.leader-portrait').first.bounding_box()['width']==44,'mobile portrait sizing')
        page.screenshot(path=str(output/'mobile-chat.png'))
        page.locator('#quick-offer').click();page.wait_for_timeout(250)
        check(page.locator('#treaty-drawer').get_attribute('aria-modal')=='true','mobile drawer is modal region')
        check(page.locator('.conversation').evaluate('e=>e.inert'),'mobile background cannot receive focus')
        check(page.locator('#treaty-drawer').bounding_box()['width']>360,'mobile drawer full width')
        page.locator('.treaty-drawer-close').focus();page.keyboard.press('Shift+Tab')
        check(page.evaluate("document.activeElement.closest('#treaty-drawer')!==null"),'mobile focus stays inside drawer')
        page.screenshot(path=str(output/'mobile-drawer.png'))
        page.keyboard.press('Escape');page.wait_for_timeout(200)
        check(page.locator('#quick-offer').evaluate('e=>e===document.activeElement'),'close restores action focus')
        page.locator('#expand-council').click();page.wait_for_timeout(250)
        check(page.locator('#diplomacy').evaluate("e=>e.classList.contains('compact')") and page.locator('.correspondence-scroll').bounding_box()['height']>100,'compact mode remains usable')
        page.locator('#quick-offer').click();page.wait_for_timeout(200)
        page.evaluate("ruler='redharbor';renderLegacy();");page.wait_for_timeout(200)
        check(page.locator('#treaty-drawer').get_attribute('aria-hidden')=='true','switching rulers closes old drawer')
        # Human proposals have no Modify action; review must only reveal their
        # original exact terms and preserve explicit receiving-ruler acceptance.
        page.evaluate("document.getElementById('proposals').innerHTML='<article class=proposal><h4>PROPOSAL RECEIVED</h4><p>75 gold for 25 iron</p><p>Expires turn 8.</p><button data-human-accept=hp-7>Accept agreement</button><button data-human-decline=hp-7>Decline</button></article>';")
        page.wait_for_timeout(100)
        page.locator('.correspondence-proposal button').click();page.wait_for_timeout(200)
        check(page.evaluate('actions.human')==0,'human proposal review never accepts')
        check(page.locator('#proposals').inner_text().find('75 gold for 25 iron')>=0,'human exact terms remain in original node')
        page.locator('[data-human-accept]').click()
        check(page.evaluate('actions.human')==1,'explicit human acceptance handler still works')
        page.locator('.treaty-drawer-close').click();page.wait_for_timeout(200)
        page.evaluate("renderLegacy();ui.refresh();ui.refresh();")
        page.wait_for_timeout(100)
        check(page.locator('.player .leader-portrait').count()==1,'repeat rendering never duplicates player portraits')
        check(page.locator('.correspondence-proposal').count()==len(page.evaluate('terms')),'repeat rendering never duplicates cards')
        # Test load-success without fetching external artwork.
        page.locator('.leader-portrait img').first.evaluate("img=>{const c=document.createElement('canvas');c.width=c.height=4;c.getContext('2d').fillRect(0,0,4,4);img.hidden=false;img.src=c.toDataURL();}")
        page.wait_for_timeout(100)
        check(page.locator('.leader-portrait').first.evaluate("e=>e.classList.contains('portrait-loaded')"),'successful portrait replaces fallback')
        check(page.locator('.leader-portrait img').first.evaluate("e=>getComputedStyle(e).imageRendering")=='pixelated','loaded portrait preserves pixel-art rendering')
        page.set_viewport_size({'width':320,'height':740});page.wait_for_timeout(150)
        check(page.locator('#diplomacy').evaluate('e=>e.scrollWidth<=e.clientWidth+1'),'320px narrow screen has no horizontal overflow')
        check(not errors,'no browser JavaScript errors')
        browser.close()
print(json.dumps({'passed':len(checks),'checks':checks},indent=2))
