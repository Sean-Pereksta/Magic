from pathlib import Path
import hashlib

root=Path('game/receiver-window-qb')
game_path=root/'game.js'
test_path=root/'gameplay-variety.test.cjs'
source=game_path.read_text()
tests=test_path.read_text()


def replace_once(text, old, new):
    count=text.count(old)
    if count != 1:
        raise RuntimeError(f'Expected one source anchor, got {count}: {old[:100]}')
    return text.replace(old,new,1)


def assert_blob(path, sha):
    data=path.read_bytes()
    actual=hashlib.sha1(f'blob {len(data)}\0'.encode()+data).hexdigest()
    if actual != sha:
        raise RuntimeError(f'{path} changed; refusing to overwrite unrelated work ({actual})')


assert_blob(game_path,'c19abdfafaa6c9c19d4e71fc40cebea1db078b88')
assert_blob(test_path,'d5c46d469fb464b35a6dab5067b990d6e57c4169')
work=Path('.github/qb-catch-work')
NEW_CONTACT=(work/'new-contact.js').read_text()
NEW_TESTS=(work/'tests.cjs').read_text()
start=source.index('function deflectBall(hit,contested=false){')
end=source.index('function finishSecuringCatch(dt){',start)
source=source[:start]+NEW_CONTACT+source[end:]
source=replace_once(source,'a.profile?a.trackingBall:a.ballSeen','a.profile?receiverCatchAware(a):a.ballSeen')
source=replace_once(source,'a.profile?5+Math.min(1,P.effective(a.profile.athleticism)/100)*3:5+currentSkill()*3','a.profile?7.5+Math.min(1,P.effective(a.profile.athleticism)/100)*3.5:5+currentSkill()*3')
source=replace_once(source,'r.trackingBall=true;r.burst','r.trackingBall=true;r.catchTrackingUntil=throwTime+.16;r.burst')
source=replace_once(source,'{a.contactLock=false;a.contactRig=null;}','{a.contactLock=false;a.contactRig=null;a.catchTrackingUntil=0;a.gatherUntil=0;a.gatherUsed=false;}')
source=replace_once(source,'({mesh,hand:i<2,a:new THREE.Vector3()','({mesh,hand:i<2,forearm:i>=4,a:new THREE.Vector3()')
source=replace_once(source,'for(const limb of rig){\n    let earliest=null;','for(const limb of rig){\n    // A wrist cushion briefly permits the gloves to gather, never another forearm catch.\n    if(a.profile&&a.gatherUntil>throwTime&&limb.forearm)continue;\n    let earliest=null;')

# Keep existing breakup regressions with an explicit losing roll, and add winning cases.
name="test('simultaneous competing hands cause a live contested tip; proximity alone does not'"
start=tests.index(name);end=tests.index('\ntest(',start+1)
block=tests[start:end]
block=replace_once(block,'c.Math.random=()=>0;','c.Math.random=()=>.999;')
block=replace_once(block,'const r2=contactScene(c)','c.Math.random=()=>0;const r2=contactScene(c)')
tests=tests[:start]+block+tests[end:]
name="test('another actual hand contact breaks a provisional catch and can be intercepted after separation'"
start=tests.index(name);end=tests.index('\ntest(',start+1)
block=tests[start:end]
block=replace_once(block,'q.ballPrev.copy(q.ball.position);','q.ballPrev.copy(q.ball.position);c.Math.random=()=>.999;')
block=replace_once(block,'ballAtHand(q,d);assert.ok','c.Math.random=()=>0;ballAtHand(q,d);assert.ok')
tests=tests[:start]+block+tests[end:]+NEW_TESTS

game_path.write_text(source)
test_path.write_text(tests)
readme=root/'README.md'
readme.write_text(readme.read_text()+"""

### Forgiving receiver catches

Routine receiver glove contacts are substantially more reliable without increasing the
collision shapes or awarding chest/helmet/proximity catches. In clear conditions, a
70-effective-CATCH/70-effective-ATH receiver with a perfect spiral and no other
modifiers has 98.85%, 98.55%, and 97.05% initial one-hand control at relative contact
speeds of 24, 40, and 50 world units/second. These are conditional control probabilities,
not overall completion rates; placement, real contact, coverage and securing still matter.

- Receiver-only control uses a smaller bullet penalty above 38 units/second; weather,
  staggering and underthrows remain relevant but less punitive. The ordinary cap is
  99.5%; contested control is capped at 94%. Defender interception odds are unchanged.
- Hand tracking moves faster within the same physical reach. A 160 ms simulation-time
  awareness grace bridges short pursuit-planner gaps and resets on every throw.
- An aware receiver may cushion one real wrist-side forearm impact per throw for up to
  75 ms. This only damps relative velocity; it never attracts, teleports or awards the
  ball. A rendered glove still has to touch it; expired/uncompleted gathers stay loose.
- A later second-glove contact can join the same securing attempt. Receivers secure in
  20 ms with two hands or 40 ms with one; defense retains its original timing.
- Receiver-first contested hand contacts can succeed or be broken up. A successful
  contest is evaluated once until the touching opponent separates, not every frame.
  Defense-first contact, genuine tips, ground and boundary authority remain intact.
- Uncontested receiver hand bobbles have softer rebounds and less tumble, making a real
  second attempt practical. No extra per-frame retry or automatic recovery is added.

Validation: `node --test game/receiver-window-qb/*.test.cjs`. The added regressions
cover conditional odds, reset/expiration, real wrist and glove contacts, both outcomes
of contact contests, second-hand arrivals, and moving catches at 30/60/120 Hz.
""")
print('Patched receiver control, physical gathers, regression tests and documentation.')
