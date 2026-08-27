const UPGRADE_POOLS = {
 firebolt:[
  ['fire_split','Forked Flame','🔥🔥','Orb splits into 3 smaller fireballs on impact.','split'],
  ['fire_burn','Living Ember','♨️','Explosion leaves a burning patch that damages enemies.','burn'],
  ['fire_huge','Furnace Core','💥','Explosion radius grows dramatically and knocks enemies back.','huge'],
  ['fire_seek','Hunting Flame','🎯','Ember Orb curves toward the toughest nearby target.','seek'],
  ['fire_echo','Afterburst','✨','A second smaller explosion triggers moments later.','echo']
 ],
 frostnova:[
  ['frost_ring','Glacier Ring','⭕','Nova becomes a traveling ice ring that hits farther enemies.','ring'],
  ['frost_shards','Shatterglass','🧊','Frozen enemies release damaging shards when hit.','shards'],
  ['frost_freeze','Deep Winter','🥶','Slow is stronger and can fully freeze weaker foes.','freeze'],
  ['frost_armor','Rime Armor','🛡️','Casting Frost Nova grants temporary damage reduction.','armor'],
  ['frost_echo','Second Winter','❄️❄️','A second nova pulses shortly after the first.','echo']
 ],
 thorns:[
  ['thorn_double','Briar Spiral','🌿🌿','Launch two rotating thorn rings in opposite directions.','double'],
  ['thorn_root','Grasping Roots','🪴','Enemies struck are briefly rooted.','root'],
  ['thorn_return','Returning Vines','↩️','Thorns contract back toward you for a second hit.','return'],
  ['thorn_bleed','Roseblood','🌹','Thorns cause a damaging bleed.','bleed'],
  ['thorn_wall','Living Rampart','🌳','Ring persists as a damaging protective hedge.','wall']
 ],
 arcaneMissiles:[
  ['missile_more','Constellation','✨','Fire 3 additional seeking missiles.','more'],
  ['missile_chain','Arcane Relay','🔗','Missiles jump once to another enemy after hitting.','chain'],
  ['missile_orbit','Orbital Script','🪐','Half your missiles orbit first, then launch at targets.','orbit'],
  ['missile_burst','Mana Bloom','💠','Each missile creates a tiny splash explosion.','burst'],
  ['missile_focus','Execution Script','🎯','Missiles deal more damage to low-health enemies.','execute']
 ],
 gust:[
  ['gust_tornado','Cyclone Seed','🌪️','Cone spawns a wandering tornado that repeatedly hits foes.','tornado'],
  ['gust_blades','Razor Gale','🗡️','Wind carries cutting blades for heavy damage.','blades'],
  ['gust_wide','Skybreaker','🌬️','Cone becomes much wider and longer.','wide'],
  ['gust_stun','Pressure Crash','💫','Enemies thrown into room edges are stunned.','stun'],
  ['gust_tailwind','Tailwind','🏃','Casting grants a burst of movement speed.','tailwind']
 ],
 ward:[
  ['ward_burst','Prism Detonation','💎','When the ward ends it explodes around you.','burst'],
  ['ward_thorns','Mirror Ward','🪞','Reflect a portion of projectile damage at attackers.','reflect'],
  ['ward_dash','Phase Ward','💨','Casting instantly refreshes your dodge.','dash'],
  ['ward_long','Crystal Shell','🔷','Ward lasts longer and absorbs more damage.','long'],
  ['ward_aura','Sanctuary','🌟','Ward slowly heals you while active.','heal']
 ],
 chain:[
  ['chain_more','Storm Network','⚡⚡','Lightning gains 3 additional jumps.','more'],
  ['chain_fork','Forked Current','🔱','Each jump can fork to a second nearby enemy.','fork'],
  ['chain_stun','Thunderclap','💫','Hits have a chance to stun non-boss enemies.','stun'],
  ['chain_ground','Ground Current','🌩️','Last target leaves a crackling damage field.','ground'],
  ['chain_repeat','Echo Current','🔁','A weaker chain repeats from the first victim.','repeat']
 ],
 poison:[
  ['poison_big','Corpse Garden','🍄','Toxic bloom grows larger whenever an enemy dies inside it.','grow'],
  ['poison_spore','Sporeburst','🫧','Pulse launches poison spores at nearby targets.','spore'],
  ['poison_slow','Numbing Venom','🐍','Poisoned enemies are slowed heavily.','slow'],
  ['poison_move','Wandering Bloom','🌺','Bloom slowly follows the nearest enemy.','move'],
  ['poison_pop','Toxic Detonation','💚','Bloom explodes for bonus damage when it expires.','pop']
 ],
 chakram:[
  ['chakram_twins','Twin Moons','🌙🌙','Throw two chakrams in a spread.','twins'],
  ['chakram_orbit','Lunar Orbit','🪐','Chakram circles you once before launching.','orbit'],
  ['chakram_bounce','Ricochet Moon','↗️','Chakram bounces toward another enemy at the far point.','bounce'],
  ['chakram_saw','Moon Saw','⚙️','Chakram slows on enemies and can hit them repeatedly.','saw'],
  ['chakram_nova','Full Moon','🌕','Returning chakram triggers a crescent nova at you.','nova']
 ],
 spirits:[
  ['spirit_more','Wisp Choir','🧚🧚','Summon 3 additional wisps.','more'],
  ['spirit_heal','Kind Spirits','💚','Wisps occasionally heal you after striking.','heal'],
  ['spirit_bomb','Last Light','💥','Wisps explode when their duration ends.','bomb'],
  ['spirit_chain','Shared Soul','🔗','Wisp hits arc a small bolt to a second enemy.','chain'],
  ['spirit_guard','Guardian Wisps','🛡️','Wisps can intercept enemy projectiles.','guard']
 ],
 quake:[
  ['quake_three','Fault Lines','〽️','Send three angled shockwaves instead of one.','three'],
  ['quake_pillars','Stone Teeth','🪨','Shockwave raises delayed stone pillars along its path.','pillars'],
  ['quake_stun','Seismic Lock','💫','Enemies struck near the center are stunned.','stun'],
  ['quake_return','Aftershock','↩️','A second shockwave returns toward you.','return'],
  ['quake_wide','Continental Break','🌋','Shockwave becomes wider and knocks enemies aside.','wide']
 ],
 meteor:[
  ['meteor_three','Meteor Shower','☄️☄️','Call three smaller meteors around the target area.','three'],
  ['meteor_crater','Burning Crater','🔥','Impact leaves a long-lived burning crater.','crater'],
  ['meteor_pull','Gravity Mark','🌀','Telegraph pulls enemies toward impact center before landing.','pull'],
  ['meteor_frag','Star Fragments','✨','Impact launches damaging fragments in every direction.','fragments'],
  ['meteor_fast','Falling Sky','⏱️','Meteor lands much faster and cooldown is reduced.','fast']
 ],
 voidrift:[
  ['void_twins','Twin Rifts','🕳️🕳️','Open two smaller linked rifts.','twins'],
  ['void_burst','Rift Collapse','💥','Rift ends with a powerful implosion.','burst'],
  ['void_orbs','Voidlings','⚫','Rift spits homing void orbs at enemies.','orbs'],
  ['void_slow','Event Horizon','🌀','Pull and slow effects become much stronger.','slow'],
  ['void_move','Hungry Rift','👁️','Rift drifts toward dense groups of enemies.','move']
 ],
 icelance:[
  ['ice_triple','Trident Ice','🔱','Fire three lances in a tight fan.','triple'],
  ['ice_shard','Shard Trail','💎','Lance leaves sharp ice crystals along its route.','trail'],
  ['ice_explode','Frozen Heart','💥','Lance explodes when it reaches max range.','explode'],
  ['ice_ricochet','Mirror Ice','↗️','Lance ricochets toward another enemy after a hit.','ricochet'],
  ['ice_execute','Absolute Zero','🥶','Greatly increased damage to slowed or frozen enemies.','execute']
 ],
 soulflame:[
  ['soul_spread','Soul Plague','🟣🟣','Soulflame can jump between living targets before a death.','spread'],
  ['soul_heal','Soul Feast','💚','Deaths of cursed enemies restore health.','heal'],
  ['soul_ghost','Ghostfire','👻','Cursed enemies release a seeking ghost on death.','ghost'],
  ['soul_big','Black Bonfire','🔥','Curse burns harder and creates an aura around its victim.','aura'],
  ['soul_boss','Grim Brand','💀','Deals bonus damage to elites and bosses.','boss']
 ],
 tempest:[
  ['tempest_big','Stormfront','⛈️','Storm radius and duration increase greatly.','big'],
  ['tempest_move','Hunting Storm','🌩️','Storm follows the strongest enemy.','move'],
  ['tempest_bolts','Forked Sky','⚡','Each strike forks to nearby targets.','fork'],
  ['tempest_wind','Eye of the Storm','🌪️','Storm pushes projectiles away from its center.','deflect'],
  ['tempest_final','Thunderhead','💥','Storm ends with a massive thunderclap.','final']
 ],
 timestop:[
  ['time_freeze','Stopped Moment','⏸️','Enemies very near the center are completely frozen.','freeze'],
  ['time_bubble','Wide Hour','🫧','Field becomes substantially larger.','big'],
  ['time_haste','Borrowed Time','🏃','You gain attack and movement speed inside the field.','haste'],
  ['time_burst','Broken Clock','💥','Field damages enemies when time resumes.','burst'],
  ['time_follow','Clockwork Aura','⌛','Field follows you instead of staying in place.','follow']
 ],
 phoenix:[
  ['phoenix_twins','Twin Phoenix','🦅🦅','A second phoenix sweeps from the opposite side.','twins'],
  ['phoenix_burn','Ash Trail','🔥','Phoenix leaves a long trail of fire.','trail'],
  ['phoenix_heal','Renewal','❤️','Phoenix heals you as it returns.','heal'],
  ['phoenix_explode','Solar Wing','☀️','Phoenix erupts in explosions when crossing enemies.','explode'],
  ['phoenix_circle','Rebirth Spiral','🌀','Phoenix circles you before departing, damaging nearby foes.','circle']
 ],
 starfall:[
  ['star_more','Endless Night','🌠🌠','Drop many more stars over a longer duration.','more'],
  ['star_seek','Fated Stars','🎯','Most stars bias toward enemy positions.','seek'],
  ['star_nova','Constellation Burst','✨','Every star releases a small radial projectile nova.','nova'],
  ['star_guard','Heavenly Shelter','🛡️','Standing under Starfall grants damage reduction.','guard'],
  ['star_final','Supernova','☀️','Final star is enormous and deals devastating damage.','final']
 ],
 singularity:[
  ['sing_big','Black Sun','🌑','Singularity grows to fill a huge portion of the room.','big'],
  ['sing_orbit','Orbiting Ruin','🪐','Enemies caught begin orbiting the core before collapse.','orbit'],
  ['sing_shards','Dark Matter','⚫','Core continuously launches void shards outward.','shards'],
  ['sing_second','Binary Collapse','⚫⚫','A second smaller singularity appears opposite the first.','second'],
  ['sing_reset','Impossible Gravity','⏱️','Enemy deaths inside reduce Singularity cooldown.','reset']
 ]
};