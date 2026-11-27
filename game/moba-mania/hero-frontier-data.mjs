const ability = (id, name, description, icon, mana, cooldown, range, radius, type, mobility = false) =>
  ({id, name, description, icon, mana, cooldown, range, radius, type, mobility});

function hero(id, name, title, role, description, color, palette, stats, passive, abilities) {
  const [hp, mana, speed, range, damage, attackRate] = stats;
  const tiers = [
    [3, ['Vigor','health','Gain 12% maximum health.',{hp:.12}], ['Focus','mana','Gain 15% maximum mana.',{mana:.15}], ['Tempo','haste','Basic attacks are 10% faster.',{attackSpeed:.1}]],
    [5, ['Long Reach','range','Aimed abilities reach 10% farther.',{abilityRange:.1}], ['Momentum','speed','Move 8% faster.',{speed:.08}], ['Potency','storm','Gain 12% spell damage.',{spellDamage:.12}]],
    [8, ['Fleet Recovery','cooldown','Ability cooldowns are 9% shorter.',{cooldown:.09}], ['Steel Edge','damage','Basic attacks deal 14% more damage.',{basicDamage:.14}], ['Deep Wells','mana','Regenerate 1.8 extra mana per second.',{manaRegen:1.8}]],
    [10, ['Expansive Cast','area','Ability areas grow by 12%.',{area:.12}], ['Hunter','hunt','Deal 25% more damage to neutral creatures.',{jungleDamage:.25}], ['Bastion','shield','Gain 14% health and 12% stronger shields.',{hp:.14,shield:.12}]],
    [12, ['Sustain','heal','Healing is 16% stronger and basic attacks have 7% lifesteal.',{healing:.16,lifesteal:.07}], ['Defiance','shield','Gain 18% maximum health.',{hp:.18}], ['Sharpness','damage','Deal 12% more damage.',{damage:.12}]],
    [15, ['Flow','cooldown','Ability cooldowns are 12% shorter.',{cooldown:.12}], ['March','speed','Move 10% faster and attack 12% faster.',{speed:.1,attackSpeed:.12}], ['Amplify','storm','Gain 18% spell damage.',{spellDamage:.18}]],
    [18, ['Siege Engine','damage','Basic attacks deal 22% more damage.',{basicDamage:.22}], ['Iron Resolve','shield','Gain 22% health and 20% stronger shields.',{hp:.22,shield:.2}], ['Spellcraft','storm','Gain 24% spell damage and 10% larger areas.',{spellDamage:.24,area:.1}]]
  ];
  return {
    id, name, title, role, description, color, palette, hp, mana, speed, range, damage, attackRate,
    passive: {name:passive[0], description:passive[1], icon:passive[2]},
    abilities: abilities.map((skill,slot)=>({...skill,color,unlock:[1,2,4,6][slot]})),
    talents: tiers.map(([level,...options])=>({
      level, choices:options.map(([name,icon,description,bonuses], index)=>({
        id:`${id}-${level}-${index}`, name, icon, description, bonuses, color
      }))
    }))
  };
}

export const FRONTIER_HEROES = {
  mirelle:hero('mirelle','Mirelle','The Marsh Apothecary','Poison support','Seeds the ground with slowing brambles and restores allies from behind the front line.','#b6db83',['#31504b','#71a268','#eac887'],[665,415,175,235,39,1.08],
    ['Field Remedy','A basic hit on a poisoned target heals the most injured nearby ally. Recharges in 5s.','heal'],[
    ability('spore-dart','Spore Dart','Fire a dart that poisons the first enemy it hits.','poison',30,5,460,10,'line'),
    ability('bramble-bed','Bramble Bed','Grow a patch that slows and damages enemies for 4s.','tree',55,13,410,100,'ground'),
    ability('marsh-remedy','Marsh Remedy','Heal allied heroes in the target area.','heal',60,12,400,125,'ground'),
    ability('bloom-tide','Bloom Tide','Warn a wide bloom that damages and poisons enemies after 0.65s.','area',95,36,510,165,'ground')
  ]),
  knell:hero('knell','Knell','The Bell Warden','Vanguard tank','A walking bronze bell who holds the front line and punishes clustered opponents.','#dfad72',['#563c45','#aa744e','#ffe3ab'],[900,335,158,63,54,1.12],
    ['Bronze Refrain','Every third successful cast grants you a shield for 3s.','shield'],[
    ability('bronze-peal','Bronze Peal','Ring a short cone that damages and slows enemies.','cone',37,6,200,200,'cone'),
    ability('bell-guard','Bell Guard','Shield nearby allied heroes and reduce their damage taken briefly.','shield',55,14,0,135,'self'),
    ability('clapper-charge','Clapper Charge','Charge until terrain stops you, then damage enemies around your landing point.','charge',55,13,260,78,'line',true),
    ability('great-silence','Great Silence','Ring out after 0.75s to damage and stun nearby enemies. An enemy stun during the windup refunds half the cost and cooldown.','quake',95,40,0,190,'self')
  ]),
  zephyr:hero('zephyr','Zephyr','The Gale Falcon','Mobile skirmisher','Throws a returning blade, vaults across fights, and cuts through grouped enemies.','#73e2c8',['#244553','#439d9d','#ffe3a0'],[620,345,196,178,44,.93],
    ['Tailwind','Your first basic hit within 3s of Wing Vault grants brief movement speed.','speed'],[
    ability('returning-quill','Returning Quill','Throw a piercing quill with a warned return along its path.','arrow',36,7,420,10,'line'),
    ability('wing-vault','Wing Vault','Vault to a clear point and slow enemies at the landing site.','dash',45,10,285,70,'line',true),
    ability('slipstream','Slipstream','Gain attack and movement speed for 3s.','haste',48,15,0,0,'self'),
    ability('gale-cross','Gale Cross','A warned cone strikes and pushes enemies away.','cone',85,31,245,245,'cone')
  ]),
  prism:hero('prism','Prism','The Glass Artillerist','Ranged artillery','Builds a network of mirror pylons that split shots, echo detonations and bend a cross of light.','#d1a5fa',['#493660','#9374bf','#ebdcff'],[595,445,166,265,40,1.15],
    ['Refraction','Your third successful cast refunds 16 mana.','mana'],[
    ability('facet-bolt','Facet Bolt','Fire three bolts. Aim the center bolt through a Mirror Pylon to split it into two new shots that fan toward visible foes.','arrows',43,6,480,8,'line'),
    ability('mirror-veil','Mirror Pylon','Deploy a breakable mirror for 24s and gain a brief shield and speed boost. Up to two mirrors can stand at once.','prism-mirror',44,10,350,17,'ground'),
    ability('glass-fault','Glass Fault','Detonate the marked circle and slow enemies. Linked pylons echo with a smaller burst around themselves.','area',60,12,525,105,'ground'),
    ability('spectrum-cross','Spectrum Cross','Fire two intersecting beams at your aim. Linked pylons send additional rays into the center. Enemies can take multiple hits.','storm',105,42,515,180,'ground')
  ])
};
