const roles = {
  tank: {
    label: 'Tank',
    color: '#bc9cff',
    effect: 'Take 14% less damage from enemy heroes. Deal 8% less damage to them.',
    incomingHeroDamage: .86,
    outgoingHeroDamage: .92
  },
  damage: {
    label: 'Damage',
    color: '#ffad78',
    effect: 'Deal 12% more damage to enemy heroes.',
    outgoingHeroDamage: 1.12
  },
  support: {
    label: 'Support',
    color: '#83e9ba',
    effect: 'Heal and shield other allied heroes for 22% more. Deal 8% less damage to enemy heroes.',
    allyProtection: 1.22,
    outgoingHeroDamage: .92
  },
  control: {
    label: 'Control',
    color: '#8bcfff',
    effect: 'Slows and stuns on enemy heroes last 20% longer. Slows are 15% stronger, up to 85%.',
    controlDuration: 1.2,
    slowStrength: 1.15
  }
};

const specialties = {
  arc: ['control', 'area denial', 'Cover an escape route with Forcefield, then make opponents move with Blue Storm.'],
  dragon: ['tank', 'fire bruiser', 'Lead the charge with Dragon Shield and keep enemies burning at close range.'],
  druid: ['support', 'summoning', 'Set healing groves behind the frontline and summon guardians to help hold it.'],
  angel: ['support', 'group sustain', 'Keep the team together with healing circles, Sanctuary shields and Inspire.'],
  ranger: ['damage', 'long-range marksman', 'Wear foes down with venom, then line up a penetrating Long Shot.'],
  bulwark: ['tank', 'shield wall', 'Protect nearby allies with Amethyst Guard and push enemies away from your backline.'],
  ember: ['damage', 'execution', 'Dash past the frontline and finish low-health targets with Execution Cut.'],
  seraph: ['control', 'frost lockdown', 'Catch clustered enemies with Winterfall and keep them slowed with Glacial Sweep.'],
  rivet: ['control', 'traps and sentries', 'Hold crossings with Shock Mines and position sentries to cover the approach.'],
  brinehook: ['tank', 'hook initiator', 'Pull opponents out of formation and use Sea Legs to stay in the fight.'],
  vela: ['damage', 'sustained marksman', 'Time six rounds around the reload and use the final round to punish exposed targets.'],
  solstice: ['support', 'timed protection', 'Ward allies just as damage lands and use Borrowed Seconds to shorten their cooldowns.'],
  ferrite: ['tank', 'magnetic vanguard', 'Build scrap for a barrier and change polarity to reposition approaching enemies.'],
  cadence: ['support', 'combat rhythm', 'Shield allies and choose the finale that gives the team the aid it needs.'],
  mirelle: ['support', 'combat medic', 'Poison enemies to trigger Field Remedy and heal several teammates with Marsh Remedy.'],
  knell: ['tank', 'team guard', 'Move alongside teammates to cover them with Bell Guard and stun enemies who group up.'],
  zephyr: ['damage', 'mobile skirmisher', 'Hit on both passes of Returning Quill and vault into position for the next attack.'],
  prism: ['damage', 'ranged artillery', 'Place mirrors on separate angles to split Facet Bolt, echo Glass Fault and converge on Spectrum Cross.']
};

export function applyHeroSpecialties(heroes) {
  for (const [heroId, [id, focus, description]] of Object.entries(specialties)) {
    const hero = heroes[heroId];
    const role = roles[id];
    hero.role = `${role.label} · ${focus}`;
    hero.specialty = Object.freeze({ id, focus, description, ...role });
  }
}
