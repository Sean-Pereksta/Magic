// Each row adds an exclusive alternative at levels 12, 15, and 18.
// Entries bind one named spell to an additional action at the cast's aim or caster.
const paths = {
  arc:      [[0,'Aftershock','echo',0],[1,'Sheltering Field','ward',0],[3,'Rift Lock','stun',1]],
  dragon:   [[0,'Ember Wake','burn',0],[2,'Dragon Vanguard','rush',1],[3,'Molten Landing','burn',1]],
  druid:    [[0,'Thorn Roots','snare',0],[1,'Bark Company','ward',0],[3,'Hunting Pack','rush',0]],
  angel:    [[0,'Mending Bloom','mend',0],[1,'Steeled Chorus','guard',0],[3,'Lance Refrain','echo',0]],
  ranger:   [[0,'Venom Volley','venom',0],[1,'Hail of Thorns','snare',0],[3,'Shadow Barrage','echo',0]],
  bulwark:  [[0,'Bash Rebound','echo',0],[1,'Marching Guard','rush',1],[3,'Royal Bastion','ward',1]],
  ember:    [[0,'Seared Heart','burn',0],[1,'Scorching Footwork','snare',1],[3,'Execution Flare','echo',0]],
  seraph:   [[0,'Icy Refrain','snare',0],[1,'Courageous March','guard',1],[3,'Winter Shelter','ward',0]],
  rivet:    [[0,'Rail Wake','echo',0],[1,'Sentry Cover','ward',0],[3,'Emergency Repairs','mend',1]],
  brinehook:[[0,'Anchor Breakers','snare',0],[2,'Foam Break','guard',1],[3,'Crashing Tide','echo',0]],
  vela:     [[0,'Second Salvo','echo',0],[1,'Smoke Step','snare',1],[3,'Covering Fire','guard',1]],
  solstice: [[0,'Sandglass Fracture','echo',0],[1,'Watchmaker’s Salve','mend',0],[3,'Closing Hour','stun',0]],
  ferrite:  [[0,'Iron Wake','snare',0],[1,'Shared Iron','ward',1],[3,'Magnetic Rebound','echo',0]],
  cadence:  [[0,'Lingering Note','echo',0],[1,'Travelling Chorus','rush',1],[3,'Encore Ward','ward',0]],
  mirelle:  [[0,'Spore Cloud','venom',0],[1,'Sheltering Bramble','ward',0],[3,'Bloom Twice','echo',0]],
  knell:    [[0,'Bronze Reverb','echo',0],[1,'Marching Chime','rush',1],[3,'Silence’s Shelter','ward',1]],
  zephyr:   [[0,'Quill Barrage','echo',0],[1,'Tailspin','snare',1],[3,'Eye of the Gale','guard',1]],
  prism:    [[0,'Splinter Point','echo',0],[2,'Shattering Ground','snare',0],[3,'Spectrum Lock','stun',0]]
};

export const LATE_EFFECTS = {
  echo:  {damage:66, description:'warns a second damage burst after 0.85s in a 92-radius circle'},
  burn:  {damage:26,burn:18,burnDuration:3,damageType:'fire',description:'warns a patch after 0.5s that hits and burns enemies for 3s'},
  venom: {damage:20,poison:16,poisonDuration:3,damageType:'poison',description:'warns a spore burst after 0.5s that hits and poisons enemies for 3s'},
  snare: {slow:.32,slowDuration:2.2,description:'warns a 32% slow pulse after 0.5s that lasts 2.2s'},
  stun:  {stun:.65,damage:35,description:'warns a second burst after 0.85s that hits and briefly stuns'},
  ward:  {shield:65,description:'also shields nearby allied units for 3s'},
  mend:  {healing:54,description:'also heals nearby allied units'},
  guard: {guard:.82,description:'also reduces damage taken by nearby allied units by 18% for 2s'},
  rush:  {haste:.22,description:'also grants nearby allied units 22% movement speed for 2s'}
};

export function addLateTalents(heroes) {
  for (const [id, definition] of Object.entries(heroes)) {
    paths[id].forEach(([slot,name,effect,centered], index)=>{
      const tier = definition.talents.find(talent=>talent.level===[12,15,18][index]);
      const ability = definition.abilities[slot];
      tier.choices.push({
        id:`${id}-evolved-${tier.level}`, name, color:definition.color, icon:ability.icon,
        description:`${ability.name} ${LATE_EFFECTS[effect].description} ${centered || ability.type==='self' ? 'around you' : 'at the cast location' }.`,
        bonuses:{}, evolution:{slot,effect,centered:!!centered}
      });
    });
  }
}
