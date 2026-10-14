(function (root) {
  'use strict';
  // Editorial metadata only: routes, modes and permissions come from GAMES.
  const config = Object.freeze({
    assetBase: '/lobby/assets',
    // Optional per-key {tile, thumb, hero} paths; relative paths use assetBase.
    artworkOverrides: Object.freeze({}),
    // Optional pick fields: badges:["New chapter"], categoryLabel:"Campaign".
    flagships: Object.freeze([
      {key:'apestogetherstrong', label:'Lead the uprising', pitch:'Rescue your tribe. Build your kingdom. Become a legend.', color:'#91e5b4'},
      {key:'ironthrone', label:'A kingdom of your own', pitch:'Raise your banners. Forge alliances. Rewrite the map.', color:'#c0b4ff'},
      {key:'warrealms', label:'Every card changes the war', pitch:'Build your command deck and challenge a Warbot or your friends.', color:'#ffd786'},
      {key:'cat', label:'The chase starts here', pitch:'Gather your friends. Build your defenses. Outsmart the cat.', color:'#ff9dbd'}
    ]),
    premium: Object.freeze([
      {key:'turncraft', label:'Three factions. One shattered front.', color:'#6ee7ff'},
      {key:'crowncouncil', label:'Build an army worth collecting', color:'#efbf79'},
      {key:'neonbreach', label:'Break into the arena', color:'#fb82dc'},
      {key:'arcanewilds', label:'Find your magic', color:'#b7a0ff'},
      {key:'chrisrunner', label:'One more run', color:'#ffbb78'},
      {key:'spacetyrants', label:'An empire among the stars', color:'#92b5ff'},
      {key:'chesswarlord', label:'The board is a battlefield', color:'#e8cf94'},
      {key:'hearthmouse', label:'A small mouse. A giant world.', color:'#c2db8e'}
    ]),
    // These complete experiences join the flagships/premium games in the top tier.
    builtOut: Object.freeze([
      {key:'cheesehold', label:'Build your last line of defense', color:'#e7ca8a'},
      {key:'gridbound', label:'A shared world of adventures', color:'#a8deb3'},
      {key:'roguecards', label:'A tiny squad. A mighty adventure.', color:'#e0b4f5'},
      {key:'dwarfworld', label:'A kingdom beneath the mountain', color:'#e5be85'},
      {key:'biblegame', label:'Begin your Scripture quest', color:'#e6d7a2'},
      {key:'bibleroguelike', label:'Adventure through Scripture', color:'#d8bd87'},
      {key:'receiverwindowqb', label:'Build your football franchise', color:'#9ec7ee'},
      {key:'basketball2', label:'Your journey to the court', color:'#efb285'},
      {key:'jesusjumper', label:'Run the journey', color:'#e7cf9e'},
      {key:'gateguard', label:'Five defenders. One last stand.', color:'#a7c2dc'},
      {key:'knightwarfare', label:'Take on the world', color:'#99d5c6'},
      {key:'onehitknight', label:'Make every move count', color:'#c2bcf2'}
    ]),
    experimental: Object.freeze([]),
    tools: Object.freeze(['pdfreader','nflradio','mtgcatalog','fantasyfootball','versemem'])
  });
  if (typeof module === 'object' && module.exports) module.exports = config;
  else root.GameHubShowcase = config;
})(globalThis);
