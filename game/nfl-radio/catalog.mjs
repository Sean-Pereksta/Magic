// These are provider destinations, NOT playable audio URLs. Never infer NFL
// streaming rights from an AM/FM affiliation or a working general radio stream.
export const directories = {
 ARI:'azcardinals.com',ATL:'atlantafalcons.com',BAL:'baltimoreravens.com',BUF:'buffalobills.com',
 CAR:'panthers.com',CHI:'chicagobears.com',CIN:'bengals.com',CLE:'clevelandbrowns.com',
 DAL:'dallascowboys.com',DEN:'denverbroncos.com',DET:'detroitlions.com',GB:'packers.com',
 HOU:'houstontexans.com',IND:'colts.com',JAX:'jaguars.com',KC:'chiefs.com',LAC:'chargers.com',
 LAR:'therams.com',LV:'raiders.com',MIA:'miamidolphins.com',MIN:'vikings.com',NE:'patriots.com',
 NO:'neworleanssaints.com',NYG:'giants.com',NYJ:'newyorkjets.com',PHI:'philadelphiaeagles.com',
 PIT:'steelers.com',SEA:'seahawks.com',SF:'49ers.com',TB:'buccaneers.com',TEN:'tennesseetitans.com',WAS:'commanders.com'
};
export const catalog = [
 {id:'det-ticket',team:'DET',name:'97.1 The Ticket',kind:'flagship',language:'en',sourceUrl:'https://www.audacy.com/stations/971theticket',access:'provider',note:'Local game availability is controlled by the broadcaster.'},
 {id:'gb-writ',team:'GB',name:'95.7 BIG FM · WRIT',kind:'flagship',language:'en',sourceUrl:'https://www.packers.com/video/radio-network',access:'provider',note:'Official Packers network and live player.'},
 {id:'gb-wixx',team:'GB',name:'101.1 WIXX · Green Bay',kind:'affiliate',language:'en',sourceUrl:'https://www.packers.com/video/radio-network',access:'provider',note:'Listed affiliate; direct game streaming unverified.'},
 {id:'gb-wiba',team:'GB',name:'101.5 WIBA · Madison',kind:'affiliate',language:'en',sourceUrl:'https://www.packers.com/video/radio-network',access:'provider',note:'Listed affiliate; direct game streaming unverified.'},
 {id:'kc-fan',team:'KC',name:'96.5 The Fan · Chiefs Radio Network',kind:'flagship',language:'en',sourceUrl:'https://www.chiefs.com/listen/96-5-the-fan-the-kansas-city-chiefs-radio-network-stream',access:'provider',note:'Official Chiefs player; provider controls availability.'},
 {id:'phi-wip',team:'PHI',name:'SportsRadio 94 WIP',kind:'flagship',language:'en',sourceUrl:'https://www.audacy.com/stations/94wip',access:'provider',note:'Local market restrictions apply to game streams.'},
 {id:'phi-spanish',team:'PHI',name:'La Mega 105.7 · Eagles en Español',kind:'affiliate',language:'es',sourceUrl:'https://www.philadelphiaeagles.com/liveradio/',access:'provider',note:'Official Spanish gameday player.'},
 {id:'nfl-plus',name:'NFL+ · Home, away and national calls',kind:'subscription',language:'en',sourceUrl:'https://www.nfl.com/plus/',access:'subscription',note:'Subscription and provider login required. US availability.'},
 {id:'westwood',name:'Westwood One · National coverage',kind:'national',language:'en',sourceUrl:'https://www.westwoodonesports.com/',access:'provider',note:'Selected games only. Check the provider’s broadcast schedule.'}
];
