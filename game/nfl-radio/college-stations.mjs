// Source-backed call signs are search keys, never a promise of internet game rights.
export const COLLEGE_STATIONS = {
 'Texas':{searches:['KVET','1300 The Zone'],url:'https://texaslonghorns.com/sports/2013/7/27/sponsor_0727135359'},
 'LSU':{searches:['WDGL','KZMZ'],url:'https://lsusports.net/radioaffiliates'},
 'Texas A&M':{searches:['WTAW','KZNE'],url:'https://12thman.com/tamu-sports-network'},
 'Alabama':{searches:['WFFN','95.3 The Bear'],url:'https://rolltide.com/sports/2016/8/25/crimson-tide-radio-and-television-information'},
 'Oregon':{searches:['KUJZ','KUGN'],url:'https://goducks.com/sports/2004/8/5/68136'},
 'Penn State':{searches:['WLGJ'],url:'https://gopsusports.com/radio-affiliates'},
 'Ohio State':{searches:['WBNS','97.1 The Fan'],url:'https://ohiostatebuckeyes.com/sports/2020/9/24/radio-broadcast-team-and-affiliates'},
 'Michigan':{searches:['WCSX','94.7 WCSX'],url:'https://mgoblue.com/sports/2017/6/16/football-broadcast-information'},
 'Georgia':{searches:['WSB','95.5 WSB'],url:'https://georgiadogs.com/sports/2017/6/23/bulldog-network-radio-affiliates'}
};
export function collegeStationSearches(team){
 if(!team)return [];
 return COLLEGE_STATIONS[team.city]?.searches||[team.name,`${team.city} sports`];
}
export function collegeExternalOptions(team){
 const mapped=COLLEGE_STATIONS[team?.city];
 return mapped?[{url:mapped.url,label:`${team.city} official radio network`,auth:'none',detail:'Official broadcast information and listening options. Online availability is controlled by the school and broadcaster.'}]:[];
}
