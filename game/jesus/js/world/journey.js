export const JOURNEY_NODES = [
  {id:"countryside",name:"Galilee Countryside",region:"countryside",difficulty:1,reward:"sandals",targetTime:900},
  {id:"desert",name:"Judean Wilderness",region:"desert",difficulty:2,reward:"shield",targetTime:1050},
  {id:"city",name:"Ancient City",region:"city",difficulty:3,reward:"holyLight",targetTime:1200},
  {id:"mountains",name:"Mountain Pass",region:"mountains",difficulty:4,reward:"wings",targetTime:1350},
  {id:"ruins",name:"Ancient Ruins",region:"ruins",difficulty:5,reward:"bread",targetTime:1450},
  {id:"storm",name:"Storm Highlands",region:"storm",difficulty:6,reward:"fireRain",targetTime:1550},
].map((node,index,array)=>({...node,index,type:"standard",next:array[index+1]?[array[index+1].id]:[]}));
export const JOURNEY_BY_ID=Object.fromEntries(JOURNEY_NODES.map(n=>[n.id,n]));
export function createJourneySave(){return{version:3,completed:[],unlocked:0,currentNode:"countryside",checkpoint:null,permanentPowers:[],maxHearts:4,records:{},updatedAt:Date.now()};}
export function reachableNodes(save){return JOURNEY_NODES.filter(n=>n.index<=Math.max(0,Number(save.unlocked)||0)).map(n=>n.id);}
export function completeJourneyNode(save,nodeId){const node=JOURNEY_BY_ID[nodeId];if(!node)return save;return{...save,completed:[...new Set([...(save.completed||[]),nodeId])],unlocked:Math.min(5,Math.max(save.unlocked||0,node.index+1)),currentNode:node.next[0]||nodeId,checkpoint:null,updatedAt:Date.now()};}
export function migrateJourney(old){
  const save=createJourneySave();if(!old||typeof old!=="object")return save;
  const mapping={galilee:"countryside",capernaum:"countryside",sea:"countryside",wilderness:"desert",rome:"city",ascension:"mountains",jerusalem:"ruins",heaven:"storm"};
  save.completed=[...new Set((Array.isArray(old.completed)?old.completed:[]).map(id=>mapping[id]).filter(Boolean))];
  save.unlocked=Math.min(5,Math.max(0,...save.completed.map(id=>JOURNEY_BY_ID[id].index+1)));
  save.currentNode=JOURNEY_NODES[save.unlocked].id;save.permanentPowers=(Array.isArray(old.permanentPowers)?old.permanentPowers:[]).filter(p=>["shield","sandals","bread","holyLight","fireRain","wings"].includes(p));
  save.legacyMigrated=true;return save;
}
