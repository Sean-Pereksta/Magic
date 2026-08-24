const RAW_CONCEPTS = {
  god: {label: "God",terms: ["God", "Lord", "the Lord", "Almighty", "God Almighty", "Most High", "the Most High", "Yahweh"]},
  jesus: {label: "Jesus / Son of God",terms: ["Jesus", "Jesus Christ", "Christ Jesus", "Christ", "Son of God", "Son of Man", "Messiah", "Savior", "Saviour", "The Angel of the Lord"]},
  love: {label: "Love",terms: ["love", "loved", "loves", "loving", "charity", "beloved"]},
  kindness: {label: "Kindness / Goodness",terms: ["kindness", "kind", "goodness", "lovingkindness", "loving kindness", "steadfast love", "compassion", "compassionate"]},
  faith: {label: "Faith / Belief",terms: ["faith", "faithful", "believe", "believed", "believes", "believing", "trust", "trusted", "trusts", "trusting"]},
  grace: {label: "Grace / Favor",terms: ["grace", "favor", "favour", "gracious"]},
  forgiveness: {label: "Forgiveness",terms: ["forgive", "forgives", "forgiven", "forgiving", "forgiveness", "pardon", "pardoned"]},
  mercy: {label: "Mercy / Compassion",terms: ["mercy", "merciful", "compassion", "compassionate", "pity"]},
  peace: {label: "Peace / Reconciliation",terms: ["peace", "peaceful", "peacemaker", "peacemakers", "reconcile", "reconciled", "reconciliation", "rest"]},
  truth: {label: "Truth",terms: ["truth", "true", "truly", "faithful and true"]},
  light: {label: "Light",terms: ["light", "lamp", "shine", "shines", "shining"]},
  holiness: {label: "Holiness",terms: ["holy", "holiness", "sanctify", "sanctified", "sanctification", "saint", "saints"]},
  salvation: {label: "Salvation / Rescue",terms: ["salvation", "save", "saved", "saves", "saving", "Savior", "Saviour", "rescue", "rescued", "deliver", "delivered", "deliverance", "redeem", "redeemed", "redemption"]},
  resurrection: {label: "Resurrection",terms: ["resurrection", "raised from the dead", "raised from dead", "rose from the dead", "rose again", "risen", "raised him", "raised Jesus"]},
  life: {label: "Life",terms: ["life", "living", "alive", "eternal life", "everlasting life", "life eternal"]},
  unity: {label: "Unity / Body of Christ",terms: ["unity", "united", "one body", "body of Christ", "body in Christ", "one in Christ", "one Spirit", "one mind", "same mind", "together", "fellowship", "members of one another", "members of his body"]},
  church: {label: "Church / God's People",terms: ["church", "churches", "body of Christ", "assembly", "congregation", "saints", "people of God", "household of God", "household of faith"]},
  freedom: {label: "Freedom",terms: ["free", "freedom", "liberty", "set free", "made free", "released", "release"]},
  humility: {label: "Humility / Meekness",terms: ["humble", "humbled", "humility", "lowly", "meek", "meekness"]},
  giving: {label: "Giving / Generosity",terms: ["give", "gives", "gave", "giving", "gift", "gifts", "generous", "generosity", "share", "shared"]},
  shepherd: {label: "Shepherd / Flock",terms: ["shepherd", "shepherds", "flock", "sheep", "good shepherd"]},
  bread: {label: "Bread / Provision",terms: ["bread", "bread of life", "daily bread", "food", "feed", "fed"]},
  kingdom: {label: "Kingdom",terms: ["kingdom", "kingdom of God", "kingdom of heaven", "his kingdom", "your kingdom"]},
  blood: {label: "Blood of Christ",terms: ["blood", "blood of Christ", "blood of Jesus", "his blood", "the blood of the Lamb"]},
  overcoming: {label: "Overcoming / Victory",terms: ["overcome", "overcomes", "overcame", "conquer", "conquers", "conquered", "victory", "triumph", "triumphed"]},
  prayer: {label: "Prayer",terms: ["pray", "prays", "prayed", "praying", "prayer", "prayers", "call upon", "called upon"]},
  spirit: {label: "Holy Spirit",terms: ["Spirit", "Holy Spirit", "Holy Ghost", "Spirit of God", "Spirit of Christ"]},
  wisdom: {label: "Wisdom / Understanding",terms: ["wisdom", "wise", "understanding", "discern", "discernment"]},
  hope: {label: "Hope",terms: ["hope", "hoped", "hopes", "hopeful", "expectation"]},
  joy: {label: "Joy / Rejoicing",terms: ["joy", "joyful", "rejoice", "rejoices", "rejoiced", "rejoicing", "glad", "gladness"]},
  law: {label: "Law / Commandments",terms: ["law", "laws", "commandment", "commandments", "statute", "statutes", "ordinance", "ordinances"]},
  word: {label: "Word of God / Scripture",terms: ["word", "word of God", "word of the Lord", "Scripture", "Scriptures", "written", "it is written"]},
  praise: {label: "Praise / Worship",terms: ["praise", "praised", "praises", "worship", "worshiped", "worshipped", "glorify", "glorified"]},
  covenant: {label: "Covenant / Promise",terms: ["covenant", "new covenant", "promise", "promised", "promises", "oath"]},
  righteousness: {label: "Righteousness",terms: ["righteous", "righteousness", "just", "justified", "justification"]},
  cross: {label: "Cross / Crucifixion",terms: ["cross", "the cross", "crucified", "crucify", "crucifixion", "tree"]},
  eternal: {label: "Eternal / Everlasting",terms: ["eternal", "everlasting", "forever", "for ever", "forevermore", "for evermore"]},
  obedience: {label: "Obedience",terms: ["obey", "obeyed", "obeys", "obedient", "obedience", "keep my commandments", "keep his commandments", "keep the commandments", "do what I command", "hearken"]},
  repentance: {label: "Repentance / Turning Back",terms: ["repent", "repented", "repents", "repentance", "turn from", "turned from", "turn away from", "return to me", "return unto me", "turn to the Lord"]},
  courage: {label: "Courage / Boldness",terms: ["courage", "courageous", "be strong", "fear not", "do not fear", "not afraid", "bold", "boldly", "boldness"]},
  justice: {label: "Justice / Right Judgment",terms: ["justice", "judgment", "judgement", "judge righteously", "justly", "execute justice", "do justice", "defend the poor", "defend the needy"]},
  patience: {label: "Patience / Endurance",terms: ["patient", "patience", "longsuffering", "long suffering", "endure", "endured", "endurance", "persevere", "perseverance", "steadfast", "steadfastness"]},
  thanksgiving: {label: "Thanksgiving / Gratitude",terms: ["thank", "thanks", "thankful", "thanksgiving", "give thanks", "gave thanks", "giving thanks", "with thanksgiving"]},
  family: {label: "Family / Household",terms: ["father", "mother", "parents", "children", "child", "son", "daughter", "husband", "wife", "household", "family", "families"]},
  creation: {label: "Creation / Creator",terms: ["create", "created", "creator", "creation", "made heaven", "made the heavens", "heaven and earth", "heavens and earth", "made all things", "all things were made"]},
  discipleship: {label: "Discipleship / Following Christ",terms: ["disciple", "disciples", "follow me", "follow him", "follow Jesus", "follow Christ", "following Jesus", "take up his cross", "take up your cross", "deny himself", "deny yourself"]},
  service: {label: "Service / Servanthood",terms: ["serve", "serves", "served", "serving", "servant", "servants", "minister", "ministered", "ministry", "wash one another's feet"]}
};

function normalizeTerm(value){
  return String(value ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[‘’]/g, "'")
    .toLowerCase()
    .replace(/\b([a-z0-9]+)'s\b/g, "$1")
    .replace(/[^a-z0-9']+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeVerseText(value){
  const normalized = normalizeTerm(value);
  return normalized ? ` ${normalized} ` : " ";
}

export const VERSE_CONCEPTS = Object.freeze(Object.fromEntries(
  Object.entries(RAW_CONCEPTS).map(([key, concept]) => [key, Object.freeze({
    key,
    label: concept.label,
    terms: Object.freeze(Array.from(new Set(concept.terms.map(normalizeTerm).filter(Boolean))))
  })])
));

export const MULTIPLAYER_CATEGORY_KEYS = Object.freeze([
  "god", "jesus", "love", "kindness", "faith", "grace", "forgiveness", "mercy", "peace", "truth",
  "light", "holiness", "salvation", "resurrection", "life", "unity", "church", "freedom", "humility",
  "giving", "shepherd", "kingdom", "blood", "overcoming", "prayer", "spirit", "wisdom", "hope", "joy",
  "law", "word", "praise", "covenant", "righteousness", "cross", "eternal", "obedience", "repentance",
  "courage", "justice", "patience", "thanksgiving", "family", "creation", "discipleship", "service"
]);

export function getVerseConcept(key){
  return VERSE_CONCEPTS[key] || null;
}

export function conceptLabel(key){
  return getVerseConcept(key)?.label || key || "Unknown concept";
}

export function conceptTerms(key){
  return getVerseConcept(key)?.terms?.slice() || [];
}

export function conceptHint(key, maxTerms = 5){
  const concept = getVerseConcept(key);
  if(!concept) return "";
  return concept.terms.slice(0, Math.max(1, maxTerms)).join(" • ");
}

export function matchVerseConcept(text, key){
  const concept = getVerseConcept(key);
  if(!concept) return null;
  const haystack = normalizeVerseText(text);
  let best = null;
  for(const term of concept.terms){
    if(!haystack.includes(` ${term} `)) continue;
    if(!best || term.length > best.term.length){
      best = {conceptKey:key, label:concept.label, term};
    }
  }
  return best;
}

export function matchVerseConcepts(text, keys = []){
  const matches=[];
  for(const key of keys){
    const match=matchVerseConcept(text,key);
    if(match) matches.push(match);
  }
  return matches;
}

const CLASS_ART_ROOT="https://pub-15a649bcaae84f2ca6c610e6ef0dde51.r2.dev/players/classes";
const DEFAULT_CLASS_ART="https://pub-15a649bcaae84f2ca6c610e6ef0dde51.r2.dev/players/wordwarden.png";
const CLASS_LEVELS=[5,10,15];
const C=(id,name,tier,parent,icon,art,bonus,concepts,desc)=>Object.freeze({id,name,tier,parent,icon,art,bonus,concepts:Object.freeze(concepts),desc});

export const RPG_SPECIALIZATION_CLASSES=Object.freeze([
  C("sword-of-truth","Sword of Truth",1,null,"⚔️","sword-of-truth",1.18,["truth","word","law"],"Build around Scripture, truth, and commandment verses."),
  C("herald-of-grace","Herald of Grace",1,null,"🕊️","herald-of-grace",1.18,["grace","forgiveness","mercy","salvation"],"Turn grace, mercy, forgiveness, and rescue verses into stronger attacks."),
  C("faithful-servant","Faithful Servant",1,null,"🤲","faithful-servant",1.18,["faith","love","service","giving","unity"],"Grow through faithful service, love, generosity, and fellowship."),
  C("scripture-knight","Scripture Knight",2,"sword-of-truth","🛡️","scripture-knight",1.24,["word","law","obedience"],"A disciplined defender strengthened by Scripture and obedience."),
  C("lightbearer","Lightbearer",2,"sword-of-truth","🔆","lightbearer",1.24,["truth","light","holiness"],"Specialize in truth that exposes darkness and calls to holiness."),
  C("kingdom-sentinel","Kingdom Sentinel",2,"sword-of-truth","👑","kingdom-sentinel",1.24,["kingdom","righteousness","justice"],"Guard the King's order through kingdom, righteousness, and justice verses."),
  C("redeemed-witness","Redeemed Witness",2,"herald-of-grace","✝️","redeemed-witness",1.24,["salvation","cross","blood"],"Center attacks on redemption, the cross, and the blood of Christ."),
  C("mercy-keeper","Mercy Keeper",2,"herald-of-grace","💧","mercy-keeper",1.24,["mercy","forgiveness","kindness"],"Lean into compassion, pardon, and goodness."),
  C("hope-herald","Hope Herald",2,"herald-of-grace","🌅","hope-herald",1.24,["hope","resurrection","eternal","life"],"Use resurrection and eternal-life promises as a weapon against despair."),
  C("prayer-disciple","Prayer Disciple",2,"faithful-servant","🙏","prayer-disciple",1.24,["prayer","faith","spirit"],"Strengthen verses of prayer, faith, and the Holy Spirit."),
  C("fellowship-shepherd","Fellowship Shepherd",2,"faithful-servant","🐑","fellowship-shepherd",1.24,["unity","church","shepherd","family"],"Protect the flock and strengthen the one body."),
  C("humble-steward","Humble Steward",2,"faithful-servant","🏺","humble-steward",1.24,["service","giving","humility","thanksgiving"],"Gain power from service, generosity, humility, and gratitude."),
  C("lawkeeper","Lawkeeper",3,"scripture-knight","📜","lawkeeper",1.32,["law","obedience","word"],"Maximum focus on commandments, obedience, and the written Word."),
  C("living-scroll","Living Scroll",3,"scripture-knight","📖","living-scroll",1.32,["word","wisdom","covenant"],"Blend Scripture, wisdom, and covenant promises into precise attacks."),
  C("beacon-saint","Beacon Saint",3,"lightbearer","🕯️","beacon-saint",1.32,["light","holiness","truth"],"Become a concentrated light-and-holiness specialist."),
  C("spirit-flame","Spirit Flame",3,"lightbearer","🔥","spirit-flame",1.32,["spirit","light","prayer"],"Channel Spirit, light, and prayer verses for explosive specialization damage."),
  C("kingdom-champion","Kingdom Champion",3,"kingdom-sentinel","🏆","kingdom-champion",1.32,["kingdom","overcoming","courage"],"Fight as a bold champion of the kingdom and victory."),
  C("righteous-judge","Righteous Judge",3,"kingdom-sentinel","⚖️","righteous-judge",1.32,["righteousness","justice","law"],"Specialize in righteous judgment, justice, and law."),
  C("cross-bearer","Cross Bearer",3,"redeemed-witness","🪵","cross-bearer",1.32,["cross","blood","salvation"],"Drive the redemption branch to its strongest cross-centered form."),
  C("resurrection-herald","Resurrection Herald",3,"redeemed-witness","🌄","resurrection-herald",1.32,["resurrection","life","eternal"],"Become a specialist in resurrection, life, and eternity."),
  C("forgiver","Forgiver",3,"mercy-keeper","🤍","forgiver",1.32,["forgiveness","grace","mercy"],"Stack grace, mercy, and forgiveness bonuses into a focused path."),
  C("compassion-keeper","Compassion Keeper",3,"mercy-keeper","💗","compassion-keeper",1.32,["kindness","mercy","love"],"Specialize in kindness, compassion, mercy, and love."),
  C("eternal-pilgrim","Eternal Pilgrim",3,"hope-herald","🌌","eternal-pilgrim",1.32,["hope","eternal","faith"],"Use hope, eternity, and faith to endure the deepest floors."),
  C("joyful-witness","Joyful Witness",3,"hope-herald","🎺","joyful-witness",1.32,["joy","praise","hope"],"Turn joy, praise, and hope into a high-energy specialization."),
  C("intercessor","Intercessor",3,"prayer-disciple","🛐","intercessor",1.32,["prayer","faith","mercy"],"Specialize in prayerful faith and mercy."),
  C("spirit-walker","Spirit Walker",3,"prayer-disciple","🕊️","spirit-walker",1.32,["spirit","prayer","holiness"],"Focus on Holy Spirit, prayer, and holiness verses."),
  C("shepherd-of-one-body","Shepherd of One Body",3,"fellowship-shepherd","⛪","shepherd-of-one-body",1.32,["unity","church","shepherd"],"Reach the deepest unity-and-church specialization."),
  C("house-guardian","House Guardian",3,"fellowship-shepherd","🏠","house-guardian",1.32,["family","love","service"],"Protect household and fellowship themes through love and service."),
  C("servant-disciple","Servant Disciple",3,"humble-steward","🧎","servant-disciple",1.32,["service","humility","discipleship"],"Commit fully to humble service and following Christ."),
  C("generous-steward","Generous Steward",3,"humble-steward","🎁","generous-steward",1.32,["giving","thanksgiving","kindness"],"Specialize in generosity, gratitude, and goodness.")
]);

const RPG_SPECIALIZATION_BY_ID=new Map(RPG_SPECIALIZATION_CLASSES.map(node=>[node.id,node]));
const rpgSpecializationState={path:[],lastLevel:1,choiceOpen:false,uiReady:false};

function classArtUrl(node){return node?`${CLASS_ART_ROOT}/${node.art}.png`:DEFAULT_CLASS_ART;}
function selectedSpecializationNodes(){return rpgSpecializationState.path.map(id=>RPG_SPECIALIZATION_BY_ID.get(id)).filter(Boolean);}
function activeSpecialization(){const nodes=selectedSpecializationNodes();return nodes[nodes.length-1]||null;}
function specializationBonusForVerse(text){
  let multiplier=1;
  const matched=[];
  for(const node of selectedSpecializationNodes()){
    if(!node.concepts.some(key=>!!matchVerseConcept(text,key)))continue;
    multiplier*=node.bonus;
    matched.push(node);
  }
  return {multiplier,matched};
}
function specializationBoostText(node){
  if(!node)return "No specialization yet";
  return `+${Math.round((node.bonus-1)*100)}% • ${node.concepts.map(conceptLabel).join(" • ")}`;
}
function createClassArt(node,size="card"){
  const wrap=document.createElement("div");wrap.className=`rpg-spec-art rpg-spec-art-${size}`;
  const img=document.createElement("img");img.src=classArtUrl(node);img.alt=node.name;img.onerror=()=>{img.remove();wrap.classList.add("fallback");};
  const icon=document.createElement("span");icon.textContent=node.icon;wrap.append(img,icon);return wrap;
}

function ensureSpecializationUi(){
  if(typeof document==="undefined"||rpgSpecializationState.uiReady)return;
  const rpgView=document.getElementById("rpgView"),levelEl=document.getElementById("playerLevel");if(!rpgView||!levelEl)return;
  rpgSpecializationState.uiReady=true;
  const style=document.createElement("style");style.id="rpgSpecializationStyles";style.textContent=`
    .rpg-spec-hidden{display:none!important}.rpg-spec-modal{position:fixed;inset:0;z-index:4000;background:rgba(4,8,16,.96);backdrop-filter:blur(8px);padding:max(12px,env(safe-area-inset-top)) 12px max(12px,env(safe-area-inset-bottom));overflow:auto;color:#f8fafc}.rpg-spec-shell{width:min(1180px,100%);margin:auto;border:1px solid #4c5d82;border-radius:20px;background:linear-gradient(145deg,#141d34,#0a1020);box-shadow:0 24px 70px rgba(0,0,0,.55);padding:16px}.rpg-spec-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}.rpg-spec-head h2{margin:5px 0}.rpg-spec-kicker{font-size:.68rem;font-weight:1000;letter-spacing:.15em;text-transform:uppercase;color:#c4b5fd}.rpg-spec-close{border:1px solid #475569;border-radius:11px;background:#11192c;color:#fff;padding:8px 11px;font-weight:900;cursor:pointer}.rpg-spec-choice-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin-top:14px}.rpg-spec-choice{border:1px solid #3f4e6d;border-radius:16px;background:#0b1326;color:#fff;padding:12px;text-align:left;cursor:pointer;box-shadow:0 8px 20px rgba(0,0,0,.2)}.rpg-spec-choice:hover{border-color:#a78bfa;transform:translateY(-1px)}.rpg-spec-choice h3{margin:8px 0 5px}.rpg-spec-choice p{margin:0;color:#cbd5e1;font-size:.78rem;line-height:1.4}.rpg-spec-boost{margin-top:8px;padding:7px 8px;border:1px solid #166534;border-radius:9px;background:#0d2819;color:#bbf7d0;font-size:.72rem;font-weight:900;line-height:1.35}.rpg-spec-art{position:relative;display:grid;place-items:center;overflow:hidden;border:1px solid #334155;background:radial-gradient(circle,#263451,#0a1020)}.rpg-spec-art img{width:100%;height:100%;object-fit:contain;position:absolute;inset:0;z-index:2}.rpg-spec-art span{font-size:2rem;z-index:1}.rpg-spec-art-card{height:112px;border-radius:12px}.rpg-spec-art-node{height:72px;border-radius:10px}.rpg-spec-tree-scroll{overflow:auto;padding:5px 2px 14px}.rpg-spec-tree{display:grid;grid-template-columns:repeat(3,minmax(520px,1fr));gap:16px;min-width:1600px;align-items:start}.rpg-spec-root{margin:12px 0 14px;padding:12px;border:1px solid #5b21b6;border-radius:14px;background:#20113c;text-align:center;font-weight:1000}.rpg-spec-branch{border:1px solid #334155;border-radius:16px;background:#091121;padding:10px}.rpg-spec-branch-title{display:flex;gap:9px;align-items:center;padding:8px;border-radius:12px;background:#121c33}.rpg-spec-branch-title .rpg-spec-art{width:70px;min-width:70px}.rpg-spec-tier2-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:9px;margin-top:10px}.rpg-spec-node{position:relative;border:1px solid #334155;border-radius:12px;background:#0c1427;padding:8px}.rpg-spec-node::before{content:'↓';display:block;text-align:center;color:#64748b;font-weight:1000;margin:-4px 0 3px}.rpg-spec-node.selected{border-color:#fbbf24;box-shadow:0 0 0 1px rgba(251,191,36,.28)}.rpg-spec-node .name{font-size:.78rem;font-weight:1000;margin:6px 0 4px}.rpg-spec-node .boost{font-size:.62rem;color:#a7f3d0;line-height:1.3}.rpg-spec-leaves{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:6px;margin-top:7px}.rpg-spec-leaf{border:1px solid #2b3955;border-radius:10px;background:#080f1e;padding:6px}.rpg-spec-leaf.selected{border-color:#fbbf24}.rpg-spec-leaf .name{font-size:.67rem}.rpg-spec-leaf .boost{font-size:.57rem}.rpg-spec-sidebar{margin-top:8px;border:1px solid #4c1d95;border-radius:11px;background:#17102c;padding:8px}.rpg-spec-sidebar button{width:100%;border:0;border-radius:9px;background:#5b21b6;color:#fff;padding:8px;font-weight:1000;cursor:pointer}.rpg-spec-sidebar-summary{font-size:.66rem;color:#ddd6fe;margin-top:6px;line-height:1.35}.rpg-spec-pathline{font-size:.72rem;color:#fde68a;font-weight:900;margin:8px 0 0}.rpg-spec-level{display:inline-flex;border:1px solid #854d0e;border-radius:999px;background:#2a1d08;color:#fde68a;padding:4px 8px;font-size:.68rem;font-weight:1000}
    @media(max-width:850px){.rpg-spec-choice-grid{grid-template-columns:1fr}.rpg-spec-shell{padding:12px}.rpg-spec-tree{grid-template-columns:repeat(3,minmax(440px,1fr));min-width:1360px}.rpg-spec-art-card{height:90px}}
  `;document.head.appendChild(style);
  const choice=document.createElement("div");choice.id="rpgSpecChoiceOverlay";choice.className="rpg-spec-modal rpg-spec-hidden";choice.innerHTML='<div class="rpg-spec-shell"><div class="rpg-spec-head"><div><div class="rpg-spec-kicker">Specialization unlocked</div><h2 id="rpgSpecChoiceTitle">Choose your path</h2><div class="rpg-spec-level" id="rpgSpecChoiceLevel"></div></div></div><p id="rpgSpecChoiceHint" style="color:#a5b4cc;line-height:1.45"></p><div class="rpg-spec-choice-grid" id="rpgSpecChoiceGrid"></div></div>';document.body.appendChild(choice);
  const tree=document.createElement("div");tree.id="rpgSpecTreeOverlay";tree.className="rpg-spec-modal rpg-spec-hidden";tree.innerHTML='<div class="rpg-spec-shell"><div class="rpg-spec-head"><div><div class="rpg-spec-kicker">Word Warden specializations</div><h2>The Great Scripture Path</h2><div class="rpg-spec-pathline" id="rpgSpecPathline">Word Warden</div></div><button class="rpg-spec-close" id="rpgSpecTreeClose" type="button">Close</button></div><p style="color:#a5b4cc;line-height:1.45">Advance at player levels 5, 10, and 15. Every node adds a verse-category damage bonus, and bonuses stack when a verse matches multiple classes along your chosen path.</p><div class="rpg-spec-root">🛡 Word Warden • Base Class</div><div class="rpg-spec-tree-scroll"><div class="rpg-spec-tree" id="rpgSpecTree"></div></div></div>';document.body.appendChild(tree);
  document.getElementById("rpgSpecTreeClose").onclick=()=>tree.classList.add("rpg-spec-hidden");tree.addEventListener("click",event=>{if(event.target===tree)tree.classList.add("rpg-spec-hidden");});
  const firstSideCard=document.querySelector("#rpgView .side-stack .side-card:first-child");if(firstSideCard){const sidebar=document.createElement("div");sidebar.className="rpg-spec-sidebar";sidebar.innerHTML='<button id="rpgSpecTreeBtn" type="button">🌿 Specialization Tree</button><div class="rpg-spec-sidebar-summary" id="rpgSpecSidebarSummary">Level 5 unlock: choose 1 of 3 paths.</div>';firstSideCard.appendChild(sidebar);document.getElementById("rpgSpecTreeBtn").onclick=()=>{renderSpecializationTree();tree.classList.remove("rpg-spec-hidden");};}
  const observer=new MutationObserver(()=>syncSpecializationLevel());observer.observe(levelEl,{childList:true,subtree:true,characterData:true});renderSpecializationTree();syncSpecializationLevel();
}

function eligibleSpecializations(tier){const parent=tier===1?null:rpgSpecializationState.path[tier-2]||null;return RPG_SPECIALIZATION_CLASSES.filter(node=>node.tier===tier&&node.parent===parent);}
function showSpecializationChoice(tier,level){
  ensureSpecializationUi();const overlay=document.getElementById("rpgSpecChoiceOverlay"),grid=document.getElementById("rpgSpecChoiceGrid");if(!overlay||!grid)return;const options=eligibleSpecializations(tier);if(!options.length)return;rpgSpecializationState.choiceOpen=true;
  document.getElementById("rpgSpecChoiceTitle").textContent=tier===1?"Choose your first specialization":tier===2?"Choose your advanced class":"Choose your final class";document.getElementById("rpgSpecChoiceLevel").textContent=`Player Level ${level} • Tier ${tier} of 3`;document.getElementById("rpgSpecChoiceHint").textContent=tier===1?"Pick one of three major paths. Your next choices will branch only from the class you choose here.":tier===2?"Your first class now branches into three more focused options.":"Your advanced class branches one last time into two final specializations.";grid.innerHTML="";
  for(const node of options){const button=document.createElement("button");button.type="button";button.className="rpg-spec-choice";button.appendChild(createClassArt(node,"card"));const title=document.createElement("h3");title.textContent=`${node.icon} ${node.name}`;const desc=document.createElement("p");desc.textContent=node.desc;const boost=document.createElement("div");boost.className="rpg-spec-boost";boost.textContent=specializationBoostText(node);button.append(title,desc,boost);button.onclick=()=>chooseSpecialization(node.id);grid.appendChild(button);}overlay.classList.remove("rpg-spec-hidden");
}
function chooseSpecialization(id){const node=RPG_SPECIALIZATION_BY_ID.get(id),tier=rpgSpecializationState.path.length+1;if(!node||node.tier!==tier||!eligibleSpecializations(tier).some(option=>option.id===id))return;rpgSpecializationState.path.push(id);rpgSpecializationState.choiceOpen=false;document.getElementById("rpgSpecChoiceOverlay")?.classList.add("rpg-spec-hidden");applySpecializationVisuals();renderSpecializationTree();setTimeout(syncSpecializationLevel,0);}
function resetSpecializationPath(){rpgSpecializationState.path=[];rpgSpecializationState.choiceOpen=false;document.getElementById("rpgSpecChoiceOverlay")?.classList.add("rpg-spec-hidden");applySpecializationVisuals();renderSpecializationTree();}
function syncSpecializationLevel(){if(typeof document==="undefined")return;const levelEl=document.getElementById("playerLevel");if(!levelEl)return;const level=Math.max(1,Number.parseInt(levelEl.textContent,10)||1);if(level<rpgSpecializationState.lastLevel&&level<=1)resetSpecializationPath();rpgSpecializationState.lastLevel=level;applySpecializationVisuals();const tier=rpgSpecializationState.path.length+1;if(tier<=3&&level>=CLASS_LEVELS[tier-1]&&!rpgSpecializationState.choiceOpen)showSpecializationChoice(tier,level);updateSpecializationSidebar(level);}
function updateSpecializationSidebar(level){const summary=document.getElementById("rpgSpecSidebarSummary");if(!summary)return;const active=activeSpecialization();if(active){const nextTier=rpgSpecializationState.path.length+1,next=nextTier<=3?` • Next class at Lv ${CLASS_LEVELS[nextTier-1]}`:" • Final specialization reached";summary.textContent=`${active.icon} ${active.name}: ${specializationBoostText(active)}${next}`;}else summary.textContent=`Level 5 unlock: choose 1 of 3 paths. Current level ${level}.`;}
function applySpecializationVisuals(){if(typeof document==="undefined")return;const node=activeSpecialization(),name=node?.name||"Word Warden",img=document.getElementById("playerArt");if(img){const wanted=classArtUrl(node);if(img.dataset.classArt!==wanted){img.dataset.classArt=wanted;img.src=wanted;img.alt=name;img.onerror=()=>{img.onerror=null;img.src=DEFAULT_CLASS_ART;};}}const barName=document.querySelector("#playerFighter .bar-label span:first-child"),fighterName=document.querySelector("#playerFighter .fighter-name"),fighterSub=document.querySelector("#playerFighter .fighter-sub"),sideTitle=document.querySelector("#rpgView .side-stack .side-card:first-child h3");if(barName)barName.textContent=name;if(fighterName)fighterName.textContent=name;if(sideTitle)sideTitle.textContent=`${node?.icon||"🛡"} ${name}`;if(fighterSub)fighterSub.textContent=node?specializationBoostText(node):"One verse at a time.";updateSpecializationSidebar(Math.max(1,Number.parseInt(document.getElementById("playerLevel")?.textContent,10)||1));}
function renderSpecializationTree(){
  if(typeof document==="undefined")return;const tree=document.getElementById("rpgSpecTree");if(!tree)return;tree.innerHTML="";const selected=new Set(rpgSpecializationState.path);
  for(const tier1 of RPG_SPECIALIZATION_CLASSES.filter(node=>node.tier===1)){const branch=document.createElement("section");branch.className="rpg-spec-branch";const head=document.createElement("div");head.className="rpg-spec-branch-title"+(selected.has(tier1.id)?" selected":"");head.appendChild(createClassArt(tier1,"node"));const copy=document.createElement("div");copy.innerHTML=`<div style="font-weight:1000">${tier1.icon} ${tier1.name}</div><div style="font-size:.66rem;color:#a7f3d0;margin-top:4px">${specializationBoostText(tier1)}</div>`;head.appendChild(copy);branch.appendChild(head);const tier2Grid=document.createElement("div");tier2Grid.className="rpg-spec-tier2-grid";
    for(const tier2 of RPG_SPECIALIZATION_CLASSES.filter(node=>node.tier===2&&node.parent===tier1.id)){const node=document.createElement("div");node.className="rpg-spec-node"+(selected.has(tier2.id)?" selected":"");node.appendChild(createClassArt(tier2,"node"));const name=document.createElement("div");name.className="name";name.textContent=`${tier2.icon} ${tier2.name}`;const boost=document.createElement("div");boost.className="boost";boost.textContent=specializationBoostText(tier2);node.append(name,boost);const leaves=document.createElement("div");leaves.className="rpg-spec-leaves";
      for(const leaf of RPG_SPECIALIZATION_CLASSES.filter(item=>item.tier===3&&item.parent===tier2.id)){const leafEl=document.createElement("div");leafEl.className="rpg-spec-leaf"+(selected.has(leaf.id)?" selected":"");const leafName=document.createElement("div");leafName.className="name";leafName.textContent=`${leaf.icon} ${leaf.name}`;const leafBoost=document.createElement("div");leafBoost.className="boost";leafBoost.textContent=specializationBoostText(leaf);leafEl.append(leafName,leafBoost);leaves.appendChild(leafEl);}node.appendChild(leaves);tier2Grid.appendChild(node);}branch.appendChild(tier2Grid);tree.appendChild(branch);}
  const pathline=document.getElementById("rpgSpecPathline");if(pathline)pathline.textContent=["Word Warden",...selectedSpecializationNodes().map(node=>`${node.icon} ${node.name}`)].join("  →  ");
}
function installSpecializationUiWhenReady(){if(typeof document==="undefined")return;const tryInstall=()=>ensureSpecializationUi();if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",tryInstall,{once:true});else setTimeout(tryInstall,0);}installSpecializationUiWhenReady();

export function matchWeightedConcepts(text, weaknesses = []){
  const matches=[];
  for(const weakness of weaknesses || []){const conceptKey=typeof weakness === "string" ? weakness : weakness?.concept;if(!conceptKey) continue;const match=matchVerseConcept(text,conceptKey);if(!match) continue;const multiplier=Number(typeof weakness === "string" ? 1 : weakness.multiplier ?? 1);matches.push({...match,multiplier:Number.isFinite(multiplier)?multiplier:1});}
  matches.sort((a,b)=>b.multiplier-a.multiplier || b.term.length-a.term.length);if(!matches.length) return null;
  const primary=matches[0],extras=matches.slice(1),comboBonus=extras.reduce((sum,match)=>sum+(Math.max(0,match.multiplier)*0.30),0),baseCombinedMultiplier=Math.min(2.5,primary.multiplier+comboBonus),spec=specializationBonusForVerse(text),combinedMultiplier=Math.min(4,baseCombinedMultiplier*spec.multiplier),specializationNames=spec.matched.map(node=>node.name),label=specializationNames.length?`${matches.map(match=>match.label).join(" + ")} • ${specializationNames.join(" + ")} class bonus`:matches.map(match=>match.label).join(" + "),common={...primary,label,multiplier:combinedMultiplier,baseMultiplier:primary.multiplier,comboBonus:baseCombinedMultiplier-primary.multiplier,comboCount:matches.length,matches,specializationMultiplier:spec.multiplier,specializationNames};
  if(!extras.length)return common;return {...common,term:matches.map(match=>match.term).join(" + ")};
}

export function verseMatchesConcept(text, key){return !!matchVerseConcept(text,key);}
