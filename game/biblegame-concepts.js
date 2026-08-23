const RAW_CONCEPTS = {
  god: {
    label: "God",
    terms: ["God", "Lord", "the Lord", "Almighty", "God Almighty", "Most High", "the Most High", "Yahweh"]
  },
  jesus: {
    label: "Jesus / Son of God",
    terms: ["Jesus", "Jesus Christ", "Christ Jesus", "Christ", "Son of God", "Son of Man", "Messiah", "Savior", "Saviour", "The Angel of the Lord"]
  },
  love: {
    label: "Love",
    terms: ["love", "loved", "loves", "loving", "charity", "beloved"]
  },
  kindness: {
    label: "Kindness / Goodness",
    terms: ["kindness", "kind", "goodness", "lovingkindness", "loving kindness", "steadfast love", "compassion", "compassionate"]
  },
  faith: {
    label: "Faith / Belief",
    terms: ["faith", "faithful", "believe", "believed", "believes", "believing", "trust", "trusted", "trusts", "trusting"]
  },
  grace: {
    label: "Grace / Favor",
    terms: ["grace", "favor", "favour", "gracious"]
  },
  forgiveness: {
    label: "Forgiveness",
    terms: ["forgive", "forgives", "forgiven", "forgiving", "forgiveness", "pardon", "pardoned"]
  },
  mercy: {
    label: "Mercy / Compassion",
    terms: ["mercy", "merciful", "compassion", "compassionate", "pity"]
  },
  peace: {
    label: "Peace / Reconciliation",
    terms: ["peace", "peaceful", "peacemaker", "peacemakers", "reconcile", "reconciled", "reconciliation", "rest"]
  },
  truth: {
    label: "Truth",
    terms: ["truth", "true", "truly", "faithful and true"]
  },
  light: {
    label: "Light",
    terms: ["light", "lamp", "shine", "shines", "shining"]
  },
  holiness: {
    label: "Holiness",
    terms: ["holy", "holiness", "sanctify", "sanctified", "sanctification", "saint", "saints"]
  },
  salvation: {
    label: "Salvation / Rescue",
    terms: ["salvation", "save", "saved", "saves", "saving", "Savior", "Saviour", "rescue", "rescued", "deliver", "delivered", "deliverance", "redeem", "redeemed", "redemption"]
  },
  resurrection: {
    label: "Resurrection",
    terms: ["resurrection", "raised from the dead", "raised from dead", "rose from the dead", "rose again", "risen", "raised him", "raised Jesus"]
  },
  life: {
    label: "Life",
    terms: ["life", "living", "alive", "eternal life", "everlasting life", "life eternal"]
  },
  unity: {
    label: "Unity / Body of Christ",
    terms: ["unity", "united", "one body", "body of Christ", "body in Christ", "one in Christ", "one Spirit", "one mind", "same mind", "together", "fellowship", "members of one another", "members of his body"]
  },
  church: {
    label: "Church / God's People",
    terms: ["church", "churches", "body of Christ", "assembly", "congregation", "saints", "people of God", "household of God", "household of faith"]
  },
  freedom: {
    label: "Freedom",
    terms: ["free", "freedom", "liberty", "set free", "made free", "released", "release"]
  },
  humility: {
    label: "Humility / Meekness",
    terms: ["humble", "humbled", "humility", "lowly", "meek", "meekness"]
  },
  giving: {
    label: "Giving / Generosity",
    terms: ["give", "gives", "gave", "giving", "gift", "gifts", "generous", "generosity", "share", "shared"]
  },
  shepherd: {
    label: "Shepherd / Flock",
    terms: ["shepherd", "shepherds", "flock", "sheep", "good shepherd"]
  },
  bread: {
    label: "Bread / Provision",
    terms: ["bread", "bread of life", "daily bread", "food", "feed", "fed"]
  },
  kingdom: {
    label: "Kingdom",
    terms: ["kingdom", "kingdom of God", "kingdom of heaven", "his kingdom", "your kingdom"]
  },
  blood: {
    label: "Blood of Christ",
    terms: ["blood", "blood of Christ", "blood of Jesus", "his blood", "the blood of the Lamb"]
  },
  overcoming: {
    label: "Overcoming / Victory",
    terms: ["overcome", "overcomes", "overcame", "conquer", "conquers", "conquered", "victory", "triumph", "triumphed"]
  },
  prayer: {
    label: "Prayer",
    terms: ["pray", "prays", "prayed", "praying", "prayer", "prayers", "call upon", "called upon"]
  },
  spirit: {
    label: "Holy Spirit",
    terms: ["Spirit", "Holy Spirit", "Holy Ghost", "Spirit of God", "Spirit of Christ"]
  },
  wisdom: {
    label: "Wisdom / Understanding",
    terms: ["wisdom", "wise", "understanding", "discern", "discernment"]
  },
  hope: {
    label: "Hope",
    terms: ["hope", "hoped", "hopes", "hopeful", "expectation"]
  },
  joy: {
    label: "Joy / Rejoicing",
    terms: ["joy", "joyful", "rejoice", "rejoices", "rejoiced", "rejoicing", "glad", "gladness"]
  },
  law: {
    label: "Law / Commandments",
    terms: ["law", "laws", "commandment", "commandments", "statute", "statutes", "ordinance", "ordinances"]
  },
  word: {
    label: "Word of God / Scripture",
    terms: ["word", "word of God", "word of the Lord", "Scripture", "Scriptures", "written", "it is written"]
  },
  praise: {
    label: "Praise / Worship",
    terms: ["praise", "praised", "praises", "worship", "worshiped", "worshipped", "glorify", "glorified"]
  },
  covenant: {
    label: "Covenant / Promise",
    terms: ["covenant", "new covenant", "promise", "promised", "promises", "oath"]
  },
  righteousness: {
    label: "Righteousness",
    terms: ["righteous", "righteousness", "just", "justified", "justification"]
  },
  cross: {
    label: "Cross / Crucifixion",
    terms: ["cross", "the cross", "crucified", "crucify", "crucifixion", "tree"]
  },
  eternal: {
    label: "Eternal / Everlasting",
    terms: ["eternal", "everlasting", "forever", "for ever", "forevermore", "for evermore"]
  }
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
  "law", "word", "praise", "covenant", "righteousness", "cross", "eternal"
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

export function matchWeightedConcepts(text, weaknesses = []){
  const matches=[];
  for(const weakness of weaknesses || []){
    const conceptKey=typeof weakness === "string" ? weakness : weakness?.concept;
    if(!conceptKey) continue;
    const match=matchVerseConcept(text,conceptKey);
    if(!match) continue;
    const multiplier=Number(typeof weakness === "string" ? 1 : weakness.multiplier ?? 1);
    matches.push({...match,multiplier:Number.isFinite(multiplier)?multiplier:1});
  }
  matches.sort((a,b)=>b.multiplier-a.multiplier || b.term.length-a.term.length);
  return matches[0] || null;
}

export function verseMatchesConcept(text, key){
  return !!matchVerseConcept(text,key);
}

function installRpgVerseEcho(){
  const rpgView=document.getElementById("rpgView");
  const battlefield=document.getElementById("battlefield");
  const input=document.getElementById("rpgRefInput");
  const attackButton=document.getElementById("rpgAttackBtn");
  const enemyFighter=document.getElementById("enemyFighter");
  if(!rpgView||!battlefield||!input||!attackButton||!enemyFighter||document.getElementById("rpgVerseEcho"))return;

  const style=document.createElement("style");
  style.textContent=`
    #rpgVerseEcho{position:absolute;z-index:7;left:50%;top:18%;width:min(80%,650px);transform:translate(-50%,12px);text-align:center;pointer-events:none;opacity:0;text-shadow:0 3px 10px rgba(0,0,0,.96),0 0 26px rgba(139,92,246,.34)}
    #rpgVerseEcho.active{animation:rpgVerseEchoFade 2.8s ease-out forwards}
    #rpgVerseEcho .rpg-verse-echo-ref{font-size:.74rem;letter-spacing:.13em;text-transform:uppercase;font-weight:1000;color:#fde68a;margin-bottom:6px}
    #rpgVerseEcho .rpg-verse-echo-text{font-family:Georgia,serif;font-size:clamp(1.05rem,2.5vw,1.55rem);font-weight:700;line-height:1.42;color:#fff}
    @keyframes rpgVerseEchoFade{0%{opacity:0;transform:translate(-50%,12px) scale(.98)}12%{opacity:1;transform:translate(-50%,0) scale(1)}68%{opacity:.96;transform:translate(-50%,-4px) scale(1)}100%{opacity:0;transform:translate(-50%,-18px) scale(1.02)}}
    @media(max-width:570px){#rpgVerseEcho{top:15%;width:91%}#rpgVerseEcho .rpg-verse-echo-text{font-size:1rem;line-height:1.35}}
  `;
  document.head.appendChild(style);

  const echo=document.createElement("div");
  echo.id="rpgVerseEcho";
  echo.setAttribute("role","status");
  echo.setAttribute("aria-live","polite");
  echo.innerHTML='<div class="rpg-verse-echo-ref"></div><div class="rpg-verse-echo-text"></div>';
  battlefield.appendChild(echo);
  const refNode=echo.querySelector(".rpg-verse-echo-ref");
  const textNode=echo.querySelector(".rpg-verse-echo-text");

  let pendingReference="";
  let showToken=0;
  const snapshotReference=()=>{pendingReference=input.value.trim()};
  attackButton.addEventListener("click",snapshotReference,true);
  input.addEventListener("keydown",event=>{if(event.key==="Enter")snapshotReference()},true);

  const normalizeBook=value=>{
    let book=String(value||"").trim().toLowerCase().replace(/\./g,"").replace(/\s+/g," ");
    book=book.replace(/^first\s+/,"1 ").replace(/^second\s+/,"2 ").replace(/^third\s+/,"3 ").replace(/^i\s+/,"1 ").replace(/^ii\s+/,"2 ").replace(/^iii\s+/,"3 ");
    return ({"psalm":"psalms","song of songs":"song of solomon","songs of solomon":"song of solomon","revelations":"revelation"})[book]||book;
  };
  const referenceKey=value=>{
    const match=String(value||"").trim().replace(/\s+/g," ").match(/^(.+?)\s+(\d+)\s*:\s*(\d+)$/);
    return match?`${normalizeBook(match[1])} ${Number(match[2])}:${Number(match[3])}`:"";
  };
  const parseCsv=text=>{
    const rows=[];let row=[],value="",quoted=false;
    for(let i=0;i<text.length;i++){
      const char=text[i];
      if(quoted){
        if(char==='"'){
          if(text[i+1]==='"'){value+='"';i++}else quoted=false;
        }else value+=char;
      }else if(char==='"')quoted=true;
      else if(char===','){row.push(value);value=""}
      else if(char==='\n'){row.push(value);rows.push(row);row=[];value=""}
      else if(char!=='\r')value+=char;
    }
    if(value.length||row.length){row.push(value);rows.push(row)}
    return rows;
  };
  const verseMapPromise=fetch("https://raw.githubusercontent.com/Sean-Pereksta/Bible-Data/refs/heads/main/web.csv",{cache:"force-cache"})
    .then(response=>response.ok?response.text():Promise.reject(new Error(`Bible data ${response.status}`)))
    .then(text=>{
      const rows=parseCsv(text),headers=(rows[0]||[]).map(value=>String(value||"").trim().toLowerCase());
      const bookIndex=headers.indexOf("bookname"),chapterIndex=headers.indexOf("chapter"),verseIndex=headers.indexOf("verse"),textIndex=headers.indexOf("text");
      const map=new Map();
      for(let i=1;i<rows.length;i++){
        const row=rows[i]||[],book=String(row[bookIndex]||"").trim(),chapter=Number(row[chapterIndex]),verse=Number(row[verseIndex]),verseText=String(row[textIndex]||"").trim();
        if(book&&chapter&&verse&&verseText)map.set(`${normalizeBook(book)} ${chapter}:${verse}`,{book,chapter,verse,text:verseText});
      }
      return map;
    })
    .catch(error=>{console.warn("RPG verse echo Bible lookup unavailable",error);return new Map()});

  const showVerse=async reference=>{
    const token=++showToken,key=referenceKey(reference);
    if(!key)return;
    const verse=(await verseMapPromise).get(key);
    if(token!==showToken||!verse)return;
    refNode.textContent=`${verse.book} ${verse.chapter}:${verse.verse}`;
    textNode.textContent=verse.text;
    echo.classList.remove("active");
    void echo.offsetWidth;
    echo.classList.add("active");
  };

  const hitObserver=new MutationObserver(()=>{
    if(!enemyFighter.classList.contains("hit")||!pendingReference)return;
    const reference=pendingReference;
    pendingReference="";
    void showVerse(reference);
  });
  hitObserver.observe(enemyFighter,{attributes:true,attributeFilter:["class"]});
}

installRpgVerseEcho();
