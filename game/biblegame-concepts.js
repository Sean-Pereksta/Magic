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
  },
  obedience: {
    label: "Obedience",
    terms: ["obey", "obeyed", "obeys", "obedient", "obedience", "keep my commandments", "keep his commandments", "keep the commandments", "do what I command", "hearken"]
  },
  repentance: {
    label: "Repentance / Turning Back",
    terms: ["repent", "repented", "repents", "repentance", "turn from", "turned from", "turn away from", "return to me", "return unto me", "turn to the Lord"]
  },
  courage: {
    label: "Courage / Boldness",
    terms: ["courage", "courageous", "be strong", "fear not", "do not fear", "not afraid", "bold", "boldly", "boldness"]
  },
  justice: {
    label: "Justice / Right Judgment",
    terms: ["justice", "judgment", "judgement", "judge righteously", "justly", "execute justice", "do justice", "defend the poor", "defend the needy"]
  },
  patience: {
    label: "Patience / Endurance",
    terms: ["patient", "patience", "longsuffering", "long suffering", "endure", "endured", "endurance", "persevere", "perseverance", "steadfast", "steadfastness"]
  },
  thanksgiving: {
    label: "Thanksgiving / Gratitude",
    terms: ["thank", "thanks", "thankful", "thanksgiving", "give thanks", "gave thanks", "giving thanks", "with thanksgiving"]
  },
  family: {
    label: "Family / Household",
    terms: ["father", "mother", "parents", "children", "child", "son", "daughter", "husband", "wife", "household", "family", "families"]
  },
  creation: {
    label: "Creation / Creator",
    terms: ["create", "created", "creator", "creation", "made heaven", "made the heavens", "heaven and earth", "heavens and earth", "made all things", "all things were made"]
  },
  discipleship: {
    label: "Discipleship / Following Christ",
    terms: ["disciple", "disciples", "follow me", "follow him", "follow Jesus", "follow Christ", "following Jesus", "take up his cross", "take up your cross", "deny himself", "deny yourself"]
  },
  service: {
    label: "Service / Servanthood",
    terms: ["serve", "serves", "served", "serving", "servant", "servants", "minister", "ministered", "ministry", "wash one another's feet"]
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
  if(!matches.length) return null;

  const primary=matches[0];
  const extras=matches.slice(1);
  const comboBonus=extras.reduce((sum,match)=>sum+(Math.max(0,match.multiplier)*0.30),0);
  const combinedMultiplier=Math.min(2.5,primary.multiplier+comboBonus);
  if(!extras.length) return {...primary,matches,comboCount:1,comboBonus:0};

  return {
    ...primary,
    label:matches.map(match=>match.label).join(" + "),
    term:matches.map(match=>match.term).join(" + "),
    multiplier:combinedMultiplier,
    baseMultiplier:primary.multiplier,
    comboBonus:combinedMultiplier-primary.multiplier,
    comboCount:matches.length,
    matches
  };
}

export function verseMatchesConcept(text, key){
  return !!matchVerseConcept(text,key);
}
