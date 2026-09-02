// ═══════════════════════════════════════════════════════════════════════════════════════
// GEO — a qué país pertenece un dominio y cómo se prioriza
// ═══════════════════════════════════════════════════════════════════════════════════════
// Extraído de index.js (2026-09-02) sin cambiarle una coma a la lógica. Vive aparte porque
// lo usan DOS áreas que no deberían depender una de la otra: la selección de emails (que
// mira el TLD para saber si una dirección es del país del sitio) y la priorización
// geográfica de la prospección. Tenerlo en el medio de index.js obligaba a que el módulo de
// email importara del archivo que lo importa a él.
//
// ⚠️ La lista de GEO_BUCKETS es una regla de NEGOCIO, no un detalle técnico: define el orden
// en que se prospecta por región. Cambiarla cambia a quién se le escribe primero.

export const COUNTRY_CODES = {
  US:"United States", MX:"Mexico", AR:"Argentina", CO:"Colombia", BR:"Brazil",
  CL:"Chile", ES:"Spain", PE:"Peru", EC:"Ecuador", VE:"Venezuela", UY:"Uruguay",
  PY:"Paraguay", BO:"Bolivia", DO:"Dominican Republic", CR:"Costa Rica",
  PA:"Panama", GT:"Guatemala", HN:"Honduras", SV:"El Salvador", NI:"Nicaragua",
  CU:"Cuba", PR:"Puerto Rico",
  GB:"United Kingdom", FR:"France", DE:"Germany", IT:"Italy", PT:"Portugal",
  CA:"Canada", AU:"Australia", JP:"Japan", KR:"South Korea", IN:"India",
  VN:"Vietnam", TH:"Thailand", ID:"Indonesia", PH:"Philippines", TR:"Turkey",
  SA:"Saudi Arabia", AE:"UAE", EG:"Egypt", MA:"Morocco", ZA:"South Africa",
  NG:"Nigeria", RU:"Russia", UA:"Ukraine", PL:"Poland", NL:"Netherlands",
  BE:"Belgium", SE:"Sweden", CH:"Switzerland", AT:"Austria", NO:"Norway",
  DK:"Denmark", FI:"Finland", IL:"Israel", SG:"Singapore", CN:"China",
  MY:"Malaysia", GR:"Greece", HU:"Hungary", CZ:"Czech Republic", RO:"Romania",
  TW:"Taiwan", HK:"Hong Kong", PK:"Pakistan",
  BG:"Bulgaria", HR:"Croatia", SI:"Slovenia", RS:"Serbia", IE:"Ireland",
  BD:"Bangladesh", LK:"Sri Lanka", KE:"Kenya", DZ:"Algeria", TN:"Tunisia",
  JO:"Jordan", LB:"Lebanon", IQ:"Iraq", KW:"Kuwait", QA:"Qatar", OM:"Oman",
  YE:"Yemen", LY:"Libya", SN:"Senegal", CI:"Ivory Coast", GH:"Ghana",
};

export const GEO_BUCKETS = {
  // LATAM + España + paises hispanos = +30 (target principal ADEQ)
  hi: new Set([
    "AR","MX","CO","PE","CL","VE","EC","BO","PY","UY","GT","DO","HN","SV","NI","CR","PA","CU","PR","ES",
    "Argentina","Mexico","Colombia","Peru","Chile","Venezuela","Ecuador","Bolivia","Paraguay","Uruguay",
    "Guatemala","Dominican Republic","Honduras","El Salvador","Nicaragua","Costa Rica","Panama","Cuba",
    "Puerto Rico","Spain"
  ]),
  // Europa continental sin UK/RU = +25
  mid: new Set([
    "DE","FR","IT","PT","NL","BE","PL","RO","CZ","SK","HU","BG","GR","SE","NO","DK","FI","IE","AT","CH","HR","SI","RS","UA",
    "Germany","France","Italy","Portugal","Netherlands","Belgium","Poland","Romania","Czech Republic",
    "Slovakia","Hungary","Bulgaria","Greece","Sweden","Norway","Denmark","Finland","Ireland","Austria",
    "Switzerland","Croatia","Slovenia","Serbia","Ukraine"
  ]),
  // África = +15 (puede servir)
  africa: new Set([
    "NG","KE","ZA","EG","MA","DZ","TN","GH","ET","UG","TZ","SN","CM","CI","ZM","ZW","RW","MZ","AO",
    "Nigeria","Kenya","South Africa","Egypt","Morocco","Algeria","Tunisia","Ghana","Ethiopia","Uganda",
    "Tanzania","Senegal","Cameroon","Ivory Coast","Zambia","Zimbabwe","Rwanda","Mozambique","Angola"
  ]),
  // Asia (no India) = +5 baja conversión
  asia: new Set([
    "JP","KR","TH","VN","ID","MY","PH","TW","SG","BD","PK","LK","KH","MM","NP","HK","MN",
    "Japan","South Korea","Thailand","Vietnam","Indonesia","Malaysia","Philippines","Taiwan","Singapore",
    "Bangladesh","Pakistan","Sri Lanka","Cambodia","Myanmar","Nepal","Hong Kong","Mongolia"
  ]),
  // DESCARTE: Tier 1 + UK + Rusia (no encajan al portfolio actual ADEQ)
  blocked: new Set([
    "US","CA","AU","NZ","GB","UK","RU","BY","IL",
    "United States","Canada","Australia","New Zealand","United Kingdom","Russia","Belarus","Israel"
  ]),
};


// ── Prefijo internacional y largo NACIONAL esperado, por país ───────────────────────────
// El largo es lo que descarta los números que NO son teléfonos: un id de 12 dígitos no pasa
// por España (que usa 9) aunque tenga forma de teléfono. Sin el país NO se agrega prefijo:
// adivinarlo sería inventar un número al que alguien va a llamar.
// Los nombres son los que devuelve SimilarWeb, que es de donde sale el GEO del prospecto.
// ⚠️ Varios países tienen DOS largos: el fijo suele tener un dígito menos que el móvil.
// Poner uno solo descarta teléfonos reales — pasó con Paraguay (fijos de 8, tabla decía 9)
// y se borraron dos números buenos antes de notarlo.
export const PREFIJO_PAIS = {
  "Argentina":[54,[10]], "Brazil":[55,[10,11]], "Chile":[56,[8,9]], "Colombia":[57,[10]],
  "Mexico":[52,[10]], "Peru":[51,[8,9]], "Uruguay":[598,[8,9]], "Paraguay":[595,[8,9]],
  "Bolivia":[591,[8]], "Ecuador":[593,[8,9]], "Venezuela":[58,[10]], "Costa Rica":[506,[8]],
  "Panama":[507,[8]], "Guatemala":[502,[8]], "Honduras":[504,[8]], "El Salvador":[503,[8]],
  "Nicaragua":[505,[8]], "Dominican Republic":[1,[10]], "Cuba":[53,[8]], "Puerto Rico":[1,[10]],
  "Spain":[34,[9]], "Portugal":[351,[9]], "Italy":[39,[9,10]], "France":[33,[9]],
  "Germany":[49,[10,11]], "Austria":[43,[10,11]], "Switzerland":[41,[9]],
  "United Kingdom":[44,[10]], "Ireland":[353,[9]], "Netherlands":[31,[9]], "Belgium":[32,[9]],
  "Poland":[48,[9]], "Greece":[30,[10]], "Sweden":[46,[9]], "Norway":[47,[8]],
  "Denmark":[45,[8]], "Finland":[358,[9]], "Turkey":[90,[10]], "Russia":[7,[10]],
  "Ukraine":[380,[9]], "Romania":[40,[9]], "Czechia":[420,[9]], "Czech Republic":[420,[9]],
  "Hungary":[36,[9]], "Bulgaria":[359,[9]], "Serbia":[381,[8,9]], "Croatia":[385,[9]],
  "Slovakia":[421,[9]], "Slovenia":[386,[8]], "Lithuania":[370,[8]], "Latvia":[371,[8]],
  "Estonia":[372,[7,8]], "United States":[1,[10]], "Canada":[1,[10]],
  "India":[91,[10]], "Indonesia":[62,[9,10,11]], "Vietnam":[84,[9]], "Philippines":[63,[10]],
  "Thailand":[66,[9]], "Malaysia":[60,[9,10]], "Singapore":[65,[8]], "Japan":[81,[10]],
  "South Korea":[82,[9,10]], "China":[86,[11]], "Taiwan":[886,[9]], "Bangladesh":[880,[10]],
  "Pakistan":[92,[10]], "Egypt":[20,[10]], "Morocco":[212,[9]], "Algeria":[213,[9]],
  "Tunisia":[216,[8]], "Saudi Arabia":[966,[9]], "United Arab Emirates":[971,[9]],
  "Israel":[972,[9]], "Jordan":[962,[9]], "Qatar":[974,[8]], "Kuwait":[965,[8]],
  "Nigeria":[234,[10]], "Kenya":[254,[9]], "Ghana":[233,[9]], "South Africa":[27,[9]],
  "Australia":[61,[9]], "New Zealand":[64,[8,9]],
};

// ⚠️ El CRM guarda los países en ESPAÑOL ("México", "España", "Grecia") y SimilarWeb en
// inglés. Sin estos alias, un teléfono mexicano perfectamente válido se quedaba sin prefijo
// —y sin bandera— sólo porque el país venía escrito en el otro idioma. Eran 22 de 121.
const _ALIAS_ES = {
  "México":"Mexico", "Mexico":"Mexico", "España":"Spain", "Grecia":"Greece", "Brasil":"Brazil",
  "Alemania":"Germany", "Francia":"France", "Italia":"Italy", "Reino Unido":"United Kingdom",
  "Estados Unidos":"United States", "Países Bajos":"Netherlands", "Paises Bajos":"Netherlands",
  "Bélgica":"Belgium", "Suiza":"Switzerland", "Austria":"Austria", "Suecia":"Sweden",
  "Noruega":"Norway", "Dinamarca":"Denmark", "Finlandia":"Finland", "Polonia":"Poland",
  "Portugal":"Portugal", "Turquía":"Turkey", "Rusia":"Russia", "Ucrania":"Ukraine",
  "Rumania":"Romania", "Rumanía":"Romania", "Hungría":"Hungary", "Chequia":"Czechia",
  "República Checa":"Czech Republic", "Bulgaria":"Bulgaria", "Serbia":"Serbia",
  "Croacia":"Croatia", "Eslovaquia":"Slovakia", "Eslovenia":"Slovenia", "Irlanda":"Ireland",
  "Canadá":"Canada", "Japón":"Japan", "Corea del Sur":"South Korea", "China":"China",
  "Taiwán":"Taiwan", "Tailandia":"Thailand", "Filipinas":"Philippines", "Vietnam":"Vietnam",
  "Indonesia":"Indonesia", "Malasia":"Malaysia", "Singapur":"Singapore", "India":"India",
  "Egipto":"Egypt", "Marruecos":"Morocco", "Argelia":"Algeria", "Túnez":"Tunisia",
  "Arabia Saudita":"Saudi Arabia", "Emiratos Árabes Unidos":"United Arab Emirates",
  "Israel":"Israel", "Jordania":"Jordan", "Nigeria":"Nigeria", "Kenia":"Kenya",
  "Sudáfrica":"South Africa", "Australia":"Australia", "Nueva Zelanda":"New Zealand",
  "República Dominicana":"Dominican Republic", "Perú":"Peru", "Panamá":"Panama",
};

/** El prefijo del país, aceptando el nombre en inglés o en español. */
export function prefijoDe(pais) {
  const p = String(pais || "").trim();
  return PREFIJO_PAIS[p] || PREFIJO_PAIS[_ALIAS_ES[p]] || null;
}
