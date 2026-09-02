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
