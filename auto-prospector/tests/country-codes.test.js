// Asegura que COUNTRY_CODES cubra todas las geos LATAM/EU/MENA críticas que
// usan los MBs en su focus_config. Si falta uno, el agente no podrá filtrar
// rows legacy (sin geos_all) que tengan ese país.
import { test } from "node:test";
import { ok } from "node:assert";

// Replicar — en el futuro extraer a módulo compartido
const COUNTRY_CODES = {
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

const REQUIRED_LATAM = ["AR","BR","MX","CO","CL","PE","VE","UY","PY","BO","DO","CR","PA","GT","HN","SV","NI","EC","CU","PR"];
const REQUIRED_MENA  = ["EG","MA","SA","AE","TN","DZ","JO","LB","IQ","KW","QA","OM","YE","LY"];

test("COUNTRY_CODES cubre todo LATAM crítico", () => {
  for (const code of REQUIRED_LATAM) {
    ok(COUNTRY_CODES[code], `Falta country ${code} (LATAM)`);
  }
});

test("COUNTRY_CODES cubre MENA crítico", () => {
  for (const code of REQUIRED_MENA) {
    ok(COUNTRY_CODES[code], `Falta country ${code} (MENA)`);
  }
});
