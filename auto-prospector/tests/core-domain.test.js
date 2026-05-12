// Asegura que coreDomain() reconozca ccSLDs (.org.cn, .or.jp, etc) y no
// agrupe sitios distintos como duplicados.
import { test } from "node:test";
import { strictEqual } from "node:assert";

const MULTI_PART_TLDS = new Set([
  "com.ar","com.br","com.cn","com.tw","com.bd","com.pk",
  "co.uk","co.in","co.kr","co.jp","co.id","co.th",
  "org.cn","org.tw","org.kr","org.jp","org.in","org.bd",
  "edu.cn","edu.tw","ac.in","ac.jp","ac.kr","ac.th",
  "net.cn","net.pk",
  "or.jp","ne.jp","ad.jp","ed.jp","gr.jp","go.jp",
]);

function coreDomain(domain) {
  if (!domain) return "";
  const parts = domain.toLowerCase().replace(/^www\./, "").split(".");
  if (parts.length <= 2) return parts[0];
  const last2 = parts.slice(-2).join(".");
  if (MULTI_PART_TLDS.has(last2) && parts.length >= 3) {
    return parts[parts.length - 3];
  }
  return parts[parts.length - 2];
}

const cases = [
  // Bug report: estos NO deben ser duplicados
  { d1: "celap.org.cn",       d2: "syth.org.cn",   distinct: true,  why: "diferentes orgs en .org.cn" },
  { d1: "fukushiokayama.or.jp", d2: "rerf.or.jp",  distinct: true,  why: "diferentes orgs en .or.jp" },
  { d1: "cust.edu.cn",        d2: "lzy.edu.cn",    distinct: true,  why: "diferentes universidades en .edu.cn" },
  { d1: "hake.net.cn",        d2: "tnt.com.cn",    distinct: true,  why: "diferentes en distintos TLDs" },
  { d1: "shinmai.co.jp",      d2: "hamee.co.jp",   distinct: true,  why: "diferentes empresas en .co.jp" },
  // Genuinos duplicados (deben matchear)
  { d1: "clarin.com",         d2: "clarin.com.ar", distinct: false, why: "mismo brand en .com y .com.ar" },
  { d1: "lanacion.com.ar",    d2: "vos.lanacion.com.ar", distinct: false, why: "subdominio mismo brand" },
];

for (const c of cases) {
  test(`coreDomain ${c.d1} vs ${c.d2}: ${c.why}`, () => {
    const o1 = coreDomain(c.d1);
    const o2 = coreDomain(c.d2);
    if (c.distinct) {
      strictEqual(o1 === o2, false, `Esperaba distintos, ambos resultaron "${o1}"`);
    } else {
      strictEqual(o1 === o2, true, `Esperaba mismo org, "${o1}" vs "${o2}"`);
    }
  });
}
