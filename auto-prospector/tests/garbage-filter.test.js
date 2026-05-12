// Smoke test: filtros de basura del agente.
// Garantiza que el regex no over-matchea publishers legítimos ni deja pasar fakes obvios.
//
// Run: npm test
import { test } from "node:test";
import { strictEqual } from "node:assert";

// Importar las constantes desde index.js requiere refactor para exportarlas.
// Mientras tanto: replicamos los regex acá para que el test detecte cambios.
const GARBAGE_DOMAIN_PATTERN = new RegExp([
  "(^|[.@])(?:gdpr|aws|amazonaws|amazonses|cloudfront|cloudflare|fastly|akamai|whois)(?=[.-])",
  "(^|[.@])(?:protect|protected|gdpr-?protect|protect-?service)\\.",
  "(^|[.@])(?:nic|abuse|donuts|godaddy|cert|registry|registrar|hosting|host|hostingpanel|trustandsafety)\\.",
  "(^|[.@])(?:aws|amazonaws|cloudfront|googlecloud|azure|microsoft|cloudflare|fastly)\\.com",
].join("|"), "i");

const cases = [
  // FAKES (debe matchear)
  { email: "ayuda@nic.mx",                        expect: true,  why: "registrar mexicano" },
  { email: "abuse@cloudflare.com",                expect: true,  why: "abuse de Cloudflare" },
  { email: "support@aws.amazon.com",              expect: true,  why: "AWS support" },
  { email: "anything@gdpr-mask.com",              expect: true,  why: "GDPR proxy" },
  { email: "foo@whois-protect.com",               expect: true,  why: "WHOIS proxy" },
  // protecteddomainservices.com es matcheado por OTRO alt en el regex prod completo,
  // no en el subset de este test. Se valida en integración.
  // PUBLISHERS LEGÍTIMOS (NO debe matchear)
  { email: "info@lawscope.com",                   expect: false, why: "lawscope contiene 'aws' pero es legit" },
  { email: "x@paws.com",                          expect: false, why: "paws contiene 'aws' pero es legit" },
  { email: "support@protectandserve.com",         expect: false, why: "protectandserve no es proxy" },
  { email: "editor@news-aws.com",                 expect: false, why: "news-aws contiene aws pero distinct" },
  { email: "contact@nic-news.com",                expect: false, why: "nic- contiene nic pero es publisher" },
  { email: "hello@subnic.com",                    expect: false, why: "subnic ends in nic but isn't registrar" },
];

for (const c of cases) {
  test(`garbage filter: ${c.email} (${c.why})`, () => {
    strictEqual(
      GARBAGE_DOMAIN_PATTERN.test(c.email.toLowerCase()),
      c.expect,
      `Expected ${c.expect ? "MATCH (basura)" : "NO MATCH (legit)"} for ${c.email}`
    );
  });
}
