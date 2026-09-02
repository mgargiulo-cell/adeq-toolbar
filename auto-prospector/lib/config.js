// ═══════════════════════════════════════════════════════════════════════════════════════
// CONFIG — de dónde salen las credenciales y las URLs
// ═══════════════════════════════════════════════════════════════════════════════════════
// Extraído de index.js el 2026-09-02. Va primero porque es la BASE: cualquier módulo que
// hable con la base o con el CRM lo necesita, y tenerlo en index.js obligaba a que esos
// módulos importaran del archivo que los importa a ellos (dependencia circular).
//
// ⚠️ Todo sale de variables de entorno de Railway. Nada de valores por defecto para las
// credenciales: si falta una, el worker tiene que fallar fuerte al arrancar, no funcionar a
// medias con permisos de menos y descubrirlo tres pantallas más adelante.
// La única con default es CRM_SYNC_URL, que es una URL pública, no un secreto.

export const SUPABASE_URL              = process.env.SUPABASE_URL;
export const SUPABASE_ANON_KEY         = process.env.SUPABASE_ANON_KEY;
export const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY; // bypass RLS (backend worker)
export const SUPABASE_EMAIL            = process.env.SUPABASE_EMAIL;
export const SUPABASE_PASSWORD         = process.env.SUPABASE_PASSWORD;
export const CLOUDFLARE_API_TOKEN      = process.env.CLOUDFLARE_API_TOKEN || null; // optional: Radar country-indexed pool
export const BACKEND_BEARER = SUPABASE_SERVICE_ROLE_KEY || null;
export const CRM_SYNC_URL = process.env.CRM_SYNC_URL || "https://console.adeqmedia.com/api/crm/sync-toolbar";
export const CRM_SYNC_SECRET = process.env.CRM_SYNC_SECRET || "";
