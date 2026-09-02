// ============================================================
// ADEQ TOOLBAR — Configuración de API Keys v3
// ============================================================

export const CONFIG = {
  // ── CRM BOARD PROPIO (Maxi 2026-09-02) ────────────────────────────────────────────────
  // Reemplaza a Monday, que queda como respaldo intacto. El secreto vive acá porque la
  // extensión no tiene backend propio; es el mismo esquema que ya se usa para las otras
  // claves del popup y el endpoint valida además que el dominio sea nuestro.
  CRM_BOARD_URL: "https://console.adeqmedia.com/api/crm/sync-toolbar",
  CRM_BOARD_SECRET: "aEuWCl5jdf9yh0kw2_EZgDfp1p298hGz",

  // ── Supabase (URL y anon key son públicas por diseño) ─────────
  SUPABASE_URL:      "https://ticjpwimhtfkbccchfyp.supabase.co",
  SUPABASE_ANON_KEY: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRpY2pwd2ltaHRma2JjY2NoZnlwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ5MDE1MTksImV4cCI6MjA5MDQ3NzUxOX0.85xb7q52QHFsUZIqgOsogexMml--Ag1K3LY-a7cstyU",

  // ── Gmail OAuth (Client ID es público) ────────────────────────
  GMAIL_CLIENT_ID: "1006462691161-6uicvg6urcco0a50534c46l4jiclfm70.apps.googleusercontent.com",

  // ── API keys sensibles — se cargan en runtime desde toolbar_config ──
  // No hardcodear aquí. Se populan vía fetchApiKeys() tras el login.
  RAPIDAPI_KEY:          "",
  RAPIDAPI_TRAFFIC_HOST: "website-insights.p.rapidapi.com",
  GEMINI_API_KEY:        "",
  APOLLO_API_KEY:        "",

  // ⚠️ Toda la config de Monday se borró el 2026-09-02: MONDAY_API_KEY, MONDAY_ACTIVE_BOARD,
  // MONDAY_BOARDS, MONDAY_USER_IDS y MONDAY_COLUMNS (ids de columnas del board).
  // Ninguna tenía un solo lector: el módulo que las usaba se eliminó al apagar Monday.
  // La key además se DESCARGABA al navegador en cada apertura de la toolbar, alimentando
  // código que ya no existía — una credencial viva en memoria del cliente, sin consumidor.

  // ── General ────────────────────────────────────────────────────
  MEDIA_BUYER: "Max",
  MIN_TRAFFIC: 350000,  // pageViews mínimos (visits × pagesPerVisit). Debajo: descartar. (User 2026-06-19: 350K pageviews; era 400K)
};
