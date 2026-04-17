// ============================================================
// ADEQ TOOLBAR — Configuración de API Keys v3
// ============================================================

export const CONFIG = {
  // ── Supabase (URL y anon key son públicas por diseño) ─────────
  SUPABASE_URL:      "https://ticjpwimhtfkbccchfyp.supabase.co",
  SUPABASE_ANON_KEY: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRpY2pwd2ltaHRma2JjY2NoZnlwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ5MDE1MTksImV4cCI6MjA5MDQ3NzUxOX0.85xb7q52QHFsUZIqgOsogexMml--Ag1K3LY-a7cstyU",

  // ── Gmail OAuth (Client ID es público) ────────────────────────
  GMAIL_CLIENT_ID: "1006462691161-6uicvg6urcco0a50534c46l4jiclfm70.apps.googleusercontent.com",

  // ── API keys sensibles — se cargan en runtime desde toolbar_config ──
  // No hardcodear aquí. Se populan vía fetchApiKeys() tras el login.
  MONDAY_API_KEY:        "",
  MONDAY_ACTIVE_BOARD:   "1420268379",
  RAPIDAPI_KEY:          "",
  RAPIDAPI_TRAFFIC_HOST: "similarweb-insights.p.rapidapi.com",
  GEMINI_API_KEY:        "",
  APOLLO_API_KEY:        "",

  // ── Monday columns (estructura, no secretos) ──────────────────
  MONDAY_BOARDS: { prospectos: "1420268379" },
  // Mapping: login email → Monday user ID (para columna Persona deal_owner)
  MONDAY_USER_IDS: {
    "mgargiulo@adeqmedia.com": 56851451, // Maximiliano
    "sales@adeqmedia.com":     60940538, // Agustina
    "dhorovitz@adeqmedia.com": 56938560, // Diego
  },
  MONDAY_COLUMNS: {
    nombre:          "name",
    estado:          "deal_stage",
    ejecutivo:       "deal_owner",
    ejecutivo_txt:   "text_mksnnqxj",   // plain-text owner column (writable)
    fecha_contacto:  "deal_close_date",
    fecha_fu1:       "fecha2",
    fecha_fu2:       "fecha_1",
    geo:             "texto6",
    trafico:         "texto7",
    idioma:          "estado_12",
    comentarios:     "texto",
    email:           "text_mkrwahsz",
  },

  // ── General ────────────────────────────────────────────────────
  MEDIA_BUYER: "Max",
  MIN_TRAFFIC: 400000,
};
