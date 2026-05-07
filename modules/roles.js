// ============================================================
// ADEQ Toolbar — Roles
// Determina si el usuario logueado es admin o media buyer.
// TEAM_EMAILS también se usa en el admin panel para pre-poblar
// dropdowns de filtro y leaderboard, incluso antes de que un MB
// haya generado actividad en la base.
// ============================================================
const ADMIN_EMAILS = new Set([
  "mgargiulo@adeqmedia.com",
]);

export const TEAM_EMAILS = [
  "mgargiulo@adeqmedia.com",
  "sales@adeqmedia.com",
  "dhorovitz@adeqmedia.com",
];

export function isAdminEmail(email) {
  return !!email && ADMIN_EMAILS.has(email.toLowerCase());
}

export function getRole(email) {
  return isAdminEmail(email) ? "admin" : "media_buyer";
}
