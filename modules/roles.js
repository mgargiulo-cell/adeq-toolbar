// ============================================================
// ADEQ Toolbar — Roles
// Determina si el usuario logueado es admin o media buyer.
// ============================================================
const ADMIN_EMAILS = new Set([
  "mgargiulo@adeqmedia.com",
]);

export function isAdminEmail(email) {
  return !!email && ADMIN_EMAILS.has(email.toLowerCase());
}

export function getRole(email) {
  return isAdminEmail(email) ? "admin" : "media_buyer";
}
