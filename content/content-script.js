// ============================================================
// ADEQ TOOLBAR — Content Script
// ============================================================
// Se inyecta en cada página. Su función es escuchar mensajes
// del popup y devolver datos del DOM que solo son accesibles
// desde dentro de la página.
// ============================================================

// Responde a solicitudes del popup
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "GET_PAGE_DATA") {
    sendResponse({
      title:    document.title,
      lang:     document.documentElement.lang || navigator.language,
      metaDesc: document.querySelector('meta[name="description"]')?.content || "",
    });
  }
  return true;
});
