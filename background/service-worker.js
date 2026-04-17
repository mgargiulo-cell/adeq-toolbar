// ============================================================
// ADEQ TOOLBAR — Service Worker (Background)
// ============================================================
// En MV3, el background es un service worker que se activa
// solo cuando hay eventos. No tiene acceso al DOM.
// ============================================================

// Se activa cuando se instala o actualiza la extensión
chrome.runtime.onInstalled.addListener((details) => {
  if (details.reason === "install") {
    console.log("ADEQ Toolbar instalada correctamente.");
    chrome.tabs.create({ url: "chrome://extensions/" });
  }
  // Configurar side panel para que abra al hacer click en el ícono
  chrome.sidePanel.setPanelBehavior({ openPanelOnActionClick: true });
});

// Escucha mensajes del popup o content scripts
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (sender.id !== chrome.runtime.id) return;
  if (message.type === "PING") {
    sendResponse({ status: "ok", version: "2.0.0" });
  }
  return true; // Mantiene el canal abierto para respuestas async
});
