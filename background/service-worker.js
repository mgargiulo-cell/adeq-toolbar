// ============================================================
// ADEQ TOOLBAR — Service Worker (Background)
// ============================================================
// En MV3, el background es un service worker que se activa
// solo cuando hay eventos. No tiene acceso al DOM.
// ============================================================

// Configurar side panel para que abra al hacer click en el ícono.
// Se llama en cada arranque del service worker (no solo en install/update)
// para cubrir usuarios que instalaron antes de que existiera esta config.
chrome.sidePanel
  .setPanelBehavior({ openPanelOnActionClick: true })
  .catch((err) => console.error("setPanelBehavior failed:", err));

// Se activa cuando se instala o actualiza la extensión
chrome.runtime.onInstalled.addListener((details) => {
  if (details.reason === "install") {
    console.log("ADEQ Toolbar instalada correctamente.");
    chrome.tabs.create({ url: "chrome://extensions/" });
  }
  chrome.sidePanel
    .setPanelBehavior({ openPanelOnActionClick: true })
    .catch((err) => console.error("setPanelBehavior failed:", err));
});

chrome.runtime.onStartup.addListener(() => {
  chrome.sidePanel
    .setPanelBehavior({ openPanelOnActionClick: true })
    .catch((err) => console.error("setPanelBehavior failed:", err));
});

// Escucha mensajes del popup o content scripts
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (sender.id !== chrome.runtime.id) return;
  if (message.type === "PING") {
    sendResponse({ status: "ok", version: "2.0.0" });
  }
  return true; // Mantiene el canal abierto para respuestas async
});
