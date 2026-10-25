"use strict";

// Theme state is independent of data requests and of dataset/scope selection.
(() => {
  const allowed = new Set(["classic", "cs", "wizard"]);
  const preferenceKey = "ri.theme";
  const assistantKey = "ri.assistant.hidden";
  let theme;
  let assistantHidden = readPreference(assistantKey) === "true";

  function readPreference(key) {
    try { return localStorage.getItem(key); } catch { return null; }
  }

  function savePreference(key, value) {
    try { localStorage.setItem(key, value); } catch { /* Storage may be blocked. */ }
  }

  function fromURL(initial = false) {
    const value = new URLSearchParams(location.search).get("theme");
    if (value !== null) return allowed.has(value) ? value : "classic";
    const saved = initial ? readPreference(preferenceKey) : null;
    return allowed.has(saved) ? saved : "classic";
  }

  function updateControls() {
    const picker = document.getElementById("theme-select");
    const toggle = document.getElementById("assistant-toggle");
    const assistant = document.getElementById("wizard-assistant");
    if (picker) picker.value = theme;
    if (toggle) {
      toggle.hidden = theme !== "wizard";
      toggle.setAttribute("aria-pressed", String(!assistantHidden));
    }
    if (assistant) assistant.hidden = theme !== "wizard" || assistantHidden;
  }

  function apply(value, remember = false) {
    theme = allowed.has(value) ? value : "classic";
    document.documentElement.dataset.theme = theme;
    if (remember) savePreference(preferenceKey, theme);
    updateControls();
  }

  function writeURL() {
    const url = new URL(location.href);
    url.searchParams.set("theme", theme);
    history.replaceState(history.state, "", url);
  }

  apply(fromURL(true));
  // Stamp the chosen theme so browser history and shared URLs are unambiguous.
  writeURL();
  window.addEventListener("popstate", () => apply(fromURL()));
  document.addEventListener("DOMContentLoaded", () => {
    updateControls();
    document.getElementById("theme-select").addEventListener("change", event => {
      apply(event.target.value, true);
      writeURL();
    });
    document.getElementById("assistant-toggle").addEventListener("click", () => {
      assistantHidden = !assistantHidden;
      savePreference(assistantKey, String(assistantHidden));
      updateControls();
    });
    if (document.body.dataset.preview === "true") {
      document.getElementById("preview-notice").hidden = false;
    }
  });
})();
