(function () {
  const LANG_ZH = "zh-CN";
  const LANG_EN = "en";
  const SUPPORTED = [LANG_ZH, LANG_EN];

  function getPathSegments() {
    return window.location.pathname.split("/").filter(Boolean);
  }

  function detectCurrentLanguage() {
    const segments = getPathSegments();
    const lang = segments.find((segment) => SUPPORTED.includes(segment));
    return lang || null;
  }

  function computeTargetUrl(targetLang) {
    const segments = getPathSegments();
    const currentLang = detectCurrentLanguage();

    if (currentLang) {
      const index = segments.indexOf(currentLang);
      segments[index] = targetLang;
      return "/" + segments.join("/");
    }

    return targetLang === LANG_ZH ? "/zh-CN/overview.html" : "/en/overview.html";
  }

  function buildButton(label, targetLang, active) {
    const link = document.createElement("a");
    link.className = "lang-switcher-btn" + (active ? " is-active" : "");
    link.href = computeTargetUrl(targetLang);
    link.textContent = label;
    link.setAttribute("aria-label", "Switch language to " + label);
    return link;
  }

  function mountSwitcher() {
    const wrapper = document.createElement("div");
    wrapper.className = "lang-switcher";

    const currentLang = detectCurrentLanguage();
    wrapper.appendChild(buildButton("中", LANG_ZH, currentLang === LANG_ZH));
    wrapper.appendChild(buildButton("EN", LANG_EN, currentLang === LANG_EN));

    document.body.appendChild(wrapper);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", mountSwitcher);
  } else {
    mountSwitcher();
  }
})();
