/**
 * landing.js
 * Dynamic hide/reveal for the Home hero heading with graceful no-JS fallback.
 *
 * Behavior:
 * - Without JS: heading is visible (default HTML), so users always see context.
 * - With JS: heading is hidden initially; it reveals when the user shows intent
 *   (focuses the form or clicks the submit button). The heading is also made smaller.
 *
 * Notes:
 * - Applies to all `.hero-cta` containers on the page.
 * - Non-destructive: only adjusts inline styles/attributes of the heading.
 */

(function () {
  "use strict";

  function ready(fn) {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", fn, { once: true });
    } else {
      fn();
    }
  }

  function makeHeadingSmaller(heading) {
    // Keep it subtle to minimize layout shifts when revealed
    heading.style.fontSize = "1.15rem";
    heading.style.lineHeight = "1.35";
    heading.style.marginBottom = "0.5rem";
    heading.style.fontWeight = "700"; // maintain emphasis at smaller size
  }

  function hideHeading(heading) {
    // Use the hidden attribute for broad compatibility
    heading.setAttribute("aria-hidden", "true");
    heading.hidden = true;
  }

  function showHeading(heading) {
    heading.hidden = false;
    heading.removeAttribute("aria-hidden");
  }

  function setupHeroCta(cta) {
    const heading = cta.querySelector("h1, h2, h3, h4, h5, h6");
    if (!heading) return;

    // Progressive enhancement: only hide after JS runs
    makeHeadingSmaller(heading);
    hideHeading(heading);

    const form = cta.querySelector("form.waitlist-form");
    const submitBtn = form
      ? form.querySelector('button[type="submit"], input[type="submit"]')
      : null;
    const toggleBtn = cta.querySelector(".waitlist-toggle-btn");

    const reveal = () => {
      showHeading(heading);
    };

    // Reveal on user intent:
    // - Click "Sign up" toggle button
    // - Focus anywhere in the form
    // - Click the submit button
    if (toggleBtn) {
      toggleBtn.addEventListener("click", reveal, { once: true });
      toggleBtn.addEventListener("pointerdown", reveal, { once: true });
    }

    if (form) {
      form.addEventListener("focusin", reveal, { once: true });
      form.addEventListener("pointerdown", reveal, { once: true });
    }
    if (submitBtn) {
      submitBtn.addEventListener("click", reveal, { once: true });
      submitBtn.addEventListener("pointerdown", reveal, { once: true });
    }
  }

  ready(() => {
    try {
      const ctAs = document.querySelectorAll(".hero-cta");
      ctAs.forEach(setupHeroCta);
    } catch (_err) {
      // Fail silently; no-JS fallback remains visible
    }
  });
})();
