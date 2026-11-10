(function () {
  var reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  // On narrow screens the page list folds into a bar: the mark and a toggle.
  var nav = document.querySelector(".nav");
  var pages = nav && nav.querySelector(":scope > ul");
  if (nav && pages) {
    var current = pages.querySelector('[aria-current="page"]');
    var toggle = document.createElement("button");
    toggle.type = "button";
    toggle.className = "nav-toggle";
    toggle.setAttribute("aria-expanded", "false");
    toggle.setAttribute("aria-controls", "nav-pages");
    pages.id = "nav-pages";
    toggle.innerHTML =
      '<span></span><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 6l4 4 4-4"/></svg>';
    toggle.firstChild.textContent = current ? current.textContent : "Pages";
    nav.querySelector(".mark").after(toggle);
    nav.classList.add("nav-collapsible");

    toggle.addEventListener("click", function () {
      var open = nav.classList.toggle("nav-open");
      toggle.setAttribute("aria-expanded", String(open));
      if (open && !reduce) {
        pages.animate(
          [
            { opacity: 0, transform: "translateY(-4px)" },
            { opacity: 1, transform: "translateY(0)" },
          ],
          { duration: 200, easing: "cubic-bezier(0.23, 1, 0.32, 1)" }
        );
      }
    });

    document.addEventListener("keydown", function (event) {
      if (event.key !== "Escape" || !nav.classList.contains("nav-open")) return;
      nav.classList.remove("nav-open");
      toggle.setAttribute("aria-expanded", "false");
      toggle.focus();
    });
  }
  var main = document.querySelector("main");
  if (!main) return;

  var easeOut = "cubic-bezier(0.23, 1, 0.32, 1)";
  var easeIn = "cubic-bezier(0.55, 0, 1, 0.45)";

  function enter(el) {
    el.animate(
      [
        { opacity: 0, transform: "translateY(4px)" },
        { opacity: 1, transform: "translateY(0)" },
      ],
      { duration: 220, easing: easeOut, fill: "backwards" }
    );
  }

  function exit(el) {
    main.getAnimations({ subtree: true }).forEach(function (anim) {
      anim.cancel();
    });
    return el.animate(
      [
        { opacity: 1, transform: "translateY(0)" },
        { opacity: 0, transform: "translateY(-4px)" },
      ],
      { duration: 180, easing: easeIn, fill: "forwards" }
    ).finished;
  }

  if (!reduce) enter(main);

  window.addEventListener("pageshow", function (event) {
    if (!event.persisted) return;
    main.getAnimations({ subtree: true }).forEach(function (anim) {
      anim.cancel();
    });
  });

  document.addEventListener("click", function (event) {
    if (reduce || event.defaultPrevented || event.button !== 0) return;
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;

    var link = event.target.closest("a");
    if (!link || !link.href) return;
    if (link.target && link.target !== "_self") return;

    var url;
    try {
      url = new URL(link.href);
    } catch (error) {
      return;
    }
    if (url.origin !== location.origin) return;
    if (url.pathname === location.pathname && url.search === location.search) return;

    event.preventDefault();
    var next = link.href;
    exit(main).then(
      function () {
        location.href = next;
      },
      function () {
        location.href = next;
      }
    );
  });
})();
