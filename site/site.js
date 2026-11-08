(function () {
  var reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  var main = document.querySelector("main");
  if (!main) return;

  var easeOut = "cubic-bezier(0.23, 1, 0.32, 1)";
  var easeIn = "cubic-bezier(0.55, 0, 1, 0.45)";

  function enter(el, delay) {
    el.animate(
      [
        { opacity: 0, transform: "translateY(8px)" },
        { opacity: 1, transform: "translateY(0)" },
      ],
      { duration: 280, delay: delay, easing: easeOut, fill: "backwards" }
    );
  }

  function exit(el) {
    main.getAnimations({ subtree: true }).forEach(function (anim) {
      anim.cancel();
    });
    return el.animate(
      [
        { opacity: 1, transform: "translateY(0)" },
        { opacity: 0, transform: "translateY(-8px)" },
      ],
      { duration: 180, easing: easeIn, fill: "forwards" }
    ).finished;
  }

  if (!reduce) {
    Array.prototype.forEach.call(main.children, function (el, index) {
      enter(el, Math.min(index, 2) * 70);
    });
  }

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
