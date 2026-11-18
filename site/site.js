(function () {
  var reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  var easeOut = "cubic-bezier(0.23, 1, 0.32, 1)";
  var easeIn = "cubic-bezier(0.55, 0, 1, 0.45)";

  // On narrow screens the page list folds into a bar: the mark and a toggle.
  var nav = document.querySelector(".nav, .site-header");
  var pages = nav && nav.querySelector(":scope > ul");
  if (nav && pages) {
    var current = pages.querySelector('[aria-current="page"]');
    var toggle = document.createElement("button");
    toggle.type = "button";
    toggle.className = "nav-toggle";
    toggle.setAttribute("aria-expanded", "false");
    toggle.setAttribute("aria-controls", "nav-pages");
    pages.id = "nav-pages";
    if (nav.classList.contains("site-header")) {
      toggle.classList.add("nav-hamburger");
      toggle.setAttribute("aria-label", "Menu");
      toggle.innerHTML =
        '<span class="nav-hamburger-box" aria-hidden="true"><span></span><span></span><span></span></span>';
    } else {
      toggle.innerHTML =
        '<span></span><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 6l4 4 4-4"/></svg>';
      toggle.firstChild.textContent = current ? current.textContent : "Menu";
    }
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
  var stars = document.querySelector("[data-stars]");
  if (stars) {
    fetch("https://api.github.com/repos/nidheesh-m-vakharia/camus")
      .then(function (response) {
        return response.json();
      })
      .then(function (repo) {
        if (typeof repo.stargazers_count === "number") {
          stars.textContent = String(repo.stargazers_count);
        }
      })
      .catch(function () {});
  }

  var main = document.querySelector("main");
  if (!main) return;

  // Diagram loops (scripts/site-diagrams.py draws the marks they move). Each
  // shows how its drawing's relationship comes about. A loop runs only while
  // its drawing is on screen and the page is visible, and never under reduced
  // motion; cancelling its animations returns every mark to the still.
  var motion = window.matchMedia("(prefers-reduced-motion: reduce)");
  var glide = "cubic-bezier(0.45, 0, 0.2, 1)";

  function at(x, y) {
    return "translate(" + x + "px, " + y + "px)";
  }

  function play(run, el, frames, options) {
    var anim = el.animate(frames, Object.assign({ fill: "forwards" }, options));
    run.anims.push(anim);
    return anim.finished;
  }

  function wait(run, ms) {
    return new Promise(function (done) {
      run.timers.push(setTimeout(done, ms));
    });
  }

  function each(list, fn) {
    return Promise.all(Array.prototype.map.call(list, fn));
  }

  var scenes = {
    // A conveyor that ends where it starts, so the loop has no seam. The full
    // tail lifts off the brokers and drops into the bucket, the sealed log
    // indexes left by one tail so the sealed end stays put, and five new
    // batches arrive: each lands on the leader first, then on the other two.
    tail: function (svg, run) {
      var still = svg.querySelector(".dg-tail-static");
      var slots = svg.querySelector(".dg-a-slots");
      var block = svg.querySelector(".dg-a-upload");
      var log = svg.querySelector(".dg-log");
      var cells = svg.querySelectorAll(".dg-a-cell");
      function cell(row, col) {
        return cells[col * 3 + row];
      }
      var down = 92;
      return wait(run, 700)
        .then(function () {
          // The uploader takes the tail: the brokers' copies go as it leaves.
          return Promise.all([
            play(run, still, [{ opacity: 1 }, { opacity: 0 }], { duration: 300 }),
            play(run, slots, [{ opacity: 0 }, { opacity: 1 }], { duration: 300 }),
            play(run, block, [{ opacity: 0 }, { opacity: 0.5 }], { duration: 300 }),
          ]);
        })
        .then(function () {
          return play(run, block, [
            { opacity: 0.5, transform: at(0, 0) },
            { opacity: 0.5, transform: at(0, down) },
          ], { duration: 900, easing: glide });
        })
        .then(function () {
          return play(run, block, [
            { opacity: 0.5, transform: at(0, down) },
            { opacity: 1, transform: at(0, down) },
          ], { duration: 200, easing: easeOut });
        })
        .then(function () { return wait(run, 300); })
        .then(function () {
          // Sealed: the log indexes left by the tail's width.
          return Promise.all([
            play(run, log, [{ transform: at(0, 0) }, { transform: at(-100, 0) }], {
              duration: 700,
              easing: easeOut,
            }),
            play(run, block, [
              { opacity: 1, transform: at(0, down) },
              { opacity: 1, transform: at(-100, down) },
            ], { duration: 700, easing: easeOut }),
          ]);
        })
        .then(function () { return wait(run, 400); })
        .then(function () {
          // Five batches, 420 ms apart. A batch slides into the leader's next
          // slot, then drops to brokers 2 and 3.
          return each([0, 1, 2, 3, 4], function (col) {
            return wait(run, col * 420)
              .then(function () {
                return play(run, cell(0, col), [
                  { opacity: 0, transform: at(-28, 0) },
                  { opacity: 1, transform: at(0, 0) },
                ], { duration: 260, easing: easeOut });
              })
              .then(function () {
                return Promise.all([1, 2].map(function (row) {
                  return play(run, cell(row, col), [
                    { opacity: 0, transform: at(0, -40 * row) },
                    { opacity: 1, transform: at(0, 0) },
                  ], { duration: 220 + row * 60, easing: glide });
                }));
              });
          });
        })
        .then(function () {
          // The tail is full again: the still takes over, highlight and all.
          return Promise.all([
            play(run, still, [{ opacity: 0 }, { opacity: 1 }], { duration: 300 }),
            play(run, slots, [{ opacity: 1 }, { opacity: 0 }], { duration: 300 }),
          ]);
        })
        .then(function () { return wait(run, 900); });
    },

    // A steady stream of batches along the time line. Each one is
    // acknowledged as it passes the selected mode's point: in wal mode early,
    // at the majority fsync; in r2 mode only at the metadata commit.
    acks: function (svg, run) {
      var mode = svg.closest("figure").getAttribute("data-mode") || "wal";
      var period = 4200;
      var gap = period / 3;
      var span = 392;
      // Where the selected acknowledgement sits along the band, 0 to 1.
      var at_ack = mode === "r2" ? 0.98 : 88 / span;
      Array.prototype.forEach.call(svg.querySelectorAll(".dg-a-batch"), function (batch, i) {
        var anim = batch.animate([
          { opacity: 0, transform: at(0, 0) },
          { opacity: 1, transform: at(span * 0.03, 0), offset: 0.03 },
          { opacity: 1, transform: at(span * 0.97, 0), offset: 0.97 },
          { opacity: 0, transform: at(span, 0) },
        ], { duration: period, iterations: Infinity, delay: -i * gap, easing: "linear" });
        run.anims.push(anim);
      });
      // One flash per batch, timed to its crossing.
      var phase = ((at_ack * period) % gap) / gap;
      var ack = svg.querySelector(".dg-a-ack-" + mode);
      var flash = ack.animate([
        { opacity: 0, offset: 0 },
        { opacity: 0, offset: Math.max(phase - 0.01, 0) },
        { opacity: 1, offset: phase },
        { opacity: 0, offset: Math.min(phase + 0.3, 1) },
        { opacity: 0, offset: 1 },
      ], { duration: gap, iterations: Infinity, easing: "ease-out" });
      run.anims.push(flash);
      // Endless: the loop only stops when the figure leaves the screen.
      return new Promise(function () {});
    },

    // One cycle of replication, then the logs index left by one entry so the
    // cycle ends where it began. A new entry lands on the leader; entry 9
    // reaches broker 2, so two of three hold it and the commit index steps
    // right; broker 3 catches up by one.
    quorum: function (svg, run) {
      var q = function (sel) {
        return svg.querySelector(sel);
      };
      var logs = q(".dg-logs");
      var note = q(".dg-note");
      var commit = q(".dg-commit");
      var dim = q(".dg-dim");
      var fresh = q(".dg-a-new");
      var rep = q(".dg-a-rep");
      var caught = q(".dg-a-catch");
      var committed = q(".dg-a-commit");
      function drop(el, dy, ms) {
        return play(run, el, [{ opacity: 0 }, { opacity: 0.5 }], { duration: 200 })
          .then(function () {
            return play(run, el, [
              { opacity: 0.5, transform: at(0, 0) },
              { opacity: 0.5, transform: at(0, dy) },
            ], { duration: ms, easing: glide });
          })
          .then(function () {
            return play(run, el, [
              { opacity: 0.5, transform: at(0, dy) },
              { opacity: 1, transform: at(0, dy) },
            ], { duration: 200, easing: easeOut });
          });
      }
      return wait(run, 700)
        .then(function () {
          return Promise.all([
            play(run, note, [{ opacity: 1 }, { opacity: 0 }], { duration: 200 }),
            play(run, fresh, [
              { opacity: 0, transform: at(24, 0) },
              { opacity: 1, transform: at(0, 0) },
            ], { duration: 380, easing: easeOut }),
          ]);
        })
        .then(function () { return drop(rep, 52, 600); })
        .then(function () {
          return Promise.all([
            play(run, dim, [{ opacity: 1 }, { opacity: 0 }], { duration: 200 }),
            play(run, commit, [{ transform: at(0, 0) }, { transform: at(28, 0) }], {
              duration: 450,
              easing: easeOut,
            }),
            play(run, committed, [{ opacity: 0 }, { opacity: 1 }], { duration: 450 }),
          ]);
        })
        .then(function () { return drop(caught, 104, 700); })
        .then(function () { return wait(run, 900); })
        .then(function () {
          var ms = { duration: 650, easing: easeOut };
          return Promise.all([
            play(run, logs, [{ transform: at(0, 0) }, { transform: at(-28, 0) }], ms),
            play(run, fresh, [{ opacity: 1, transform: at(0, 0) }, { opacity: 1, transform: at(-28, 0) }], ms),
            play(run, rep, [{ opacity: 1, transform: at(0, 52) }, { opacity: 1, transform: at(-28, 52) }], ms),
            play(run, caught, [{ opacity: 1, transform: at(0, 104) }, { opacity: 1, transform: at(-28, 104) }], ms),
            play(run, committed, [{ opacity: 1, transform: at(0, 0) }, { opacity: 1, transform: at(-28, 0) }], ms),
            play(run, commit, [{ transform: at(28, 0) }, { transform: at(0, 0) }], ms),
          ]);
        })
        .then(function () {
          return Promise.all([
            play(run, note, [{ opacity: 0 }, { opacity: 1 }], { duration: 250 }),
            play(run, dim, [{ opacity: 0 }, { opacity: 1 }], { duration: 250 }),
          ]);
        })
        .then(function () { return wait(run, 700); });
    },

    // Each part is carried into its slot while its arrow's dashes run toward
    // the process; the slot then owns the edges.
    process: function (svg, run) {
      var parts = svg.querySelectorAll(".dg-a-part");
      var flows = svg.querySelectorAll(".dg-flow");
      return each(parts, function (part, i) {
        var flow = play(run, flows[i], [{ strokeDashoffset: 0 }, { strokeDashoffset: -20 }], {
          duration: 600,
          iterations: 2,
          delay: i * 180,
          fill: "none",
        });
        return wait(run, i * 180)
          .then(function () {
            return play(run, part, [{ opacity: 0 }, { opacity: 1 }], { duration: 200, easing: "ease-out" });
          })
          .then(function () {
            return play(run, part, [
              { opacity: 1, transform: at(0, 0) },
              { opacity: 1, transform: at(272, 0) },
            ], { duration: 1000, easing: easeOut });
          })
          .then(function () {
            return play(run, part, [
              { opacity: 1, transform: at(272, 0) },
              { opacity: 0, transform: at(272, 0) },
            ], { duration: 300 });
          })
          .then(function () { return flow; });
      }).then(function () { return wait(run, 1100); });
    },

    // The migration: the client starts in Apache Kafka's sockets, unseats,
    // and seats in Camus's, which have the same shape. Then it returns to the
    // still between the two.
    address: function (svg, run) {
      var client = svg.querySelector(".dg-client");
      var quiet = svg.querySelectorAll(".dg-route, .dg-fit");
      function go(from, to, ms, easing) {
        return play(run, client, [{ transform: at(from[0], from[1]) }, { transform: at(to[0], to[1]) }], {
          duration: ms,
          easing: easing,
        });
      }
      return wait(run, 600)
        .then(function () {
          return each(quiet, function (el) {
            return play(run, el, [{ opacity: 1 }, { opacity: 0 }], { duration: 300 });
          });
        })
        .then(function () { return go([0, 0], [120, -160], 1100, glide); })
        .then(function () { return wait(run, 900); })
        .then(function () { return go([120, -160], [40, -160], 450, "ease-in"); })
        .then(function () { return go([40, -160], [40, 0], 700, glide); })
        .then(function () { return go([40, 0], [120, 0], 500, easeOut); })
        .then(function () { return wait(run, 1500); })
        .then(function () { return go([120, 0], [0, 0], 800, glide); })
        .then(function () {
          return each(quiet, function (el) {
            return play(run, el, [{ opacity: 0 }, { opacity: 1 }], { duration: 300 });
          });
        })
        .then(function () { return wait(run, 700); });
    },

    // A cursor crosses the 0.93 ms at constant speed; each bar appears as
    // time reaches it, so the fsync ends inside the append.
    ack: function (svg, run) {
      var reveal = svg.querySelector(".dg-reveal");
      var cursor = svg.querySelector(".dg-a-cursor");
      return Promise.all([
        play(run, reveal, [{ transform: at(-480, 0) }, { transform: at(-80, 0) }], {
          duration: 2800,
          easing: "linear",
        }),
        play(run, cursor, [
          { opacity: 0, transform: at(0, 0) },
          { opacity: 0.32, transform: at(24, 0), offset: 0.06 },
          { opacity: 0.32, transform: at(400, 0) },
        ], { duration: 2800, easing: "linear" }),
      ])
        .then(function () {
          return Promise.all([
            play(run, reveal, [{ transform: at(-80, 0) }, { transform: at(0, 0) }], { duration: 200 }),
            play(run, cursor, [
              { opacity: 0.32, transform: at(400, 0) },
              { opacity: 0, transform: at(400, 0) },
            ], { duration: 250 }),
          ]);
        })
        .then(function () { return wait(run, 2200); })
        .then(function () {
          return play(run, reveal, [{ transform: at(0, 0) }, { transform: at(-484, 0) }], {
            duration: 500,
            easing: "ease-in",
          });
        })
        .then(function () { return wait(run, 400); });
    },
  };

  function loop(svg, scene) {
    var run = null;
    var visible = false;

    function stop() {
      if (!run) return;
      run.live = false;
      run.timers.forEach(clearTimeout);
      run.anims.forEach(function (anim) {
        anim.cancel();
      });
      run = null;
    }

    function start() {
      if (run || motion.matches || !visible || document.hidden) return;
      var current = (run = { live: true, anims: [], timers: [] });
      (function cycle() {
        if (!current.live) return;
        scene(svg, current).then(
          function () {
            if (!current.live) return;
            // Back to the still in the same task that starts the next cycle,
            // so no frame shows the reset.
            current.anims.forEach(function (anim) {
              anim.cancel();
            });
            current.anims = [];
            cycle();
          },
          function () {}
        );
      })();
    }

    new IntersectionObserver(function (entries) {
      visible = entries[0].isIntersecting;
      if (visible) start();
      else stop();
    }).observe(svg);
    document.addEventListener("visibilitychange", function () {
      if (document.hidden) stop();
      else start();
    });
    motion.addEventListener("change", function () {
      if (motion.matches) stop();
      else start();
    });
    return function restart() {
      stop();
      start();
    };
  }

  var restarts = {};
  if ("animate" in Element.prototype && "IntersectionObserver" in window) {
    Object.keys(scenes).forEach(function (name) {
      var svg = document.querySelector(".dg-" + name);
      if (svg) restarts[name] = loop(svg, scenes[name]);
    });
  }

  // The durability figure: the mode buttons choose which acknowledgement the
  // drawing shows, and its loop starts again for that mode.
  document.querySelectorAll(".plate[data-mode]").forEach(function (figure) {
    var group = figure.querySelector(".modes");
    if (!group) return;
    figure.classList.add("modes-live");
    group.addEventListener("click", function (event) {
      var button = event.target.closest("button[data-mode]");
      if (!button) return;
      figure.setAttribute("data-mode", button.getAttribute("data-mode"));
      group.querySelectorAll("button").forEach(function (other) {
        other.setAttribute("aria-pressed", String(other === button));
      });
      if (restarts.acks) restarts.acks();
    });
  });

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
