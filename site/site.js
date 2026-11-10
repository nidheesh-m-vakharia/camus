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
    toggle.innerHTML =
      '<span></span><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 6l4 4 4-4"/></svg>';
    toggle.firstChild.textContent = current ? current.textContent : "Menu";
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

  // The home page scene. A producer sends a batch to the leader, the leader
  // replicates it to a majority's WAL, the ack goes back, and a consumer
  // waiting at the end of the log gets it. Every other batch an object
  // lands in the bucket, consumers read history from there, and at five
  // objects compaction folds them away. Raft heartbeats run between the
  // brokers. It all pauses while the scene is off screen.
  var iso = document.querySelector(".iso");
  if (iso) isoScene(iso);

  function isoScene(svg) {
    var NS = "http://www.w3.org/2000/svg";
    var cfg = JSON.parse(svg.getAttribute("data-iso"));
    var COS = Math.cos(Math.PI / 6);
    var ROWS = [0, 3, 6];
    var LEADER = 1;
    var figure = svg.closest("figure");
    var caption = figure.querySelector("figcaption");
    var mode = "wal";
    var CAPTIONS = {
      wal: "<code>wal</code>: a batch goes to the leader and reaches a majority's write-ahead log, and then the producer gets its ack. Objects go up to the bucket afterwards.",
      r2: "<code>r2</code>: a batch goes to the leader and straight into the bucket as an object, and its offsets are committed through a majority. Only then does the producer get its ack. The write-ahead log is not used.",
    };
    ROWS.forEach(function (r, i) {
      svg.querySelector("#iso-disk-" + i).classList.add("iso-disk-group");
    });
    figure.classList.add("iso-live");
    figure.querySelectorAll("[data-mode]").forEach(function (button) {
      button.addEventListener("click", function () {
        mode = button.getAttribute("data-mode");
        figure.querySelectorAll("[data-mode]").forEach(function (b) {
          b.setAttribute("aria-pressed", String(b === button));
        });
        svg.classList.toggle("iso-r2", mode === "r2");
        caption.innerHTML = CAPTIONS[mode];
        if (!reduce) caption.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 250, easing: easeOut });
      });
    });
    var packets = svg.querySelector("#iso-packets");
    var stack = svg.querySelector("#iso-stack");
    var slabs = 0;
    var glide = "cubic-bezier(0.45, 0, 0.2, 1)";

    function at(x, y, z) {
      return [cfg.ox + (x - y) * COS * cfg.s, cfg.oy + (x + y) * 0.5 * cfg.s - z * cfg.s];
    }
    function pts(list) {
      return list
        .map(function (p) {
          var q = at(p[0], p[1], p[2]);
          return q[0].toFixed(1) + "," + q[1].toFixed(1);
        })
        .join(" ");
    }
    function shape(parent, cls, points) {
      var el = document.createElementNS(NS, "polygon");
      el.setAttribute("class", cls);
      el.setAttribute("points", points);
      parent.appendChild(el);
      return el;
    }
    // A flat isometric square centred on the origin, placed with transforms.
    function token(cls, r) {
      var o = at(0, 0, 0);
      var corners = [at(r, 0, 0), at(0, r, 0), at(-r, 0, 0), at(0, -r, 0)]
        .map(function (q) {
          return (q[0] - o[0]).toFixed(1) + "," + (q[1] - o[1]).toFixed(1);
        })
        .join(" ");
      return shape(packets, cls, corners);
    }
    function place(p) {
      return "translate(" + p[0].toFixed(1) + "px, " + p[1].toFixed(1) + "px)";
    }
    function arc(a, b, lift, n) {
      var out = [];
      for (var i = 0; i <= n; i++) {
        var t = i / n;
        out.push(
          at(
            a[0] + (b[0] - a[0]) * t,
            a[1] + (b[1] - a[1]) * t,
            a[2] + (b[2] - a[2]) * t + lift * 4 * t * (1 - t)
          )
        );
      }
      return out;
    }
    function gone(el) {
      return function () {
        el.remove();
      };
    }
    function fly(el, path, ms) {
      var anim = el.animate(
        path.map(function (p) {
          return { transform: place(p) };
        }),
        { duration: ms, easing: glide, fill: "both" }
      );
      return anim.finished.then(gone(el), gone(el));
    }
    function indigo() {
      return getComputedStyle(svg).getPropertyValue("--indigo").trim();
    }
    function flash(id) {
      var c = indigo();
      svg.querySelectorAll("#" + id + " line").forEach(function (l) {
        l.animate({ stroke: [c, getComputedStyle(l).stroke] }, { duration: 700, easing: "ease-out" });
      });
    }
    function ripple(p) {
      var r = token("iso-ack", 0.45);
      var q = at(p[0], p[1], p[2]);
      r.animate(
        [
          { transform: place(q) + " scale(0.5)", opacity: 1 },
          { transform: place(q) + " scale(2.4)", opacity: 0 },
        ],
        { duration: 650, easing: easeOut, fill: "both" }
      ).finished.then(gone(r), gone(r));
    }
    function slab(animate) {
      if (slabs >= 5) return;
      var z = 0.12 + slabs * 0.38;
      var h = 0.26;
      var x0 = 11.35, x1 = 13.65, y0 = 0.85, y1 = 6.75;
      var g = document.createElementNS(NS, "g");
      shape(g, "iso-slab", pts([[x0, y0, z + h], [x1, y0, z + h], [x1, y0, z], [x1, y1, z], [x0, y1, z], [x0, y1, z + h]]));
      shape(g, "iso-slab", pts([[x0, y0, z + h], [x1, y0, z + h], [x1, y1, z + h], [x0, y1, z + h]]));
      stack.appendChild(g);
      slabs++;
      if (animate) {
        g.animate(
          [
            { opacity: 0, transform: "translateY(-12px)" },
            { opacity: 1, transform: "translateY(0)" },
          ],
          { duration: 420, easing: easeOut }
        );
      }
    }
    function compact() {
      var all = Array.prototype.slice.call(stack.children);
      return Promise.all(
        all.map(function (g, i) {
          return g.animate(
            [
              { opacity: 1, transform: "translateY(0)" },
              { opacity: 0, transform: "translateY(" + (i * 4 + 4) + "px)" },
            ],
            { duration: 520, delay: (all.length - i) * 70, easing: easeIn, fill: "forwards" }
          ).finished;
        })
      ).then(function () {
        stack.replaceChildren();
        slabs = 0;
        slab(true);
      });
    }

    var top = function (row) {
      return [6.8, ROWS[row] + 0.8, 2.8];
    };
    var producer = function (row) {
      return [0.5, ROWS[row] + 0.8, 1.0];
    };
    var consumer = function (row) {
      return [17.5, ROWS[row] + 0.8, 1.0];
    };

    if (reduce) {
      slab(false);
      slab(false);
      return;
    }

    // Draw the scene in: the floor, then each object rising into place.
    svg.querySelectorAll(".iso-grid").forEach(function (l, i) {
      var len = l.getTotalLength();
      l.style.strokeDasharray = len;
      l.animate([{ strokeDashoffset: len }, { strokeDashoffset: 0 }], {
        duration: 700,
        delay: i * 25,
        easing: easeOut,
        fill: "backwards",
      }).finished.then(function () {
        l.style.strokeDasharray = "";
      });
    });
    svg.querySelectorAll(".iso-obj, .iso-raft, .iso-label").forEach(function (el, i) {
      el.animate(
        [
          { opacity: 0, transform: "translateY(8px)" },
          { opacity: 1, transform: "translateY(0)" },
        ],
        { duration: 480, delay: 300 + i * 45, easing: easeOut, fill: "backwards" }
      );
    });

    // Raft heartbeats run out from the leader along both links.
    svg.querySelectorAll(".iso-raft").forEach(function (l, i) {
      var away = i === 0 ? 10 : -10;
      l.animate([{ strokeDashoffset: 0 }, { strokeDashoffset: away }], {
        duration: 900,
        iterations: Infinity,
      });
    });

    // The leader's marker floats above it.
    var crown = token("iso-crown", 0.3);
    var c0 = at(6.8, ROWS[LEADER] + 0.8, 3.5);
    var c1 = at(6.8, ROWS[LEADER] + 0.8, 3.75);
    crown.animate([{ transform: place(c0) }, { transform: place(c1) }], {
      duration: 1400,
      iterations: Infinity,
      direction: "alternate",
      easing: "ease-in-out",
    });

    var visible = true;
    var looping = false;
    var n = 0;

    function cycle() {
      if (!visible || document.hidden) {
        looping = false;
        return;
      }
      looping = true;
      var row = n % 3;
      var reader = (n + 1) % 3;
      n++;
      var lead = top(LEADER);
      flash("iso-producer-" + row);
      var sent = fly(token("iso-packet", 0.32), arc(producer(row), lead, 1.6, 24), 900);
      (mode === "r2" ? r2Flow(sent, row, reader, lead) : walFlow(sent, row, reader, lead)).then(function () {
        setTimeout(cycle, 260);
      });
    }

    // The ack back to the producer, and the batch to a consumer waiting at
    // the end of the log.
    function ackAndRead(row, reader, lead) {
      return Promise.all([
        fly(token("iso-ack", 0.3), arc(lead, producer(row), 1.2, 24), 820).then(function () {
          ripple(producer(row));
          flash("iso-producer-" + row);
        }),
        fly(token("iso-packet", 0.26), arc(lead, consumer(reader), 3.2, 32), 1100).then(function () {
          flash("iso-consumer-" + reader);
        }),
      ]);
    }

    // Commit through a majority: the leader and the first follower answer.
    function majority(lead, prefix) {
      flash(prefix + LEADER);
      return Promise.race(
        [0, 2].map(function (r) {
          return fly(token("iso-packet", 0.24), arc(lead, top(r), 0.6, 16), 380 + r * 60).then(function () {
            flash(prefix + r);
          });
        })
      );
    }

    function toBucket(lead, ms) {
      if (slabs >= 5) return compact();
      return fly(token("iso-packet", 0.4), arc(lead, [12.5, 3.8, 0.3 + slabs * 0.38], 2.2, 24), ms).then(function () {
        slab(true);
      });
    }

    // r2: the object is stored first, then its offsets are committed, and
    // only then is the batch acknowledged. Slower, and nothing on local disk.
    function r2Flow(sent, row, reader, lead) {
      return sent
        .then(function () {
          return toBucket(lead, 1300);
        })
        .then(function () {
          return majority(lead, "iso-broker-");
        })
        .then(function () {
          return ackAndRead(row, reader, lead);
        });
    }

    // wal: a majority's write-ahead log, the ack, and the upload later.
    function walFlow(sent, row, reader, lead) {
      return sent
        .then(function () {
          return majority(lead, "iso-disk-");
        })
        .then(function () {
          return ackAndRead(row, reader, lead);
        })
        .then(function () {
          if (n % 2 !== 0) return;
          return toBucket(lead, 900).then(function () {
            // A consumer catching up reads history from the bucket.
            if (slabs > 1 && n % 4 === 0) {
              var late = (n + 2) % 3;
              fly(token("iso-packet", 0.26), arc([12.5, 3.8, 2.2], consumer(late), 1.4, 20), 800).then(function () {
                flash("iso-consumer-" + late);
              });
            }
          });
        });
    }


    new IntersectionObserver(function (entries) {
      visible = entries[0].isIntersecting;
      if (visible && !looping) cycle();
    }).observe(svg);
    document.addEventListener("visibilitychange", function () {
      if (!document.hidden && visible && !looping) cycle();
    });
  }

  // Chart bars grow in once, when the chart scrolls into view.
  if (!reduce && "IntersectionObserver" in window) {
    var seen = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (!entry.isIntersecting) return;
          seen.unobserve(entry.target);
          entry.target.querySelectorAll(".bar").forEach(function (bar, i) {
            bar.animate([{ transform: "scaleX(0)" }, { transform: "scaleX(1)" }], {
              duration: 520,
              delay: i * 40,
              easing: "cubic-bezier(0.23, 1, 0.32, 1)",
              fill: "backwards",
            });
          });
        });
      },
      { threshold: 0.3 }
    );
    document.querySelectorAll(".chart").forEach(function (chart) {
      seen.observe(chart);
    });
  }


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
