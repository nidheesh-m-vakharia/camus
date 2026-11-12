/* Monthly cost. Keep the arithmetic equal to scripts/cost.py. */
(function () {
  var MONTH_S = 30 * 24 * 3600;
  var CLASS_A = 4.5 / 1000000;
  var CLASS_B = 0.36 / 1000000;
  var STORAGE_PER_GB = 0.015;
  var FREE_CLASS_A = 1000000;
  var FREE_CLASS_B = 10000000;
  var FREE_GB = 10;
  var CAP_MIB = 8;
  var COMPACT_MIB = 128;
  var SNAP_S = 30;
  var SWEEP_S = 300;
  var LIST_PAGE = 1000;
  var GIB = 1024 * 1024 * 1024;
  var GB = 1000000000;

  function billable(amount, allowance) {
    return Math.max(0, amount - allowance);
  }

  function putsPerSecond(mibS, streams, flushS, capMib) {
    if (mibS <= 0 || streams <= 0 || flushS <= 0 || capMib <= 0) return 0;
    var perStream = mibS / streams;
    var filling = perStream >= capMib / 2 / flushS;
    var each = filling
      ? Math.max(1 / flushS, perStream / capMib)
      : Math.max(perStream / capMib, 1 / 30);
    return streams * each;
  }

  function num(form, name) {
    return Number(new FormData(form).get(name));
  }

  function estimate(input) {
    var flushS = input.flushMs / 1000;
    var hoursDay = Math.min(Math.max(input.activeHours, 0), 24);
    var duty = hoursDay / 24;
    var putsS = putsPerSecond(input.mibS, input.streams, flushS, CAP_MIB);
    var rejectedS = putsPerSecond(input.mibS, input.partitions, flushS, CAP_MIB);
    var ingestBytes = input.mibS * 1024 * 1024 * MONTH_S * duty;
    var ingestPuts = putsS * MONTH_S * duty;
    var snapPuts = MONTH_S / SNAP_S;
    var compactOps = input.compact ? (input.mibS * duty * MONTH_S) / COMPACT_MIB : 0;
    var retained = input.mibS * 1024 * 1024 * input.retentionDays * 86400 * duty;
    var objects = 0;
    if (input.compact && retained > 0) {
      objects = retained / (COMPACT_MIB * 1024 * 1024);
    } else if (MONTH_S) {
      objects = (ingestPuts / MONTH_S) * input.retentionDays * 86400;
    }
    var pages = objects > 0 ? Math.ceil(objects / LIST_PAGE) : 0;
    var listRequests = pages * (MONTH_S / SWEEP_S);
    var classA = ingestPuts + snapPuts + compactOps + listRequests;
    var classB = putsS * input.fanout * input.miss * MONTH_S * duty + compactOps;
    var diskBytes = 0;
    var follower = 0;
    if (input.mode === "wal") {
      var footprint = input.rf * input.mibS * 1024 * 1024 * input.lagS;
      var cap = input.brokers * 8 * GIB;
      diskBytes = Math.min(footprint, cap);
      follower = Math.max(input.rf - 1, 0) * ingestBytes;
    }
    var clientEgress = ingestBytes * input.fanout;
    var upload = ingestBytes;
    var netBytes = ingestBytes + follower + upload + clientEgress;
    var hours = MONTH_S / 3600;
    var perStream = input.streams > 0 ? input.mibS / input.streams : 0;
    var filling = flushS > 0 && perStream >= CAP_MIB / 2 / flushS;
    return {
      putsS: putsS,
      rejectedS: rejectedS,
      listRequests: listRequests,
      classA: classA,
      classB: classB,
      classAUsd: billable(classA, FREE_CLASS_A) * CLASS_A,
      classBUsd: billable(classB, FREE_CLASS_B) * CLASS_B,
      storageGb: retained / GB,
      storageUsd: billable(retained / GB, FREE_GB) * STORAGE_PER_GB,
      diskBytes: diskBytes,
      diskGib: diskBytes / GIB,
      diskUsd: (diskBytes / GB) * input.diskPerGb,
      ingressGib: ingestBytes / GIB,
      followerGib: follower / GIB,
      uploadGib: upload / GIB,
      egressGib: clientEgress / GIB,
      networkGib: netBytes / GIB,
      networkUsd: (netBytes / GIB) * input.netPerGib,
      computeUsd: input.brokers * input.hourly * hours,
      timerBinds:
        input.mibS > 0 &&
        hoursDay > 0 &&
        input.streams > 0 &&
        filling &&
        perStream < CAP_MIB / flushS,
    };
  }

  function money(n) {
    return n.toLocaleString("en-US", {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: 2,
    });
  }

  function count(n) {
    return Math.round(n).toLocaleString("en-US");
  }

  function gib(n) {
    return n.toLocaleString("en-US", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }) + " GiB";
  }

  function disk(bytes) {
    if (bytes < GIB) {
      return (bytes / (1024 * 1024)).toLocaleString("en-US", {
        maximumFractionDigits: 1,
      }) + " MiB";
    }
    return gib(bytes / GIB);
  }

  function render(form) {
    var input = {
      mibS: num(form, "mib_s"),
      activeHours: num(form, "active_hours"),
      partitions: num(form, "partitions"),
      streams: num(form, "streams"),
      retentionDays: num(form, "retention_days"),
      fanout: num(form, "fanout"),
      miss: num(form, "miss"),
      brokers: num(form, "brokers"),
      hourly: num(form, "hourly"),
      mode: new FormData(form).get("mode"),
      rf: num(form, "rf"),
      lagS: num(form, "lag_s"),
      flushMs: num(form, "flush_ms"),
      diskPerGb: num(form, "disk_per_gb"),
      netPerGib: num(form, "net_per_gib"),
      compact: form.elements.compact.checked,
    };
    var row = estimate(input);
    var total =
      row.classAUsd +
      row.classBUsd +
      row.storageUsd +
      row.diskUsd +
      row.networkUsd +
      row.computeUsd;
    var netUsd = row.networkGib > 0 ? row.networkUsd / row.networkGib : 0;
    var lines = [
      ["Class A requests / month", count(row.classA), money(row.classAUsd)],
      ["Class B requests / month", count(row.classB), money(row.classBUsd)],
      ["R2 storage", gib(row.storageGb * (GB / GIB)), money(row.storageUsd)],
      ["Compute", input.brokers + (Number(input.brokers) === 1 ? " broker" : " brokers"), money(row.computeUsd)],
      ["WAL disk", disk(row.diskBytes), money(row.diskUsd)],
      ["Client ingress", gib(row.ingressGib), money(row.ingressGib * netUsd)],
      ["Follower replication", gib(row.followerGib), money(row.followerGib * netUsd)],
      ["Upload to the bucket", gib(row.uploadGib), money(row.uploadGib * netUsd)],
      ["Out to consumers", gib(row.egressGib), money(row.egressGib * netUsd)],
      ["Total", "", money(total)],
    ];
    var body = document.getElementById("cost-rows");
    body.replaceChildren();
    lines.forEach(function (line) {
      var tr = document.createElement("tr");
      line.forEach(function (cell) {
        var td = document.createElement("td");
        td.textContent = cell;
        tr.appendChild(td);
      });
      body.appendChild(tr);
    });
    document.getElementById("cost-puts").textContent = row.putsS.toLocaleString("en-US", {
      maximumFractionDigits: 2,
    });
    document.getElementById("cost-rejected").textContent = row.rejectedS.toLocaleString("en-US", {
      maximumFractionDigits: 2,
    });
    document.getElementById("cost-lists").textContent = count(row.listRequests);
    var notes = [];
    if (row.timerBinds) {
      notes.push(
        "The upload timer binds. Each active stream flushes every " +
          input.flushMs +
          " ms instead of filling an 8 MiB object, so Class A per byte stays high. A longer flush interval cuts puts and, in r2 mode, raises acknowledgement latency."
      );
    }
    if (input.streams > 1 && row.timerBinds) {
      notes.push(
        "Each active WAL shard has its own timer. Spreading a small ingest across more shards multiplies Class A puts. Partitions that share a shard do not."
      );
    }
    if (input.fanout * input.miss >= 2) {
      notes.push(
        "Sealed reads dominate Class B: fan-out times the cache-miss share, once per ingest object. Reads served from the write-ahead log are not in this count. After compaction, a historical read is one GET per 128 MiB."
      );
    }
    if (input.activeHours < 24) {
      notes.push(
        "MiB/s is the rate while ingest is flowing, " +
          input.activeHours +
          " hours a day. The other hours add no puts and no bytes. Snapshots and the orphan sweep still run all month."
      );
    }
    if (row.listRequests > 0) {
      var shape = input.compact
        ? "Compaction is on, so the live set is about one key per 128 MiB."
        : "Compaction is off, so one key per flush stays for the retention window.";
      notes.push(
        "The orphan sweep lists bundle/ every 5 minutes. R2 bills one Class A request per 1,000 keys, " +
          count(row.listRequests) +
          " list requests this month. " +
          shape
      );
    }
    notes.push(
      "Dollars are after R2 Standard’s free tier: 1 million Class A requests, 10 million Class B requests, and 10 GB-month. Cloudflare also rounds a class up to the next million requests and storage up to the next GB. This table does not."
    );
    var list = document.getElementById("cost-notes");
    list.replaceChildren();
    notes.forEach(function (text) {
      var li = document.createElement("li");
      li.textContent = text;
      list.appendChild(li);
    });
  }

  if (typeof module !== "undefined" && module.exports) {
    module.exports = { putsPerSecond: putsPerSecond, estimate: estimate };
  }
  if (typeof document === "undefined") return;
  var form = document.getElementById("cost-form");
  if (!form) return;
  form.addEventListener("input", function () {
    render(form);
  });
  form.addEventListener("pointerdown", function (event) {
    var input = event.target;
    if (!input || input.type !== "number") return;
    var rect = input.getBoundingClientRect();
    if (event.clientX < rect.right - 28) return;
    event.preventDefault();
    var step = input.step === "any" || input.step === "" ? 1 : Number(input.step);
    if (!isFinite(step) || step <= 0) step = 1;
    var current = Number(input.value);
    if (!isFinite(current)) current = 0;
    var direction = event.clientY < rect.top + rect.height / 2 ? 1 : -1;
    var next = current + direction * step;
    if (input.min !== "" && next < Number(input.min)) next = Number(input.min);
    if (input.max !== "" && next > Number(input.max)) next = Number(input.max);
    var places = (String(step).split(".")[1] || "").length;
    input.value = next.toFixed(places);
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
  render(form);
})();
