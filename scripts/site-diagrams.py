#!/usr/bin/env python3
"""Draw the site's construction diagrams and place them in the pages.

Each drawing is a flat line drawing on a 560 x 440 canvas in the Plotter
grammar: one ink (`currentColor`), 1-unit strokes, opacity for hierarchy,
dash patterns for meaning, quiet labels in the page's own font.

    python3 scripts/site-diagrams.py

writes `site/diagrams/<name>.svg` and replaces the block between
`<!-- diagram:<name> -->` and `<!-- /diagram:<name> -->` in every page that
has one. Numbers drawn here must match `Benchmarks/README.md`.
"""

from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SITE = ROOT / "site"

# Opacity roles (Plotter drafting standard).
PRIMARY, SECONDARY, GUIDE = 1.0, 0.32, 0.14
NAME, VALUE = 0.72, 0.5
HIGHLIGHT = 0.055
# Dash roles.
ABSENT, MOVE = "3 5", "4 6"


def fmt(n):
    return f"{n:.2f}".rstrip("0").rstrip(".")


class Drawing:
    def __init__(self, name, title, desc, crop):
        self.name, self.title, self.desc = name, title, desc
        # (top, height) shown on the page: the full width, without the empty
        # bands above and below the drawing. The standalone file keeps the
        # whole 560 x 440 canvas.
        self.crop = crop
        self.layers = {}

    def _add(self, key, mark):
        self.layers.setdefault(key, []).append(mark)

    # Strokes, grouped by role so opacity is applied once per group.
    def path(self, d, opacity=PRIMARY, dash=None):
        self._add(("p", opacity, dash), f'<path d="{d}"/>')

    def line(self, x1, y1, x2, y2, opacity=PRIMARY, dash=None):
        self.path(f"M{fmt(x1)} {fmt(y1)}L{fmt(x2)} {fmt(y2)}", opacity, dash)

    def rect(self, x, y, w, h, opacity=PRIMARY, dash=None, r=0):
        if r:
            self._add(
                ("p", opacity, dash),
                f'<rect x="{fmt(x)}" y="{fmt(y)}" width="{fmt(w)}" height="{fmt(h)}" rx="{fmt(r)}"/>',
            )
        else:
            self.path(f"M{fmt(x)} {fmt(y)}h{fmt(w)}v{fmt(h)}h{fmt(-w)}Z", opacity, dash)

    def highlight(self, d, cls=None, standalone=True):
        attr = f' class="{cls}"' if cls else ""
        self._add(("h",), (f'<path{attr} d="{d}"/>', standalone))

    def text(self, x, y, s, role=NAME, anchor="start", size=None):
        size = size or (11 if role == NAME else 10)
        self._add(
            ("t", role, anchor, size),
            f'<text x="{fmt(x)}" y="{fmt(y)}">{escape(s)}</text>',
        )

    def arrow(self, x1, y1, x2, y2, opacity=SECONDARY, dash=MOVE):
        """A straight arrow with a solid 6-unit, 45-degree head at (x2, y2)."""
        import math

        self.line(x1, y1, x2, y2, opacity, dash)
        a = math.atan2(y2 - y1, x2 - x1)
        legs = []
        for turn in (math.pi * 0.75, -math.pi * 0.75):
            legs.append(
                f"M{fmt(x2)} {fmt(y2)}l{fmt(6 * math.cos(a + turn))} {fmt(6 * math.sin(a + turn))}"
            )
        self.path("".join(legs), opacity)

    def hdim(self, x1, x2, y, label, ext_from=None, gap=None):
        """A horizontal dimension: witnesses from `ext_from` (y of the form) to
        just past `y`, 8-unit slashes at each end, the value centered with
        the line split around it."""
        if ext_from is not None:
            top, bottom = sorted((ext_from, y))
            reach = 4 if ext_from > y else -4
            s = y - reach
            self.line(x1, ext_from - (4 if ext_from > y else -4), x1, s, SECONDARY)
            self.line(x2, ext_from - (4 if ext_from > y else -4), x2, s, SECONDARY)
        mid = (x1 + x2) / 2
        half = gap if gap is not None else len(label) * 2.7 + 5
        self.line(x1, y, mid - half, y, SECONDARY)
        self.line(mid + half, y, x2, y, SECONDARY)
        self.path(f"M{fmt(x1 - 4)} {fmt(y + 4)}l8-8M{fmt(x2 - 4)} {fmt(y + 4)}l8-8", SECONDARY)
        self.text(mid, y + 3.5, label, VALUE, "middle")

    def svg(self, standalone):
        ident = f"dg-{self.name}"
        out = []
        style = (
            "overflow:visible;font-family:inherit;font-optical-sizing:auto;"
            "font-synthesis:none;font-weight:400"
        )
        if standalone:
            style = style.replace("font-family:inherit", "font-family:Pollen, sans-serif")
            style += ";color:#171717"
        size = ' width="560" height="440"' if standalone else ""
        ns = ' xmlns="http://www.w3.org/2000/svg"' if standalone else ""
        view = "0 0 560 440" if standalone else f"0 {self.crop[0]} 560 {self.crop[1]}"
        out.append(
            f'<svg{ns} class="dg dg-{self.name}" viewBox="{view}"{size} fill="none"'
            f' stroke="currentColor" stroke-width="1" focusable="false" style="{style}"'
            f' role="img" aria-labelledby="{ident}-title" aria-describedby="{ident}-desc">'
        )
        out.append(f'<title id="{ident}-title">{escape(self.title)}</title>')
        out.append(f'<desc id="{ident}-desc">{escape(self.desc)}</desc>')
        order = sorted(
            self.layers.items(),
            key=lambda item: {"h": 0, "p": 1, "t": 2}[item[0][0]] * 10
            - (item[0][1] if item[0][0] == "p" else 0),
        )
        for key, marks in order:
            kind = key[0]
            if kind == "h":
                attrs = f'fill="currentColor" fill-opacity="{HIGHLIGHT}" stroke="none"'
            elif kind == "p":
                _, opacity, dash = key
                attrs = f'opacity="{fmt(opacity)}"'
                if dash:
                    attrs += f' stroke-dasharray="{dash}"'
            else:
                _, role, anchor, size = key
                attrs = (
                    f'fill="currentColor" stroke="none" font-size="{size}"'
                    f' text-anchor="{anchor}" opacity="{fmt(role)}"'
                )
            if kind == "h":
                marks = [m for m, shown in marks if shown or not standalone]
            out.append(f"<g {attrs}>" + "".join(marks) + "</g>")
        out.append("</svg>")
        return "\n".join(out)


# --- A. One log, a short tail on disk ------------------------------------
def tail():
    d = Drawing(
        "tail",
        "One log, a short tail on disk",
        "The log's sealed body lives as objects in the bucket. Each of three "
        "brokers holds only the same newest stretch, the unsealed tail, until "
        "the uploader moves it into the bucket.",
        (72, 304),
    )
    cell, h = 20, 24
    start, sealed, end = 60, 380, 480
    log_y = 296
    # The bucket and the sealed log inside it.
    d.rect(44, 280, 456, 56, SECONDARY)
    d.rect(start, log_y, sealed - start, h)
    d.path("".join(f"M{x} {log_y}v{h}" for x in range(start + cell, sealed, cell)))
    # The tail's place in the log, not yet uploaded.
    d.path(
        f"M{sealed} {log_y}h{end - sealed}v{h}h{sealed - end}"
        + "".join(f"M{x} {log_y}v{h}" for x in range(sealed + cell, end, cell)),
        SECONDARY,
        ABSENT,
    )
    d.text(52, 356, "R2 or any S3 bucket")
    d.hdim(start, sealed, 262, "sealed log, as objects", ext_from=log_y)
    # Three brokers, each with the same tail.
    rows = [124, 164, 204]
    for i, y in enumerate(rows):
        d.rect(sealed, y, end - sealed, h)
        d.path("".join(f"M{x} {y}v{h}" for x in range(sealed + cell, end, cell)))
        d.text(sealed - 12, y + 16, f"broker {i + 1}", NAME, "end")
    d.highlight(f"M{sealed} {rows[0]}h{end - sealed}v{rows[-1] + h - rows[0]}h{sealed - end}Z")
    d.hdim(sealed, end, 96, "unsealed tail", ext_from=rows[0])
    # The sealed end, carried down through the bucket.
    d.line(sealed, rows[-1] + h + 8, sealed, log_y - 8, GUIDE)
    d.line(end, rows[-1] + h + 8, end, log_y - 8, GUIDE)
    # The uploader moves the tail down.
    d.arrow(508, rows[1] + h / 2, 508, log_y + h / 2, SECONDARY)
    d.text(516, 232, "upload", VALUE)
    return d


# --- B. Two places to acknowledge ----------------------------------------
def acks():
    d = Drawing(
        "acks",
        "Two places to acknowledge",
        "One batch on a time line. In wal mode the producer is acknowledged "
        "once a majority has fsynced the batch; in r2 mode only after the "
        "object is stored and its extent committed. Intervals are not to scale.",
        (100, 280),
    )
    t0, wal, put, r2 = 80, 168, 372, 472
    y, h = 196, 64
    d.rect(t0, y, r2 - t0, h)
    d.path(f"M{wal} {y}v{h}M{put} {y}v{h}")
    d.text((t0 + wal) / 2, y + 36, "replicate")
    d.text((wal + put) / 2, y + 36, "waits in the WAL")
    d.text((put + r2) / 2, y + 36, "put, commit")
    for node in d.layers.get(("t", NAME, "start", 11), [])[-3:]:
        pass
    # Centre the three interval names.
    names = d.layers.pop(("t", NAME, "start", 11))
    d.layers[("t", NAME, "middle", 11)] = names
    # The two acknowledgements, projected up from the band.
    d.line(wal, y - 8, wal, 136, SECONDARY)
    d.line(r2, y - 8, r2, 136, SECONDARY)
    d.text(wal, 124, "wal acknowledges", NAME, "middle")
    d.text(r2, 124, "r2 acknowledges", NAME, "middle")
    # What each mode waits for, highlighted by the page's toggle.
    d.highlight(f"M{t0} {y}h{wal - t0}v{h}h{t0 - wal}Z", "dg-wait-wal")
    d.highlight(f"M{t0} {y}h{r2 - t0}v{h}h{t0 - r2}Z", "dg-wait-r2", standalone=False)
    # Events under the band.
    for x, label in ((t0, "produce"), (wal, "majority fsync"), (put, "upload"), (r2, "metadata commit")):
        d.line(x, y + h + 4, x, y + h + 14, SECONDARY)
        d.text(x, y + h + 27, label, VALUE, "middle")
    d.arrow(t0, 340, r2, 340, SECONDARY, None)
    d.text(t0, 360, "time, not to scale", VALUE)
    return d


# --- C. Committed where two of three agree -------------------------------
def quorum():
    d = Drawing(
        "quorum",
        "Committed where two of three agree",
        "Three copies of the write-ahead log, aligned by entry. An entry is "
        "committed once two of the three voters have it on disk; the third "
        "catches up behind.",
        (72, 296),
    )
    x0, cell, h = 160, 28, 24
    rows = [(140, 9, "broker 1, leader"), (192, 8, "broker 2"), (244, 5, "broker 3")]
    commit = x0 + 8 * cell
    for y, n, label in rows:
        end = x0 + n * cell
        d.rect(x0, y, end - x0, h)
        d.path("".join(f"M{x} {y}v{h}" for x in range(x0 + cell, end, cell)))
        d.text(x0 - 12, y + 16, label, NAME, "end")
        if end < commit:
            d.path(
                f"M{end} {y}h{commit - end}v{h}h{end - commit}"
                + "".join(f"M{x} {y}v{h}" for x in range(end + cell, commit, cell)),
                SECONDARY,
                ABSENT,
            )
    d.text(x0 + 5 * cell + 3 * cell / 2, 244 + h + 18, "catching up", VALUE, "middle")
    d.text(x0 + 9 * cell + 10, 140 + 16, "leader only", VALUE)
    # Committed: the entries a majority holds.
    d.highlight(f"M{x0} 140h{commit - x0}v{h}h{x0 - commit}ZM{x0} 192h{commit - x0}v{h}h{x0 - commit}Z")
    d.line(commit, 104, commit, 132, SECONDARY)
    d.line(commit, 300, commit, 324, SECONDARY)
    d.text(commit, 94, "commit index", NAME, "middle")
    d.hdim(x0, commit, 344, "committed on two of three", ext_from=284)
    return d


# --- D. Four parts, one process -------------------------------------------
def process():
    d = Drawing(
        "process",
        "Four parts, one process",
        "The Kafka listener, metadata Raft group, write-ahead log and "
        "uploader fit one process. A deployment adds no JVM, ZooKeeper, "
        "KRaft or separate database.",
        (64, 324),
    )
    parts = ["Kafka listener", "Metadata Raft", "Write-ahead log", "Uploader"]
    h, gap = 36, 14
    lx, lw = 56, 150
    px, pw, py = 316, 188, 84
    # Exploded parts.
    for i, name in enumerate(parts):
        y = py + 22 + i * (h + gap)
        d.rect(lx, y, lw, h, SECONDARY)
        d.text(lx + 12, y + 22, name, VALUE)
    # The process they fit: one outline, the same four rows.
    rows_top = py + 22
    total = 4 * h + 3 * gap
    d.rect(px, py, pw, total + 44)
    d.text(px + 12, py + 15, "camus-broker", NAME)
    for i, name in enumerate(parts):
        y = rows_top + i * (h + gap)
        d.rect(px + 12, y, pw - 24, h)
        d.text(px + 24, y + 22, name, NAME)
        # Each part projects onto its place.
        d.arrow(lx + lw + 8, y + h / 2, px + 4, y + h / 2, SECONDARY)
    # What a deployment does not add.
    absent = [("JVM", 56), ("ZooKeeper", 138), ("KRaft", 240), ("separate database", 322)]
    by = 344
    widths = {"JVM": 66, "ZooKeeper": 86, "KRaft": 66, "separate database": 130}
    for name, x in absent:
        d.rect(x, by, widths[name], 28, SECONDARY, ABSENT)
        d.text(x + widths[name] / 2, by + 18, name, VALUE, "middle")
    d.text(56, by - 10, "not added", VALUE)
    names = d.layers.pop(("t", VALUE, "middle", 10))
    d.layers[("t", VALUE, "middle", 10)] = names
    return d


# --- E. One address changes -----------------------------------------------
def address():
    d = Drawing(
        "address",
        "One address changes",
        "A Kafka client keeps its code and its configuration. Camus presents "
        "the same Kafka 4.4.0 wire protocol as the broker it replaces, so only "
        "the bootstrap address moves.",
        (48, 344),
    )
    tabs = [0, 1, 2]
    depth, size, pitch = 12, 12, 24

    def socket_edge(x, y, h):
        # Left edge with three sockets, top to bottom.
        top = y + (h - (2 * pitch + size)) / 2
        s = f"M{fmt(x)} {fmt(y)}"
        for i in tabs:
            t = top + i * pitch
            s += f"V{fmt(t)}h{depth}v{size}h{-depth}"
        return s + f"V{fmt(y + h)}"

    def broker(x, y, w, h, opacity, dash):
        d.path(socket_edge(x, y, h) + f"H{fmt(x + w)}V{fmt(y)}Z", opacity, dash)

    bx, bw, bh = 340, 160, 100
    kafka_y, camus_y = 70, 230
    broker(bx, kafka_y, bw, bh, SECONDARY, ABSENT)
    d.text(bx + 30, kafka_y + 56, "Apache Kafka 4.4.0", VALUE)
    broker(bx, camus_y, bw, bh, PRIMARY, None)
    d.text(bx + 30, camus_y + 56, "Camus", NAME)
    # The client, with tabs that fit either broker's sockets.
    cx, cw = 60, 160
    top = camus_y + (bh - (2 * pitch + size)) / 2
    s = f"M{cx} {camus_y}H{cx + cw}"
    for i in tabs:
        t = top + i * pitch
        s += f"V{fmt(t)}h{depth}v{size}h{-depth}"
    s += f"V{camus_y + bh}H{cx}Z"
    d.path(s)
    d.text(cx + 14, camus_y + 46, "your client", NAME)
    d.text(cx + 14, camus_y + 62, "unchanged", VALUE)
    # The sockets line up: the same protocol on both brokers.
    d.line(bx, kafka_y + bh + 8, bx, camus_y - 8, GUIDE)
    d.line(bx + depth, kafka_y + bh + 8, bx + depth, camus_y - 8, GUIDE)
    d.text(bx + depth + 8, 200, "same wire protocol", VALUE)
    # The address: where it pointed, and where it points now.
    d.line(cx + cw / 2, camus_y - 8, cx + cw / 2, kafka_y + bh / 2, SECONDARY, ABSENT)
    d.line(cx + cw / 2, kafka_y + bh / 2, bx - 8, kafka_y + bh / 2, SECONDARY, ABSENT)
    # Each tab meets its socket: the client fits either broker unchanged.
    for i in tabs:
        t = top + i * pitch + size / 2
        d.line(cx + cw + depth + 8, t, bx - 8, t, GUIDE)
    d.text(cx, 372, "bootstrap.servers moves to Camus; nothing else does", VALUE)
    return d


# --- F. Inside one ack -------------------------------------------------------
def ack():
    d = Drawing(
        "ack",
        "Inside one ack",
        "Means over 100,000 wal acks on three i4i.2xlarge brokers: a 0.93 ms "
        "round trip, of which the leader's 0.17 ms write and fsync runs inside "
        "the 0.48 ms append round trip to the followers.",
        (76, 352),
    )
    x0, scale = 80, 400 / 0.93  # units per millisecond
    h = 28

    def bar(start_ms, ms, y, label, value, opacity=PRIMARY):
        x = x0 + start_ms * scale
        w = ms * scale
        d.rect(x, y, w, h, opacity)
        d.text(x + 10, y + 18, label, NAME)
        if value:
            d.text(x + w - 10, y + 18, value, VALUE, "end")
        return x, x + w

    # Starts aligned for comparison: the network share of the round trip is
    # split evenly around the handler.
    net = (0.93 - 0.79) / 2
    a0, a1 = bar(0, 0.93, 120, "client round trip", None)
    bar(net, 0.79, 168, "produce handler", "0.79 ms")
    bar(net, 0.75, 216, "Raft commit and apply", "0.75 ms")
    r0, r1 = bar(net, 0.48, 264, "append to followers", None)
    f0, f1 = bar(net, 0.17, 312, "fsync", None)
    d.highlight(f"M{fmt(f0)} 264h{fmt(f1 - f0)}v{h}h{fmt(f0 - f1)}Z")
    d.line(f1, 300, f1, 308, GUIDE)
    d.hdim(a0, a1, 96, "0.93 ms", ext_from=120)
    d.hdim(r0, r1, 368 - 4, "0.48 ms", ext_from=292 + 44)
    d.text(f1 + 10, 330, "0.17 ms, inside the append", VALUE)
    d.text(x0, 410, "means of 100,000 acks, 3 × i4i.2xlarge; bars start together", VALUE)
    return d


DRAWINGS = [tail, acks, quorum, process, address, ack]


def main():
    out = SITE / "diagrams"
    out.mkdir(exist_ok=True)
    drawings = [make() for make in DRAWINGS]
    for drawing in drawings:
        (out / f"{drawing.name}.svg").write_text(drawing.svg(standalone=True) + "\n")
    for page in SITE.glob("*.html"):
        text = page.read_text()
        changed = text
        for drawing in drawings:
            start = f"<!-- diagram:{drawing.name} -->"
            end = f"<!-- /diagram:{drawing.name} -->"
            if start not in changed:
                continue
            head, rest = changed.split(start, 1)
            _, tail_text = rest.split(end, 1)
            changed = f"{head}{start}\n{drawing.svg(standalone=False)}\n{end}{tail_text}"
        if changed != text:
            page.write_text(changed)
            print(f"updated {page.relative_to(ROOT)}")
    print(f"wrote {len(drawings)} drawings to {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
