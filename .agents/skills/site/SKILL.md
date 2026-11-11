---
name: site
description: Edit the Camus website in site/ (static HTML, CSS, JS) and deploy it to Cloudflare with wrangler.
---

# Site

- Plain static files with no build step. Every page uses `styles.css`
  (tokens on `:root`, dark mode via `prefers-color-scheme`) and `site.js`
  (nav folding, motion that respects reduced-motion).
- A new page needs the shared header and footer copied from a sibling page,
  `aria-current="page"` in the nav, and an entry in `sitemap.xml`.
- Use the existing CSS variables (`--gray-*`, `--step-*`). Don't add new
  colors or type sizes.
- Numbers on the site must match `Benchmarks/README.md` and
  `docs/compatibility.md`.
- Preview: `npx wrangler dev`, or open the HTML file directly. Deploy:
  `npx wrangler deploy` (serves `./site`, see `wrangler.toml`). Ask before
  deploying.
- Design skills, if your harness has them: frontend-design,
  better-typography, better-accessibility, emil-ui-review.
