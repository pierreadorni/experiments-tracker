# Design study 01

A separate, interactive frontend mock for visual iteration. It uses synthetic fixture content, makes no backend API requests, and does not dispatch experiments or contact Codex. Local edits reset on reload. The deployed client still enters through `web/index.html`; this mock is not included in its production bundle.

From the repository root, with Node 24 on PATH:

```sh
npm run dev --prefix web -- --port 5174
```

Open `http://127.0.0.1:5174/mock/`. The existing app stays at `/`.

The first study focuses on experiment 006's report: one reading surface, warm neutral colours, restrained olive accents, simple rows, optional desktop discussion, and a phone discussion sheet. The existing five detail tabs remain. Try progress disclosures, notes, report version selection, saved ideas, experiment filtering/search, keyboard navigation, and the local brief form. Other experiment reports link to the sample; file names, operational status, model selection, replies, and timestamps are illustrative. Downloads explicitly identify themselves as design fixtures and do not claim verification.

Source: `web/src/mock/main.tsx` and `mock.css`, with custom dropdowns in `Select.tsx` and progress examples in `ProgressStudy.tsx`. The two chart assets are generated from declared illustrative values with `render_figure.py`; the mobile chart uses its own dimensions so axis labels stay readable. DM Sans uses Google Fonts with a system-font fallback. The mock creates no service worker or persistent browser data.

Iteration 02 replaces focus outlines with gentle surface/shadow cues and browser dropdowns with matching keyboard-accessible menus. Open the running width experiment from Recent, or use **Progress → Preview** to inspect complete, training, preparing and queued states. Training has an animated spinner and a measured 244/400-epoch bar with an explicitly illustrative throughput-based ETA. Agent work remains indeterminate; queued jobs have no invented completion estimate. Future stages are muted, and reduced-motion preferences suppress animations. Dropdowns support arrow keys, Home/End, Enter, Escape and Tab; Escape closes a dropdown before its containing sheet.

Iteration 03 uses research and operational language throughout. Experiment titles describe methods and tasks; report text states measurements, methodology, interpretation and limitations. Remove redundant headings and introductory copy rather than replacing them with slogans. Use direct page labels and execution status; reserve explanatory text for information the researcher needs to act or interpret a result. The report opens with its result summary, Notes and Protocol omit repeated headings, and Progress shows completion counts and current execution state.

Iteration 04 removes repeated fine print and secondary captions rather than packing information into small type. Retained interface text is at least 14px, report paragraphs are 16px, and charts use larger axis labels. Stage descriptions appear inside disclosures; ETA methodology, freshness and resources are available from an information control. File icons replace type captions, agent model details live in Protocol, and one readable synthetic-preview indicator replaces per-view reminders. Scientific limitations and error-bar definitions remain visible. At widths up to 900px, Protocol and Evidence are grouped under **More** so primary tabs retain readable labels. Use icons for familiar concepts and progressive disclosure for optional detail; do not replace essential or ambiguous information with unexplained symbols.

Validation: Node 24 type checking, a separate Vite mock bundle, and the required `make check` suite. Approved Chromium checks covered 320, 360, 390, 768, 1024 and 1440px layouts, the main interactive flows, keyboard tabs, and zero backend API requests. This is frontend mock acceptance, not physical-phone sign-in, scientific validation, or Slurm acceptance.
