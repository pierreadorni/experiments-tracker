import { useEffect, useRef, useState } from 'react';
import type { Theme } from './theme';

// Presentation only: original artifact bytes, hashes and exported reports stay intact.
const surface = `
@font-face { font-family: 'DM Sans'; font-style: normal; font-weight: 400 700; font-display: swap; src: url('/fonts/dm-sans-latin.woff2') format('woff2'); }

html { overflow-x: hidden; }
body { margin: 0; padding: 0; background: #faf9f6; color: #292b26;
  font: 16px/1.75 'DM Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
article { max-width: 780px; margin: 0 auto; padding: 0 0 32px; }
h1, h2, h3, .eyebrow { font-family: inherit; }
p, li { overflow-wrap: anywhere; }
h1 { font-size: 28px; font-weight: 550; letter-spacing: -.7px; }
h2 { font-size: 22px; font-weight: 550; letter-spacing: -.5px; }
.standfirst { font-size: 18px; color: #5c6154; }
.eyebrow, .evidence, .report-footer { font-size: 14px; }
img, svg { max-width: 100%; height: auto; }
figure { margin: 28px 0; }
figcaption, caption { font: 14px/1.65 -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
table { font-size: 15px; }
pre { white-space: pre-wrap; overflow-wrap: anywhere; }
a { color: #596c44; overflow-wrap: anywhere; }
small { font-size: 14px; }
@media (max-width: 480px) { article { padding: 0 0 24px; } h1 { font-size: 25px; } }
`;

const darkSurface = `
:root { color-scheme: dark; }
body { background: #1c1f1a; color: #e8e9e2; }
.standfirst { color: #bdc3b4; }
.eyebrow, .evidence, .report-footer, small, figcaption, caption { color: #acb2a2; }
a { color: #b4cc91; }
table, thead, th, td, .report-footer { border-color: #464e3e; }
pre { background: #252922; }
/* Scientific figures retain their original colors and a readable light canvas. */
img { background: #fff; border-radius: 8px; }
`;

function applySurface(doc: Document, theme: Theme) {
  let style = doc.getElementById('experimenttracker-reading-style');
  if (!style) {
    style = doc.createElement('style');
    style.id = 'experimenttracker-reading-style';
    doc.head.append(style);
  }
  style.textContent = surface + (theme === 'dark' ? darkSurface : '');
}

export function Report({ id, theme }: { id: string; theme: Theme }) {
  const frame = useRef<HTMLIFrameElement>(null);
  const cleanup = useRef<() => void>(() => {});
  const [height, setHeight] = useState(800);
  useEffect(() => () => cleanup.current(), []);
  useEffect(() => {
    const doc = frame.current?.contentDocument;
    if (doc?.body) applySurface(doc, theme);
  }, [theme]);
  return (
    <iframe
      key={id}
      ref={frame}
      title="Experiment report"
      sandbox="allow-same-origin"
      src={`/api/artifacts/${id}`}
      style={{ height }}
      onLoad={() => {
        cleanup.current();
        const doc = frame.current?.contentDocument;
        if (!doc?.body) return;
        applySurface(doc, theme);
        // Observe the content rather than the viewport to allow shrinking after resize.
        const resize = () => setHeight(Math.ceil(doc.body.getBoundingClientRect().height) + 4);
        const observer = new ResizeObserver(resize);
        observer.observe(doc.body);
        resize();
        cleanup.current = () => observer.disconnect();
      }}
    />
  );
}
