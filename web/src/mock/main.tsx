import { useEffect, useRef, useState, type ReactNode } from 'react';
import { createRoot } from 'react-dom/client';
import {
  ArrowDownToLine,
  ArrowLeft,
  ArrowRight,
  ArrowUp,
  Beaker,
  Check,
  ChevronRight,
  FileText,
  FileJson,
  FileCode2,
  Clock3,
  Inbox,
  Lightbulb,
  Menu,
  MessageCircle,
  MoreHorizontal,
  Plus,
  Search,
  Settings2,
  X,
} from 'lucide-react';
import './mock.css';
import { Select } from './Select';
import { ProgressStudy, type ProgressMode } from './ProgressStudy';

type Tab = 'Progress' | 'Report' | 'Notes' | 'Protocol' | 'Evidence';
type Page = 'experiments' | 'ideas' | 'inbox' | 'settings';
type Experiment = {
  id: number;
  title: string;
  brief: string;
  state: 'complete' | 'running' | 'queued';
  date: string;
};
const initial: Experiment[] = [
  {
    id: 7,
    title: 'MLP width comparison at fixed depth',
    brief:
      'Compare 8, 16 and 32 units per hidden layer at depth 2 using a fixed data split and training budget.',
    state: 'running',
    date: 'Today, 15:02',
  },
  {
    id: 6,
    title: 'MLP depth comparison on synthetic regression',
    brief: 'Compare one, two and three hidden layers on a synthetic regression task.',
    state: 'complete',
    date: 'Today, 14:32',
  },
  {
    id: 5,
    title: 'Decision-tree regression against the reference baseline',
    brief: 'Compare a decision tree with the reference baseline.',
    state: 'complete',
    date: 'Today, 11:08',
  },
  {
    id: 4,
    title: 'Linear and quadratic regression baselines',
    brief: 'Compare linear and quadratic fits on a fixed synthetic dataset.',
    state: 'complete',
    date: 'Yesterday',
  },
];
const files = [
  { name: 'results.json', type: 'Measurements', size: '2.4 KB' },
  { name: 'predictions.json', type: 'Held-out predictions', size: '38 KB' },
  { name: 'recipe.py', type: 'Frozen experiment source', size: '6.1 KB' },
  { name: 'parameters.json', type: 'Protocol parameters', size: '1.2 KB' },
  { name: 'report-v2.json', type: 'Presentation provenance', size: '4.8 KB' },
];
function download(name: string, content: string, mime = 'text/plain') {
  const url = URL.createObjectURL(new Blob([content], { type: mime }));
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = name;
  anchor.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function Overlay({
  title,
  close,
  children,
  className = '',
}: {
  title: string;
  close: () => void;
  children: ReactNode;
  className?: string;
}) {
  const ref = useRef<HTMLElement>(null);
  const closeRef = useRef(close);
  closeRef.current = close;
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    ref.current?.querySelector<HTMLElement>('button, textarea, input')?.focus();
    const keydown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') closeRef.current();
      if (event.key !== 'Tab') return;
      const elements = [
        ...(ref.current?.querySelectorAll<HTMLElement>(
          'button:not(:disabled), input, textarea, select, a[href]',
        ) || []),
      ];
      const first = elements[0],
        last = elements.at(-1);
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last?.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first?.focus();
      }
    };
    const overflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    document.addEventListener('keydown', keydown);
    return () => {
      document.body.style.overflow = overflow;
      document.removeEventListener('keydown', keydown);
      previous?.focus();
    };
  }, []);
  return (
    <div className={`overlay ${className}`} onClick={close}>
      <section
        ref={ref}
        className="dialog"
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onClick={(e) => e.stopPropagation()}
      >
        {children}
      </section>
    </div>
  );
}

function App() {
  const [experiments, setExperiments] = useState(initial);
  const [selected, setSelected] = useState<number | null>(6);
  const [page, setPage] = useState<Page>('experiments');
  const [tab, setTab] = useState<Tab>('Report');
  const [navOpen, setNavOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const [discussion, setDiscussion] = useState(false);
  const [version, setVersion] = useState('2');
  const [progressMode, setProgressMode] = useState<ProgressMode>('complete');
  const [compactViews, setCompactViews] = useState(
    () => window.matchMedia('(max-width: 900px)').matches,
  );
  const [discussionModel, setDiscussionModel] = useState('medium');
  const [creationModel, setCreationModel] = useState('sol');
  const [creationEffort, setCreationEffort] = useState('medium');
  const [query, setQuery] = useState('');
  const [filter, setFilter] = useState('All');
  const [brief, setBrief] = useState('');
  const [draft, setDraft] = useState('');
  const [messages, setMessages] = useState<{ text: string; reply: string }[]>([]);
  const [notesById, setNotesById] = useState<Record<number, string[]>>({
    6: [
      'Keep the split fixed when testing a wider network. That will make the next comparison easier to interpret.',
    ],
  });
  const [note, setNote] = useState('');
  const [saved, setSaved] = useState(false);
  const [read, setRead] = useState(false);
  const [paused, setPaused] = useState(false);
  const [toast, setToast] = useState('');
  const exp = experiments.find((e) => e.id === selected);
  const notes = exp ? notesById[exp.id] || [] : [];
  useEffect(() => {
    const query = window.matchMedia('(max-width: 900px)');
    const update = (event: MediaQueryListEvent) => setCompactViews(event.matches);
    query.addEventListener('change', update);
    return () => query.removeEventListener('change', update);
  }, []);
  useEffect(() => {
    if (!toast) return;
    const timer = setTimeout(() => setToast(''), 3600);
    return () => clearTimeout(timer);
  }, [toast]);
  const navigate = (next: Page) => {
    setPage(next);
    setSelected(null);
    setNavOpen(false);
    setDiscussion(false);
  };
  const open = (id: number) => {
    setSelected(id);
    setPage('experiments');
    const target = experiments.find((e) => e.id === id);
    setTab(target?.state === 'complete' ? 'Report' : 'Progress');
    setProgressMode(
      target?.state === 'complete'
        ? 'complete'
        : target?.state === 'running'
          ? 'training'
          : 'queued',
    );
    setNavOpen(false);
    setDiscussion(false);
    setMessages([]);
  };
  function send(text: string) {
    if (!text.trim()) return;
    const reply = /next|follow|wider/i.test(text)
      ? 'A useful next comparison would hold depth at two layers and vary width. Keep the same split and training budget, then record parameter count alongside error. A second dataset would help test whether the pattern transfers.'
      : 'In this illustrative comparison, two hidden layers have the lowest mean held-out error. The five initializations describe sensitivity to initialization on one split; they do not establish statistical superiority or performance on new datasets.';
    setMessages((old) => [...old, { text: text.trim(), reply }]);
    setDraft('');
  }
  const sidebar = (
    <>
      <div className="brand">
        <Beaker size={21} strokeWidth={1.6} />
        <span>Experiments</span>
        <button
          className="icon-button close-nav"
          aria-label="Close navigation"
          onClick={() => setNavOpen(false)}
        >
          <X size={18} />
        </button>
      </div>
      <button
        className="new-button"
        onClick={() => {
          setNavOpen(false);
          setCreating(true);
        }}
      >
        <Plus size={17} />
        New experiment
      </button>
      <nav aria-label="Workspace">
        <button
          className={page === 'experiments' ? 'nav-item active' : 'nav-item'}
          onClick={() => navigate('experiments')}
        >
          <Beaker size={17} />
          Experiments
        </button>
        <button
          className={page === 'inbox' ? 'nav-item active' : 'nav-item'}
          onClick={() => navigate('inbox')}
        >
          <Inbox size={17} />
          Inbox{!read && <span className="unread-dot" />}
        </button>
        <button
          className={page === 'ideas' ? 'nav-item active' : 'nav-item'}
          onClick={() => navigate('ideas')}
        >
          <Lightbulb size={17} />
          Results & ideas
        </button>
      </nav>
      <div className="sidebar-label">Recent</div>
      <div className="recent-list">
        {experiments.slice(0, 4).map((e) => (
          <button
            key={e.id}
            className={e.id === selected ? 'recent selected' : 'recent'}
            title={e.title}
            onClick={() => open(e.id)}
          >
            <span className={`status-dot ${e.state}`} />
            <span>{e.title}</span>
          </button>
        ))}
      </div>
      <div className="sidebar-bottom">
        <button
          className="executor-link"
          aria-label="Executor settings: acacia, online"
          onClick={() => navigate('settings')}
        >
          <span className="status-dot complete" />
          <span>acacia</span>
          <Settings2 size={16} />
        </button>
      </div>
    </>
  );
  const conversation = (
    <>
      <div className="discussion-heading">
        <h2>
          <MessageCircle size={17} />
          Discuss
        </h2>
        <button
          className="icon-button"
          aria-label="Close discussion"
          onClick={() => setDiscussion(false)}
        >
          <X size={18} />
        </button>
      </div>
      <div className="discussion-messages" aria-live="polite">
        {messages.length === 0 ? (
          <div className="discussion-empty">
            <p>Discuss the results, inspect evidence, or request a report revision.</p>
            <button onClick={() => send('What does this result establish?')}>
              Assess conclusions and limitations
              <ArrowRight size={14} />
            </button>
            <button onClick={() => send('What should we try next?')}>
              Suggest follow-up experiments
              <ArrowRight size={14} />
            </button>
          </div>
        ) : (
          messages.map((m, i) => (
            <div className="exchange" key={i}>
              <p className="user-message">{m.text}</p>
              <div className="reply">
                <strong>
                  <Beaker size={14} />
                  Codex
                </strong>
                <p>{m.reply}</p>
                <button
                  className="text-button"
                  onClick={() => {
                    setTab('Evidence');
                    setDiscussion(false);
                  }}
                >
                  View supporting evidence
                  <ArrowRight size={13} />
                </button>
              </div>
            </div>
          ))
        )}
      </div>
      <form
        className="composer"
        onSubmit={(e) => {
          e.preventDefault();
          send(draft);
        }}
      >
        <textarea
          aria-label="Message Codex"
          placeholder="Ask about this experiment…"
          rows={2}
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
              e.preventDefault();
              send(draft);
            }
          }}
        />
        <div className="composer-bottom">
          <Select
            label="Discussion model"
            value={discussionModel}
            onChange={setDiscussionModel}
            placement="up"
            className="model-choice"
            options={[
              { value: 'medium', label: 'GPT-6.1 Sol · medium' },
              { value: 'high', label: 'GPT-6.1 Sol · high' },
            ]}
          />
          <button className="send-button" aria-label="Send message" disabled={!draft.trim()}>
            <ArrowUp size={18} />
          </button>
        </div>
      </form>
    </>
  );
  return (
    <div className={`app ${discussion ? 'with-discussion' : ''}`}>
      <aside className="sidebar">{sidebar}</aside>
      {navOpen && (
        <Overlay
          title="Workspace navigation"
          close={() => setNavOpen(false)}
          className="nav-overlay"
        >
          {sidebar}
        </Overlay>
      )}
      <div className="workspace">
        <header className="topbar">
          <div>
            <button
              className="icon-button mobile-menu"
              aria-label="Open navigation"
              onClick={() => setNavOpen(true)}
            >
              <Menu size={20} />
            </button>
            <span className="workspace-name">Personal workspace</span>
            <ChevronRight className="breadcrumb-divider" size={13} />
            <span className="breadcrumb-current">
              {exp
                ? `#${String(exp.id).padStart(3, '0')}`
                : page === 'ideas'
                  ? 'Results & ideas'
                  : page === 'settings'
                    ? 'Executor & quota'
                    : page === 'inbox'
                      ? 'Inbox'
                      : 'Experiments'}
            </span>
          </div>
          <span
            className="preview-badge"
            title="Synthetic frontend mock. No model turns or compute jobs are dispatched."
          >
            Synthetic preview
          </span>
        </header>
        {page === 'experiments' && exp ? (
          <>
            <div className="experiment-heading">
              <button className="back-link" onClick={() => setSelected(null)}>
                <ArrowLeft size={15} />
                All experiments
              </button>
              <div className="title-row">
                <h1>{exp.title}</h1>
                <button
                  className="icon-button"
                  aria-label="Experiment actions"
                  onClick={() =>
                    setToast('Preview experiment · source and results are illustrative.')
                  }
                >
                  <MoreHorizontal size={21} />
                </button>
              </div>
              <p className="experiment-brief">{exp.brief}</p>
              <div className="experiment-meta">
                <span className={`status ${exp.state}`}>
                  <span className={`status-dot ${exp.state}`} />
                  {exp.state === 'complete'
                    ? 'Complete'
                    : exp.state === 'running'
                      ? 'Running'
                      : 'Queued'}
                </span>
                <span>
                  <Clock3 size={14} />
                  {exp.date}
                </span>
              </div>
            </div>
            <div className="tabbar">
              <div className="tabs" role="tablist" aria-label="Experiment views">
                {(['Progress', 'Report', 'Notes', 'Protocol', 'Evidence'] as Tab[]).map((t) => (
                  <button
                    key={t}
                    id={`tab-${t}`}
                    aria-controls="view-panel"
                    role="tab"
                    aria-selected={tab === t}
                    tabIndex={
                      tab === t ||
                      (t === 'Progress' &&
                        (tab === 'Protocol' || tab === 'Evidence') &&
                        compactViews)
                        ? 0
                        : -1
                    }
                    className={`${tab === t ? 'selected' : ''} ${t === 'Protocol' || t === 'Evidence' ? 'secondary-tab' : ''}`}
                    onClick={() => setTab(t)}
                    onKeyDown={(e) => {
                      const keys = ['ArrowLeft', 'ArrowRight', 'Home', 'End'];
                      if (!keys.includes(e.key)) return;
                      e.preventDefault();
                      const buttons = [
                        ...(e.currentTarget.parentElement?.querySelectorAll<HTMLButtonElement>(
                          '[role="tab"]',
                        ) || []),
                      ].filter((button) => button.getClientRects().length > 0);
                      const index = buttons.indexOf(e.currentTarget);
                      const next =
                        e.key === 'Home'
                          ? 0
                          : e.key === 'End'
                            ? buttons.length - 1
                            : (index + (e.key === 'ArrowRight' ? 1 : -1) + buttons.length) %
                              buttons.length;
                      buttons[next]?.click();
                      buttons[next]?.focus();
                    }}
                  >
                    {t}
                  </button>
                ))}
              </div>
              <Select
                className="more-views"
                label="More experiment views"
                value={tab}
                displayValue={tab === 'Protocol' || tab === 'Evidence' ? tab : 'More'}
                onChange={(value) => setTab(value as Tab)}
                options={[
                  { value: 'Protocol', label: 'Protocol' },
                  { value: 'Evidence', label: 'Evidence' },
                ]}
              />
              <button
                className={`discuss-button ${discussion ? 'active' : ''}`}
                aria-label="Discuss"
                onClick={() => setDiscussion(!discussion)}
                aria-expanded={discussion}
              >
                <MessageCircle size={16} />
                <span>Discuss</span>
              </button>
            </div>
            <main className="detail-body">
              <div
                key={tab}
                className="reading-surface"
                id="view-panel"
                role="tabpanel"
                aria-labelledby={`tab-${tab}`}
              >
                {tab === 'Report' && exp.id !== 6 && (
                  <section className="plain-section">
                    <h2>
                      {exp.state === 'complete'
                        ? 'Report not included in this mock'
                        : 'Report not available'}
                    </h2>
                    <p className="section-intro">
                      {exp.state === 'complete'
                        ? 'The depth comparison is the report example for this first design study.'
                        : 'Your brief stays in this mock. No preparation, compute or analysis has been started.'}
                    </p>
                    <button className="text-button" onClick={() => open(6)}>
                      Open the sample report
                      <ArrowRight size={15} />
                    </button>
                  </section>
                )}
                {tab === 'Report' && exp.id === 6 && (
                  <>
                    <div className="report-tools">
                      <Select
                        label="Report version"
                        value={version}
                        onChange={setVersion}
                        className="version-choice"
                        options={[
                          { value: '2', label: 'Version 2 · current' },
                          { value: '1', label: 'Version 1 · original' },
                        ]}
                      />
                      <button
                        className="icon-button"
                        aria-label="Download mock report"
                        onClick={() =>
                          download(
                            'preview-report.txt',
                            `SYNTHETIC DESIGN PREVIEW — not a scientific result\n\n${exp.title}\n\nTwo hidden layers have the lowest illustrative mean error. Values are fixture data for layout review. No experiment was run.`,
                          )
                        }
                      >
                        <ArrowDownToLine size={17} />
                      </button>
                    </div>
                    <article className="report">
                      <p className="report-lead">
                        {version === '2'
                          ? 'The two-layer MLP had the lowest mean held-out MSE (0.0447 ± 0.0003 SD). The three-layer MLP had higher mean error (0.0497 ± 0.0039 SD) and greater variation across initializations.'
                          : 'Mean held-out MSE was 0.0579, 0.0447 and 0.0497 for one, two and three hidden layers, respectively. Each architecture was evaluated across five fixed initializations.'}
                      </p>
                      <p>
                        MLPs with one, two and three hidden layers were trained on 64 samples and
                        evaluated on 128 held-out samples. Each hidden layer had 16 units with tanh
                        activation. The data split and 400-epoch Adam training budget were fixed.
                      </p>
                      <figure className="result-figure">
                        <picture>
                          <source
                            media="(max-width: 700px)"
                            srcSet="/mock/depth-comparison-mobile.svg"
                          />
                          <img
                            src="/mock/depth-comparison.svg"
                            alt="Illustrative mean held-out error: one layer 0.0579, two layers 0.0447, three layers 0.0497. Error bars show standard deviation across five initializations."
                          />
                        </picture>
                        <figcaption>
                          <strong>Fig. 1</strong> Held-out MSE by network depth. Bars show ±1 SD
                          across five initializations on a single fixed split.
                        </figcaption>
                      </figure>
                      <table className="metrics-table">
                        <thead>
                          <tr>
                            <th>Layers</th>
                            <th>
                              Mean MSE <span>↓</span>
                            </th>
                            <th title="Standard deviation across five initializations">SD</th>
                          </tr>
                        </thead>
                        <tbody>
                          <tr>
                            <td>1</td>
                            <td>0.0579</td>
                            <td>0.0059</td>
                          </tr>
                          <tr className="best">
                            <td>2</td>
                            <td>0.0447</td>
                            <td>0.0003</td>
                          </tr>
                          <tr>
                            <td>3</td>
                            <td>0.0497</td>
                            <td>0.0039</td>
                          </tr>
                        </tbody>
                      </table>
                      <div className="source-line">
                        <button className="text-button" onClick={() => setTab('Evidence')}>
                          Inspect evidence
                          <ArrowRight size={12} />
                        </button>
                      </div>
                      <section className="interpretation">
                        <h3>Interpretation</h3>
                        <p>
                          Observed error decreased from one to two hidden layers and increased at
                          three. Parameter count increased with depth; this comparison does not
                          isolate model capacity from architecture.
                        </p>
                        <h3>Limitations</h3>
                        <p>
                          The comparison uses one synthetic dataset and one train/test split. The
                          standard deviations describe sensitivity to initialization; they are not
                          confidence intervals or estimates of variation across datasets.
                        </p>
                      </section>
                      <section className="next-step">
                        <div>
                          <h3>MLP width comparison at fixed depth</h3>
                          <p>
                            Compare 8, 16 and 32 units per hidden layer at depth 2. Keep the data
                            split and training budget fixed and record parameter count.
                          </p>
                        </div>
                        <button
                          className="icon-button"
                          aria-label={saved ? 'Unsave next idea' : 'Save next idea'}
                          onClick={() => {
                            setSaved(!saved);
                            setToast(
                              saved ? 'Idea removed from preview' : 'Idea saved in Results & ideas',
                            );
                          }}
                        >
                          {saved ? <Check size={20} /> : <Plus size={20} />}
                        </button>
                      </section>
                    </article>
                  </>
                )}
                {tab === 'Progress' && (
                  <ProgressStudy mode={progressMode} onChange={setProgressMode} />
                )}
                {tab === 'Notes' && (
                  <section className="plain-section">
                    {notes.map((n, i) => (
                      <div className="note" key={i}>
                        <div>
                          <span className="avatar">A</span>
                          <strong>You</strong>
                          <time>Today</time>
                        </div>
                        <p>{n}</p>
                      </div>
                    ))}
                    <form
                      className="note-form"
                      onSubmit={(e) => {
                        e.preventDefault();
                        if (note.trim()) {
                          setNotesById({ ...notesById, [exp.id]: [...notes, note.trim()] });
                          setNote('');
                        }
                      }}
                    >
                      <textarea
                        aria-label="Add a note"
                        placeholder="Add a research note…"
                        value={note}
                        onChange={(e) => setNote(e.target.value)}
                        rows={3}
                      />
                      <button className="primary" disabled={!note.trim()}>
                        Add note
                        <ArrowUp size={15} />
                      </button>
                    </form>
                  </section>
                )}
                {tab === 'Protocol' && (
                  <section className="plain-section">
                    <dl className="protocol-list">
                      {[
                        ['Task', 'Synthetic quadratic regression'],
                        ['Architectures', '1, 2 and 3 hidden layers · 16 units per layer'],
                        ['Training', '400 epochs · Adam · tanh activation'],
                        ['Data split', '64 training samples · 128 held-out samples'],
                        ['Repetitions', '5 fixed initializations · one split'],
                        ['Resources', 'Bounded CPU jobs · no GPUs'],
                        ['Agent model', 'GPT-6.1 Sol · medium'],
                        ['Reference', 'Linear and quadratic regression baselines'],
                        ['Presentation', `Version ${version} · original evidence preserved`],
                      ].map(([label, value]) => (
                        <div key={label}>
                          <dt>{label}</dt>
                          <dd>{value}</dd>
                        </div>
                      ))}
                    </dl>
                  </section>
                )}
                {tab === 'Evidence' && (
                  <section className="plain-section">
                    <div className="section-title">
                      <h2>Artifacts</h2>
                      <button
                        className="text-button"
                        onClick={() =>
                          download(
                            'preview-evidence.json',
                            JSON.stringify(
                              {
                                mode: 'synthetic-design-preview',
                                verified: false,
                                measurements: [
                                  { layers: 1, mean: 0.0579, sd: 0.0059 },
                                  { layers: 2, mean: 0.0447, sd: 0.0003 },
                                  { layers: 3, mean: 0.0497, sd: 0.0039 },
                                ],
                              },
                              null,
                              2,
                            ),
                            'application/json',
                          )
                        }
                      >
                        <ArrowDownToLine size={15} />
                        Download preview
                      </button>
                    </div>

                    <div className="file-list">
                      {files.map((f) => (
                        <div className="file-row" key={f.name}>
                          {f.name.endsWith('.py') ? (
                            <FileCode2 size={21} />
                          ) : f.name.endsWith('.json') ? (
                            <FileJson size={21} />
                          ) : (
                            <FileText size={21} />
                          )}
                          <div>
                            <strong>{f.name}</strong>
                          </div>
                          <span>{f.size}</span>
                          <button
                            className="icon-button"
                            aria-label={`Inspect ${f.name}`}
                            onClick={() =>
                              setToast(`${f.name} · ${f.type}. Illustrative artifact.`)
                            }
                          >
                            <ChevronRight size={17} />
                          </button>
                        </div>
                      ))}
                    </div>
                  </section>
                )}
              </div>
              {discussion && (
                <aside className="discussion desktop-discussion">{conversation}</aside>
              )}
            </main>
            {discussion && (
              <div className="mobile-discussion">
                <Overlay title="Discuss this experiment" close={() => setDiscussion(false)}>
                  {conversation}
                </Overlay>
              </div>
            )}
          </>
        ) : (
          <main className="overview">
            {page === 'experiments' && (
              <>
                <div className="overview-heading">
                  <div>
                    <h1>Experiments</h1>
                  </div>
                  <button className="primary" onClick={() => setCreating(true)}>
                    <Plus size={17} />
                    New experiment
                  </button>
                </div>
                <div className="list-toolbar">
                  <div className="filters">
                    {['All', 'Active', 'Complete'].map((f) => (
                      <button
                        className={filter === f ? 'selected' : ''}
                        key={f}
                        onClick={() => setFilter(f)}
                      >
                        {f}
                      </button>
                    ))}
                  </div>
                  <label className="search">
                    <Search size={16} />
                    <input
                      aria-label="Search experiments"
                      placeholder="Search experiments"
                      value={query}
                      onChange={(e) => setQuery(e.target.value)}
                    />
                  </label>
                </div>
                <div className="experiment-list">
                  {experiments
                    .filter(
                      (e) =>
                        e.title.toLowerCase().includes(query.toLowerCase()) &&
                        (filter === 'All' ||
                          (filter === 'Complete'
                            ? e.state === 'complete'
                            : e.state !== 'complete')),
                    )
                    .map((e) => (
                      <button className="experiment-row" key={e.id} onClick={() => open(e.id)}>
                        <span className="row-number">{String(e.id).padStart(3, '0')}</span>
                        <div>
                          <h2>{e.title}</h2>
                          <p>{e.brief}</p>
                          <span className={`status ${e.state}`}>
                            <span className={`status-dot ${e.state}`} />
                            {e.state === 'complete'
                              ? 'Complete'
                              : e.state === 'running'
                                ? 'Running'
                                : 'Queued'}
                            <span className="row-date">{e.date}</span>
                          </span>
                        </div>
                        <ChevronRight size={18} />
                      </button>
                    ))}
                  {!experiments.some(
                    (e) =>
                      e.title.toLowerCase().includes(query.toLowerCase()) &&
                      (filter === 'All' ||
                        (filter === 'Complete' ? e.state === 'complete' : e.state !== 'complete')),
                  ) && (
                    <div className="empty-state">
                      <Search size={25} />
                      <h2>No matching experiments</h2>
                      <p>Adjust the search or status filter.</p>
                    </div>
                  )}
                </div>
              </>
            )}
            {page === 'ideas' && (
              <>
                <h1>Results &amp; ideas</h1>

                <h2 className="subheading">Completed experiments</h2>
                {initial
                  .filter((e) => e.state === 'complete')
                  .map((e) => (
                    <button className="finding-row" key={e.id} onClick={() => open(e.id)}>
                      <div>
                        <strong>{e.title}</strong>
                      </div>
                      <ArrowRight size={17} />
                    </button>
                  ))}
                <h2 className="subheading">Saved ideas</h2>
                {saved ? (
                  <div className="saved-idea">
                    <Lightbulb size={18} />
                    <div>
                      <h3>MLP width comparison at fixed depth</h3>
                      <p>
                        Compare 8, 16 and 32 units per hidden layer using the same split and
                        training budget.
                      </p>
                      <button className="text-button" onClick={() => open(6)}>
                        From experiment 006
                        <ArrowRight size={13} />
                      </button>
                    </div>
                  </div>
                ) : (
                  <p className="muted">No saved ideas.</p>
                )}
              </>
            )}
            {page === 'inbox' && (
              <>
                <div className="section-title">
                  <h1>Inbox</h1>
                  <button className="text-button" onClick={() => setRead(true)}>
                    {read ? (
                      <>
                        <Check size={15} />
                        All read
                      </>
                    ) : (
                      'Mark as read'
                    )}
                  </button>
                </div>

                <button
                  className="inbox-row"
                  onClick={() => {
                    setRead(true);
                    open(6);
                  }}
                >
                  <span className={`status-dot ${read ? 'read' : 'complete'}`} />
                  <div>
                    <strong>MLP depth comparison completed</strong>
                  </div>
                  <time>14:32</time>
                  <ArrowRight size={18} />
                </button>
              </>
            )}
            {page === 'settings' && (
              <>
                <h1>Executor &amp; quota</h1>

                <dl className="protocol-list">
                  <div>
                    <dt>Executor</dt>
                    <dd>
                      <span className="status">
                        <span className="status-dot complete" />
                        acacia · online in preview
                      </span>
                    </dd>
                  </div>
                  <div>
                    <dt>Resource envelope</dt>
                    <dd>Up to three bounded CPU jobs · no GPUs</dd>
                  </div>
                  <div>
                    <dt>Authentication</dt>
                    <dd>Managed ChatGPT authentication</dd>
                  </div>
                  <div>
                    <dt>Quota</dt>
                    <dd>Fresh admission required before real work</dd>
                  </div>
                </dl>
                <div className="setting-row">
                  <div>
                    <h3>Pause new agent work</h3>
                    <p>Current observations and published results stay available.</p>
                  </div>
                  <button
                    role="switch"
                    aria-checked={paused}
                    aria-label="Pause new agent work"
                    className={`toggle ${paused ? 'on' : ''}`}
                    onClick={() => setPaused(!paused)}
                  >
                    <span />
                  </button>
                </div>
              </>
            )}
          </main>
        )}
      </div>
      {creating && (
        <Overlay title="New experiment" close={() => setCreating(false)}>
          <div className="modal-heading">
            <Beaker size={22} />
            <button
              className="icon-button"
              aria-label="Close new experiment"
              onClick={() => setCreating(false)}
            >
              <X size={20} />
            </button>
          </div>
          <h2>New experiment</h2>

          <form
            onSubmit={(e) => {
              e.preventDefault();
              const id = Math.max(...experiments.map((x) => x.id)) + 1;
              setExperiments([
                {
                  id,
                  title: brief.trim().split('\n')[0].slice(0, 90),
                  brief: brief.trim(),
                  state: 'queued',
                  date: 'Just now',
                },
                ...experiments,
              ]);
              setCreating(false);
              setBrief('');
              setSelected(id);
              setTab('Progress');
              setProgressMode('queued');
              setPage('experiments');
              setToast('Added to the mock only. No experiment was submitted.');
            }}
          >
            <label className="field-label" htmlFor="new-brief">
              Experiment brief
            </label>
            <textarea
              id="new-brief"
              autoFocus
              placeholder="Describe the hypothesis, dataset, methods and evaluation criteria…"
              required
              minLength={8}
              value={brief}
              onChange={(e) => setBrief(e.target.value)}
              rows={5}
            />
            <div className="form-options">
              <div>
                <span className="field-label">Model</span>
                <Select
                  label="Experiment model"
                  value={creationModel}
                  onChange={setCreationModel}
                  placement="up"
                  options={[
                    {
                      value: 'sol',
                      label: 'GPT-6.1 Sol',
                    },
                  ]}
                />
              </div>
              <div>
                <span className="field-label">Reasoning</span>
                <Select
                  label="Reasoning effort"
                  value={creationEffort}
                  onChange={setCreationEffort}
                  placement="up"
                  options={[
                    { value: 'medium', label: 'Medium' },
                    { value: 'high', label: 'High' },
                  ]}
                />
              </div>
            </div>
            <button className="primary create-submit" disabled={brief.trim().length < 8}>
              Preview experiment
              <ArrowRight size={16} />
            </button>
          </form>
        </Overlay>
      )}
      {toast && (
        <div className="toast" role="status">
          <Check size={16} />
          {toast}
        </div>
      )}
    </div>
  );
}
createRoot(document.getElementById('root')!).render(<App />);
