import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Description } from './Description';
import { ExecutorUpdate } from './ExecutorUpdate';
import {
  ArrowLeft,
  ArrowRight,
  Beaker,
  Check,
  Trash2,
  ChevronRight,
  Clock3,
  Menu,
  Moon,
  Sun,
  FileJson,
  FileCode2,
  Download,
  FileText,
  Inbox,
  LockKeyhole,
  MessageSquare,
  Pause,
  Play,
  Plus,
  RefreshCw,
  Search,
  Settings2,
  X,
} from 'lucide-react';
import { api, command, Unauthorized } from './api';
import type { Experiment, InboxEntry, Status } from './types';
import './style.css';
import { registerWorkspaceTools } from './webmcp';
import { ProjectKnowledge } from './ProjectKnowledge';
import { downloadEvidenceBundle } from './evidence';
import { currentReport, availableReports } from './reports';
import { IncrementalResults } from './IncrementalResults';
import { StageTimeline } from './Progress';
import { timelineStages, experimentStatus } from './progress';
import { PlanReview, Steering } from './PlanReview';
import { Conversation } from './Conversation';
import { Select } from './Select';
import { Report } from './Report';
import { Protocol } from './Protocol';
import { ExperimentActions } from './ExperimentActions';
import { DeleteExperiment } from './DeleteExperiment';
import { CancelExperiment } from './CancelExperiment';
import { useDialog } from './useDialog';
import { NewProject } from './NewProject';
import { ProjectSettings } from './ProjectSettings';
import { useTheme } from './theme';
import { FollowExperiment, forgetNotificationDevice } from './FollowExperiment';

const terminal = ['complete', 'failed', 'cancelled'];
const pretty = (value: string) => value.replaceAll('_', ' ');
const experimentTitle = (exp: Experiment) => {
  if (exp.title || exp.analysis?.title) return exp.title || exp.analysis!.title;
  if (exp.contract.planning_version && !exp.plan_review && !terminal.includes(exp.phase))
    return 'Planning experiment…';
  return exp.brief.length <= 100 ? exp.brief : `${exp.brief.slice(0, 97).replace(/\s+\S*$/, '')}…`;
};
const date = (seconds: number) =>
  new Date(seconds * 1000).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
function age(seconds: number | null) {
  if (!seconds) return 'Not observed';
  const n = Math.max(0, Math.floor(Date.now() / 1000 - seconds));
  return n < 60
    ? `${n}s ago`
    : n < 3600
      ? `${Math.floor(n / 60)}m ago`
      : `${Math.floor(n / 3600)}h ago`;
}
function Badge({ exp }: { exp: Experiment }) {
  return (
    <span
      className={`badge ${exp.phase === 'complete' ? 'success' : exp.phase === 'failed' ? 'danger' : exp.hold_reason ? 'held' : 'active'}`}
    >
      <span className="dot" />
      {experimentStatus(exp)}
    </span>
  );
}

function App() {
  const { preference, theme, changePreference } = useTheme();
  const appearance = (
    <div className="appearance-selector">
      <Select
        label="Appearance"
        value={preference}
        options={[
          { value: 'system', label: 'System theme' },
          { value: 'light', label: 'Light theme' },
          { value: 'dark', label: 'Dark theme' },
        ]}
        onChange={changePreference}
      />
    </div>
  );
  const [auth, setAuth] = useState<boolean | null>(null);
  const [token, setToken] = useState('');
  const [allExperiments, setExperiments] = useState<Experiment[]>([]);
  const [projectId, setProjectId] = useState(
    () => new URLSearchParams(location.search).get('project') || 'test-project',
  );
  const experiments = useMemo(
    () => allExperiments.filter((e) => (e.project_id || 'test-project') === projectId),
    [allExperiments, projectId],
  );
  const [status, setStatus] = useState<Status | null>(null);
  const [inbox, setInbox] = useState<InboxEntry[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const notificationLink = useRef(new URLSearchParams(location.search).get('experiment'));
  const [page, setPage] = useState<'experiments' | 'inbox' | 'settings' | 'memory'>('experiments');
  const [tab, setTab] = useState('Progress');
  const [filter, setFilter] = useState('All');
  const [search, setSearch] = useState('');
  const [creating, setCreating] = useState(false);
  const [creatingProject, setCreatingProject] = useState(() =>
    new URLSearchParams(location.search).has('projectRequest'),
  );
  const [projectSettingsOpen, setProjectSettingsOpen] = useState(() =>
    new URLSearchParams(location.search).has('projectSettingsRequest'),
  );
  const [navOpen, setNavOpen] = useState(false);
  const [mobileNavigation, setMobileNavigation] = useState(
    () => window.matchMedia('(max-width: 760px)').matches,
  );
  useEffect(() => {
    const query = window.matchMedia('(max-width: 760px)');
    const update = (event: MediaQueryListEvent) => {
      setMobileNavigation(event.matches);
      if (!event.matches) setNavOpen(false);
    };
    query.addEventListener('change', update);
    return () => query.removeEventListener('change', update);
  }, []);
  const [compactViews, setCompactViews] = useState(
    () => window.matchMedia('(max-width: 900px)').matches,
  );
  useEffect(() => {
    const query = window.matchMedia('(max-width: 900px)');
    const update = (event: MediaQueryListEvent) => setCompactViews(event.matches);
    query.addEventListener('change', update);
    return () => query.removeEventListener('change', update);
  }, []);
  useEffect(() => setNavOpen(false), [page, selected, auth]);
  const [referenceId, setReferenceId] = useState('');
  const [model, setModel] = useState('');
  const [effort, setEffort] = useState('medium');
  const [workspaceMode, setWorkspaceMode] = useState('auto');
  const [reportChoice, setReportChoice] = useState<string | null>(null);
  const [conversationOpen, setConversationOpen] = useState(() => window.innerWidth >= 1360);
  const [deleting, setDeleting] = useState<{ eid: string; requestId: string } | null>(null);
  const [cancelTarget, setCancelTarget] = useState<Experiment | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<Experiment | null>(null);
  const [brief, setBrief] = useState('');
  useEffect(() => {
    setSelected(null);
    setReferenceId('');
    setCreating(false);
    setSearch('');
    setFilter('All');
    setBrief('');
    setNote('');
    setAnswer('');
    setError(null);
    const url = new URL(location.href);
    url.searchParams.set('project', projectId);
    history.replaceState(null, '', url);
  }, [projectId]);
  useEffect(() => {
    if (status?.mode === 'demo' && projectId === 'test-project')
      setBrief(
        'Compare a linear model and a quadratic model on the reproducible synthetic quadratic regression dataset. Evaluate held-out error, plot predictions, and explain the limitations.',
      );
  }, [status?.mode, projectId]);
  const [note, setNote] = useState('');
  const [answer, setAnswer] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [offline, setOffline] = useState(false);
  const [busy, setBusy] = useState(false);
  const fetching = useRef(false);
  const modalRef = useRef<HTMLElement>(null);
  const navRef = useRef<HTMLElement>(null);
  useDialog(modalRef, creating && auth === true, () => {
    if (!busy) setCreating(false);
  });
  useDialog(navRef, navOpen, () => setNavOpen(false));
  useEffect(() => {
    const models = status?.models || [];
    if (!model && models.length) {
      const choice =
        models.find((m) => m.id === 'gpt-6.1-sol') ||
        models.find((m) => m.id === status?.default_model) ||
        models[0];
      setModel(choice.id);
      setEffort(choice.reasoning_efforts.includes('medium') ? 'medium' : choice.default_effort);
    }
  }, [status?.models, status?.default_model, model]);
  useEffect(() => {
    if (!auth) return;
    const rendered = () =>
      new Promise<void>((resolve) =>
        requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
      );
    return registerWorkspaceTools({
      async list() {
        const value = await api<{ experiments: Experiment[] }>('/api/experiments');
        setExperiments(value.experiments);
        await rendered();
        return value.experiments
          .filter((e) => (e.project_id || 'test-project') === projectId)
          .map((e) => ({
            id: e.id,
            brief: e.brief,
            phase: e.phase,
            hold_reason: e.hold_reason,
            summary: e.analysis?.summary || null,
          }));
      },
      async open(id) {
        const e = await api<Experiment>(`/api/experiments/${id}`);
        setExperiments((list) =>
          list.some((item) => item.id === id)
            ? list.map((item) => (item.id === id ? e : item))
            : [...list, e],
        );
        if ((e.project_id || 'test-project') !== projectId)
          throw new Error('Experiment belongs to another project; select that project first.');
        setSelected(id);
        setPage('experiments');
        setTab(e.phase === 'complete' ? 'Report' : 'Progress');
        await rendered();
      },
      async stage(value) {
        setReferenceId('');
        if (value) setBrief(value);
        setCreating(true);
        await rendered();
      },
    });
  }, [auth, projectId]);
  const reload = useCallback(async () => {
    if (fetching.current) return;
    fetching.current = true;
    try {
      const [list, stat, entries] = await Promise.all([
        api<{ experiments: Experiment[] }>('/api/experiments'),
        api<Status>('/api/status'),
        api<{ entries: InboxEntry[] }>('/api/inbox'),
      ]);
      setExperiments(list.experiments);
      setStatus(stat);
      setInbox(entries.entries);
      setAuth(true);
      setOffline(false);
    } catch (e) {
      if (e instanceof Unauthorized) setAuth(false);
      else setOffline(true);
    } finally {
      fetching.current = false;
    }
  }, []);
  useEffect(() => {
    void reload();
    const timer = setInterval(() => {
      if (auth !== false) void reload();
    }, 5000);
    return () => clearInterval(timer);
  }, [reload, auth]);
  const act = async (fn: () => Promise<void>) => {
    setBusy(true);
    setError(null);
    try {
      await fn();
      await reload();
      return true;
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      return false;
    } finally {
      setBusy(false);
    }
  };
  const choose = (id: string) => {
    setSelected(id);
    setTab(
      experiments.find((item) => item.id === id)?.phase === 'complete' ? 'Report' : 'Progress',
    );
    setReportChoice(null);
    setPage('experiments');
    setNote('');
    setConversationOpen(window.innerWidth >= 1360);
  };
  const exp = experiments.find((e) => e.id === selected);
  useEffect(() => {
    if (!notificationLink.current || auth !== true) return;
    const target = allExperiments.find((e) => e.id === notificationLink.current);
    if (!target) return;
    const project = target.project_id || 'test-project';
    if (project !== projectId) {
      setProjectId(project);
      return;
    }
    setSelected(target.id);
    setPage('experiments');
    setTab(target.phase === 'complete' ? 'Report' : 'Progress');
    notificationLink.current = null;
  }, [allExperiments, auth, projectId]);
  useEffect(() => {
    if (!deleting) return;
    if (!allExperiments.some((e) => e.id === deleting.eid)) {
      setSelected((id) => (id === deleting.eid ? null : id));
      setDeleting(null);
      return;
    }
    let current = true;
    void api<{ state: string; error?: string }>(`/api/commands/${deleting.requestId}`)
      .then((receipt) => {
        if (current && receipt.state === 'rejected') {
          setError(receipt.error || 'The executor rejected deletion. Check the experiment status.');
          setDeleting(null);
        }
      })
      .catch(() => {});
    return () => {
      current = false;
    };
  }, [deleting, allExperiments]);
  const projects = status?.projects || [
    { id: 'test-project', name: 'Test project', available: true },
  ];
  const currentProject = projects.find((p) => p.id === projectId);
  const projectOptions = [
    ...projects.map((p) => ({
      value: p.id,
      label: p.name,
      description: p.available ? p.folder : 'Folder unavailable',
    })),
    { value: '__new_project', label: 'New project…' },
    ...(currentProject ? [{ value: '__project_settings', label: 'Project settings…' }] : []),
    ...(!currentProject
      ? [
          {
            value: projectId,
            label: projectId || 'No projects',
            description: 'Add a project to get started',
          },
        ]
      : []),
  ];
  const scopedInbox = inbox.filter((entry) =>
    experiments.some((e) => e.id === entry.experiment_id),
  );
  const report = exp ? currentReport(exp, reportChoice) : undefined;
  const reportOptions = exp ? availableReports(exp) : [];
  const active = experiments.filter((e) => !terminal.includes(e.phase));
  const attention = experiments.filter(
    (e) =>
      e.phase === 'failed' ||
      e.plan_review?.state === 'awaiting_review' ||
      [
        'needs_attention',
        'agent_dispatch_uncertain',
        'submission_uncertain',
        'agent_unavailable',
        'agent_session_unavailable',
        'cancellation_unconfirmed',
      ].includes(e.hold_reason || ''),
  );
  const completed = experiments.filter((e) => e.phase === 'complete');
  const visibleExperiments = experiments.filter(
    (experiment) =>
      (!search ||
        `${experiment.title || ''} ${experiment.brief}`
          .toLowerCase()
          .includes(search.toLowerCase())) &&
      (filter === 'All' ||
        (filter === 'Active' && !terminal.includes(experiment.phase)) ||
        (filter === 'Complete' && experiment.phase === 'complete') ||
        (filter === 'Attention' && attention.includes(experiment))),
  );
  const executorOnline = Boolean(
    status?.last_tick && Date.now() / 1000 - status.last_tick < 60 && !offline,
  );

  if (auth === null)
    return (
      <main className="login">
        <div className="login-appearance">{appearance}</div>
        <div className="login-brand">
          <Beaker size={30} />
          <span>ExperimentTracker</span>
        </div>
        <div className="session-loading" role="status" aria-live="polite">
          <RefreshCw size={24} className={offline ? undefined : 'spinner'} aria-hidden="true" />
          <p>{offline ? 'Unable to reach your workspace' : 'Opening your workspace…'}</p>
          {offline && <small>Check your connection. Retrying automatically…</small>}
        </div>
      </main>
    );

  if (auth === false)
    return (
      <main className="login">
        <div className="login-appearance">{appearance}</div>
        <div className="login-brand">
          <Beaker size={30} />
          <span>ExperimentTracker</span>
        </div>
        <div className="login-card">
          <h1>ExperimentTracker</h1>
          <p>Sign in to your private experiment workspace.</p>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              void act(async () => {
                await api('/api/session', {
                  method: 'POST',
                  headers: { Authorization: `Bearer ${token}` },
                });
                setToken('');
                setAuth(true);
              });
            }}
          >
            <label htmlFor="token">Access token</label>
            <input
              id="token"
              type="password"
              value={token}
              onChange={(e) => setToken(e.target.value)}
              autoComplete="off"
              required
              placeholder="Your local or gateway access token"
            />
            <button className="primary" disabled={busy}>
              <LockKeyhole size={17} />
              {busy ? 'Signing in…' : 'Open workspace'}
              <ArrowRight size={17} />
            </button>
          </form>
          {error && (
            <p role="alert" className="error">
              {error}
            </p>
          )}
          {offline && (
            <p role="status">
              The service is unreachable. Start it and connect through your SSH tunnel.
            </p>
          )}
          <small>
            Your token stays in a private session cookie. Experiment data is never cached offline.
          </small>
        </div>
      </main>
    );

  return (
    <div className="shell">
      {navOpen && (
        <button
          data-overlay-backdrop
          className="nav-backdrop"
          aria-label="Close navigation"
          onClick={() => setNavOpen(false)}
        />
      )}
      <aside
        ref={navRef}
        inert={mobileNavigation && !navOpen}
        role={mobileNavigation && navOpen ? 'dialog' : 'complementary'}
        aria-modal={mobileNavigation && navOpen ? true : undefined}
        className={`sidebar ${navOpen ? 'nav-open' : ''}`}
        aria-label="Workspace navigation"
      >
        <button
          className="icon-button close-nav"
          aria-label="Close navigation"
          onClick={() => setNavOpen(false)}
        >
          <X size={20} />
        </button>
        <a
          className="brand"
          href="/"
          onClick={(e) => {
            e.preventDefault();
            setSelected(null);
            setPage('experiments');
          }}
        >
          <span className="brand-symbol">
            <Beaker size={22} />
          </span>
          <span>ExperimentTracker</span>
        </a>
        <div className="project-selector">
          <label>Project</label>
          <Select
            label="Project"
            value={projectId}
            options={projectOptions}
            onChange={(id) => {
              if (id === '__new_project') setCreatingProject(true);
              else if (id === '__project_settings') setProjectSettingsOpen(true);
              else setProjectId(id);
              setNavOpen(false);
            }}
            disabled={busy}
          />
        </div>
        <button
          className="new-button"
          onClick={() => {
            setCreating(true);
            setNavOpen(false);
          }}
        >
          <Plus size={18} /> New experiment
        </button>
        <nav onClick={() => setNavOpen(false)}>
          <button
            className={page === 'experiments' ? 'nav-item selected' : 'nav-item'}
            onClick={() => {
              setPage('experiments');
              setSelected(null);
            }}
          >
            <Beaker size={19} />
            Experiments<span>{experiments.length}</span>
          </button>
          <button
            className={page === 'inbox' ? 'nav-item selected' : 'nav-item'}
            onClick={() => {
              setPage('inbox');
              setSelected(null);
            }}
          >
            <Inbox size={19} />
            Inbox{scopedInbox.length > 0 && <span>{scopedInbox.length}</span>}
          </button>
          <button
            className={page === 'memory' ? 'nav-item selected' : 'nav-item'}
            onClick={() => {
              setPage('memory');
              setSelected(null);
            }}
          >
            <FileText size={19} />
            Project knowledge
          </button>
          <button
            className={page === 'settings' ? 'nav-item selected' : 'nav-item'}
            onClick={() => {
              setPage('settings');
              setSelected(null);
            }}
          >
            <Settings2 size={19} />
            Executor & quota
          </button>
        </nav>
        <div className="sidebar-bottom">
          {appearance}
          <ExecutorUpdate
            compact
            onOpen={() => {
              setPage('settings');
              setSelected(null);
            }}
          />
          <div className="executor-label">
            <span className={`dot ${executorOnline ? 'online' : ''}`} />
            <strong>{status?.executor_id || 'Executor'}</strong>
            <span>{executorOnline ? 'Online' : 'Offline'}</span>
          </div>
          <p>
            {status?.mode === 'demo' ? 'Local simulation · no Slurm jobs' : 'Research executor'}
          </p>
          <button
            className="text-button"
            onClick={() =>
              void act(async () => {
                await forgetNotificationDevice();
                if (status?.auth_mode === 'sites') {
                  window.location.assign('/signout-with-chatgpt?return_to=%2F');
                  return;
                }
                await api('/api/session', { method: 'DELETE' });
                setAuth(false);
              })
            }
          >
            Sign out
          </button>
        </div>
      </aside>
      <div className="main">
        <header className="topbar">
          <button
            className="icon-button mobile-menu"
            aria-label="Open navigation"
            onClick={() => setNavOpen(true)}
          >
            <Menu size={20} />
          </button>
          <div className="breadcrumb">
            <span>{currentProject?.name || projectId}</span> <ChevronRight size={14} />
            <strong>
              {exp
                ? 'Experiment'
                : page === 'inbox'
                  ? 'Inbox'
                  : page === 'settings'
                    ? 'Executor & quota'
                    : page === 'memory'
                      ? 'Project knowledge'
                      : 'Experiments'}
            </strong>
          </div>
          <div className="topbar-right">
            <button
              className="icon-button"
              title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
              aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
              onClick={() => changePreference(theme === 'dark' ? 'light' : 'dark')}
            >
              {theme === 'dark' ? <Sun size={17} /> : <Moon size={17} />}
            </button>
            <span className="mode-label">
              {status?.mode === 'demo'
                ? 'Synthetic demo'
                : status?.mode === 'gateway'
                  ? 'Private workspace'
                  : 'Live executor'}
            </span>
            <button
              className="icon-button"
              title="Refresh status"
              aria-label="Refresh status"
              onClick={() => void reload()}
            >
              <RefreshCw size={16} />
            </button>
          </div>
        </header>
        <main className="content">
          {offline && (
            <div role="status" className="notice warning">
              Connection lost. Showing the last received update. Check your connection before
              sending a new action.
            </div>
          )}
          {error && (
            <div role="alert" className="notice error">
              {error}
              <button
                className="icon-button"
                aria-label="Dismiss error"
                onClick={() => setError(null)}
              >
                <X size={16} />
              </button>
            </div>
          )}
          {!currentProject?.available && (
            <div role="status" className="notice warning">
              This project is unavailable. Restore its checkout or select another project.
            </div>
          )}
          {page === 'memory' && (
            <>
              <div className="page-heading">
                <h1>Project knowledge</h1>
              </div>
              <ProjectKnowledge
                key={projectId}
                projectId={projectId}
                models={status?.models || []}
                open={choose}
                experiments={experiments}
                useIdea={(idea) => {
                  setBrief(idea.text);
                  setReferenceId(idea.experiment_id);
                  setCreating(true);
                }}
              />
            </>
          )}
          {page === 'experiments' && !exp && (
            <>
              <div className="page-heading">
                <div>
                  <h1>Experiments</h1>
                </div>
                <button className="primary" onClick={() => setCreating(true)}>
                  <Plus size={18} />
                  New experiment
                </button>
              </div>
              <div className="toolbar">
                <div className="filters">
                  {['All', 'Active', 'Complete', 'Attention'].map((f) => (
                    <button
                      key={f}
                      className={filter === f ? 'chosen' : ''}
                      aria-pressed={filter === f}
                      onClick={() => setFilter(f)}
                    >
                      {f}
                      <span>
                        {f === 'All'
                          ? experiments.length
                          : f === 'Active'
                            ? active.length
                            : f === 'Complete'
                              ? completed.length
                              : attention.length}
                      </span>
                    </button>
                  ))}
                </div>
                <label className="search">
                  <Search size={16} />
                  <input
                    aria-label="Search experiments"
                    placeholder="Search experiments"
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                  />
                </label>
              </div>
              <div className="experiment-list">
                {visibleExperiments.map((e) => (
                  <button className="experiment-row" key={e.id} onClick={() => choose(e.id)}>
                    <span
                      className={`experiment-icon ${e.phase === 'complete' ? 'complete-icon' : ''}`}
                    >
                      {e.phase === 'complete' ? <Check size={20} /> : <Beaker size={20} />}
                    </span>
                    <div className="row-description">
                      <strong>{experimentTitle(e)}</strong>
                      {e.phase === 'complete' && e.analysis?.summary && (
                        <span className="row-finding">{e.analysis.summary}</span>
                      )}
                      <small>
                        {Boolean(e.contract.synthetic_demo) && (
                          <>
                            Synthetic experiment <span>·</span>{' '}
                          </>
                        )}
                        {date(e.created_at)}
                      </small>
                    </div>
                    <div className="row-status">
                      <Badge exp={e} />
                    </div>
                    <ChevronRight size={18} />
                  </button>
                ))}
                {experiments.length > 0 && visibleExperiments.length === 0 && (
                  <div className="empty" role="status">
                    <Search size={36} />
                    <h3>No matching experiments.</h3>
                    <p>
                      {search
                        ? `No experiments match “${search}” in ${filter.toLowerCase()}.`
                        : `No experiments in ${filter.toLowerCase()} for this project.`}
                    </p>
                    {search && (
                      <button className="text-button" onClick={() => setSearch('')}>
                        Clear search
                      </button>
                    )}
                    <button
                      className="text-button"
                      onClick={() => {
                        setSearch('');
                        setFilter('All');
                      }}
                    >
                      Show all experiments <ArrowRight size={16} />
                    </button>
                  </div>
                )}
                {experiments.length === 0 && (
                  <div className="empty">
                    <Beaker size={36} />
                    <h3>No experiments yet.</h3>
                    <p>
                      Create an experiment with a research brief, requested methods and outputs.
                    </p>
                    <button className="text-button" onClick={() => setCreating(true)}>
                      Start an experiment <ArrowRight size={16} />
                    </button>
                  </div>
                )}
              </div>
            </>
          )}

          {page === 'experiments' && exp && (
            <>
              <button className="back" onClick={() => setSelected(null)}>
                <ArrowLeft size={16} />
                All experiments
              </button>
              <div className="detail-heading">
                <div className="title-row">
                  <h1>{experimentTitle(exp)}</h1>
                  <ExperimentActions>
                    <button
                      className="text-button"
                      disabled={
                        busy ||
                        exp.pending_commands?.some((c) => c.action === 'summarize_title') ||
                        exp.agent_actions.some(
                          (a) =>
                            ['title', 'summarize_title'].includes(a.phase) &&
                            ['pending', 'dispatching', 'running', 'uncertain'].includes(a.state),
                        )
                      }
                      onClick={() =>
                        void act(async () => {
                          await command(`/api/experiments/${exp.id}/actions`, {
                            action: 'summarize_title',
                          });
                        })
                      }
                    >
                      Summarize title
                    </button>
                    {!['complete', 'cancelled'].includes(exp.phase) && (
                      <button
                        className="text-button delete-experiment"
                        disabled={
                          busy ||
                          !status?.experiment_cancellation ||
                          Boolean(exp.pending_commands?.some((c) => c.action === 'cancel'))
                        }
                        title={
                          status?.experiment_cancellation
                            ? 'Stop work and pending transfers'
                            : 'Install the executor update to enable cancellation'
                        }
                        onClick={() => {
                          setError(null);
                          setCancelTarget(exp);
                        }}
                      >
                        <X size={15} />
                        {exp.pending_commands?.some((c) => c.action === 'cancel')
                          ? 'Cancellation pending'
                          : 'Cancel experiment'}
                      </button>
                    )}
                    {terminal.includes(exp.phase) && (
                      <button
                        className="text-button delete-experiment"
                        disabled={
                          busy ||
                          deleting?.eid === exp.id ||
                          ['cancellation_pending', 'cancellation_unconfirmed'].includes(
                            exp.hold_reason || '',
                          ) ||
                          exp.report_versions?.some((r) =>
                            ['queued', 'building', 'publishing'].includes(r.state),
                          ) ||
                          exp.jobs.some((j) => ['submitting', 'submitted'].includes(j.state)) ||
                          exp.agent_actions.some((a) =>
                            ['dispatching', 'running', 'uncertain'].includes(a.state),
                          ) ||
                          exp.conversation?.some((m) =>
                            ['running', 'uncertain'].includes(m.state),
                          ) ||
                          Boolean(exp.pending_messages?.length)
                        }
                        onClick={() => {
                          setError(null);
                          setDeleteTarget(exp);
                        }}
                      >
                        <Trash2 size={15} />
                        {deleting?.eid === exp.id ? 'Deletion pending' : 'Delete experiment'}
                      </button>
                    )}
                  </ExperimentActions>
                </div>
                {exp.agent_actions
                  .filter((a) => a.phase === 'summarize_title')
                  .slice(-1)
                  .filter((a) => a.state !== 'completed' && a.state !== 'superseded')
                  .map((a) => (
                    <p key={a.id} role="status" className="muted small">
                      {a.state === 'pending'
                        ? 'Title summary queued; waiting for the managed summary model and quota.'
                        : a.state === 'uncertain'
                          ? 'Title summary needs reconciliation before another request.'
                          : a.state === 'failed'
                            ? `Title summary failed: ${a.error || 'Please try again.'}`
                            : 'Summarizing the experiment title…'}
                    </p>
                  ))}
                <div className="detail-meta">
                  <Badge exp={exp} />
                  {Boolean(exp.contract.synthetic_demo) && <span>Synthetic experiment</span>}
                  <span>
                    <Clock3 size={14} />
                    {date(exp.created_at)}
                  </span>
                </div>
                {experimentTitle(exp) !== exp.brief && (
                  <Description key={exp.id} text={exp.brief} />
                )}
                <FollowExperiment key={exp.id} id={exp.id} />
              </div>
              {exp.reference_experiment_id && (
                <p className="reference-link">
                  Reference result{' '}
                  <button
                    className="text-button"
                    disabled={!experiments.some((e) => e.id === exp.reference_experiment_id)}
                    onClick={() => choose(exp.reference_experiment_id!)}
                  >
                    {exp.reference_experiment_id.slice(0, 8)} <ArrowRight size={14} />
                  </button>
                </p>
              )}
              {exp.replaces_experiment_id && (
                <p className="reference-link">
                  Corrected run ·{' '}
                  <button
                    className="text-button"
                    onClick={() => choose(exp.replaces_experiment_id!)}
                  >
                    View earlier experiment <ArrowRight size={14} />
                  </button>
                </p>
              )}
              {exp.steering
                ?.filter((item) => item.replacement_experiment_id)
                .map((item) => (
                  <p className="reference-link" key={item.request_id}>
                    Restarted with your correction ·{' '}
                    <button
                      className="text-button"
                      onClick={() => choose(item.replacement_experiment_id!)}
                    >
                      Open replacement experiment <ArrowRight size={14} />
                    </button>
                  </p>
                ))}
              {Boolean(exp.pending_commands?.length) && (
                <section className="notice warning" role="status">
                  <strong>Waiting for executor</strong>
                  <span>
                    {exp
                      .pending_commands!.map((c) =>
                        c.action
                          ? pretty(c.action)
                          : c.kind === 'create'
                            ? 'New experiment'
                            : c.kind === 'note'
                              ? 'Note delivery'
                              : c.kind === 'delete'
                                ? 'Delete experiment'
                                : c.kind === 'plan_review'
                                  ? 'Plan review'
                                  : c.kind === 'steer'
                                    ? 'Steering instruction'
                                    : c.kind === 'discuss'
                                      ? 'Conversation message'
                                      : 'Answer delivery',
                      )
                      .join(' · ')}
                    . Saved on the gateway; acacia has not applied these commands yet.
                  </span>
                </section>
              )}
              {exp.hold_reason && (
                <div className="notice warning" role="status">
                  <strong>{holdTitle(exp.hold_reason)}</strong>
                  <span>{exp.error || holdExplanation(exp.hold_reason)}</span>
                  <span>{holdGuidance(exp.hold_reason)}</span>
                </div>
              )}
              <div
                className={`experiment-workspace ${conversationOpen && exp.phase === 'complete' ? 'with-conversation' : ''}`}
              >
                <div className="experiment-pane">
                  <div className="tabbar">
                    <div
                      className="tabs"
                      role="tablist"
                      aria-label="Experiment views"
                      onKeyDown={(event) => {
                        if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
                        const buttons = [
                          ...event.currentTarget.querySelectorAll<HTMLButtonElement>(
                            '[role="tab"]',
                          ),
                        ].filter((button) => button.getClientRects().length);
                        const index = buttons.indexOf(document.activeElement as HTMLButtonElement);
                        const next =
                          event.key === 'Home'
                            ? 0
                            : event.key === 'End'
                              ? buttons.length - 1
                              : (index + (event.key === 'ArrowRight' ? 1 : -1) + buttons.length) %
                                buttons.length;
                        event.preventDefault();
                        buttons[next].click();
                        buttons[next].focus();
                      }}
                    >
                      {['Progress', 'Report', 'Notes', 'Protocol', 'Evidence'].map((t) => (
                        <button
                          key={t}
                          id={`tab-${t}`}
                          role="tab"
                          aria-selected={tab === t}
                          tabIndex={
                            tab === t ||
                            (compactViews &&
                              ['Protocol', 'Evidence'].includes(tab) &&
                              t === 'Progress')
                              ? 0
                              : -1
                          }
                          className={`${tab === t ? 'selected' : ''} ${['Protocol', 'Evidence'].includes(t) ? 'secondary-tab' : ''}`}
                          onClick={() => setTab(t)}
                        >
                          {t}
                          {t === 'Notes' && exp.notes.length > 0 && <span>{exp.notes.length}</span>}
                        </button>
                      ))}
                      <Select
                        label="More experiment views"
                        className={`more-views ${['Protocol', 'Evidence'].includes(tab) ? 'current' : ''}`}
                        value={tab}
                        displayValue={['Protocol', 'Evidence'].includes(tab) ? tab : 'More'}
                        options={['Protocol', 'Evidence'].map((value) => ({ value, label: value }))}
                        onChange={setTab}
                      />
                    </div>
                    {exp.phase === 'complete' && (
                      <button
                        className={`discuss-button ${conversationOpen ? 'active' : ''}`}
                        aria-label="Discuss results"
                        aria-expanded={conversationOpen}
                        onClick={() => setConversationOpen(!conversationOpen)}
                      >
                        <MessageSquare size={17} />
                        <span>Discuss</span>
                      </button>
                    )}
                  </div>
                  <div role="tabpanel" aria-labelledby={`tab-${tab}`}>
                    {tab === 'Progress' && (
                      <div className="progress-view">
                        <PlanReview
                          key={`${exp.id}:${exp.plan_review?.revision || 0}`}
                          exp={exp}
                          busy={
                            busy ||
                            Boolean(exp.pending_commands?.some((c) => c.kind === 'plan_review'))
                          }
                          onDecision={(decision, feedback) =>
                            act(async () => {
                              await command(`/api/experiments/${exp.id}/plan-review`, {
                                decision,
                                plan_revision: exp.plan_review!.revision,
                                ...(feedback ? { feedback } : {}),
                              });
                            })
                          }
                        />
                        <section className="panel progress-panel" aria-label="Progress">
                          <div className="section-heading">
                            <span className="muted">
                              {(timelineStages(exp).length === 0 ||
                                (exp.recipe_id === 'agent_experiment_v1' &&
                                  !exp.snapshot &&
                                  !exp.plan_review?.stages.length)) &&
                              !terminal.includes(exp.phase)
                                ? exp.plan_review?.state === 'awaiting_review'
                                  ? 'Plan ready for review'
                                  : 'Defining stages…'
                                : 'Stages'}
                            </span>
                          </div>
                          <StageTimeline key={exp.id} exp={exp} status={status} />
                        </section>
                        <Steering
                          key={exp.id}
                          exp={exp}
                          busy={
                            busy || Boolean(exp.pending_commands?.some((c) => c.kind === 'steer'))
                          }
                          onSend={(text, mode) =>
                            act(async () => {
                              await command(`/api/experiments/${exp.id}/steering`, { text, mode });
                            })
                          }
                        />
                        {!['complete', 'cancelled'].includes(exp.phase) && (
                          <section className="panel control-panel">
                            <h3>Automation controls</h3>
                            {(exp.hold_reason === 'agent_session_unavailable' ||
                              (exp.phase === 'failed' &&
                                exp.recipe_id === 'agent_experiment_v1' &&
                                ((exp.snapshot &&
                                  [
                                    'compute_failed',
                                    'validation_failed',
                                    'agent_recovery_exhausted',
                                  ].includes(exp.hold_reason || '')) ||
                                  [
                                    'agent_recovery_exhausted',
                                    'agent_correction_stalled',
                                    'agent_budget_exhausted',
                                  ].includes(exp.hold_reason || '')))) && (
                              <button
                                disabled={
                                  busy ||
                                  Boolean(
                                    exp.pending_commands?.some((c) => c.action === 'retry_agent'),
                                  )
                                }
                                onClick={() =>
                                  void act(async () => {
                                    await command(`/api/experiments/${exp.id}/actions`, {
                                      action: 'retry_agent',
                                    });
                                  })
                                }
                              >
                                <Play size={16} />
                                Retry with agent
                              </button>
                            )}
                            {!terminal.includes(exp.phase) && (
                              <>
                                <button
                                  disabled={busy}
                                  onClick={() =>
                                    void act(async () => {
                                      await command(`/api/experiments/${exp.id}/actions`, {
                                        action: exp.pause_agent ? 'resume_agent' : 'pause_agent',
                                      });
                                    })
                                  }
                                >
                                  {exp.pause_agent ? <Play size={16} /> : <Pause size={16} />}
                                  {exp.pause_agent ? 'Resume agent work' : 'Pause agent work'}
                                </button>
                                <button
                                  disabled={busy}
                                  onClick={() =>
                                    void act(async () => {
                                      await command(`/api/experiments/${exp.id}/actions`, {
                                        action: exp.pause_compute
                                          ? 'resume_new_compute'
                                          : 'pause_new_compute',
                                      });
                                    })
                                  }
                                >
                                  {exp.pause_compute ? <Play size={16} /> : <Pause size={16} />}
                                  {exp.pause_compute ? 'Resume new compute' : 'Pause new compute'}
                                </button>
                                <small>
                                  Running scheduler jobs keep going. These controls never cancel a
                                  job.
                                </small>
                              </>
                            )}
                          </section>
                        )}
                      </div>
                    )}
                    {tab === 'Evidence' && (
                      <section className="panel">
                        <div className="section-heading">
                          <h2>
                            {exp.contract.execution_contract === 3
                              ? 'Published results & files'
                              : 'Validated artifacts'}
                          </h2>
                          {exp.phase === 'complete' && (
                            <button
                              className="text-button"
                              disabled={busy}
                              onClick={() => void act(() => downloadEvidenceBundle(exp))}
                            >
                              <Download size={17} />
                              {busy
                                ? 'Checking files…'
                                : exp.contract.execution_contract === 3
                                  ? 'Download results'
                                  : 'Download evidence bundle'}
                            </button>
                          )}
                        </div>
                        <p className="muted">
                          {exp.contract.execution_contract === 3 ? (
                            'Small displayed results and files are retained here. Downloads contain published objects and task context.'
                          ) : (
                            <>
                              Evidence revision {exp.evidence_revision}. The bundle is verified
                              against the saved file hashes.
                            </>
                          )}
                        </p>
                        {exp.artifacts.map((a) => (
                          <a
                            className="artifact-row"
                            key={a.id}
                            href={`/api/artifacts/${a.id}`}
                            target="_blank"
                            rel="noreferrer"
                          >
                            {a.name.endsWith('.json') ? (
                              <FileJson size={20} />
                            ) : /\.(py|js|ts|sh)$/.test(a.name) ? (
                              <FileCode2 size={20} />
                            ) : (
                              <FileText size={20} />
                            )}
                            <div>
                              <strong>{a.name}</strong>
                            </div>
                            <span className="file-size">{(a.size / 1024).toFixed(1)} KiB</span>
                            <span className="muted small">
                              {status?.mode === 'gateway'
                                ? a.remote_published
                                  ? 'Published'
                                  : 'Upload pending'
                                : exp.contract.execution_contract === 3
                                  ? 'Registered'
                                  : 'Local validated'}
                            </span>
                            <Download size={17} />
                          </a>
                        ))}
                        {exp.artifacts.length > 0 && (
                          <details className="technical-details">
                            <summary>Artifact identities & hashes</summary>
                            {exp.artifacts.map((a) => (
                              <dl className="protocol-list" key={a.id}>
                                <div>
                                  <dt>{a.name}</dt>
                                  <dd>
                                    {a.id}
                                    <br />
                                    SHA-256 {a.sha256}
                                  </dd>
                                </div>
                              </dl>
                            ))}
                          </details>
                        )}
                        {!exp.artifacts.length && <p>No registered files yet.</p>}
                      </section>
                    )}
                    {tab === 'Report' && (
                      <section className="panel report-panel">
                        <IncrementalResults experiment={exp} />
                        {report ? (
                          <>
                            <div className="report-toolbar">
                              {reportOptions.length > 1 && (
                                <Select
                                  label="Report version"
                                  value={report.id}
                                  onChange={setReportChoice}
                                  options={reportOptions.map((r) => ({
                                    value: r.id,
                                    label:
                                      r.version === 1
                                        ? 'Original · version 1'
                                        : `Version ${r.version}`,
                                  }))}
                                />
                              )}
                              <a
                                className="text-button"
                                href={`/api/artifacts/${report.id}`}
                                target="_blank"
                                rel="noreferrer"
                              >
                                Open report <ArrowRight size={16} />
                              </a>
                            </div>
                            {exp.report_versions
                              ?.filter((r) => r.state !== 'ready')
                              .map((r) => (
                                <p className="muted small" key={r.version}>
                                  Report version {r.version}:{' '}
                                  {r.state === 'failed' ? r.error : pretty(r.state)}
                                </p>
                              ))}
                            <Report id={report.id} theme={theme} />
                          </>
                        ) : (
                          <div className="empty">
                            <FileText size={32} />
                            <h3>Interpretation is still pending.</h3>
                            <p>
                              The working report appears when interpretation is ready. Available
                              measurements and figures appear above.
                            </p>
                          </div>
                        )}
                      </section>
                    )}
                    {tab === 'Notes' && (
                      <section className="panel notes-panel">
                        <h2>Notes</h2>
                        {exp.notes.map((n) => (
                          <article className="note" key={n.id}>
                            <div>
                              <strong>{n.author}</strong>
                              <small>{date(n.created_at)}</small>
                            </div>
                            <p>{n.text}</p>
                          </article>
                        ))}
                        <form
                          onSubmit={(e) => {
                            e.preventDefault();
                            void act(async () => {
                              await command(`/api/experiments/${exp.id}/notes`, { text: note });
                              setNote('');
                            });
                          }}
                        >
                          <label htmlFor="note">Add a note</label>
                          <textarea
                            id="note"
                            value={note}
                            onChange={(e) => setNote(e.target.value)}
                            maxLength={10000}
                            required
                            placeholder="An observation, question, or reminder…"
                          />
                          <button className="primary" disabled={busy || !note.trim()}>
                            Save note <Plus size={16} />
                          </button>
                        </form>
                      </section>
                    )}
                    {tab === 'Protocol' && <Protocol exp={exp} />}
                  </div>
                  {exp.pending_request && (
                    <section className="panel decision">
                      <h2>
                        {exp.pending_request.expired
                          ? 'Expired agent request'
                          : 'Codex needs your decision'}
                      </h2>
                      <p>
                        {exp.pending_request.expired
                          ? 'This request belonged to a previous process connection. It cannot be replayed; use the runbook to reconcile the saved action.'
                          : 'Answer this request to continue the current turn.'}
                      </p>
                      <pre>{JSON.stringify(exp.pending_request.params, null, 2)}</pre>
                      {!exp.pending_request.expired && (
                        <form
                          onSubmit={(e) => {
                            e.preventDefault();
                            void act(async () => {
                              await command(`/api/experiments/${exp.id}/answers`, {
                                pending_request_id: exp.pending_request!.id,
                                answer,
                              });
                              setAnswer('');
                            });
                          }}
                        >
                          <label htmlFor="answer">
                            {exp.pending_request.method.includes('Approval')
                              ? 'Decision: accept, decline, or cancel'
                              : 'Your answer (multiple questions: JSON keyed by question ID)'}
                          </label>
                          <input
                            id="answer"
                            value={answer}
                            onChange={(e) => setAnswer(e.target.value)}
                            required
                          />
                          <button className="primary" disabled={busy}>
                            Send answer
                          </button>
                        </form>
                      )}
                    </section>
                  )}
                </div>
                {exp.phase === 'complete' && (
                  <Conversation
                    key={exp.id}
                    open={conversationOpen}
                    exp={exp}
                    status={status}
                    reload={reload}
                    openExperiment={choose}
                    close={() => setConversationOpen(false)}
                    openReport={(version) => {
                      const report = exp.report_versions?.find((r) => r.version === version);
                      setReportChoice(report?.report_artifact_id || null);
                      setTab('Report');
                    }}
                  />
                )}
              </div>
            </>
          )}

          {page === 'inbox' && (
            <>
              <div className="page-heading">
                <div>
                  <h1>Inbox</h1>
                  <p>Interpreted results and decisions that need your attention.</p>
                </div>
              </div>
              <div className="experiment-list">
                {scopedInbox.map((n) => (
                  <button
                    className="experiment-row"
                    key={n.id}
                    onClick={() => choose(n.experiment_id)}
                  >
                    <span
                      className={`experiment-icon ${n.kind === 'result_ready' ? 'complete-icon' : ''}`}
                    >
                      {n.kind === 'result_ready' ? <Check size={20} /> : <Inbox size={20} />}
                    </span>
                    <div className="row-description">
                      <strong>{n.text}</strong>
                      <small>
                        {pretty(n.kind)} · {date(n.created_at)}
                      </small>
                    </div>
                    <ChevronRight size={18} />
                  </button>
                ))}
                {!scopedInbox.length && (
                  <div className="empty">
                    <Inbox size={34} />
                    <h3>Nothing needs your attention.</h3>
                    <p>Completed interpretations and required decisions will appear here.</p>
                  </div>
                )}
              </div>
            </>
          )}
          {page === 'settings' && (
            <>
              <div className="page-heading">
                <div>
                  <h1>Executor & quota</h1>
                  <p>Compute and agent usage have separate controls.</p>
                </div>
              </div>
              <ExecutorUpdate />
              <div className="bottom-grid">
                <section className="panel">
                  <h2>{status?.executor_id}</h2>
                  <p>
                    <span className={`dot ${executorOnline ? 'online' : ''}`} />{' '}
                    {executorOnline ? 'Executor online' : 'Executor offline or stale'}
                  </p>
                  <p>Last observed {age(status?.last_tick || null)}</p>
                  <p>
                    Mode:{' '}
                    {status?.mode === 'demo'
                      ? 'Local deterministic simulation'
                      : status?.mode === 'gateway'
                        ? 'Private outbound gateway'
                        : 'Live Slurm and Codex'}
                  </p>
                  <p>
                    {status?.mode === 'demo'
                      ? 'Synthetic CPU demonstration. No Slurm jobs or GPU allocations.'
                      : status?.compute_policy?.agent_selected_resources
                        ? 'Small checks run in an agent tool loop with immediate fixes. Sustained compute uses Slurm CPU/GPU resources selected from cluster capabilities and your permissions. Dependencies and requests are saved in the protocol.'
                        : 'Waiting for the executor’s current resource policy.'}
                  </p>
                  {status?.compute_policy?.agent_selected_resources && (
                    <p>
                      {status.compute_policy.max_jobs_per_experiment == null
                        ? 'No fixed three-job limit.'
                        : `Up to ${status.compute_policy.max_jobs_per_experiment} jobs per experiment.`}{' '}
                      Optional executor limits apply alongside Slurm account and partition limits.
                    </p>
                  )}
                  {status?.cluster && (
                    <p>Cluster capabilities observed {age(status.cluster.observed_at)}</p>
                  )}
                  {status?.cluster_error && <p className="error">{status.cluster_error}</p>}
                  {status?.agent_error && <p className="error">{status.agent_error}</p>}
                </section>
                <section className="panel">
                  <h2>Subscription reserve</h2>
                  <Quota
                    status={status}
                    busy={busy}
                    onSave={async (five_hour_percent, weekly_percent) => {
                      let receipt: { state: string; request_id: string; error?: string } | null =
                        null;
                      const saved = await act(async () => {
                        receipt = await command('/api/quota/reserve', {
                          five_hour_percent,
                          weekly_percent,
                        });
                      });
                      return saved ? receipt : null;
                    }}
                    onRefresh={reload}
                  />
                  <p>Drag the markers to set the remaining quota reserved for other work.</p>
                  <p className="muted">
                    No inference while watching jobs. No purchased credits, quota resets, or API
                    billing fallback.
                  </p>
                </section>
              </div>
            </>
          )}
        </main>
      </div>
      {creatingProject && (
        <NewProject
          onClose={() => setCreatingProject(false)}
          onReady={async (project) => {
            await reload();
            setProjectId(project.id);
            setCreatingProject(false);
          }}
        />
      )}
      {projectSettingsOpen && currentProject && (
        <ProjectSettings
          project={currentProject}
          onClose={() => setProjectSettingsOpen(false)}
          onSaved={reload}
          onDeleted={async () => {
            setProjectSettingsOpen(false);
            setProjectId(projects.find((p) => p.id !== currentProject.id)?.id || '');
            setSelected(null);
            await reload();
          }}
        />
      )}
      {cancelTarget && (
        <CancelExperiment
          title={experimentTitle(cancelTarget)}
          busy={busy}
          error={error}
          onClose={() => setCancelTarget(null)}
          onConfirm={async () => {
            await act(async () => {
              await command(`/api/experiments/${cancelTarget.id}/actions`, { action: 'cancel' });
              setCancelTarget(null);
            });
          }}
        />
      )}
      {deleteTarget && (
        <DeleteExperiment
          title={experimentTitle(deleteTarget)}
          busy={busy}
          error={error}
          onClose={() => setDeleteTarget(null)}
          onConfirm={async () => {
            await act(async () => {
              const result = await command<{ state: string; request_id: string }>(
                `/api/experiments/${deleteTarget.id}/delete`,
                {},
              );
              if (result.state === 'applied') setSelected(null);
              else setDeleting({ eid: deleteTarget.id, requestId: result.request_id });
              setDeleteTarget(null);
            });
          }}
        />
      )}
      {creating && (
        <div
          data-overlay-backdrop
          className="modal-backdrop"
          onClick={() => !busy && setCreating(false)}
        >
          <section
            ref={modalRef}
            className="modal"
            role="dialog"
            aria-modal="true"
            aria-labelledby="new-title"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="section-heading">
              <div>
                <h2 id="new-title">New experiment</h2>
                <p className="muted small">{currentProject?.name || projectId}</p>
              </div>
              <button
                className="icon-button"
                aria-label="Close new experiment"
                disabled={busy}
                onClick={() => setCreating(false)}
              >
                <X size={20} />
              </button>
            </div>
            <form
              onSubmit={(e) => {
                e.preventDefault();
                void act(async () => {
                  const result = await command<{ experiment_id: string }>('/api/experiments', {
                    brief,
                    project_id: projectId,
                    execution: currentProject?.execution,
                    workspace_mode: workspaceMode,
                    ...(model ? { model, reasoning_effort: effort } : {}),
                    recipe_id: status?.mode === 'demo' ? 'toy_compare_v1' : 'agent_experiment_v1',
                    ...(referenceId ? { reference_experiment_id: referenceId } : {}),
                  });
                  setCreating(false);
                  choose(result.experiment_id);
                });
              }}
            >
              <p className="brief-guidance" id="brief-guidance">
                {status?.mode === 'demo'
                  ? 'Synthetic demonstration. No model calls or Slurm jobs.'
                  : 'Describe what you want to learn. You’ll review the plan before implementation starts.'}
              </p>
              <label htmlFor="brief">Research brief</label>
              <textarea
                id="brief"
                value={brief}
                onChange={(e) => setBrief(e.target.value)}
                minLength={10}
                maxLength={4000}
                required
                rows={5}
                aria-describedby="brief-guidance"
                placeholder="Describe the question, methods, data and outputs for this project."
              />
              <p className="muted execution-summary">
                {status?.models?.find((item) => item.id === model)?.name ||
                  'Waiting for model catalog'}
                {' · '}
                {effort} reasoning{' · '}
                {workspaceMode === 'auto'
                  ? 'Automatic working folder isolation'
                  : workspaceMode === 'worktree'
                    ? 'Separate Git worktree'
                    : 'Shared working folder'}
              </p>
              <details className="execution-options">
                <summary>Execution options</summary>
                <p className="muted small execution-location">
                  Compute on{' '}
                  {currentProject?.execution?.context === 'jean-zay'
                    ? `Jean Zay · checked every ${currentProject.execution.poll_seconds / 60} minutes`
                    : 'Acacia'}
                  . Agents and results stay on acacia.
                </p>
                <label>Working folder</label>
                <Select
                  label="Working folder"
                  value={workspaceMode}
                  options={[
                    { value: 'auto', label: 'Automatic isolation when this project is busy' },
                    { value: 'worktree', label: 'Always use a separate Git worktree' },
                    { value: 'shared', label: 'Shared folder · wait while this project is busy' },
                  ]}
                  onChange={setWorkspaceMode}
                />
                {currentProject?.supports_worktrees === false && workspaceMode !== 'shared' && (
                  <p className="muted">
                    This project needs Git with an initial commit for worktree isolation. Initialize
                    Git and commit its source to implement parallel experiments; otherwise
                    implementation waits while this project is busy.
                  </p>
                )}
                <div className="model-fields">
                  <div>
                    <label>Codex model</label>
                    <Select
                      label="Codex model"
                      value={model}
                      disabled={!status?.models?.length}
                      displayValue={
                        !status?.models?.length ? 'Waiting for model catalog' : undefined
                      }
                      options={(status?.models || []).map((m) => ({ value: m.id, label: m.name }))}
                      onChange={(value) => {
                        const choice = status?.models?.find((m) => m.id === value);
                        setModel(value);
                        if (choice)
                          setEffort(
                            choice.reasoning_efforts.includes('medium')
                              ? 'medium'
                              : choice.default_effort,
                          );
                      }}
                    />
                  </div>
                  <div>
                    <label>Reasoning effort</label>
                    <Select
                      label="Reasoning effort"
                      value={effort}
                      disabled={!model}
                      onChange={setEffort}
                      options={(
                        status?.models?.find((m) => m.id === model)?.reasoning_efforts || [effort]
                      ).map((value) => ({
                        value,
                        label: value.charAt(0).toUpperCase() + value.slice(1),
                      }))}
                    />
                  </div>
                </div>
                <label>Reference experiment (optional)</label>
                <Select
                  label="Reference experiment (optional)"
                  value={referenceId}
                  onChange={setReferenceId}
                  options={[
                    { value: '', label: 'No reference' },
                    ...completed.map((e) => ({
                      value: e.id,
                      label: experimentTitle(e),
                    })),
                  ]}
                />
                {referenceId && (
                  <p className="muted">
                    The saved code, results and interpretation will be included as context.
                  </p>
                )}
              </details>
              {status?.mode !== 'demo' && (
                <p className="resource-note">
                  Managed Codex · interactive checks and Slurm CPU/GPU stages
                </p>
              )}
              {error && (
                <p role="alert" className="error">
                  {error}
                </p>
              )}
              <button
                className="primary full"
                disabled={busy || !model || !currentProject?.available}
              >
                {busy
                  ? 'Submitting…'
                  : status?.mode === 'demo'
                    ? 'Prepare and run experiment'
                    : 'Plan experiment'}
                <ArrowRight size={18} />
              </button>
            </form>
          </section>
        </div>
      )}
    </div>
  );
}
function Quota({
  status,
  busy,
  onSave,
  onRefresh,
}: {
  status: Status | null;
  busy: boolean;
  onSave: (
    fiveHour: number,
    weekly: number,
  ) => Promise<{ state: string; request_id: string; error?: string } | null>;
  onRefresh: () => Promise<void>;
}) {
  const [draft, setDraft] = useState<{ fiveHour: number; weekly: number } | null>(null);
  const [pendingId, setPendingId] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);
  const fiveHour = draft?.fiveHour ?? status?.quota_reserve?.five_hour_percent ?? 25;
  const weekly = draft?.weekly ?? status?.quota_reserve?.weekly_percent ?? 10;
  const savedFiveHour = status?.quota_reserve?.five_hour_percent ?? 25;
  const savedWeekly = status?.quota_reserve?.weekly_percent ?? 10;
  const changed = fiveHour !== savedFiveHour || weekly !== savedWeekly;
  useEffect(() => {
    if (pendingId && draft?.fiveHour === savedFiveHour && draft.weekly === savedWeekly) {
      setPendingId(null);
      setDraft(null);
    }
  }, [pendingId, draft, savedFiveHour, savedWeekly]);
  useEffect(() => {
    if (!pendingId) return;
    const poll = async () => {
      try {
        const receipt = await api<{ state: string; error?: string }>(`/api/commands/${pendingId}`);
        if (receipt.state === 'rejected') {
          setSaveError(receipt.error || 'The executor rejected the reserve change.');
          setPendingId(null);
        } else if (receipt.state === 'applied') {
          await onRefresh();
        }
      } catch {
        // Keep the saved command visible while the gateway or executor is unavailable.
      }
    };
    void poll();
    const timer = setInterval(() => void poll(), 5000);
    return () => clearInterval(timer);
  }, [pendingId, onRefresh]);
  return (
    <div className="quota">
      {[
        { key: '300', label: 'Five-hour window', reserve: fiveHour, field: 'fiveHour' as const },
        { key: '10080', label: 'Weekly window', reserve: weekly, field: 'weekly' as const },
      ].map(({ key, label, reserve, field }) => {
        const value = status?.quota.remaining[String(key)];
        return (
          <div key={key}>
            <div className="quota-label">
              <span>{label}</span>
              <strong>{value === undefined ? 'Unknown' : `${value.toFixed(0)}% remaining`}</strong>
            </div>
            <div className="quota-track">
              <progress value={value ?? 0} max={100} aria-label={`${label} remaining`} />
              <input
                type="range"
                min="0"
                max="100"
                step="1"
                value={reserve}
                disabled={Boolean(pendingId)}
                aria-label={`${label} subscription reserve`}
                aria-valuetext={`${reserve}% reserve`}
                onChange={(event) =>
                  setDraft({
                    fiveHour,
                    weekly,
                    [field]: Math.min(90, Number(event.target.value)),
                  })
                }
              />
            </div>
            <small>
              {reserve}% reserve · resume above {Math.min(100, reserve + 5)}%
            </small>
          </div>
        );
      })}
      {changed && (
        <div className="quota-actions">
          <button
            className="primary"
            disabled={busy || Boolean(pendingId)}
            onClick={() =>
              void onSave(fiveHour, weekly).then((receipt) => {
                if (receipt?.state === 'rejected')
                  setSaveError(receipt.error || 'The executor rejected the reserve change.');
                else if (receipt?.request_id) {
                  setPendingId(receipt.request_id);
                  setSaveError(null);
                }
              })
            }
          >
            Save reserve
          </button>
          <button disabled={busy || Boolean(pendingId)} onClick={() => setDraft(null)}>
            Reset
          </button>
        </div>
      )}
      {pendingId && <p className="small muted">Reserve change saved; waiting for executor.</p>}
      {saveError && (
        <p className="error" role="alert">
          {saveError}
        </p>
      )}
      <p className="small muted">
        {status?.quota.admitted
          ? 'Automatic turns admitted'
          : status?.quota.reason || 'Quota not observed'}{' '}
        · {age(status?.quota.observed_at || null)}
      </p>
    </div>
  );
}
function holdTitle(reason: string) {
  const titles: Record<string, string> = {
    model_unavailable: 'Selected model unavailable',
    quota_hold: 'Waiting for subscription quota',
    agent_paused: 'Agent work paused',
    new_compute_paused: 'New compute paused',
    accounting_pending: 'Confirming the job outcome',
    submission_uncertain: 'Checking whether the job was submitted',
    agent_dispatch_uncertain: 'Checking whether agent work started',
    waiting_for_executor: 'Waiting for the executor',
    agent_session_unavailable: 'Experiment conversation unavailable',
    agent_busy: 'Waiting for agent availability',
    compute_busy: 'Waiting for a compute slot',
    project_busy: 'Waiting for this project’s working folder',
    workspace_unavailable: 'Working folder unavailable',
    project_unavailable: 'Project folder unavailable',
    plan_review: 'Your plan review is needed',
    publication_pending: 'Publishing experiment evidence',
    unsupported_request: 'This brief needs different capabilities',
    agent_correction_stalled: 'Agent corrections need your guidance',
    agent_budget_exhausted: 'Agent work reached its configured limit',
  };
  if (reason.startsWith('steering_')) return 'Applying your correction';
  if (reason.startsWith('automatic_repair')) return 'Repairing the experiment';
  return titles[reason] || 'Experiment waiting to continue';
}
function holdGuidance(reason: string) {
  if (reason.includes('uncertain'))
    return 'Do not resubmit. The saved submission or turn must be checked before work can continue.';
  if (reason === 'plan_review') return 'Review the proposed plan below to continue.';
  if (reason === 'agent_paused' || reason === 'new_compute_paused')
    return 'Use the matching Resume control below when you are ready.';
  if (reason === 'project_unavailable' || reason === 'workspace_unavailable')
    return 'Check this project’s folder and Git setup in Project settings.';
  if (reason === 'agent_session_unavailable')
    return 'Restore access to the saved conversation, then use Retry with agent below.';
  if (reason === 'agent_correction_stalled')
    return 'Provide a correction below or use Retry with agent when the cause is resolved.';
  if (reason === 'model_unavailable')
    return 'Check the managed runtime model catalog before creating a replacement experiment.';
  if (reason === 'agent_budget_exhausted')
    return 'Review the configured agent limit before requesting more work.';
  if (reason === 'unsupported_request')
    return 'Review the recorded limitation and revise the brief or project inputs.';
  if (
    [
      'quota_hold',
      'agent_busy',
      'compute_busy',
      'project_busy',
      'accounting_pending',
      'waiting_for_executor',
      'publication_pending',
      'agent_continuation',
      'agent_prerequisite',
      'automatic_repair_pending',
      'automatic_repair',
    ].includes(reason) ||
    reason.startsWith('steering_')
  )
    return 'Your work is saved. Progress updates automatically; no resubmission is needed.';
  return 'Review the recorded reason above before taking further action.';
}
function holdExplanation(reason: string) {
  const explanations: Record<string, string> = {
    model_unavailable:
      'The chosen model or reasoning effort is unavailable in the managed runtime. No alternative model was used.',
    quota_hold:
      'Agent work is waiting for a fresh quota snapshot above the reserve. Compute observation continues.',
    agent_paused: 'Agent work is paused. Running compute continues.',
    new_compute_paused: 'New submissions are paused. Existing scheduler jobs continue.',
    automatic_repair_pending:
      'Preparing automatic repair after the saved jobs and turns are reconciled.',
    automatic_repair: 'Repairing the working implementation and reusing checked successful stages.',
    agent_continuation:
      'Agent continuation queued; previous work is saved. It resumes automatically when quota and the agent slot are available.',
    agent_prerequisite:
      'Waiting to retry an agent prerequisite automatically. No turn has started.',
    agent_correction_stalled:
      'The same correction failed repeatedly. Retry with agent or provide guidance.',
    agent_budget_exhausted: 'The configured agent turn budget is exhausted.',
    accounting_pending: 'The job left the queue; terminal accounting has not arrived yet.',
    submission_uncertain: 'Submission needs reconciliation before any retry.',
    waiting_for_executor: 'Your brief is saved and waiting for the executor.',
    agent_session_unavailable:
      'Codex could not resume this conversation. Unarchive it if needed, then select Retry with agent. No turn was started; other experiments continue.',
    agent_busy: 'Waiting for the current Codex turn or its saved outcome to be reconciled.',
    compute_busy: 'Waiting for a free executor job slot. Other experiments continue.',
    project_busy: 'Implementation is waiting for active experiments in this project to finish.',
    workspace_unavailable:
      'The executor could not create or verify the separate working folder. See the recorded reason.',
    plan_review: 'Review the proposed plan to continue.',
    steering_restart:
      'Your correction is saved. The executor is resolving current work before starting the replacement plan.',
    steering_waiting_for_agent:
      'Waiting for the saved agent turn to finish or be reconciled before restarting.',
    steering_waiting_for_job_identity:
      'The submission outcome must be reconciled before its job can be cancelled.',
    steering_cancelling_job:
      'Cancellation was requested for the recorded job. Waiting for terminal scheduler accounting.',
    steering_cancellation_uncertain:
      'Cancellation could not be confirmed. The executor checks the exact job identity before another attempt.',
    project_unavailable:
      'The saved project folder is missing or changed. Restore the original folder to continue safely.',
    cancellation_pending:
      'Cancellation is saved. Pending transfers are removed; the executor is stopping recorded work.',
    cancellation_unconfirmed:
      'Pending transfers are removed. A recorded job or agent turn still needs its stopped outcome confirmed.',
    publication_pending: 'Evidence is ready; required artifact publication is still pending.',
    unsupported_request:
      'The available recipe does not satisfy this brief. No compute was launched for this request.',
  };
  return explanations[reason] || 'The workflow is held at its recorded stage.';
}

createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
if ('serviceWorker' in navigator) navigator.serviceWorker.register('/sw.js').catch(() => {});
