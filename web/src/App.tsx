import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { experimentTitle } from './format';
import { readLastProject, rememberProject } from './lastProject';
import { SessionLoading, SignIn } from './views/Login';
import { Sidebar } from './views/Sidebar';
import { Topbar } from './views/Topbar';
import { KnowledgePage } from './views/KnowledgePage';
import { ExperimentList, type ExperimentFilter, type ExperimentSort } from './views/ExperimentList';
import { ExperimentDetail } from './views/ExperimentDetail';
import { InboxPage } from './views/InboxPage';
import { Settings } from './views/Settings';
import { NewExperiment } from './views/NewExperiment';
import type { Page } from './views/props';
import { X } from 'lucide-react';
import { Unauthorized } from './api';
import {
  experimentKey,
  getProjection,
  listProjections,
  loadStatus,
  onRejection,
  refreshPending,
  send,
  usePendingCommands,
  withPending,
} from './relay';
import { contractFourCreateIssue, contractFourForProject } from './createContract.mjs';
import type { Experiment, InboxEntry, Status } from './types';
import { registerWorkspaceTools } from './webmcp';
import { currentReport, availableReports } from './reports';
import { Select } from './Select';
import { DeleteExperiment } from './DeleteExperiment';
import { CancelExperiment } from './CancelExperiment';
import { useDialog } from './useDialog';
import { NewProject } from './NewProject';
import { ProjectSettings } from './ProjectSettings';
import { useTheme } from './theme';

export function App() {
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
    () =>
      new URLSearchParams(location.search).get('project') || readLastProject() || 'test-project',
  );
  const pendingCommands = usePendingCommands();
  const experiments = useMemo(
    () =>
      withPending(
        allExperiments.filter((e) => (e.project_id || 'test-project') === projectId),
        pendingCommands,
      ),
    [allExperiments, projectId, pendingCommands],
  );
  const [status, setStatus] = useState<Status | null>(null);
  const [inbox, setInbox] = useState<InboxEntry[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const notificationLink = useRef(new URLSearchParams(location.search).get('experiment'));
  const [page, setPage] = useState<Page>(() =>
    new URLSearchParams(location.search).get('view') === 'memory' ? 'memory' : 'experiments',
  );
  const [autopilotActive, setAutopilotActive] = useState(false);
  const [tab, setTab] = useState('Progress');
  const [filter, setFilter] = useState<ExperimentFilter>('All');
  const [search, setSearch] = useState('');
  const [sort, setSort] = useState<ExperimentSort>('Newest first');
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
      const choice = models.find((m) => m.id === status?.default_model) || models[0];
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
        const value = (await listProjections<Experiment>('experiment:')).map((p) => p.document);
        setExperiments(value);
        await rendered();
        return value
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
        const e = (await getProjection<Experiment>(experimentKey(id))).document;
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
        listProjections<Experiment>('experiment:'),
        loadStatus(),
        getProjection<{ entries: InboxEntry[] }>('inbox'),
        refreshPending(),
      ]);
      setExperiments(list.map((p) => p.document));
      setStatus(stat);
      setInbox(entries.document.entries);
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
  const stageExecutionAvailable =
    exp?.contract.execution_contract === 3 && !status?.execution_contract_3_archived;
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
    if (!status) return;
    const knownProjects = status.projects || [{ id: 'test-project', name: 'Test project' }];
    if (knownProjects.some((p) => p.id === projectId)) return;
    // The remembered or URL project no longer exists (for example a deleted test-project).
    setProjectId(knownProjects[0]?.id || '');
  }, [status, projectId]);
  useEffect(() => {
    if (projectId && status) rememberProject(projectId);
  }, [projectId, status]);
  useEffect(() => {
    if (!deleting) return;
    if (!allExperiments.some((e) => e.id === deleting.eid)) {
      setSelected((id) => (id === deleting.eid ? null : id));
      setDeleting(null);
      return;
    }
    return onRejection((receipt) => {
      if (receipt.request_id !== deleting.requestId) return;
      setError(receipt.error || 'The executor rejected deletion. Check the experiment status.');
      setDeleting(null);
    });
  }, [deleting, allExperiments]);
  useEffect(
    () =>
      onRejection((receipt) => {
        // Conversation messages and deletions report their own refusal.
        if (receipt.kind !== 'discuss' && receipt.kind !== 'delete')
          setError(receipt.error || `The executor refused the ${receipt.kind} command.`);
      }),
    [],
  );
  const projects = status?.projects || [
    { id: 'test-project', name: 'Test project', available: true },
  ];
  const currentProject = projects.find((p) => p.id === projectId);
  const researchLoopProject = status?.research_loop_projects?.includes(projectId) === true;
  const contractFourCreateDefault = status?.execution_contract_create_default === 4;
  const contractFourProject = researchLoopProject || contractFourCreateDefault;
  const contractFourAvailable = contractFourForProject(status, projectId);
  const researchLoopInputError = contractFourProject
    ? !contractFourAvailable
      ? 'Contract 4 is enabled for this project, but the gateway has not recently observed an executor advertising contract-4 create support. Refresh status or ask the administrator to update the executor; no contract-3 fallback will be used.'
      : contractFourCreateIssue(status, projectId, workspaceMode, referenceId)
    : null;
  const projectOptions = [
    ...projects.map((p) => ({
      value: p.id,
      label: p.name,
      description: p.available ? undefined : 'Folder unavailable',
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
  const scopedInbox = inbox.filter(
    (entry) =>
      entry.project_id === projectId || experiments.some((e) => e.id === entry.experiment_id),
  );
  const report = exp ? currentReport(exp, reportChoice) : undefined;
  const reportOptions = exp ? availableReports(exp) : [];
  const completed = experiments.filter((e) => e.phase === 'complete');
  const executorOnline = Boolean(
    status?.last_tick && Date.now() / 1000 - status.last_tick < 60 && !offline,
  );

  if (auth === null) return <SessionLoading appearance={appearance} offline={offline} />;

  if (auth === false)
    return (
      <SignIn
        act={act}
        appearance={appearance}
        busy={busy}
        error={error}
        offline={offline}
        setAuth={setAuth}
        setToken={setToken}
        token={token}
      />
    );

  return (
    <div className="shell">
      <Sidebar
        act={act}
        appearance={appearance}
        busy={busy}
        executorOnline={executorOnline}
        experiments={experiments}
        mobileNavigation={mobileNavigation}
        navOpen={navOpen}
        navRef={navRef}
        page={page}
        projectId={projectId}
        projectOptions={projectOptions}
        scopedInbox={scopedInbox}
        setAuth={setAuth}
        setCreating={setCreating}
        setCreatingProject={setCreatingProject}
        setNavOpen={setNavOpen}
        setPage={setPage}
        setProjectId={setProjectId}
        setProjectSettingsOpen={setProjectSettingsOpen}
        setSelected={setSelected}
        status={status}
      />
      <div className="main">
        <Topbar
          changePreference={changePreference}
          currentProject={currentProject}
          exp={exp}
          page={page}
          projectId={projectId}
          reload={reload}
          setNavOpen={setNavOpen}
          status={status}
          theme={theme}
        />
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
            <KnowledgePage
              autopilotActive={autopilotActive}
              choose={choose}
              experiments={experiments}
              projectId={projectId}
              setAutopilotActive={setAutopilotActive}
              setBrief={setBrief}
              setCreating={setCreating}
              setReferenceId={setReferenceId}
              status={status}
            />
          )}
          {page === 'experiments' && !exp && (
            <ExperimentList
              choose={choose}
              experiments={experiments}
              filter={filter}
              projectId={projectId}
              search={search}
              setCreating={setCreating}
              setFilter={setFilter}
              setSearch={setSearch}
              sort={sort}
              setSort={setSort}
            />
          )}

          {page === 'experiments' && exp && (
            <ExperimentDetail
              act={act}
              answer={answer}
              busy={busy}
              choose={choose}
              compactViews={compactViews}
              conversationOpen={conversationOpen}
              deleting={deleting}
              error={error}
              exp={exp}
              experiments={experiments}
              note={note}
              reload={reload}
              report={report}
              reportOptions={reportOptions}
              setAnswer={setAnswer}
              setCancelTarget={setCancelTarget}
              setConversationOpen={setConversationOpen}
              setDeleteTarget={setDeleteTarget}
              setError={setError}
              setNote={setNote}
              setReportChoice={setReportChoice}
              setSelected={setSelected}
              setTab={setTab}
              stageExecutionAvailable={stageExecutionAvailable}
              status={status}
              tab={tab}
              theme={theme}
            />
          )}

          {page === 'inbox' && (
            <InboxPage
              choose={choose}
              scopedInbox={scopedInbox}
              setPage={setPage}
              setSelected={setSelected}
            />
          )}
          {page === 'settings' && (
            <Settings
              act={act}
              busy={busy}
              executorOnline={executorOnline}
              reload={reload}
              status={status}
            />
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
              await send('action', experimentKey(cancelTarget.id), { action: 'cancel' });
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
              const result = await send('delete', experimentKey(deleteTarget.id));
              if (result.state === 'applied') setSelected(null);
              else setDeleting({ eid: deleteTarget.id, requestId: result.request_id });
              setDeleteTarget(null);
            });
          }}
        />
      )}
      {creating && (
        <NewExperiment
          act={act}
          brief={brief}
          busy={busy}
          completed={completed}
          choose={choose}
          contractFourProject={contractFourProject}
          currentProject={currentProject}
          effort={effort}
          error={error}
          modalRef={modalRef}
          model={model}
          projectId={projectId}
          referenceId={referenceId}
          researchLoopInputError={researchLoopInputError}
          setBrief={setBrief}
          setCreating={setCreating}
          setEffort={setEffort}
          setModel={setModel}
          setReferenceId={setReferenceId}
          setStatus={setStatus}
          setWorkspaceMode={setWorkspaceMode}
          status={status}
          workspaceMode={workspaceMode}
        />
      )}
    </div>
  );
}
