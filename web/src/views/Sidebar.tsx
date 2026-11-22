import { ExecutorUpdate } from '../ExecutorUpdate';
import { forgetNotificationDevice } from '../FollowExperiment';
import { Select } from '../Select';
import { api } from '../api';
import { Beaker, FileText, Inbox, Plus, Settings2, X } from 'lucide-react';
import type { ViewProps } from './props';

export function Sidebar(
  props: Pick<
    ViewProps,
    | 'act'
    | 'appearance'
    | 'busy'
    | 'executorOnline'
    | 'experiments'
    | 'mobileNavigation'
    | 'navOpen'
    | 'navRef'
    | 'page'
    | 'projectId'
    | 'projectOptions'
    | 'scopedInbox'
    | 'setAuth'
    | 'setCreating'
    | 'setCreatingProject'
    | 'setNavOpen'
    | 'setPage'
    | 'setProjectId'
    | 'setProjectSettingsOpen'
    | 'setSelected'
    | 'status'
  >,
) {
  const {
    act,
    appearance,
    busy,
    executorOnline,
    experiments,
    mobileNavigation,
    navOpen,
    navRef,
    page,
    projectId,
    projectOptions,
    scopedInbox,
    setAuth,
    setCreating,
    setCreatingProject,
    setNavOpen,
    setPage,
    setProjectId,
    setProjectSettingsOpen,
    setSelected,
    status,
  } = props;
  return (
    <>
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
    </>
  );
}
