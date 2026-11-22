import { ChevronRight, Menu, Moon, RefreshCw, Sun } from 'lucide-react';
import type { ViewProps } from './props';

export function Topbar(
  props: Pick<
    ViewProps,
    | 'changePreference'
    | 'currentProject'
    | 'exp'
    | 'page'
    | 'projectId'
    | 'reload'
    | 'setNavOpen'
    | 'status'
    | 'theme'
  >,
) {
  const {
    changePreference,
    currentProject,
    exp,
    page,
    projectId,
    reload,
    setNavOpen,
    status,
    theme,
  } = props;
  return (
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
  );
}
