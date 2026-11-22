import { Badge } from '../Badge';
import { date, experimentTitle } from '../format';
import { ArrowRight, Beaker, Check, ChevronRight, Plus, Search } from 'lucide-react';
import { usePendingCommands } from '../relay';
import type { ViewProps } from './props';

export function ExperimentList(
  props: Pick<
    ViewProps,
    | 'active'
    | 'attention'
    | 'choose'
    | 'completed'
    | 'experiments'
    | 'filter'
    | 'projectId'
    | 'search'
    | 'setCreating'
    | 'setFilter'
    | 'setSearch'
    | 'visibleExperiments'
  >,
) {
  const {
    active,
    attention,
    choose,
    completed,
    experiments,
    filter,
    projectId,
    search,
    setCreating,
    setFilter,
    setSearch,
    visibleExperiments,
  } = props;
  // A hosted create has no projection until the executor applies it, so the queue shows it.
  const creates = usePendingCommands().filter(
    (c) => c.kind === 'create' && (!c.payload?.project_id || c.payload.project_id === projectId),
  );
  return (
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
        {creates.map((c) => (
          <div className="experiment-row" key={c.request_id} role="status">
            <span className="experiment-icon">
              <Beaker size={20} />
            </span>
            <div className="row-description">
              <strong>
                {typeof c.payload?.brief === 'string' ? c.payload.brief : 'New experiment'}
              </strong>
              <small>Waiting for the executor · {date(c.received_at)}</small>
            </div>
          </div>
        ))}
        {visibleExperiments.map((e) => (
          <button className="experiment-row" key={e.id} onClick={() => choose(e.id)}>
            <span className={`experiment-icon ${e.phase === 'complete' ? 'complete-icon' : ''}`}>
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
        {experiments.length === 0 && creates.length === 0 && (
          <div className="empty">
            <Beaker size={36} />
            <h3>No experiments yet.</h3>
            <p>Create an experiment with a research brief, requested methods and outputs.</p>
            <button className="text-button" onClick={() => setCreating(true)}>
              Start an experiment <ArrowRight size={16} />
            </button>
          </div>
        )}
      </div>
    </>
  );
}
