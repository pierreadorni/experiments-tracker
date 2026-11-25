import { useRef } from 'react';
import { ArrowRight, Beaker, Check, ChevronRight, Plus, Search, X } from 'lucide-react';
import { Badge } from '../Badge';
import { Select } from '../Select';
import { date, experimentTitle, terminal } from '../format';
import { leadRunId } from '../lead';
import { experimentStatus } from '../progress';
import { usePendingCommands } from '../relay';
import type { Experiment } from '../types';
import type { ViewProps } from './props';

const filters = ['All', 'Active', 'Complete', 'Attention', 'Cancelled'] as const;
const sorts = ['Newest first', 'Oldest first', 'Last updated', 'Title (A–Z)'] as const;
export type ExperimentFilter = (typeof filters)[number];
export type ExperimentSort = (typeof sorts)[number];

const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });
const waiting = 'Waiting for the executor';

type Row = {
  id: string;
  title: string;
  createdAt: number;
  updatedAt: number;
  experiment?: Experiment;
};

function matchesStatus(row: Row, filter: ExperimentFilter) {
  const { experiment } = row;
  if (filter === 'All') return true;
  if (!experiment) return filter === 'Active';
  switch (filter) {
    case 'Active':
      return !terminal.includes(experiment.phase);
    case 'Complete':
      return experiment.phase === 'complete';
    case 'Cancelled':
      return experiment.phase === 'cancelled';
    case 'Attention':
      return (
        experiment.phase === 'failed' ||
        experiment.plan_review?.state === 'awaiting_review' ||
        [
          'needs_attention',
          'agent_dispatch_uncertain',
          'submission_uncertain',
          'agent_unavailable',
          'agent_session_unavailable',
          'cancellation_unconfirmed',
        ].includes(experiment.hold_reason || '')
      );
  }
}

function matchesSearch(row: Row, terms: string[]) {
  if (terms.length === 0) return true;
  const { experiment } = row;
  const text = (
    experiment
      ? [
          row.title,
          experiment.brief,
          experiment.analysis?.title,
          experiment.analysis?.summary,
          experimentStatus(experiment),
          experiment.id,
        ].join(' ')
      : `${row.title} ${waiting}`
  ).toLowerCase();
  return terms.every((term) => text.includes(term));
}

function compareRows(a: Row, b: Row, sort: ExperimentSort) {
  const newest = b.createdAt - a.createdAt || a.id.localeCompare(b.id);
  switch (sort) {
    case 'Oldest first':
      return a.createdAt - b.createdAt || newest;
    case 'Last updated':
      return b.updatedAt - a.updatedAt || newest;
    case 'Title (A–Z)':
      return collator.compare(a.title, b.title) || newest;
    default:
      return newest;
  }
}

type ListProps = Pick<ViewProps, 'choose' | 'experiments' | 'projectId' | 'setCreating'> & {
  filter: ExperimentFilter;
  search: string;
  sort: ExperimentSort;
  setFilter: (value: ExperimentFilter) => void;
  setSearch: (value: string) => void;
  setSort: (value: ExperimentSort) => void;
};

export function ExperimentList({
  choose,
  experiments,
  filter,
  projectId,
  search,
  setCreating,
  setFilter,
  setSearch,
  setSort,
  sort,
}: ListProps) {
  const searchInput = useRef<HTMLInputElement>(null);
  // Hosted creates appear as waiting rows until the executor applies them.
  const creates = usePendingCommands().filter(
    (c) => c.kind === 'create' && (!c.payload?.project_id || c.payload.project_id === projectId),
  );
  const rows: Row[] = [
    ...experiments.map((experiment) => ({
      id: experiment.id,
      title: experimentTitle(experiment),
      createdAt: experiment.created_at,
      updatedAt: experiment.updated_at ?? experiment.created_at,
      experiment,
    })),
    ...creates.map((command) => ({
      id: command.request_id,
      title: typeof command.payload?.brief === 'string' ? command.payload.brief : 'New experiment',
      createdAt: command.received_at,
      updatedAt: command.received_at,
    })),
  ];
  const terms = search.toLowerCase().match(/\S+/g) || [];
  const isFiltered = filter !== 'All' || terms.length > 0;
  const visible = rows
    .filter((row) => matchesStatus(row, filter) && matchesSearch(row, terms))
    .sort((a, b) => compareRows(a, b, sort));
  const resultCount = isFiltered ? `${visible.length} of ${rows.length}` : rows.length;

  function clearSearch() {
    setSearch('');
    searchInput.current?.focus();
  }

  return (
    <>
      <div className="page-heading">
        <h1>Experiments</h1>
        <button className="primary" onClick={() => setCreating(true)}>
          <Plus size={18} />
          New experiment
        </button>
      </div>
      <div className="experiment-controls">
        <div className="experiment-control experiment-search">
          <label htmlFor="experiment-search">Search</label>
          <div className="experiment-search-field">
            <Search size={16} aria-hidden="true" />
            <input
              id="experiment-search"
              ref={searchInput}
              type="search"
              aria-label="Search experiments"
              placeholder="Search experiments"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
            />
            {search && (
              <button type="button" aria-label="Clear search" onClick={clearSearch}>
                <X size={16} aria-hidden="true" />
              </button>
            )}
          </div>
        </div>
        <div className="experiment-control">
          <span>Status</span>
          <Select
            label="Status"
            value={filter}
            options={filters.map((value) => ({ value, label: value }))}
            onChange={(value) => setFilter(value as ExperimentFilter)}
          />
        </div>
        <div className="experiment-control">
          <span>Sort by</span>
          <Select
            label="Sort by"
            value={sort}
            options={sorts.map((value) => ({ value, label: value }))}
            onChange={(value) => setSort(value as ExperimentSort)}
          />
        </div>
      </div>
      <div className="experiment-results">
        <span role="status">
          {resultCount} {rows.length === 1 ? 'experiment' : 'experiments'}
        </span>
        {isFiltered && (
          <button
            className="text-button"
            onClick={() => {
              setFilter('All');
              clearSearch();
            }}
          >
            Clear filters
          </button>
        )}
      </div>
      <div className="experiment-list">
        {visible.map(({ id, title, experiment: e, createdAt, updatedAt }) =>
          e ? (
            <button className="experiment-row" key={`experiment:${id}`} onClick={() => choose(id)}>
              <span className={`experiment-icon ${e.phase === 'complete' ? 'complete-icon' : ''}`}>
                {e.phase === 'complete' ? <Check size={20} /> : <Beaker size={20} />}
              </span>
              <div className="row-description">
                <strong>{title}</strong>
                {e.phase === 'complete' && e.analysis?.summary && (
                  <span className="row-finding">{e.analysis.summary}</span>
                )}
                <small>
                  {leadRunId(e.contract) && (
                    <>
                      Autopilot lead <span>·</span>{' '}
                    </>
                  )}
                  {Boolean(e.contract.synthetic_demo) && (
                    <>
                      Synthetic experiment <span>·</span>{' '}
                    </>
                  )}
                  {sort === 'Last updated' ? `Updated ${date(updatedAt)}` : date(createdAt)}
                </small>
              </div>
              <div className="row-status">
                <Badge exp={e} />
              </div>
              <ChevronRight size={18} />
            </button>
          ) : (
            <div className="experiment-row" key={`pending:${id}`}>
              <span className="experiment-icon">
                <Beaker size={20} />
              </span>
              <div className="row-description">
                <strong>{title}</strong>
                <small>
                  {waiting} · {date(createdAt)}
                </small>
              </div>
            </div>
          ),
        )}
        {rows.length > 0 && visible.length === 0 && (
          <div className="empty">
            <Search size={36} />
            <h3>No matching experiments.</h3>
            <p>Try a different search or status.</p>
          </div>
        )}
        {rows.length === 0 && (
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
