import { experimentStatus } from './progress';
import type { Experiment } from './types';

export function Badge({ exp }: { exp: Experiment }) {
  return (
    <span
      className={`badge ${exp.phase === 'complete' ? 'success' : exp.phase === 'failed' ? 'danger' : exp.hold_reason ? 'held' : 'active'}`}
    >
      <span className="dot" />
      {experimentStatus(exp)}
    </span>
  );
}
