import { ExecutorUpdate } from '../ExecutorUpdate';
import { api } from '../api';
import { send } from '../relay';
import { age } from '../format';
import { Status } from '../types';
import { useEffect, useState } from 'react';
import type { ViewProps } from './props';

export function Settings(
  props: Pick<ViewProps, 'act' | 'busy' | 'executorOnline' | 'reload' | 'status'>,
) {
  const { act, busy, executorOnline, reload, status } = props;
  return (
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
          {status?.mode === 'demo' && (
            <p>Synthetic CPU demonstration. No Slurm jobs or GPU allocations.</p>
          )}
          {status?.cluster && <p>Cluster snapshot · {age(status.cluster.observed_at)}</p>}
          {status?.cluster_error && <p className="error">{status.cluster_error}</p>}
          {status?.agent_error && <p className="error">{status.agent_error}</p>}
        </section>
        <section className="panel">
          <h2>Subscription reserve</h2>
          <Quota
            status={status}
            busy={busy}
            onSave={async (five_hour_percent, weekly_percent) => {
              let receipt: { state: string; request_id: string; error?: string | null } | null =
                null;
              const saved = await act(async () => {
                receipt = await send('reserve', null, {
                  five_hour_percent,
                  weekly_percent,
                });
              });
              return saved ? receipt : null;
            }}
            onRefresh={reload}
          />
        </section>
      </div>
    </>
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
  ) => Promise<{ state: string; request_id: string; error?: string | null } | null>;
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
        const receipt = await api<{ state: string; error?: string | null }>(
          `/api/commands/${pendingId}`,
        );
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
