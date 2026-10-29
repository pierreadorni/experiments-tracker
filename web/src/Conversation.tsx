import { useEffect, useRef, useState } from 'react';
import { ArrowUp, LoaderCircle, MessageSquare, X } from 'lucide-react';
import { api, command } from './api';
import type { Discussion, Experiment, Status } from './types';
import { Select } from './Select';
import { useDialog } from './useDialog';

const waiting: Record<string, string> = {
  quota_hold: 'Waiting for Codex usage to recover above the reserve.',
  agent_paused: 'Agent work is paused. Resume it in Progress to continue.',
  agent_busy: 'Queued behind another Codex turn or a turn that needs reconciliation.',
  agent_unavailable: 'Waiting for Codex to reconnect.',
  report_publication: 'Waiting for the current report revision to publish.',
  needs_attention: 'Waiting for your answer to the Codex request.',
  agent_dispatch_uncertain: 'This turn needs reconciliation before the conversation can continue.',
};

export function Conversation({
  exp,
  status,
  reload,
  close,
  openReport,
  openExperiment,
  open,
}: {
  exp: Experiment;
  status: Status | null;
  reload: () => Promise<void>;
  close: () => void;
  openReport: (version: number) => void;
  openExperiment: (id: string) => void;
  open: boolean;
}) {
  const [draft, setDraft] = useState('');
  const panel = useRef<HTMLElement>(null);
  const [sheet, setSheet] = useState(() => window.matchMedia('(max-width: 1359px)').matches);
  useEffect(() => {
    const query = window.matchMedia('(max-width: 1359px)');
    const update = (event: MediaQueryListEvent) => setSheet(event.matches);
    query.addEventListener('change', update);
    return () => query.removeEventListener('change', update);
  }, []);
  useDialog(panel, open && sheet, close);
  const [model, setModel] = useState('');
  const [effort, setEffort] = useState('medium');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [older, setOlder] = useState<Discussion[]>([]);
  const [pending, setPending] = useState<{ id: string; text: string; created_at: number } | null>(
    null,
  );
  const [hasMore, setHasMore] = useState(true);
  const scroll = useRef<HTMLDivElement>(null);
  const nearBottom = useRef(true);
  const input = useRef<HTMLTextAreaElement>(null);
  const messages = [
    ...new Map([...older, ...(exp.conversation || [])].map((m) => [m.id, m])).values(),
  ].sort((a, b) => a.seq - b.seq);
  const queued = [
    ...new Map(
      [...(exp.pending_messages || []), ...(pending ? [pending] : [])]
        .filter((m) => !messages.some((saved) => saved.id === m.id))
        .map((m) => [m.id, m]),
    ).values(),
  ];
  const catalog = status?.models || [];
  const option = catalog.find((m) => m.id === model);
  useEffect(() => {
    if (model || !catalog.length) return;
    const choice =
      catalog.find((m) => m.id === 'gpt-6.1-sol') ||
      catalog.find((m) => m.id === status?.default_model) ||
      catalog[0];
    setModel(choice.id);
    setEffort(choice.reasoning_efforts.includes('medium') ? 'medium' : choice.default_effort);
  }, [catalog, model, status?.default_model]);
  useEffect(() => {
    if (nearBottom.current && scroll.current)
      scroll.current.scrollTop = scroll.current.scrollHeight;
  }, [exp.conversation, exp.pending_messages, pending]);
  useEffect(() => {
    if (!pending) return;
    if (messages.some((m) => m.id === pending.id)) {
      setPending(null);
      return;
    }
    let active = true;
    void api<{ state: string; error?: string }>(`/api/commands/${pending.id}`)
      .then((receipt) => {
        if (active && receipt.state === 'rejected') {
          setError(receipt.error || 'Message could not be applied.');
          setDraft(pending.text);
          setPending(null);
        }
      })
      .catch(() => {});
    return () => {
      active = false;
    };
  }, [pending, exp.conversation, exp.pending_commands]);
  const send = async () => {
    if (!draft.trim() || busy) return;
    setBusy(true);
    setError(null);
    const text = draft;
    try {
      const result = await command<{ request_id: string; state: string }>(
        `/api/experiments/${exp.id}/conversation`,
        { text, model, reasoning_effort: effort },
      );
      setPending({ id: result.request_id, text, created_at: Date.now() / 1000 });
      setDraft('');
      nearBottom.current = true;
      await reload();
      input.current?.focus();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };
  const loadOlder = async () => {
    setBusy(true);
    setError(null);
    try {
      const result = await api<{ messages: Discussion[]; has_more: boolean }>(
        `/api/experiments/${exp.id}/conversation?before=${messages[0]?.seq || Number.MAX_SAFE_INTEGER}`,
      );
      nearBottom.current = false;
      setOlder((list) => [...result.messages, ...list]);
      setHasMore(result.has_more);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <>
      {open && sheet && (
        <button
          data-overlay-backdrop
          className="discussion-backdrop"
          aria-label="Close discussion"
          onClick={close}
        />
      )}
      <aside
        ref={panel}
        role={sheet ? 'dialog' : 'complementary'}
        aria-modal={sheet && open ? true : undefined}
        className={`conversation-panel panel ${open ? '' : 'closed'}`}
        aria-label="Discuss with Codex"
        onKeyDown={(e) => {
          if (e.key === 'Escape') close();
        }}
      >
        <div className="conversation-heading">
          <h2>
            <MessageSquare size={18} /> Discuss with Codex
          </h2>
          <button className="icon-button" aria-label="Close discussion" onClick={close}>
            <X size={18} />
          </button>
        </div>
        <div
          className="conversation-history"
          ref={scroll}
          onScroll={() => {
            const el = scroll.current!;
            nearBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < 80;
          }}
        >
          {hasMore && (messages[0]?.seq || 0) > 1 && (
            <button className="text-button" disabled={busy} onClick={() => void loadOlder()}>
              Load earlier messages
            </button>
          )}
          {!messages.length && !queued.length && (
            <div className="conversation-empty">
              <p>Ask about findings, interpretations, next steps, or changes to this report.</p>
              <button
                onClick={() => {
                  setDraft('What does this result establish, and what remains uncertain?');
                  input.current?.focus();
                }}
              >
                What does this establish?
              </button>
              <button
                onClick={() => {
                  setDraft('What would be the most useful next experiment?');
                  input.current?.focus();
                }}
              >
                Suggest a follow-up experiment
              </button>
            </div>
          )}
          {messages.map((message) => {
            const report = exp.report_versions?.find((r) => r.version === message.report_version);
            return (
              <article className="discussion-exchange" key={message.id}>
                <div className="discussion-user">
                  <span className="discussion-author">You</span>
                  <p>{message.text}</p>
                </div>
                <div className="discussion-assistant">
                  <span className="discussion-author">Codex</span>
                  {message.reply ? (
                    <p className="discussion-reply">{message.reply}</p>
                  ) : (
                    <div className="discussion-status" role="status">
                      <span>
                        {message.state === 'running' && (
                          <LoaderCircle className="spinner" size={17} />
                        )}
                        {message.state === 'running'
                          ? 'Codex is working…'
                          : message.state === 'failed'
                            ? 'Reply could not be completed'
                            : waiting[message.hold_reason || ''] ||
                              'Message saved · waiting for Codex'}
                      </span>
                      {message.state === 'running' && !!message.activity.length && (
                        <p
                          className="discussion-current-activity"
                          aria-live="polite"
                          aria-atomic="true"
                        >
                          {message.activity.at(-1)?.summary}
                        </p>
                      )}
                    </div>
                  )}
                  {!!message.activity.length && (
                    <details className="discussion-activity">
                      <summary>Activity · {message.activity.length} updates</summary>
                      <ol>
                        {message.activity.map((item, i) => (
                          <li key={i} className={item.state === 'failed' ? 'error' : undefined}>
                            {item.summary}
                          </li>
                        ))}
                      </ol>
                    </details>
                  )}
                  {message.error && <p className="error small">{message.error}</p>}
                  {!!(message.evidence_ids.length || message.artifact_ids?.length) && (
                    <div className="discussion-sources">
                      {[...new Set([...message.evidence_ids, ...(message.artifact_ids || [])])].map(
                        (id) => {
                          const a = exp.artifacts.find((a) => a.id === id);
                          return (
                            a && (
                              <a
                                key={id}
                                href={`/api/artifacts/${id}`}
                                target="_blank"
                                rel="noreferrer"
                              >
                                {a.name}
                              </a>
                            )
                          );
                        },
                      )}
                    </div>
                  )}
                  {message.followup_experiment_id && (
                    <button
                      className="text-button"
                      onClick={() => openExperiment(message.followup_experiment_id!)}
                    >
                      Open follow-up experiment
                    </button>
                  )}
                  {report &&
                    (report.state === 'ready' ? (
                      <button className="text-button" onClick={() => openReport(report.version)}>
                        Open report · version {report.version}
                      </button>
                    ) : (
                      <p className="small muted">
                        Report version {report.version}:{' '}
                        {report.state === 'failed' ? report.error : 'publishing…'}
                      </p>
                    ))}
                </div>
              </article>
            );
          })}
          {queued.map((m) => (
            <article className="discussion-exchange" key={m.id}>
              <div className="discussion-user">
                <span className="discussion-author">You</span>
                <p>{m.text}</p>
              </div>
              <div className="discussion-status" role="status">
                Message saved · waiting for executor
              </div>
            </article>
          ))}
        </div>
        <form
          className="conversation-composer"
          onSubmit={(e) => {
            e.preventDefault();
            void send();
          }}
        >
          <div className="conversation-model-fields">
            <Select
              label="Discussion model"
              value={model}
              placement="up"
              disabled={!catalog.length}
              options={catalog.map((m) => ({ value: m.id, label: m.name }))}
              onChange={(value) => {
                const m = catalog.find((m) => m.id === value)!;
                setModel(m.id);
                setEffort(m.reasoning_efforts.includes('medium') ? 'medium' : m.default_effort);
              }}
            />
            <Select
              label="Discussion reasoning effort"
              value={effort}
              placement="up"
              disabled={!option}
              options={(option?.reasoning_efforts || []).map((value) => ({
                value,
                label: value.charAt(0).toUpperCase() + value.slice(1),
              }))}
              onChange={setEffort}
            />
          </div>
          {error && (
            <p className="error small" role="alert">
              {error}
            </p>
          )}
          <div className="conversation-input">
            <textarea
              ref={input}
              aria-label="Message Codex"
              placeholder="Ask about these results or request a plot…"
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              rows={3}
              maxLength={4000}
              disabled={busy}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
                  e.preventDefault();
                  void send();
                }
              }}
            />
            <button
              className="primary"
              aria-label="Send message"
              disabled={busy || !draft.trim() || !model}
            >
              <ArrowUp size={18} />
            </button>
          </div>
        </form>
      </aside>
    </>
  );
}
