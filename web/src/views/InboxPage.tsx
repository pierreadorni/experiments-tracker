import { date, pretty } from '../format';
import { Check, ChevronRight, Inbox } from 'lucide-react';
import type { ViewProps } from './props';

export function InboxPage(
  props: Pick<ViewProps, 'choose' | 'scopedInbox' | 'setPage' | 'setSelected'>,
) {
  const { choose, scopedInbox, setPage, setSelected } = props;
  return (
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
            onClick={() => {
              if (n.project_id) {
                setPage('memory');
                setSelected(null);
              } else choose(n.experiment_id);
            }}
          >
            <span className={`experiment-icon ${n.kind === 'result_ready' ? 'complete-icon' : ''}`}>
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
  );
}
