import { useRef, useState } from 'react';
import { ArrowRight, FileText, Trash2 } from 'lucide-react';
import { Description } from './Description';
import type { Experiment } from './types';

export type SavedIdea = { experiment_id: string; text: string };

export function ResearchMemory({
  experiments,
  dismissed,
  busy,
  open,
  useIdea,
  remove,
}: {
  experiments: Experiment[];
  dismissed: SavedIdea[];
  busy: boolean;
  open: (id: string) => void;
  useIdea: (idea: SavedIdea) => void;
  remove: (idea: SavedIdea) => Promise<boolean>;
}) {
  const heading = useRef<HTMLHeadingElement>(null);
  const [showAll, setShowAll] = useState(false);
  const [confirm, setConfirm] = useState<SavedIdea | null>(null);
  const ideas = experiments
    .filter((e) => e.phase === 'complete')
    .flatMap((e) =>
      [...new Set(e.analysis?.next_ideas || [])]
        .filter((text) => !dismissed.some((d) => d.experiment_id === e.id && d.text === text))
        .map((text) => ({
          experiment_id: e.id,
          text,
          title: e.title || e.analysis?.title || e.brief,
        })),
    );
  return (
    <section className="panel saved-ideas" aria-labelledby="saved-ideas-heading">
      <div className="section-heading">
        <h2 ref={heading} tabIndex={-1} id="saved-ideas-heading">
          Saved next ideas <span className="muted">({ideas.length})</span>
        </h2>
      </div>
      <p className="muted">
        Turn a suggestion into an editable brief, or remove it from this list.
      </p>
      {!ideas.length && (
        <p>No saved ideas. Suggestions from completed experiments will appear here.</p>
      )}
      {(showAll ? ideas : ideas.slice(0, 5)).map((idea) => (
        <article className="next-idea" key={JSON.stringify([idea.experiment_id, idea.text])}>
          <Description text={idea.text} lines={3} />
          <div className="idea-source">
            <button className="text-button" onClick={() => open(idea.experiment_id)}>
              <FileText size={14} /> <span>{idea.title}</span>
            </button>
          </div>
          <div className="idea-actions">
            {confirm?.experiment_id === idea.experiment_id && confirm.text === idea.text ? (
              <>
                <span>Remove this idea?</span>
                <button
                  className="text-button idea-confirm"
                  disabled={busy}
                  onClick={async () => {
                    if (await remove(idea)) {
                      setConfirm(null);
                      heading.current?.focus({ preventScroll: true });
                    }
                  }}
                >
                  Confirm removal
                </button>
                <button className="text-button" disabled={busy} onClick={() => setConfirm(null)}>
                  Keep idea
                </button>
              </>
            ) : (
              <>
                <button className="text-button" onClick={() => useIdea(idea)}>
                  Use as brief <ArrowRight size={15} />
                </button>
                <button
                  className="text-button idea-remove"
                  aria-label="Remove idea"
                  disabled={busy}
                  onClick={() => setConfirm(idea)}
                >
                  <Trash2 size={14} /> Remove
                </button>
              </>
            )}
          </div>
        </article>
      ))}
      {ideas.length > 5 && (
        <button
          className="text-button ideas-disclosure"
          aria-expanded={showAll}
          onClick={() => setShowAll(!showAll)}
        >
          {showAll ? 'Show fewer ideas' : `Show all ${ideas.length} ideas`}
        </button>
      )}
    </section>
  );
}
