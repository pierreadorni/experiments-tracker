import { useEffect, useRef, useState, type ReactNode, type RefObject } from 'react';
import { MoreHorizontal } from 'lucide-react';

export function ExperimentActions({
  children,
  triggerRef,
}: {
  children: ReactNode;
  triggerRef: RefObject<HTMLButtonElement | null>;
}) {
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    root.current?.querySelector<HTMLElement>('.action-menu button:not(:disabled)')?.focus();
    const dismiss = (event: PointerEvent) => {
      if (!root.current?.contains(event.target as Node)) setOpen(false);
    };
    document.addEventListener('pointerdown', dismiss);
    return () => document.removeEventListener('pointerdown', dismiss);
  }, [open]);
  return (
    <div
      ref={root}
      className="experiment-actions"
      onKeyDown={(event) => {
        if (event.key === 'Escape') {
          event.stopPropagation();
          setOpen(false);
          triggerRef.current?.focus();
        }
      }}
      onBlur={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget)) setOpen(false);
      }}
    >
      <button
        ref={triggerRef}
        className="icon-button"
        aria-label="Experiment actions"
        aria-expanded={open}
        onClick={() => setOpen(!open)}
      >
        <MoreHorizontal size={21} />
      </button>
      {open && (
        <div
          className="action-menu"
          onClick={(event) => {
            if (!(event.target as Element).closest('button:not(:disabled)')) return;
            setOpen(false);
            triggerRef.current?.focus();
          }}
        >
          {children}
        </div>
      )}
    </div>
  );
}
