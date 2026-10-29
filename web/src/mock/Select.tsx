import { useEffect, useId, useRef, useState, type KeyboardEvent } from 'react';
import { Check, ChevronDown } from 'lucide-react';

type Option = { value: string; label: string; description?: string };

export function Select({
  label,
  value,
  options,
  onChange,
  placement = 'down',
  className = '',
  displayValue,
}: {
  label: string;
  value: string;
  options: Option[];
  onChange: (value: string) => void;
  placement?: 'up' | 'down';
  className?: string;
  displayValue?: string;
}) {
  const id = useId();
  const root = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const selected = Math.max(
    0,
    options.findIndex((option) => option.value === value),
  );
  useEffect(() => {
    if (!open) return;
    root.current?.querySelector<HTMLButtonElement>(`[data-index="${active}"]`)?.focus();
  }, [open, active]);
  useEffect(() => {
    if (!open) return;
    const dismiss = (event: PointerEvent) => {
      if (!root.current?.contains(event.target as Node)) setOpen(false);
    };
    document.addEventListener('pointerdown', dismiss);
    return () => document.removeEventListener('pointerdown', dismiss);
  }, [open]);
  const choose = (index: number) => {
    onChange(options[index].value);
    setOpen(false);
    trigger.current?.focus();
  };
  const keyboard = (event: KeyboardEvent) => {
    if (event.key === 'Escape' && open) {
      event.preventDefault();
      event.stopPropagation();
      setOpen(false);
      trigger.current?.focus();
    } else if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      event.stopPropagation();
      setActive(
        open
          ? (active + (event.key === 'ArrowDown' ? 1 : -1) + options.length) % options.length
          : selected,
      );
      setOpen(true);
    } else if ((event.key === 'Home' || event.key === 'End') && open) {
      event.preventDefault();
      setActive(event.key === 'Home' ? 0 : options.length - 1);
    } else if (event.key === 'Tab' && open) {
      setOpen(false);
      trigger.current?.focus();
    }
  };
  return (
    <div
      ref={root}
      className={`custom-select ${className} ${open ? 'is-open' : ''}`}
      onKeyDown={keyboard}
      onBlur={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget)) setOpen(false);
      }}
    >
      <button
        ref={trigger}
        type="button"
        className="select-trigger"
        aria-label={label}
        aria-describedby={`${id}-value`}
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-controls={open ? id : undefined}
        onClick={() => {
          setActive(selected);
          setOpen(!open);
        }}
      >
        <span id={`${id}-value`}>{displayValue ?? options[selected].label}</span>
        <ChevronDown size={13} />
      </button>
      {open && (
        <div id={id} role="listbox" aria-label={label} className={`select-menu ${placement}`}>
          {options.map((option, index) => (
            <button
              key={option.value}
              type="button"
              role="option"
              aria-selected={option.value === value}
              tabIndex={index === active ? 0 : -1}
              data-index={index}
              onClick={() => choose(index)}
              className="select-option"
            >
              <span>
                <strong>{option.label}</strong>
                {option.description && <small>{option.description}</small>}
              </span>
              {option.value === value && <Check size={14} />}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
