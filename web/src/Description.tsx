import { useId, useLayoutEffect, useRef, useState } from 'react';

export function Description({
  text,
  className = '',
  lines = 4,
}: {
  text: string;
  className?: string;
  lines?: number;
}) {
  const id = useId();
  const paragraph = useRef<HTMLParagraphElement>(null);
  const [expanded, setExpanded] = useState(false);
  const [long, setLong] = useState(false);
  useLayoutEffect(() => {
    setExpanded(false);
    const node = paragraph.current;
    if (!node) return;
    const measure = () => {
      const lineHeight = parseFloat(getComputedStyle(node).lineHeight);
      setLong(node.scrollHeight > lineHeight * lines + 2);
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(node);
    return () => observer.disconnect();
  }, [text, lines]);
  return (
    <div className={`description ${className}`}>
      <p
        ref={paragraph}
        id={id}
        className={expanded ? '' : 'description-collapsed'}
        style={expanded ? undefined : { WebkitLineClamp: lines }}
      >
        {text}
      </p>
      {long && (
        <button
          type="button"
          className="text-button description-toggle"
          aria-expanded={expanded}
          aria-controls={id}
          onClick={() => setExpanded(!expanded)}
        >
          {expanded ? 'See less' : 'See more'}
        </button>
      )}
    </div>
  );
}
