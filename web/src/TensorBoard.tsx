import { useRef } from 'react';
import { X } from 'lucide-react';
import type { Job } from './types';

const number = (value: number) =>
  new Intl.NumberFormat(undefined, { maximumSignificantDigits: 4, notation: 'compact' }).format(
    value,
  );

export function TensorBoard({ job }: { job: Job }) {
  const imageDialog = useRef<HTMLDialogElement>(null);
  const preview = job.tensorboard;
  if (!preview) return null;
  const scalar = preview.scalar;
  const points = scalar?.points || [];
  const latest = points.at(-1);
  const minX = points[0]?.[0] || 0;
  const maxX = latest?.[0] ?? 1;
  const minY = Math.min(...points.map((point) => point[1]));
  const maxY = Math.max(...points.map((point) => point[1]));
  const coordinates = points.map(
    ([x, y]) =>
      `${12 + ((x - minX) / (maxX - minX || 1)) * 296},${118 - ((y - minY) / (maxY - minY || 1)) * 100}`,
  );
  return (
    <div className="tensorboard-previews">
      {scalar && (
        <figure>
          <figcaption>
            <strong title={scalar.tag}>{scalar.tag}</strong>
            {latest && <span>{number(latest[1])}</span>}
          </figcaption>
          {latest ? (
            <>
              <svg
                viewBox="0 0 320 140"
                role="img"
                aria-label={`${scalar.tag}, latest ${number(latest[1])} at step ${latest[0]}; range ${number(minY)} to ${number(maxY)}`}
              >
                <title>{scalar.tag} by step</title>
                <line x1="12" y1="128" x2="308" y2="128" className="curve-baseline" />
                <polyline
                  points={coordinates.join(' ')}
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2.5"
                />
                {points.length === 1 && <circle cx="12" cy="118" r="4" fill="currentColor" />}
              </svg>
              <div className="curve-labels">
                <span>Step {number(minX)}</span>
                <span>{number(maxX)}</span>
              </div>
              <p className="curve-range" title="Sampled value range">
                {number(minY)}–{number(maxY)}
              </p>
            </>
          ) : (
            <p className="muted small">Waiting for the first scalar sample.</p>
          )}
        </figure>
      )}
      {preview.image && (
        <figure>
          <figcaption>
            <strong title={preview.image.tag}>{preview.image.tag}</strong>
            <span>Step {number(preview.image.step)}</span>
          </figcaption>
          <button
            type="button"
            className="preview-image-button"
            aria-label={`Enlarge ${preview.image.tag}`}
            onClick={() => imageDialog.current?.showModal()}
          >
            <img
              src={preview.image.src}
              alt={`${preview.image.tag} at step ${preview.image.step}`}
              loading="lazy"
            />
          </button>
          <dialog
            ref={imageDialog}
            className="preview-image-dialog"
            aria-label={`${preview.image.tag} at step ${preview.image.step}`}
            onClick={(event) => {
              if (event.target === event.currentTarget) event.currentTarget.close();
            }}
          >
            <div>
              <header>
                <p>
                  {preview.image.tag} · Step {number(preview.image.step)}
                </p>
                <button
                  type="button"
                  className="icon-button"
                  aria-label="Close image preview"
                  onClick={() => imageDialog.current?.close()}
                >
                  <X size={20} />
                </button>
              </header>
              <img
                src={preview.image.src}
                alt={`${preview.image.tag} at step ${preview.image.step}`}
              />
            </div>
          </dialog>
        </figure>
      )}
      {preview.image_pending && <p className="muted small">Waiting for the first image.</p>}
      <p className="muted small tensorboard-updated">
        Sampled TensorBoard preview
        {preview.updated_at > 0 && (
          <>
            {' '}
            ·{' '}
            <time
              title={new Date(preview.updated_at * 1000).toLocaleString()}
              dateTime={new Date(preview.updated_at * 1000).toISOString()}
            >
              {new Date(preview.updated_at * 1000).toLocaleTimeString(undefined, {
                hour: '2-digit',
                minute: '2-digit',
              })}
            </time>
          </>
        )}
        {preview.limited && ' · event reading limit reached'}
      </p>
    </div>
  );
}
