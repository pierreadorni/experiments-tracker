import { useEffect, useRef, type RefObject } from 'react';

const locks = new Map<HTMLElement, { count: number; previous: boolean }>();
let scrollLocks = 0;
let previousOverflow = '';

// Used by phone sheets: keep focus in the open surface and restore it on dismissal.
export function useDialog(ref: RefObject<HTMLElement | null>, active: boolean, close: () => void) {
  const dismiss = useRef(close);
  dismiss.current = close;
  useEffect(() => {
    const dialog = ref.current;
    if (!active || !dialog) return;
    const previous = document.activeElement as HTMLElement | null;
    if (!scrollLocks) previousOverflow = document.body.style.overflow;
    scrollLocks += 1;
    const background: HTMLElement[] = [];
    let node: HTMLElement = dialog;
    while (node.parentElement && node !== document.body) {
      for (const sibling of node.parentElement.children) {
        if (
          sibling === node ||
          !(sibling instanceof HTMLElement) ||
          sibling.hasAttribute('data-overlay-backdrop')
        )
          continue;
        const lock = locks.get(sibling) || { count: 0, previous: sibling.inert };
        lock.count += 1;
        locks.set(sibling, lock);
        background.push(sibling);
        sibling.inert = true;
      }
      node = node.parentElement;
    }
    document.body.style.overflow = 'hidden';
    dialog.querySelector<HTMLElement>('textarea, button:not(:disabled), input')?.focus();
    const key = (event: KeyboardEvent) => {
      if (event.key === 'Escape') dismiss.current();
      if (event.key !== 'Tab') return;
      const controls = [
        ...dialog.querySelectorAll<HTMLElement>(
          'button:not(:disabled):not([tabindex="-1"]), textarea, input, a[href], summary',
        ),
      ].filter((el) => el.getClientRects().length);
      const first = controls[0],
        last = controls.at(-1);
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last?.focus();
      }
      if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first?.focus();
      }
    };
    document.addEventListener('keydown', key);
    return () => {
      document.removeEventListener('keydown', key);
      background.forEach((el) => {
        const lock = locks.get(el)!;
        lock.count -= 1;
        if (!lock.count) {
          el.inert = lock.previous;
          locks.delete(el);
        }
      });
      scrollLocks -= 1;
      if (!scrollLocks) document.body.style.overflow = previousOverflow;
      if (previous?.isConnected) previous.focus();
    };
  }, [active, ref]);
}
