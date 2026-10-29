import { useEffect, useLayoutEffect, useState } from 'react';

export type ThemePreference = 'system' | 'light' | 'dark';
export type Theme = 'light' | 'dark';
const storageKey = 'experimenttracker-theme';
const preferenceOf = (value: string | null | undefined): ThemePreference =>
  value === 'light' || value === 'dark' ? value : 'system';

export function useTheme() {
  const [preference, setPreference] = useState<ThemePreference>(() =>
    preferenceOf(document.documentElement.dataset.themePreference),
  );
  const [systemDark, setSystemDark] = useState(
    () => matchMedia('(prefers-color-scheme: dark)').matches,
  );
  const theme: Theme = preference === 'system' ? (systemDark ? 'dark' : 'light') : preference;

  useLayoutEffect(() => {
    document.documentElement.dataset.themePreference = preference;
    document.documentElement.dataset.theme = theme;
    document.documentElement.style.colorScheme = theme;
    document
      .querySelector('meta[name="theme-color"]')
      ?.setAttribute('content', theme === 'dark' ? '#1c1f1a' : '#faf9f6');
  }, [preference, theme]);

  useEffect(() => {
    const query = matchMedia('(prefers-color-scheme: dark)');
    const update = () => setSystemDark(query.matches);
    const sync = (event: StorageEvent) => {
      if (event.key === storageKey || event.key === null)
        setPreference(preferenceOf(event.newValue));
    };
    update();
    query.addEventListener('change', update);
    window.addEventListener('storage', sync);
    return () => {
      query.removeEventListener('change', update);
      window.removeEventListener('storage', sync);
    };
  }, []);

  const changePreference = (value: string) => {
    const next = preferenceOf(value);
    setPreference(next);
    try {
      localStorage.setItem(storageKey, next);
    } catch {
      // Keep the in-memory choice when browser storage is unavailable.
    }
  };
  return { preference, theme, changePreference };
}
