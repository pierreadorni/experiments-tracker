// Apply the saved appearance before styles load, including on the sign-in screen.
(() => {
  let preference = 'system';
  try {
    const saved = localStorage.getItem('experimenttracker-theme');
    if (saved === 'light' || saved === 'dark') preference = saved;
  } catch {
    // Restricted storage still permits a session-only appearance preference.
  }
  const theme =
    preference === 'system'
      ? matchMedia('(prefers-color-scheme: dark)').matches
        ? 'dark'
        : 'light'
      : preference;
  document.documentElement.dataset.themePreference = preference;
  document.documentElement.dataset.theme = theme;
  document
    .querySelector('meta[name="theme-color"]')
    ?.setAttribute('content', theme === 'dark' ? '#1c1f1a' : '#faf9f6');
})();
