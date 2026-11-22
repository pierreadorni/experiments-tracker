const lastProjectKey = 'experiment-tracker:last-project';
export function readLastProject(): string {
  try {
    return localStorage.getItem(lastProjectKey) || '';
  } catch {
    // Browser storage can be unavailable; the app then falls back to the project list.
    return '';
  }
}
export function rememberProject(id: string) {
  try {
    localStorage.setItem(lastProjectKey, id);
  } catch {
    // Keep the in-memory choice when browser storage is unavailable.
  }
}
