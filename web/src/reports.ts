import type { Artifact, Experiment } from './types';

export function availableReports(exp: Experiment): (Artifact & { version: number })[] {
  const original = exp.artifacts.find((a) => a.name === 'report.html');
  const originalReady =
    exp.phase === 'complete' ||
    exp.report_versions?.some((r) => r.version === 1 && r.state === 'ready');
  const options = original && originalReady ? [{ ...original, version: 1 }] : [];
  for (const r of exp.report_versions || []) {
    if (r.state !== 'ready' || r.version === 1) continue;
    const file = exp.artifacts.find(
      (a) => a.id === r.report_artifact_id && a.name === `report-v${r.version}.html`,
    );
    if (file && r.artifact_ids.every((id) => exp.artifacts.some((a) => a.id === id)))
      options.push({ ...file, version: r.version });
  }
  return options.sort((a, b) => a.version - b.version);
}
export function currentReport(exp: Experiment, choice: string | null = null): Artifact | undefined {
  const options = availableReports(exp);
  return (
    options.find((a) => a.id === choice) ||
    options.find((a) => a.id === exp.active_report_id) ||
    options.at(-1)
  );
}
