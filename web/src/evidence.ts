import { strToU8, zipSync } from 'fflate';
import type { Artifact, Experiment } from './types';
import { currentReport } from './reports.ts';

const maxArtifactBytes = 10 * 1024 * 1024;
const maxBundleBytes = 128 * 1024 * 1024;

export async function checkedBytes(artifact: Artifact, bytes: Uint8Array): Promise<Uint8Array> {
  if (bytes.length !== artifact.size || bytes.length > maxArtifactBytes) {
    throw new Error(`Size mismatch for ${artifact.name}`);
  }
  const digest = await crypto.subtle.digest('SHA-256', new Uint8Array(bytes).buffer);
  const actual = [...new Uint8Array(digest)].map((n) => n.toString(16).padStart(2, '0')).join('');
  if (actual !== artifact.sha256) throw new Error(`Hash mismatch for ${artifact.name}`);
  return bytes;
}

export async function fetchEvidence(artifact: Artifact): Promise<Uint8Array> {
  const response = await fetch(`/api/artifacts/${artifact.id}`, {
    credentials: 'same-origin',
    signal: AbortSignal.timeout(30000),
    cache: 'no-store',
  });
  if (!response.ok || !response.body)
    throw new Error(`Unable to read ${artifact.name} (${response.status})`);
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.length;
      if (total > maxArtifactBytes) throw new Error(`Artifact exceeds 10 MiB: ${artifact.name}`);
      chunks.push(value);
    }
  } finally {
    await reader.cancel();
    reader.releaseLock();
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  return checkedBytes(artifact, bytes);
}

export function portableReport(report: string, artifacts: Artifact[]): string {
  const byId = new Map(artifacts.map((a) => [a.id, a.name]));
  return report.replace(
    /\b(href|src)=(['"])\/api\/artifacts\/([0-9a-f-]{36})\2/gi,
    (_match, attribute: string, quote: string, id: string) => {
      const name = byId.get(id);
      if (!name || !/^[a-zA-Z0-9_][a-zA-Z0-9._-]*(?:\/[a-zA-Z0-9_][a-zA-Z0-9._-]*)*$/.test(name))
        throw new Error('Report cites missing evidence');
      return `${attribute}=${quote}evidence/${name}${quote}`;
    },
  );
}

export async function evidenceBundle(
  exp: Experiment,
  fetcher = fetchEvidence,
): Promise<Uint8Array> {
  if (exp.phase !== 'complete')
    throw new Error('The evidence bundle is available after publication');
  if (exp.artifacts.reduce((n, a) => n + a.size, 0) > maxBundleBytes)
    throw new Error('Bundle exceeds 128 MiB');
  const iteration = exp.contract?.execution_contract === 3;
  const archiveName = (a: Artifact) => (iteration ? `${a.id}/${a.name}` : a.name);
  const files: Record<string, Uint8Array> = Object.create(null);
  for (const artifact of exp.artifacts) {
    if (
      !/^[a-zA-Z0-9_][a-zA-Z0-9._-]*(?:\/[a-zA-Z0-9_][a-zA-Z0-9._-]*)*$/.test(artifact.name) ||
      files[`evidence/${archiveName(artifact)}`]
    ) {
      throw new Error('Invalid or duplicate artifact filename');
    }
    files[`evidence/${archiveName(artifact)}`] = await checkedBytes(
      artifact,
      await fetcher(artifact),
    );
  }
  const report = currentReport(exp);
  if (!report || !files[`evidence/${archiveName(report)}`])
    throw new Error('Published report is missing');
  files['report.html'] = strToU8(
    portableReport(
      new TextDecoder().decode(files[`evidence/${archiveName(report)}`]),
      exp.artifacts.map((a) => ({ ...a, name: archiveName(a) })),
    ),
  );
  files['experiment.json'] = strToU8(JSON.stringify(exp, null, 2));
  files['checksums.json'] = strToU8(
    JSON.stringify(
      {
        schema_version: 1,
        experiment_id: exp.id,
        revision: exp.revision,
        evidence_revision: exp.evidence_revision,
        artifacts: exp.artifacts.map((a) => ({
          path: `evidence/${archiveName(a)}`,
          sha256: a.sha256,
          size: a.size,
        })),
        selected_report_artifact_id: report.id,
        portable_report_sha256: [
          ...new Uint8Array(
            await crypto.subtle.digest('SHA-256', new Uint8Array(files['report.html']).buffer),
          ),
        ]
          .map((n) => n.toString(16).padStart(2, '0'))
          .join(''),
      },
      null,
      2,
    ),
  );
  if (iteration) {
    files['README.txt'] = strToU8(
      'ExperimentTracker working results download\n\nOpen report.html. Small published metrics, reports and figures are retained under evidence/ by object ID. This convenience archive does not promise reproducibility, include large scientific data/checkpoints, or certify scientific validity. Original producer associations and task status are in experiment.json. This is private research content.\n',
    );
    return zipSync(files, { level: 0 });
  }
  const generic = exp.recipe_id === 'agent_experiment_v1';
  const stages = generic
    ? (exp.snapshot?.plan as { stages: { id: string; script?: string | null }[] }).stages
    : ['train_linear', 'train_quadratic', 'evaluate_and_plot'].map((id) => ({ id }));
  const reproduce =
    (generic
      ? '  python -m venv .venv\n  .venv/bin/python -m pip install -r evidence/requirements.txt\n'
      : '') +
    stages
      .map((stage: { id: string; script?: string | null }) =>
        stage.script
          ? `  EXPERIMENT_STAGE=${stage.id} bash evidence/${stage.script}`
          : `  ${generic ? '.venv/bin/python' : 'python'} evidence/recipe.py ${stage.id} --output reproduction`,
      )
      .join('\n');
  files['README.txt'] = strToU8(
    'ExperimentTracker evidence bundle\n\nExtract the ZIP and open report.html. Its figure and citations use local relative paths.\nOriginal immutable artifacts remain in evidence/; checksums.json records their SHA-256 values.\nexperiment.json includes the saved protocol, provenance, interpretation, notes and timeline.\nThe portable report changes links only; it has its own checksum.\nNew runs include frozen sources and evidence/manifest.json. Reproduction requires the saved datasets, project sources/environment and CPU/GPU allocation described by the protocol. Review scripts before running them. Restore project-source files using the project_sources mapping in manifest.json.\nFor shell stages export EXPERIMENT_ROOT to your reproduction directory, EXPERIMENT_PROJECT to the restored project and EXPERIMENT_PYTHON to your environment interpreter. Create those directories first. Run stages in this order, inside the matching allocation when needed:\n' +
      reproduce +
      '\n\nExecutor-only checkpoints and intermediate data are not included in this bundle; local-output-*.json records their paths relative to the experiment root, SHA-256 and size. Archived metadata verifies identity, not remote file availability. Historical exports without frozen sources require the matching repository source.\nSee the saved protocol for data sources, validation checks and limitations. Synthetic demonstrations do not establish real-world scientific findings.\nAuthentication credentials are not added to the bundle. It contains your private research notes.\n',
  );
  return zipSync(files, { level: 0 });
}

export async function downloadEvidenceBundle(exp: Experiment): Promise<void> {
  const zip = await evidenceBundle(exp);
  const url = URL.createObjectURL(
    new Blob([new Uint8Array(zip).buffer], { type: 'application/zip' }),
  );
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = `experiment-${exp.id}.zip`;
  document.body.append(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 60000);
}
