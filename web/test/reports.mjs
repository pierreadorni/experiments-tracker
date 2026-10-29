import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { unzipSync, strFromU8 } from 'fflate';
import { currentReport, availableReports } from '../src/reports.ts';
import { evidenceBundle } from '../src/evidence.ts';
const first = '00000000-0000-4000-8000-000000000001',
  second = '00000000-0000-4000-8000-000000000002';
const oldBytes = new TextEncoder().encode('<html>original</html>'),
  newBytes = new TextEncoder().encode('<html>new report</html>');
const artifact = (id, name, bytes) => ({
  id,
  name,
  size: bytes.length,
  sha256: createHash('sha256').update(bytes).digest('hex'),
});
const exp = {
  id: 'example',
  schema_version: 1,
  phase: 'complete',
  revision: 2,
  evidence_revision: 1,
  recipe_id: 'toy_compare_v1',
  artifacts: [
    artifact(first, 'report.html', oldBytes),
    artifact(second, 'report-v2.html', newBytes),
  ],
  report_versions: [
    { version: 2, state: 'publishing', artifact_ids: [second], report_artifact_id: second },
  ],
  active_report_id: null,
};
test('pending versions retain original; ready versions become default and allow original selection', () => {
  assert.equal(currentReport(exp).id, first);
  const ready = {
    ...exp,
    active_report_id: second,
    report_versions: [{ ...exp.report_versions[0], state: 'ready' }],
  };
  assert.equal(currentReport(ready).id, second);
  assert.equal(currentReport(ready, first).id, first);
  assert.deepEqual(
    availableReports(ready).map((r) => r.version),
    [1, 2],
  );
  assert.equal(
    currentReport({
      ...ready,
      report_versions: [{ ...ready.report_versions[0], artifact_ids: [second, 'missing'] }],
    }).id,
    first,
  );
});
test('offline bundle opens current report while preserving original bytes and selected identity', async () => {
  const ready = {
    ...exp,
    active_report_id: second,
    report_versions: [{ ...exp.report_versions[0], state: 'ready' }],
  };
  const files = unzipSync(
    await evidenceBundle(ready, async (a) => (a.id === first ? oldBytes : newBytes)),
  );
  assert.equal(strFromU8(files['report.html']), '<html>new report</html>');
  assert.deepEqual(files['evidence/report.html'], oldBytes);
  assert.equal(JSON.parse(strFromU8(files['checksums.json'])).selected_report_artifact_id, second);
});
