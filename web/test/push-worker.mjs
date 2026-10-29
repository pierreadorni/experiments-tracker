import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
const code = await readFile(new URL('../public/sw.js', import.meta.url), 'utf8');
const id = '11a87d3b-7016-4f4f-9a27-6d2d54d43dad';
function worker(existing = []) {
  const listeners = {},
    notices = [],
    navigations = [];
  const client = {
    url: 'https://private.test/',
    navigate: async (url) => navigations.push(url),
    focus: async () => {},
  };
  const self = {
    location: { origin: 'https://private.test' },
    addEventListener: (event, callback) => (listeners[event] = callback),
    registration: {
      getNotifications: async () => existing,
      showNotification: async (title, options) => notices.push({ title, ...options }),
    },
    clients: { matchAll: async () => [client], openWindow: async (url) => navigations.push(url) },
  };
  vm.runInNewContext(code, { self, URL, caches: {} });
  async function push(data) {
    let pending;
    listeners.push({ data: { json: () => data }, waitUntil: (promise) => (pending = promise) });
    await pending;
  }
  async function click(data) {
    let pending,
      closed = false;
    listeners.notificationclick({
      notification: { data, close: () => (closed = true) },
      waitUntil: (promise) => (pending = promise),
    });
    await pending;
    return closed;
  }
  return { push, click, notices, navigations };
}
test('one experiment tag carries quiet stage changes and explicit alerts without private fetches', async () => {
  const w = worker();
  const data = {
    experiment_id: id,
    revision: 1,
    title: 'Synthetic experiment',
    body: 'Fit · Running',
    url: `/?project=demo&experiment=${id}`,
    alert: false,
  };
  await w.push(data);
  await w.push({ ...data, revision: 2, body: 'Report ready', alert: true });
  assert.equal(w.notices[0].tag, w.notices[1].tag);
  assert.equal(w.notices[0].silent, true);
  assert.equal(w.notices[0].renotify, false);
  assert.equal(w.notices[1].silent, false);
  assert.equal(w.notices[1].renotify, true);
  assert.equal(w.notices[1].data.revision, 2);
});
test('delayed updates preserve a newer visible notification while still showing a notification', async () => {
  const w = worker([
    { title: 'Current', body: 'Report ready', data: { revision: 5, url: '/?project=demo' } },
  ]);
  await w.push({ experiment_id: id, revision: 2, title: 'Older', body: 'Training', alert: true });
  assert.equal(w.notices[0].title, 'Current');
  assert.equal(w.notices[0].data.revision, 5);
  assert.equal(w.notices[0].renotify, false);
});
test('notification click reuses an app window and refuses external navigation', async () => {
  const w = worker();
  assert.equal(await w.click({ url: `/?project=demo&experiment=${id}` }), true);
  assert.equal(w.navigations[0], `https://private.test/?project=demo&experiment=${id}`);
  await w.click({ url: 'https://evil.test/' });
  assert.equal(w.navigations.length, 1);
  await w.push({ url: 'https://evil.test/' });
  assert.equal(w.notices[0].data.url, '/');
});
