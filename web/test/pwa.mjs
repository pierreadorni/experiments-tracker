import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';

test('phone manifest has full-size PNG icons and an Apple touch icon', async () => {
  const manifest = JSON.parse(
    await readFile(new URL('../public/manifest.webmanifest', import.meta.url)),
  );
  assert.equal(manifest.start_url, '/');
  assert.equal(manifest.scope, '/');
  assert.equal(manifest.display, 'standalone');
  for (const size of [192, 512]) {
    const icon = manifest.icons.find(
      (icon) => icon.sizes === `${size}x${size}` && icon.purpose === 'any',
    );
    assert.ok(icon);
    const bytes = await readFile(new URL('../public' + icon.src, import.meta.url));
    assert.equal(bytes.subarray(0, 8).toString('hex'), '89504e470d0a1a0a');
    assert.equal(bytes.readUInt32BE(16), size);
    assert.equal(bytes.readUInt32BE(20), size);
  }
  const html = await readFile(new URL('../index.html', import.meta.url), 'utf8');
  assert.match(html, /apple-touch-icon/);
  assert.match(html, /crossorigin="use-credentials"/);
});

test('service worker only caches installation icons and leaves private/authentication traffic alone', async () => {
  const handlers = {},
    deleted = [],
    added = [];
  vm.runInNewContext(await readFile(new URL('../public/sw.js', import.meta.url), 'utf8'), {
    URL,
    self: {
      location: { origin: 'https://example.test' },
      addEventListener: (name, fn) => {
        handlers[name] = fn;
      },
      skipWaiting() {},
      clients: { claim() {} },
    },
    caches: {
      open: async () => ({ addAll: async (urls) => added.push(...urls) }),
      keys: async () => [
        'experimenttracker-shell-v1',
        'experimenttracker-shell-v2',
        'unrelated-app',
      ],
      delete: async (key) => deleted.push(key),
      match: async () => null,
    },
    fetch: async () => new Response('icon'),
    Response,
  });
  let pending;
  handlers.install({
    waitUntil: (promise) => {
      pending = promise;
    },
  });
  await pending;
  assert.ok(added.includes('/icon-512.png'));
  assert.ok(added.every((url) => url.endsWith('.png') || url === '/icon.svg'));
  handlers.activate({
    waitUntil: (promise) => {
      pending = promise;
    },
  });
  await pending;
  assert.deepEqual(deleted, ['experimenttracker-shell-v1']);
  for (const [pathname, method] of [
    ['/', 'GET'],
    ['/api/experiments', 'GET'],
    ['/api/artifacts/private', 'GET'],
    ['/signin-with-chatgpt', 'GET'],
    ['/manifest.webmanifest', 'GET'],
    ['/icon.svg?token=private', 'GET'],
    ['/icon.svg', 'POST'],
  ]) {
    handlers.fetch({
      request: { url: 'https://example.test' + pathname, method },
      respondWith: () => assert.fail('intercepted private/non-icon request'),
    });
  }
  let intercepted = false;
  handlers.fetch({
    request: { url: 'https://example.test/icon-512.png', method: 'GET' },
    respondWith: () => {
      intercepted = true;
    },
  });
  assert.ok(intercepted);
});
