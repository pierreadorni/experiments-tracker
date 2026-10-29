import { build } from 'esbuild';
import { cp, mkdir, readdir, readFile, writeFile, rm } from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const root = path.resolve(import.meta.dirname, '..');
const web = (await import('node:fs')).existsSync(path.join(root, 'web/package.json'))
  ? path.join(root, 'web')
  : path.join(root, '../web');
// Sites installs the checkout's root package. Its nested frontend has its own lockfile.
if (web === path.join(root, 'web')) {
  const install = spawnSync('npm', ['ci'], { cwd: web, stdio: 'inherit' });
  if (install.status !== 0) process.exit(install.status || 1);
}
const result = spawnSync('npm', ['run', 'build'], { cwd: web, stdio: 'inherit' });
if (result.status !== 0) process.exit(result.status || 1);
const mime = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.woff2': 'font/woff2',
  '.webmanifest': 'application/manifest+json',
};
const assets = {};
async function collect(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const filename = path.join(directory, entry.name);
    if (entry.isDirectory()) await collect(filename);
    else {
      const binary = ['.png', '.woff2'].includes(path.extname(filename));
      assets['/' + path.relative(path.join(web, 'dist'), filename).split(path.sep).join('/')] = {
        body: (await readFile(filename)).toString(binary ? 'base64' : 'utf8'),
        type: mime[path.extname(filename)] || 'text/plain',
        ...(binary ? { encoding: 'base64' } : {}),
      };
    }
  }
}
await collect(path.join(web, 'dist'));
await writeFile(
  path.join(root, 'src/generated-assets.ts'),
  'export const assets: Record<string,{body:string,type:string,encoding?:"base64"}> = ' +
    JSON.stringify(assets) +
    ';\n',
);
await rm(path.join(root, 'dist'), { recursive: true, force: true });
await mkdir(path.join(root, 'dist/server'), { recursive: true });
await mkdir(path.join(root, 'dist/.openai'), { recursive: true });
await build({
  entryPoints: [path.join(root, 'src/index.ts')],
  outfile: path.join(root, 'dist/server/index.js'),
  bundle: true,
  format: 'esm',
  platform: 'browser',
  target: 'es2022',
  minify: true,
});
await cp(path.join(root, '.openai/hosting.json'), path.join(root, 'dist/.openai/hosting.json'));
// Include schema migrations in build output as well as local Sites archives.
await cp(path.join(root, 'drizzle'), path.join(root, 'dist/.openai/drizzle'), { recursive: true });
console.log('Built gateway Worker with embedded shared UI; D1/R2 stay server-side.');
