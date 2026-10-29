// Rasterize the existing vector identity using the locked development dependencies.
import sharp from 'sharp';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { existsSync } from 'node:fs';
const root = path.resolve(import.meta.dirname, '..');
const web = existsSync(path.join(root, 'web/package.json'))
  ? path.join(root, 'web')
  : path.join(root, '../web');
const directory = path.join(web, 'public');
const source = await readFile(path.join(directory, 'icon.svg'), 'utf8');
for (const size of [192, 512])
  await sharp(Buffer.from(source))
    .resize(size, size)
    .png()
    .toFile(path.join(directory, `icon-${size}.png`));
const square = source.replace('rx="28"', 'rx="0"');
await sharp(Buffer.from(square))
  .resize(180, 180)
  .png()
  .toFile(path.join(directory, 'apple-touch-icon.png'));
await sharp(Buffer.from(square))
  .resize(512, 512)
  .png()
  .toFile(path.join(directory, 'icon-maskable-512.png'));
