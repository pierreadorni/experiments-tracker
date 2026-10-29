// Run once with an absolute ignored/protected output path. Never rotate deployed
// application server keys without replacing the devices' push subscriptions.
import { createECDH } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
const output = process.argv[2];
if (!output || !path.isAbsolute(output))
  throw new Error('An absolute protected output path is required');
const key = createECDH('prime256v1');
key.generateKeys();
const privateBytes = Buffer.alloc(32);
key.getPrivateKey().copy(privateBytes, 32 - key.getPrivateKey().length);
await mkdir(path.dirname(output), { recursive: true, mode: 0o700 });
await writeFile(
  output,
  JSON.stringify({
    publicKey: key.getPublicKey().toString('base64url'),
    privateKey: privateBytes.toString('base64url'),
  }) + '\n',
  { mode: 0o600, flag: 'wx' },
);
console.log('Generated Web Push application keys in the protected output file.');
