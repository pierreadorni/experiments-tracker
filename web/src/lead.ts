// The Autopilot lead is a contract 4 experiment whose id is uuid5(NAMESPACE_URL,
// "autopilot-lead:" + run_id) (autopilot_policy.lead_id), so the view derives it from the run.
const NAMESPACE_URL = '6ba7b8119dad11d180b400c04fd430c8';

function sha1(bytes: Uint8Array): Uint8Array {
  const words = new Uint32Array(80);
  const total = Math.ceil((bytes.length + 9) / 64) * 64;
  const padded = new Uint8Array(total);
  padded.set(bytes);
  padded[bytes.length] = 0x80;
  const view = new DataView(padded.buffer);
  view.setUint32(total - 4, (bytes.length * 8) >>> 0);
  view.setUint32(total - 8, Math.floor((bytes.length * 8) / 2 ** 32));
  const state = [0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476, 0xc3d2e1f0];
  const rotate = (value: number, n: number) => (value << n) | (value >>> (32 - n));
  for (let offset = 0; offset < total; offset += 64) {
    for (let i = 0; i < 16; i++) words[i] = view.getUint32(offset + i * 4);
    for (let i = 16; i < 80; i++)
      words[i] = rotate(words[i - 3] ^ words[i - 8] ^ words[i - 14] ^ words[i - 16], 1);
    let [a, b, c, d, e] = state;
    for (let i = 0; i < 80; i++) {
      const [f, k] =
        i < 20
          ? [(b & c) | (~b & d), 0x5a827999]
          : i < 40
            ? [b ^ c ^ d, 0x6ed9eba1]
            : i < 60
              ? [(b & c) | (b & d) | (c & d), 0x8f1bbcdc]
              : [b ^ c ^ d, 0xca62c1d6];
      const next = (rotate(a, 5) + f + e + k + words[i]) >>> 0;
      [e, d, c, b, a] = [d, c, rotate(b, 30) >>> 0, a, next];
    }
    [a, b, c, d, e].forEach((v, i) => (state[i] = (state[i] + v) >>> 0));
  }
  const out = new Uint8Array(20);
  state.forEach((v, i) => new DataView(out.buffer).setUint32(i * 4, v));
  return out;
}

export function uuid5(name: string): string {
  const text = new TextEncoder().encode(name);
  const space = Uint8Array.from(NAMESPACE_URL.match(/../g)!.map((h) => parseInt(h, 16)));
  const input = new Uint8Array(space.length + text.length);
  input.set(space);
  input.set(text, space.length);
  const hash = sha1(input).slice(0, 16);
  hash[6] = (hash[6] & 0x0f) | 0x50;
  hash[8] = (hash[8] & 0x3f) | 0x80;
  const hex = Array.from(hash, (b) => b.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export const leadId = (runId: string) => uuid5('autopilot-lead:' + runId);

// The run a saved contract leads, or null for every ordinary experiment.
export function leadRunId(contract: Record<string, unknown> | undefined): string | null {
  const marker = contract?.autopilot_lead as { run_id?: unknown } | undefined;
  return typeof marker?.run_id === 'string' ? marker.run_id : null;
}
