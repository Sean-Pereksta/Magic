// Browser-native compression keeps battle checkpoints below Firestore's document
// limit. No build step, Node runtime, third-party relay, or service key is used.
export async function pack(value) {
  const json = JSON.stringify(value);
  if (!globalThis.CompressionStream) return "json:" + json;
  const stream = new Blob([json])
    .stream()
    .pipeThrough(new CompressionStream("gzip"));
  const bytes = new Uint8Array(await new Response(stream).arrayBuffer());
  let binary = "";
  for (let i = 0; i < bytes.length; i += 8192)
    binary += String.fromCharCode(...bytes.subarray(i, i + 8192));
  return "gzip:" + btoa(binary);
}
export async function unpack(value) {
  if (typeof value !== "string" || value.length > 900000)
    throw new Error("The realm checkpoint is invalid.");
  if (value.startsWith("json:")) return JSON.parse(value.slice(5));
  if (!value.startsWith("gzip:") || !globalThis.DecompressionStream)
    throw new Error("Update your browser to restore this realm.");
  const bytes = Uint8Array.from(atob(value.slice(5)), (c) => c.charCodeAt(0));
  const reader = new Blob([bytes])
    .stream()
    .pipeThrough(new DecompressionStream("gzip"))
    .getReader();
  const chunks = [];
  let size = 0;
  for (;;) {
    const { done, value: chunk } = await reader.read();
    if (done) break;
    size += chunk.length;
    if (size > 16000000) {
      await reader.cancel();
      throw new Error("The realm checkpoint is too large.");
    }
    chunks.push(chunk);
  }
  return JSON.parse(
    new TextDecoder().decode(await new Blob(chunks).arrayBuffer()),
  );
}
