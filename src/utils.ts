import { isStream } from 'is-stream';
import normalizePath from 'normalize-path';
import { PassThrough } from 'readable-stream';

export function collectStream(
  source: NodeJS.ReadableStream,
  callback: (err: Error | null, buf?: Buffer) => void,
): void {
  const collection: Buffer[] = [];
  let size = 0;

  source.on('error', callback as any);

  source.on('data', function (chunk: Buffer) {
    collection.push(chunk);
    size += chunk.length;
  });

  source.on('end', function () {
    const buf = Buffer.alloc(size);
    let offset = 0;

    collection.forEach(function (data) {
      data.copy(buf, offset);
      offset += data.length;
    });

    callback(null, buf);
  });
}

export function dateify(dateish?: string | Date | null): Date {
  if (dateish instanceof Date) {
    return dateish;
  } else if (typeof dateish === 'string') {
    return new Date(dateish);
  } else {
    return new Date();
  }
}

export function normalizeInputSource(source: any): Buffer | NodeJS.ReadableStream {
  if (source === null) {
    return Buffer.alloc(0);
  } else if (typeof source === 'string') {
    return Buffer.from(source);
  } else if (isStream(source)) {
    // Always pipe through a PassThrough to ensure proper pausing behavior
    return (source as NodeJS.ReadableStream).pipe(new PassThrough()) as NodeJS.ReadableStream;
  }

  return source;
}

export function sanitizePath(filepath: string): string {
  return normalizePath(filepath, false)
    .replace(/^\w+:/, '')
    .replace(/^(\.\.\/|\/)+/, '');
}

export function trailingSlashIt(str: string): string {
  return str.slice(-1) !== '/' ? str + '/' : str;
}
