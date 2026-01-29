import { createRequire } from 'module';
import { Transform } from 'readable-stream';
import type { EntryData } from '../types.js';
import { collectStream } from '../utils.js';

const require = createRequire(import.meta.url);
const crc32 = require('buffer-crc32');

export default class Json extends Transform {
  files: EntryData[];

  constructor(options?: any) {
    super({ ...options });
    this.files = [];
  }

  _transform(
    chunk: Buffer,
    encoding: string,
    callback: (err?: Error | null, data?: Buffer) => void,
  ) {
    callback(null, chunk);
  }

  _writeStringified() {
    const fileString = JSON.stringify(this.files);
    this.write(fileString);
  }

  append(source: any, data: EntryData, callback: (err?: any, data?: any) => void) {
    data.crc32 = 0 as any;
    const self = this;

    function onend(err: any, sourceBuffer?: Buffer) {
      if (err) {
        callback(err);
        return;
      }
      data.size = (sourceBuffer && sourceBuffer.length) || 0;
      data.crc32 = crc32.unsigned(sourceBuffer || Buffer.alloc(0));
      self.files.push(data);
      callback(null, data);
    }

    if (data.sourceType === 'buffer') {
      onend(null, source);
    } else if (data.sourceType === 'stream') {
      collectStream(source, onend);
    }
  }

  finalize() {
    this._writeStringified();
    this.end();
  }
}
