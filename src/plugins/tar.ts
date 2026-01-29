import zlib from 'zlib';
import engine from 'tar-stream';
import type { EntryData } from '../types.js';
import { collectStream } from '../utils.js';

export default class Tar {
  options: any;
  engine: any;
  compressor: any;

  constructor(options?: any) {
    options = this.options = {
      gzip: false,
      ...options,
    };
    if (typeof options.gzipOptions !== 'object') {
      options.gzipOptions = {};
    }
    this.engine = engine.pack(options);
    this.compressor = false;
    if (options.gzip) {
      this.compressor = zlib.createGzip(options.gzipOptions);
      this.compressor.on('error', this._onCompressorError.bind(this));
    }
  }

  _onCompressorError(err: any) {
    this.engine.emit('error', err);
  }

  append(source: any, data: EntryData, callback: (err?: any, data?: any) => void) {
    const self = this;
    data.mtime = data.date as any;

    function appendFn(err: any, sourceBuffer?: Buffer) {
      if (err) {
        callback(err);
        return;
      }
      self.engine.entry(data, sourceBuffer, function (err: any) {
        callback(err, data);
      });
    }

    if (data.sourceType === 'buffer') {
      appendFn(null, source);
    } else if (data.sourceType === 'stream' && data.stats) {
      data.size = data.stats.size as any;
      const entry = self.engine.entry(data, function (err: any) {
        callback(err, data);
      });
      source.pipe(entry);
    } else if (data.sourceType === 'stream') {
      collectStream(source, appendFn);
    }
  }

  finalize() {
    this.engine.finalize();
  }

  on(...args: any[]) {
    return this.engine.on.apply(this.engine, args as any);
  }

  pipe(destination: any, options?: any) {
    if (this.compressor) {
      return this.engine.pipe.apply(this.engine, [this.compressor]).pipe(destination, options);
    } else {
      return this.engine.pipe(destination, options);
    }
  }

  unpipe(...args: any[]) {
    if (this.compressor) {
      return this.compressor.unpipe.apply(this.compressor, args as any);
    } else {
      return this.engine.unpipe.apply(this.engine, args as any);
    }
  }
}
