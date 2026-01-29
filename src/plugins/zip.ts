import engine from 'zip-stream';
import type { EntryData } from '../types.js';

export default class Zip {
  options: any;
  engine: any;

  constructor(options?: any) {
    options = this.options = {
      comment: '',
      forceUTC: false,
      namePrependSlash: false,
      store: false,
      ...options,
    };
    this.engine = new engine(options);
  }

  append(source: any, data: EntryData, callback: (err?: any) => void) {
    this.engine.entry(source, data, callback);
  }

  finalize() {
    this.engine.finalize();
  }

  on(...args: any[]) {
    return this.engine.on.apply(this.engine, args as any);
  }

  pipe(...args: any[]) {
    return this.engine.pipe.apply(this.engine, args as any);
  }

  unpipe(...args: any[]) {
    return this.engine.unpipe.apply(this.engine, args as any);
  }
}
