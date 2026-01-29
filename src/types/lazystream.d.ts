declare module 'lazystream' {
  import { Readable as NodeReadable } from 'stream';

  export class LazystreamReadable extends NodeReadable {
    constructor(factory: () => NodeJS.ReadableStream);
  }

  export { LazystreamReadable as Readable };
  const _default: { Readable: typeof LazystreamReadable };
  export default _default;
}
