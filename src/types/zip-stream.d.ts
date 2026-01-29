declare module 'zip-stream' {
  import { Writable, Readable } from 'stream';

  interface ZipStream {
    entry(source: any, data: any, callback: (err?: any) => void): void;
    finalize(): void;
    pipe(dest: NodeJS.WritableStream, options?: any): any;
    on(event: string, listener: (...args: any[]) => void): any;
    unpipe(...args: any[]): any;
  }

  const ZipStreamFactory: any;
  export default ZipStreamFactory;
}
