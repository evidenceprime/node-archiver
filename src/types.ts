import type { Stats } from 'node:fs';
import type { TransformOptions } from 'stream';

export type Source = Buffer | NodeJS.ReadableStream;

export interface EntryData {
  name: string | null;
  date?: string | Date | null;
  mode?: number | null;
  prefix?: string | null;
  sourcePath?: string | null;
  stats?: Stats | false;
  sourceType?: 'buffer' | 'stream';
  type?: 'file' | 'directory' | 'symlink';
  linkname?: string;
  callback?: () => void;
  size?: number;
  crc32?: number;
  [key: string]: any;
}

export interface CoreOptions extends TransformOptions {
  statConcurrency?: number;
}

export interface ArchiveModule {
  append?: (
    source: Source | any,
    data: EntryData,
    callback: (err?: any, data?: any) => void,
  ) => void;
  finalize?: () => void;
  end?: () => void;
  on?: (event: string, listener: (...args: any[]) => void) => any;
  pipe?: (dest: any, options?: any) => any;
  unpipe?: (...args: any[]) => any;
}

export interface QueueTask {
  source: Source | null;
  filepath?: string;
  data: EntryData;
}
