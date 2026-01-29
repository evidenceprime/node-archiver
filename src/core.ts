import { Stats, createReadStream, lstat, readlinkSync } from 'node:fs';
import { dirname, relative as relativePath, resolve as resolvePath } from 'node:path';
import { queue } from 'async';
import { isStream } from 'is-stream';
import { Readable } from 'lazystream';
import { Transform } from 'readable-stream';
import { readdirGlob } from 'readdir-glob';
import { ArchiverError } from './error.js';
import type { ArchiveModule, CoreOptions } from './types.js';
import { dateify, normalizeInputSource, sanitizePath, trailingSlashIt } from './utils.js';

const { ReaddirGlob } = readdirGlob as any;
const win32 = process.platform === 'win32';

const abortedError = new ArchiverError('ABORTED');
const finalizingError = new ArchiverError('FINALIZING');

export default class Archiver extends Transform {
  _supportsDirectory = false;
  _supportsSymlink = false;

  options: CoreOptions | any;
  _format: string | false = false;
  _module: ArchiveModule | any = false;
  _pending = 0;
  _pointer = 0;
  _entriesCount = 0;
  _entriesProcessedCount = 0;
  _fsEntriesTotalBytes = 0;
  _fsEntriesProcessedBytes = 0;
  _queue: import('async').QueueObject<any>;
  _statQueue: import('async').QueueObject<any>;
  _state: {
    aborted: boolean;
    finalize: boolean;
    finalizing: boolean;
    finalized: boolean;
    modulePiped: boolean;
  };
  _streams: any[];
  _task: any;

  constructor(options?: CoreOptions) {
    const opts: CoreOptions = {
      highWaterMark: 1024 * 1024,
      statConcurrency: 4,
      ...options,
    };
    super(opts as any);
    this.options = opts;
    this._format = false;
    this._module = false;
    this._pending = 0;
    this._pointer = 0;
    this._entriesCount = 0;
    this._entriesProcessedCount = 0;
    this._fsEntriesTotalBytes = 0;
    this._fsEntriesProcessedBytes = 0;
    this._queue = queue(this._onQueueTask.bind(this), 1);
    this._queue.drain(this._onQueueDrain.bind(this));
    this._statQueue = queue(this._onStatQueueTask.bind(this), opts.statConcurrency);
    this._statQueue.drain(this._onQueueDrain.bind(this));
    this._state = {
      aborted: false,
      finalize: false,
      finalizing: false,
      finalized: false,
      modulePiped: false,
    };
    this._streams = [];
  }

  _abort(): void {
    this._state.aborted = true;
    this._queue.kill();
    this._statQueue.kill();
    if (this._queue.idle()) {
      this._shutdown();
    }
  }

  _append(filepath: string, data?: any): void {
    data = data || {};
    let task: any = {
      source: null,
      filepath: filepath,
    };
    if (!data.name) {
      data.name = filepath;
    }
    data.sourcePath = filepath;
    task.data = data;
    this._entriesCount++;
    if (data.stats && (data.stats as Stats) instanceof Stats) {
      task = this._updateQueueTaskWithStats(task, data.stats as Stats);
      if (task) {
        if (data.stats.size) {
          this._fsEntriesTotalBytes += data.stats.size;
        }
        this._queue.push(task);
      }
    } else {
      this._statQueue.push(task);
    }
  }

  _finalize(): void {
    if (this._state.finalizing || this._state.finalized || this._state.aborted) {
      return;
    }
    this._state.finalizing = true;
    this._moduleFinalize();
    this._state.finalizing = false;
    this._state.finalized = true;
  }

  _maybeFinalize(): boolean {
    if (this._state.finalizing || this._state.finalized || this._state.aborted) {
      return false;
    }
    if (
      this._state.finalize &&
      this._pending === 0 &&
      this._queue.idle() &&
      this._statQueue.idle()
    ) {
      this._finalize();
      return true;
    }
    return false;
  }

  _moduleAppend(source: any, data: any, callback: () => void): void {
    if (this._state.aborted) {
      callback();
      return;
    }
    this._module.append(
      source,
      data,
      function (this: Archiver, err: any) {
        this._task = null;
        if (this._state.aborted) {
          this._shutdown();
          return;
        }
        if (err) {
          this.emit('error', err);
          setImmediate(callback);
          return;
        }
        this.emit('entry', data);
        this._entriesProcessedCount++;
        if (data.stats?.size) {
          this._fsEntriesProcessedBytes += data.stats.size;
        }
        this.emit('progress', {
          entries: {
            total: this._entriesCount,
            processed: this._entriesProcessedCount,
          },
          fs: {
            totalBytes: this._fsEntriesTotalBytes,
            processedBytes: this._fsEntriesProcessedBytes,
          },
        });
        setImmediate(callback);
      }.bind(this),
    );
  }

  _moduleFinalize(): void {
    if (typeof this._module.finalize === 'function') {
      this._module.finalize();
    } else if (typeof this._module.end === 'function') {
      this._module.end();
    } else {
      this.emit('error', new ArchiverError('NOENDMETHOD'));
    }
  }

  _modulePipe(): void {
    this._module.on('error', this._onModuleError.bind(this));
    this._module.pipe(this);
    this._state.modulePiped = true;
  }

  _moduleUnpipe(): void {
    this._module.unpipe(this);
    this._state.modulePiped = false;
  }

  _normalizeEntryData(data: any, stats?: Stats): any {
    data = {
      type: 'file',
      name: null,
      date: null,
      mode: null,
      prefix: null,
      sourcePath: null,
      stats: false,
      ...data,
    };
    if (stats && data.stats === false) {
      data.stats = stats;
    }
    let isDir = data.type === 'directory';
    if (data.name) {
      if (typeof data.prefix === 'string' && '' !== data.prefix) {
        data.name = data.prefix + '/' + data.name;
        data.prefix = null;
      }
      data.name = sanitizePath(data.name);
      if (data.type !== 'symlink' && data.name.slice(-1) === '/') {
        isDir = true;
        data.type = 'directory';
      } else if (isDir) {
        data.name += '/';
      }
    }
    if (typeof data.mode === 'number') {
      if (win32) {
        data.mode &= 511;
      } else {
        data.mode &= 4095;
      }
    } else if (data.stats && data.mode === null) {
      if (win32) {
        data.mode = data.stats.mode & 511;
      } else {
        data.mode = data.stats.mode & 4095;
      }
      if (win32 && isDir) {
        data.mode = 493;
      }
    } else if (data.mode === null) {
      data.mode = isDir ? 493 : 420;
    }
    if (data.stats && data.date === null) {
      data.date = data.stats.mtime;
    } else {
      data.date = dateify(data.date);
    }
    return data;
  }

  _onModuleError(err: any): void {
    this.emit('error', err);
  }

  _onQueueDrain(): void {
    if (this._state.finalizing || this._state.finalized || this._state.aborted) {
      return;
    }
    if (
      this._state.finalize &&
      this._pending === 0 &&
      this._queue.idle() &&
      this._statQueue.idle()
    ) {
      this._finalize();
    }
  }

  _onQueueTask(task: any, callback: () => void): void {
    const fullCallback = () => {
      if (task.data.callback) {
        task.data.callback();
      }
      callback();
    };
    if (this._state.finalizing || this._state.finalized || this._state.aborted) {
      fullCallback();
      return;
    }
    this._task = task;
    this._moduleAppend(task.source, task.data, fullCallback);
  }

  _onStatQueueTask(task: any, callback: () => void): void {
    if (this._state.finalizing || this._state.finalized || this._state.aborted) {
      callback();
      return;
    }
    lstat(
      task.filepath,
      function (this: Archiver, err: NodeJS.ErrnoException | null, stats: Stats) {
        if (this._state.aborted) {
          setImmediate(callback);
          return;
        }
        if (err) {
          this._entriesCount--;
          this.emit('warning', err);
          setImmediate(callback);
          return;
        }
        task = this._updateQueueTaskWithStats(task, stats as Stats);
        if (task) {
          if (stats.size) {
            this._fsEntriesTotalBytes += stats.size;
          }
          this._queue.push(task);
        }
        setImmediate(callback);
      }.bind(this),
    );
  }

  _shutdown(): void {
    this._moduleUnpipe();
    this.end();
  }

  _transform(
    chunk: Buffer,
    encoding: string,
    callback: (err?: Error | null, data?: Buffer) => void,
  ) {
    if (chunk) {
      this._pointer += chunk.length;
    }
    callback(null, chunk);
  }

  _updateQueueTaskWithStats(task: any, stats: Stats): any {
    if (stats.isFile()) {
      task.data.type = 'file';
      task.data.sourceType = 'stream';
      task.source = new Readable(() => {
        return createReadStream(task.filepath);
      });
    } else if (stats.isDirectory() && this._supportsDirectory) {
      task.data.name = trailingSlashIt(task.data.name);
      task.data.type = 'directory';
      task.data.sourcePath = trailingSlashIt(task.filepath);
      task.data.sourceType = 'buffer';
      task.source = Buffer.concat([]);
    } else if (stats.isSymbolicLink() && this._supportsSymlink) {
      const linkPath = readlinkSync(task.filepath);
      const dirName = dirname(task.filepath);
      task.data.type = 'symlink';
      task.data.linkname = relativePath(dirName, resolvePath(dirName, linkPath));
      task.data.sourceType = 'buffer';
      task.source = Buffer.concat([]);
    } else {
      if (stats.isDirectory()) {
        this.emit('warning', new ArchiverError('DIRECTORYNOTSUPPORTED', task.data));
      } else if (stats.isSymbolicLink()) {
        this.emit('warning', new ArchiverError('SYMLINKNOTSUPPORTED', task.data));
      } else {
        this.emit('warning', new ArchiverError('ENTRYNOTSUPPORTED', task.data));
      }
      return null;
    }
    task.data = this._normalizeEntryData(task.data, stats);
    return task;
  }

  abort(): this {
    if (this._state.aborted || this._state.finalized) {
      return this;
    }
    this._abort();
    return this;
  }

  append(source: any, data: any): this {
    if (this._state.finalize || this._state.aborted) {
      this.emit('error', new ArchiverError('QUEUECLOSED'));
      return this;
    }
    data = this._normalizeEntryData(data);
    if (typeof data.name !== 'string' || data.name.length === 0) {
      this.emit('error', new ArchiverError('ENTRYNAMEREQUIRED'));
      return this;
    }
    if (data.type === 'directory' && !this._supportsDirectory) {
      this.emit('error', new ArchiverError('DIRECTORYNOTSUPPORTED', { name: data.name }));
      return this;
    }
    source = normalizeInputSource(source);
    if (Buffer.isBuffer(source)) {
      data.sourceType = 'buffer';
    } else if (isStream(source)) {
      data.sourceType = 'stream';
    } else {
      this.emit('error', new ArchiverError('INPUTSTEAMBUFFERREQUIRED', { name: data.name }));
      return this;
    }
    this._entriesCount++;
    this._queue.push({
      data: data,
      source: source,
    });
    return this;
  }

  directory(dirpath: string, destpath?: string | boolean, data?: any): this {
    if (this._state.finalize || this._state.aborted) {
      this.emit('error', new ArchiverError('QUEUECLOSED'));
      return this;
    }
    if (typeof dirpath !== 'string' || dirpath.length === 0) {
      this.emit('error', new ArchiverError('DIRECTORYDIRPATHREQUIRED'));
      return this;
    }
    this._pending++;
    if (destpath === false) {
      destpath = '';
    } else if (typeof destpath !== 'string') {
      destpath = dirpath;
    }
    let dataFunction: false | ((entryData: any) => any) = false;
    if (typeof data === 'function') {
      dataFunction = data as any;
      data = {};
    } else if (typeof data !== 'object') {
      data = {};
    }
    const globOptions: any = {
      stat: true,
      dot: true,
    };
    const onGlobEnd = function (this: Archiver) {
      this._pending--;
      this._maybeFinalize();
    };
    const onGlobError = function (this: Archiver, err: any) {
      this.emit('error', err);
    };
    const onGlobMatch = function (this: Archiver, match: any) {
      globber.pause();
      let ignoreMatch = false;
      let entryData = Object.assign({}, data);
      entryData.name = match.relative;
      entryData.prefix = destpath as string;
      entryData.stats = match.stat;
      entryData.callback = globber.resume.bind(globber);
      try {
        if (dataFunction) {
          entryData = dataFunction(entryData);
          if (entryData === false) {
            ignoreMatch = true;
          } else if (typeof entryData !== 'object') {
            throw new ArchiverError('DIRECTORYFUNCTIONINVALIDDATA', { dirpath: dirpath });
          }
        }
      } catch (e) {
        this.emit('error', e);
        return;
      }
      if (ignoreMatch) {
        globber.resume();
        return;
      }
      this._append(match.absolute, entryData);
    };
    const globber = readdirGlob(dirpath, globOptions);
    globber.on('error', onGlobError.bind(this));
    globber.on('match', onGlobMatch.bind(this));
    globber.on('end', onGlobEnd.bind(this));
    return this;
  }

  file(filepath: string, data?: any): this {
    if (this._state.finalize || this._state.aborted) {
      this.emit('error', new ArchiverError('QUEUECLOSED'));
      return this;
    }
    if (typeof filepath !== 'string' || filepath.length === 0) {
      this.emit('error', new ArchiverError('FILEFILEPATHREQUIRED'));
      return this;
    }
    this._append(filepath, data);
    return this;
  }

  glob(pattern: string, options?: any, data?: any): this {
    this._pending++;
    options = {
      stat: true,
      pattern: pattern,
      ...options,
    };
    const onGlobEnd = function (this: Archiver) {
      this._pending--;
      this._maybeFinalize();
    };
    const onGlobError = function (this: Archiver, err: any) {
      this.emit('error', err);
    };
    const onGlobMatch = function (this: Archiver, match: any) {
      globber.pause();
      const entryData = Object.assign({}, data);
      entryData.callback = globber.resume.bind(globber);
      entryData.stats = match.stat;
      entryData.name = match.relative;
      this._append(match.absolute, entryData);
    };
    const globber = new ReaddirGlob(options.cwd || '.', options);
    globber.on('error', onGlobError.bind(this));
    globber.on('match', onGlobMatch.bind(this));
    globber.on('end', onGlobEnd.bind(this));
    return this;
  }

  finalize(): Promise<void> {
    if (this._state.aborted) {
      this.emit('error', abortedError);
      return Promise.reject(abortedError);
    }
    if (this._state.finalize) {
      this.emit('error', finalizingError);
      return Promise.reject(finalizingError);
    }
    this._state.finalize = true;
    if (this._pending === 0 && this._queue.idle() && this._statQueue.idle()) {
      this._finalize();
    }
    const self = this;
    return new Promise((resolve, reject) => {
      let errored = false;
      self._module.on('end', () => {
        if (!errored) {
          resolve();
        }
      });
      self._module.on('error', (err: any) => {
        errored = true;
        reject(err);
      });
    });
  }

  symlink(filepath: string, target: string, mode?: number): this {
    if (this._state.finalize || this._state.aborted) {
      this.emit('error', new ArchiverError('QUEUECLOSED'));
      return this;
    }
    if (typeof filepath !== 'string' || filepath.length === 0) {
      this.emit('error', new ArchiverError('SYMLINKFILEPATHREQUIRED'));
      return this;
    }
    if (typeof target !== 'string' || target.length === 0) {
      this.emit('error', new ArchiverError('SYMLINKTARGETREQUIRED', { filepath: filepath }));
      return this;
    }
    if (!this._supportsSymlink) {
      this.emit('error', new ArchiverError('SYMLINKNOTSUPPORTED', { filepath: filepath }));
      return this;
    }
    const data: any = {};
    data.type = 'symlink';
    data.name = filepath.replace(/\\/g, '/');
    data.linkname = target.replace(/\\/g, '/');
    data.sourceType = 'buffer';
    if (typeof mode === 'number') {
      data.mode = mode;
    }
    this._entriesCount++;
    this._queue.push({
      data: data,
      source: Buffer.concat([]),
    });
    return this;
  }

  pointer(): number {
    return this._pointer;
  }
}
