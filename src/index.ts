import Archiver from './core.js';
import Json from './plugins/json.js';
import Tar from './plugins/tar.js';
import Zip from './plugins/zip.js';

export { Archiver };

export class ZipArchive extends Archiver {
  constructor(options?: any) {
    super(options);
    this._format = 'zip';
    this._module = new Zip(options);
    this._supportsDirectory = true;
    this._supportsSymlink = true;
    this._modulePipe();
  }
}

export class TarArchive extends Archiver {
  constructor(options?: any) {
    super(options);
    this._format = 'tar';
    this._module = new Tar(options);
    this._supportsDirectory = true;
    this._supportsSymlink = true;
    this._modulePipe();
  }
}

export class JsonArchive extends Archiver {
  constructor(options?: any) {
    super(options);
    this._format = 'json';
    this._module = new Json(options);
    this._supportsDirectory = true;
    this._supportsSymlink = true;
    this._modulePipe();
  }
}
