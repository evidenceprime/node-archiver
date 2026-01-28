const ERROR_CODES: Record<string, string> = {
  ABORTED: 'archive was aborted',
  DIRECTORYDIRPATHREQUIRED: 'diretory dirpath argument must be a non-empty string value',
  DIRECTORYFUNCTIONINVALIDDATA: 'invalid data returned by directory custom data function',
  ENTRYNAMEREQUIRED: 'entry name must be a non-empty string value',
  FILEFILEPATHREQUIRED: 'file filepath argument must be a non-empty string value',
  FINALIZING: 'archive already finalizing',
  QUEUECLOSED: 'queue closed',
  NOENDMETHOD: 'no suitable finalize/end method defined by module',
  DIRECTORYNOTSUPPORTED: 'support for directory entries not defined by module',
  FORMATSET: 'archive format already set',
  INPUTSTEAMBUFFERREQUIRED: 'input source must be valid Stream or Buffer instance',
  MODULESET: 'module already set',
  SYMLINKNOTSUPPORTED: 'support for symlink entries not defined by module',
  SYMLINKFILEPATHREQUIRED: 'symlink filepath argument must be a non-empty string value',
  SYMLINKTARGETREQUIRED: 'symlink target argument must be a non-empty string value',
  ENTRYNOTSUPPORTED: 'entry not supported',
};

export class ArchiverError extends Error {
  code: string;
  data: any;

  constructor(code: string, data?: any) {
    super(ERROR_CODES[code] || code);
    this.name = 'ArchiverError';
    this.code = code;
    this.data = data;
    Error.captureStackTrace(this, this.constructor);
  }
}

// Backwards compatible export style for CommonJS interop
export default ArchiverError;
