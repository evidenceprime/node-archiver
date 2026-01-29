import fs from "fs";

import { mkdirpSync } from "mkdirp";
import streamBench from "stream-bench";

import { ZipArchive } from "../../dist/index.js";
import { binaryBuffer } from "../common.js";

const BITS_IN_BYTE = 1024;
const BITS_IN_MBYTE = BITS_IN_BYTE * 1024;

let file = false;
let level = 1;

if (process.argv[2]) {
  if (isNaN(parseInt(process.argv[2], 10))) {
    file = process.argv[2];

    if (process.argv[3]) {
      level = parseInt(process.argv[3], 10);

      if (level > 9) {
        level = 1;
      }
    }
  } else {
    level = parseInt(process.argv[2], 10);
  }
}

var archive = new ZipArchive({
  zlib: {
    level: level,
  },
});

if (file === false) {
  mkdirpSync("tmp");

  file = "tmp/20mb.dat";
  fs.writeFileSync(file, binaryBuffer(BITS_IN_MBYTE * 20));
}

console.log("zlib level: " + level);

var bench = streamBench({
  logReport: true,
  interval: 500,
  dump: true,
});

archive.pipe(bench);

archive.file(file, { name: "large file" }).finalize();
