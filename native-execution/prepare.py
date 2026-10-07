#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Prepare pinned C++ headers and binary dependencies; no Java artifact installation."""
import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import tarfile
import urllib.request
import zipfile

MODULE = Path(__file__).resolve().parent
VELOX = 'f68e7ae9f04d7d36225e615e8c5c4e6ca045c438'
BOOTSTRAP_SHA = '42ec226a78d5b9cfdfeaf14a010101662dcf181dbecea48b54f6cd63355f8617'
SOURCES = {
    'velox': ('https://codeload.github.com/facebookincubator/velox/tar.gz/' + VELOX, VELOX),
    'folly': ('https://codeload.github.com/facebook/folly/tar.gz/refs/tags/v2026.01.05.00', 'v2026.01.05.00'),
    'glog': ('https://codeload.github.com/google/glog/tar.gz/refs/tags/v0.6.0', '0.6.0'),
    'gflags': ('https://codeload.github.com/gflags/gflags/tar.gz/refs/tags/v2.3.0', '2.3.0'),
    'fmt': ('https://codeload.github.com/fmtlib/fmt/tar.gz/refs/tags/11.2.0', '11.2.0'),
    'xsimd': ('https://codeload.github.com/xtensor-stack/xsimd/tar.gz/refs/tags/10.0.0', '10.0.0'),
}


def download(url, destination):
    if destination.exists():
        return
    temporary = destination.with_suffix(destination.suffix + '.part')
    try:
        with urllib.request.urlopen(url, timeout=120) as response, temporary.open('wb') as output:
            while data := response.read(1024 * 1024):
                output.write(data)
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def source(cache, name, url):
    target = cache / 'sources' / name
    if target.exists():
        return
    archive_path = cache / (name + '.tar.gz')
    download(url, archive_path)
    staging = target.with_name(name + '.extracting')
    staging.mkdir(exist_ok=True)
    with tarfile.open(archive_path) as archive:
        members = archive.getmembers()
        prefix = members[0].name.split('/')[0] + '/'
        for member in members:
            if member.name.startswith(prefix):
                member.name = member.name[len(prefix):]
                if member.name:
                    archive.extract(member, staging, filter='data')
    staging.rename(target)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache', type=Path, default=MODULE.parent / '.tools/native-runtime')
    args = parser.parse_args()
    cache = args.cache.resolve()
    (cache / 'sources').mkdir(parents=True, exist_ok=True)
    libraries = cache / 'lib'
    libraries.mkdir(exist_ok=True)
    binary = cache.parent / 'velox4j-0.1.0.jar'
    download('https://repo.maven.apache.org/maven2/org/boostscale/velox4j/0.1.0/velox4j-0.1.0.jar', binary)
    if hashlib.sha256(binary.read_bytes()).hexdigest() != BOOTSTRAP_SHA:
        raise RuntimeError('Unexpected C++ bootstrap archive checksum')
    # The public archive supplies matching Velox/Folly libraries only. Do not
    # load its Java classes, libvelox4j entrypoints or bundled glibc librt.
    with zipfile.ZipFile(binary) as archive:
        for name in archive.namelist():
            filename = Path(name).name
            if name.startswith('velox4j-lib/Linux/amd64/') and (name.endswith('.so') or '.so.' in name):
                if filename not in ('libvelox4j.so', 'librt.so.1'):
                    (libraries / filename).write_bytes(archive.read(name))
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        jobs = [executor.submit(source, cache, name, url) for name, (url, _) in SOURCES.items()]
        for job in jobs:
            job.result()
    manifest = {
        'source_revisions': {name: revision for name, (_, revision) in SOURCES.items()},
        'string_abi': 0, 'cpu_target': 'AVX2', 'bootstrap_archive_sha256': BOOTSTRAP_SHA,
        'libraries': {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in sorted(libraries.iterdir()) if p.name not in ('libvelox4j.so', 'librt.so.1')},
        'source_archive_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                 for p in sorted(cache.glob('*.tar.gz'))},
    }
    (cache / 'dependencies.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print('Prepared C++ dependencies:', cache)


if __name__ == '__main__':
    main()
