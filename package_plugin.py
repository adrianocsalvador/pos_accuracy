# -*- coding: utf-8 -*-
"""Build the ZIP uploaded to https://plugins.qgis.org/

Usage (from the plugin root):
    python package_plugin.py

Creates dist/pos_accuracy-<version>.zip with a single top-level folder
pos_accuracy/ containing only the files QGIS needs to run the plugin.

Ignored on purpose (ZIP only — scripts* stay in the GitHub repo):
    scripts* folders, IDE folders, i18n build tools, unused icons, __pycache__, local zips.
"""
from __future__ import annotations

import configparser
import fnmatch
import re
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PLUGIN_DIR = 'pos_accuracy'
DIST = ROOT / 'dist'

REQUIRED_METADATA = (
    'name',
    'description',
    'version',
    'qgisMinimumVersion',
    'author',
    'email',
    'about',
    'tracker',
    'repository',
)

# Directories that must never go into the QGIS package.
SKIP_DIR_NAMES = {
    '.git',
    '.idea',
    '.vscode',
    '.cursor',
    '__pycache__',
    'dist',
}

# Extra files never needed at runtime in QGIS.
SKIP_FILE_GLOBS = (
    '*.pyc',
    '*.pyo',
    '*.zip',
    '.gitignore',
    'package_plugin.py',
    'symbology-style.db',
    'i18n/_m33.txt',
    'i18n/*.bat',
    'i18n/*.pro',
    'i18n/*.py',
    'i18n/*.txt',
    'i18n/*.ts',
    'icons/UFV_v2.png',
    'icons/icon_ufv.png',
)

INCLUDE_ROOT_FILES = {
    '__init__.py',
    'metadata.txt',
    'LICENSE',
    'README.md',
}

RUNTIME_DIRS = ('mods', 'i18n', 'icons', 'styles')


def _read_metadata() -> configparser.ConfigParser:
    parser = configparser.ConfigParser()
    parser.optionxform = str
    parser.read(ROOT / 'metadata.txt', encoding='utf-8')
    if not parser.has_section('general'):
        raise SystemExit("metadata.txt must contain a [general] section.")
    missing = [k for k in REQUIRED_METADATA if not parser.has_option('general', k)]
    if missing:
        raise SystemExit(f"metadata.txt missing required fields: {', '.join(missing)}")
    if not (ROOT / 'LICENSE').is_file():
        raise SystemExit('LICENSE file is required (no extension).')
    if not (ROOT / '__init__.py').is_file():
        raise SystemExit('__init__.py is required.')
    version = parser.get('general', 'version').strip()
    if not re.match(r'^\d+(\.\d+)+$', version):
        raise SystemExit(f'Invalid version in metadata.txt: {version!r}')
    icon = parser.get('general', 'icon', fallback='')
    if icon and not (ROOT / icon.replace('\\', '/')).is_file():
        raise SystemExit(f'Icon not found: {icon}')
    return parser


def _skip_dir(rel: Path) -> bool:
    for part in rel.parts[:-1] if rel.suffix else rel.parts:
        if part in SKIP_DIR_NAMES or part.startswith('scripts'):
            return True
    # Also skip if any ancestor is scripts*
    return any(p in SKIP_DIR_NAMES or p.startswith('scripts') for p in rel.parts)


def _skip_file(rel: Path) -> bool:
    posix = rel.as_posix()
    name = rel.name
    if name.startswith('.') and posix not in INCLUDE_ROOT_FILES:
        return True
    for pat in SKIP_FILE_GLOBS:
        if fnmatch.fnmatch(name, pat) or fnmatch.fnmatch(posix, pat):
            return True
    return False


def _iter_files():
    for path in ROOT.rglob('*'):
        if not path.is_file():
            continue
        rel = path.relative_to(ROOT)
        if _skip_dir(rel):
            continue
        if _skip_file(rel):
            continue
        top = rel.parts[0]
        if top in INCLUDE_ROOT_FILES or top in RUNTIME_DIRS:
            yield rel


def main() -> int:
    meta = _read_metadata()
    version = meta.get('general', 'version').strip()
    DIST.mkdir(exist_ok=True)
    zip_path = DIST / f'{PLUGIN_DIR}-{version}.zip'
    files = sorted(_iter_files())
    if not files:
        raise SystemExit('No files selected for the package.')

    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for rel in files:
            zf.write(ROOT / rel, arcname=f'{PLUGIN_DIR}/{rel.as_posix()}')

    size_mb = zip_path.stat().st_size / (1024 * 1024)
    print(f'Wrote {zip_path}')
    print(f'Files: {len(files)}')
    for rel in files:
        print(f'  {rel.as_posix()}')
    print(f'Size:  {size_mb:.2f} MB (limit 25 MB)')
    if size_mb >= 25:
        raise SystemExit('Package exceeds the 25 MB plugins.qgis.org limit.')
    print('Upload: https://plugins.qgis.org/plugins/upload/')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
