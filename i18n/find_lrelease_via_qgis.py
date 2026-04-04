# -*- coding: utf-8 -*-
"""Imprime o caminho absoluto de lrelease.exe (Qt do QGIS). Uso: python find_lrelease_via_qgis.py [raiz_QGIS]"""
import os
import sys

EXE_NAMES = ('lrelease.exe', 'lrelease')


def _iter_qlib_dirs():
    try:
        from qgis.PyQt.QtCore import QLibraryInfo, QT_VERSION_STR
    except ImportError:
        return
    major = int(QT_VERSION_STR.split('.')[0])
    if major >= 6:
        Lib = QLibraryInfo.LibraryPath
        for name in ('LibraryExecutables', 'Executables', 'BinariesPath', 'LibraryExecutablesPath'):
            if hasattr(Lib, name):
                try:
                    p = QLibraryInfo.path(getattr(Lib, name))
                    if p:
                        yield p
                except (TypeError, AttributeError):
                    continue
        try:
            prefix = QLibraryInfo.path(QLibraryInfo.LibraryPath.PrefixPath)
            if prefix:
                yield os.path.join(prefix, 'bin')
                yield os.path.join(prefix, 'libexec')
        except (TypeError, AttributeError):
            pass
        return
    for loc_name in ('BinariesPath', 'HostBinariesPath'):
        try:
            loc = getattr(QLibraryInfo, loc_name)
            p = QLibraryInfo.location(loc)
            if p:
                yield p
        except (TypeError, AttributeError):
            continue
    try:
        prefix = QLibraryInfo.location(QLibraryInfo.PrefixPath)
        if prefix:
            yield os.path.join(prefix, 'bin')
            yield os.path.join(prefix, 'libexec')
    except (TypeError, AttributeError):
        pass


def _first_existing():
    for base in _iter_qlib_dirs():
        if not base or not os.path.isdir(base):
            continue
        for name in EXE_NAMES:
            full = os.path.join(base, name)
            if os.path.isfile(full):
                return full
    return None


def _walk_lrelease(root, max_depth=12):
    if not root or not os.path.isdir(root):
        return None
    root = os.path.abspath(root)
    for dirpath, dirnames, filenames in os.walk(root):
        rel = os.path.relpath(dirpath, root)
        depth = 0 if rel == '.' else rel.count(os.sep) + 1
        if depth > max_depth:
            dirnames[:] = []
            continue
        for fn in filenames:
            if fn.lower() in ('lrelease.exe', 'lrelease'):
                return os.path.join(dirpath, fn)
    return None


def main():
    qgis_root = sys.argv[1] if len(sys.argv) > 1 else None
    p = _first_existing()
    if p:
        print(p)
        return 0
    if qgis_root:
        p = _walk_lrelease(qgis_root)
        if p:
            print(p)
            return 0
    return 1


if __name__ == '__main__':
    sys.exit(main())
