# -*- coding: utf-8 -*-
"""Exceções não fatais — substitui try/except/pass (Bandit B110)."""


def ignore_exc(exc=None):
    """Consome fallback intencional (Qt5/Qt6, QSettings legado, I/O)."""
    return exc
