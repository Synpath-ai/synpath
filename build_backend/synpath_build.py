"""The build backend: maturin, with a plain answer when Rust is missing.

pip only builds Synpath from source on a platform with no prebuilt wheel
(Linux, macOS and Windows on 64-bit machines all have one). Building needs a
Rust toolchain. Without one, maturin's own error is a cargo failure deep in
pip's output; this checks first and says what to do instead.

Rust installed in this session but not yet on PATH (the usual state right
after rustup, before a new terminal) is found in ~/.cargo/bin and used.
"""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import maturin

MESSAGE = """
==============================================================================
 synpath: no prebuilt wheel for this platform, so pip is building it from
 source, and that needs Rust, which is not installed.

 Install Rust (one command, about a minute):

     curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

 (Windows: download rustup-init.exe from https://rustup.rs)

 Then open a new terminal and run the install again:

     pip install synpath
==============================================================================
"""


def _need_rust() -> None:
    if shutil.which("cargo"):
        return
    home_cargo = Path.home() / ".cargo" / "bin"
    if (home_cargo / ("cargo.exe" if os.name == "nt" else "cargo")).exists():
        os.environ["PATH"] = f"{home_cargo}{os.pathsep}{os.environ.get('PATH', '')}"
        return
    sys.stderr.write(MESSAGE)
    raise SystemExit(1)


def get_requires_for_build_wheel(config_settings=None):
    _need_rust()
    return maturin.get_requires_for_build_wheel(config_settings)


def get_requires_for_build_editable(config_settings=None):
    _need_rust()
    return maturin.get_requires_for_build_editable(config_settings)


def get_requires_for_build_sdist(config_settings=None):
    return maturin.get_requires_for_build_sdist(config_settings)


def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    _need_rust()
    return maturin.prepare_metadata_for_build_wheel(metadata_directory, config_settings)


def prepare_metadata_for_build_editable(metadata_directory, config_settings=None):
    _need_rust()
    return maturin.prepare_metadata_for_build_editable(metadata_directory, config_settings)


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    _need_rust()
    return maturin.build_wheel(wheel_directory, config_settings, metadata_directory)


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    _need_rust()
    return maturin.build_editable(wheel_directory, config_settings, metadata_directory)


def build_sdist(sdist_directory, config_settings=None):
    return maturin.build_sdist(sdist_directory, config_settings)
