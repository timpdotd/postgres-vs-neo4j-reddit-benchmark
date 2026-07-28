#!/usr/bin/env python3
"""
setup_env.py — Turnkey Cross-Platform Virtual Environment & Dependency Setup.

Course: Data Management (2025/2026) | Author: Davide Timperi (1950722)

This script automates the creation of an isolated Python virtual environment (.venv)
and installs all required project dependencies without cluttering the global OS environment
or git tracking.

Usage:
    python scripts/setup_env.py
"""

import os
import sys
import venv
import subprocess
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration & Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_DIR = PROJECT_ROOT / ".venv"
REQUIREMENTS_FILE = PROJECT_ROOT / "requirements.txt"


def print_header(msg: str) -> None:
    print("\n" + "=" * 70)
    print(f"  {msg}")
    print("=" * 70)


def check_python_version() -> None:
    """Ensure system Python is at least 3.10."""
    print(f"[*] Checking system Python version... ({sys.version.split()[0]})")
    if sys.version_info < (3, 10):
        print("[!] ERROR: This project requires Python 3.10 or higher.")
        print(f"    Current version is {sys.version.split()[0]}. Please upgrade Python.")
        sys.exit(1)
    print("    [+] Python version OK.")


def get_venv_python() -> Path:
    """Return the executable path for Python inside the virtual environment."""
    if sys.platform == "win32":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def get_venv_pip() -> Path:
    """Return the executable path for pip inside the virtual environment."""
    if sys.platform == "win32":
        return VENV_DIR / "Scripts" / "pip.exe"
    return VENV_DIR / "bin" / "pip"


def create_virtual_environment() -> None:
    """Create .venv in the project root if it does not already exist."""
    print(f"[*] Checking virtual environment directory at: {VENV_DIR}")
    if VENV_DIR.exists():
        print("    [!] Virtual environment (.venv) already exists. Re-using existing directory.")
    else:
        print("    [*] Creating new Python virtual environment (.venv)...")
        try:
            venv.create(VENV_DIR, with_pip=True, clear=False)
            print("    [+] Virtual environment created successfully.")
        except Exception as exc:
            print(f"[!] ERROR creating virtual environment: {exc}")
            sys.exit(1)


def install_dependencies() -> None:
    """Upgrade pip and install dependencies from requirements.txt."""
    pip_exe = get_venv_pip()
    python_exe = get_venv_python()

    if not pip_exe.exists():
        print(f"[!] ERROR: Could not find pip inside virtual environment at {pip_exe}")
        sys.exit(1)

    if not REQUIREMENTS_FILE.exists():
        print(f"[!] ERROR: requirements.txt not found at {REQUIREMENTS_FILE}")
        sys.exit(1)

    print("\n[*] Upgrading pip inside virtual environment...")
    try:
        subprocess.run(
            [str(python_exe), "-m", "pip", "install", "--upgrade", "pip"],
            check=True,
            cwd=PROJECT_ROOT,
        )
        print("    [+] pip upgrade complete.")
    except subprocess.CalledProcessError as exc:
        print(f"[!] WARNING: Failed to upgrade pip: {exc}")

    print(f"\n[*] Installing project dependencies from {REQUIREMENTS_FILE.name}...")
    try:
        subprocess.run(
            [str(python_exe), "-m", "pip", "install", "-r", str(REQUIREMENTS_FILE)],
            check=True,
            cwd=PROJECT_ROOT,
        )
        print("\n    [+] All project dependencies installed successfully!")
    except subprocess.CalledProcessError as exc:
        print(f"\n[!] ERROR: Dependency installation failed with exit code {exc.returncode}")
        sys.exit(1)


def print_activation_instructions() -> None:
    """Display OS-specific instructions on how to activate the venv."""
    print_header("🎉 Environment Setup Complete!")
    print("\nTo activate the virtual environment and run the benchmark pipeline, use:")
    
    if sys.platform == "win32":
        print("  [PowerShell]:")
        print(f"    .\\.venv\\Scripts\\Activate.ps1")
        print("\n  [Command Prompt (cmd.exe)]:")
        print(f"    .\\.venv\\Scripts\\activate.bat")
    else:
        print("  [Bash / zsh / macOS / Linux]:")
        print(f"    source .venv/bin/activate")

    print("\nOnce activated, you can run the ETL loader and benchmarks:")
    print("    python scripts/load_data.py")
    print("    python scripts/run_benchmarks.py")
    print("    jupyter notebook notebooks/results_analysis.ipynb\n")
    print("=" * 70 + "\n")


def main() -> None:
    print_header("PostgreSQL vs. Neo4j Benchmark — Environment Setup")
    check_python_version()
    create_virtual_environment()
    install_dependencies()
    print_activation_instructions()


if __name__ == "__main__":
    main()
