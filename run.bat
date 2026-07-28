@echo off
REM =============================================================================
REM Turnkey Master Pipeline Shortcut (Batch / CMD)
REM Course: Data Management (2025/2026) | Author: Davide Timperi (1950722)
REM =============================================================================
if exist .venv\Scripts\python.exe (
    echo Executing master benchmark pipeline inside .venv...
    .venv\Scripts\python.exe scripts\run_pipeline.py
) else (
    echo [!] Virtual environment not found. Running setup first...
    python scripts\setup_env.py
    if not errorlevel 1 (
        .venv\Scripts\python.exe scripts\run_pipeline.py
    )
)
