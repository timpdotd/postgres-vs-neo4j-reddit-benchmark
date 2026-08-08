@echo off
echo ==========================================
echo Benchmark Cleanup Utility
echo ==========================================

echo.
echo [*] Cleaning generated CSV files...
del /q "data\posts.csv" 2>nul
del /q "data\posts_pg.csv" 2>nul
del /q "data\links.csv" 2>nul
del /q "data\links_pg.csv" 2>nul
del /q "data\subreddits.csv" 2>nul
echo [+] CSVs removed (original TSV dataset preserved).

echo.
echo [*] Cleaning logs...
del /q "logs\*.log" 2>nul
echo [+] Logs removed.

echo.
echo [*] Cleaning benchmark results and reports...
del /q "data\benchmark_results.json" 2>nul
del /q "data\etl_metrics.json" 2>nul
del /q "data\storage_metrics.json" 2>nul
del /q "data\concurrency_results.json" 2>nul
del /q "data\scalability_results.json" 2>nul
del /q "extended_analysis.md" 2>nul
echo [+] Results and reports removed.

echo.
set /p wipe_db="Do you also want to wipe the databases (Docker volumes)? (y/N): "
if /i "%wipe_db%"=="y" (
    echo [*] Wiping Docker volumes...
    cd docker
    docker-compose down -v
    cd ..
    echo [+] Databases wiped.
) else (
    echo [*] Databases preserved.
)

echo.
echo ==========================================
echo Cleanup Complete!
echo ==========================================
pause
