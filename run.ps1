param(
    [Parameter(Position=0)]
    [string]$Target = "help"
)

$env:PYTHONUTF8 = "1"
$VENV_PY = ".venv\Scripts\python.exe"
$VENV_DBT = ".venv\Scripts\dbt.exe"

# Uu tien dung Python / dbt trong .venv neu co
$PY = if (Test-Path $VENV_PY) { $VENV_PY } else { "python" }
$DBT = if (Test-Path $VENV_DBT) { $VENV_DBT } else { "dbt" }

switch ($Target.ToLower()) {
    "setup" {
        if (-not (Test-Path ".venv")) {
            python -m venv .venv
        }
        & $PY -m pip install -q --upgrade pip
        & $PY -m pip install -q -r requirements.txt
        & $PY seed\generate.py
        Write-Host "`n  xong. Buoc tiep theo: .\run.ps1 pipeline roi .\run.ps1 verify" -ForegroundColor Green
    }
    "seed" {
        & $PY seed\generate.py
    }
    "seed-extra" {
        & $PY seed\generate.py --extra
        & $PY tools\explain.py --save-baseline
    }
    "pipeline" {
        & $PY tools\run_pipeline.py
    }
    "verify" {
        & $PY tools\verify.py
    }
    "quick" {
        & $PY tools\verify.py --runs 1
    }
    "explain" {
        & $PY tools\explain.py
    }
    "plan" {
        & $PY tools\explain.py --plan
    }
    "compact" {
        & $PY tools\compact.py
    }
    "dbt-test" {
        & $DBT test --project-dir dbt --profiles-dir dbt --target-path dbt\target --log-path dbt\logs
    }
    "dbt-docs" {
        & $DBT docs generate --project-dir dbt --profiles-dir dbt --target-path dbt\target --log-path dbt\logs
        & $DBT docs serve --project-dir dbt --profiles-dir dbt --target-path dbt\target
    }
    "crash-test" {
        & $PY tools\crash_test.py
    }
    "reset" {
        Remove-Item -Force -ErrorAction SilentlyContinue warehouse.duckdb, warehouse.duckdb.wal
        Write-Host "  kho da xoa." -ForegroundColor Yellow
    }
    "clean" {
        Remove-Item -Recurse -Force -ErrorAction SilentlyContinue warehouse.duckdb, warehouse.duckdb.wal, dbt\target, dbt\logs, data\crash
        Write-Host "  da don dep." -ForegroundColor Yellow
    }
    Default {
        Write-Host "`n  LAB 17 - Data Pipeline Engineering (PowerShell Runner)`n" -ForegroundColor Cyan
        Write-Host "  Cach dung: .\run.ps1 <lenh>`n"
        Write-Host "    .\run.ps1 setup        - venv + thu vien + sinh du lieu"
        Write-Host "    .\run.ps1 pipeline     - chay duong ong 1 luot (14 ngay)"
        Write-Host "    .\run.ps1 verify       - xoa kho, chay 3 luot, in bang cham diem (dung lien tuc)"
        Write-Host "    .\run.ps1 quick        - nhu verify nhung chi 1 luot"
        Write-Host "    .\run.ps1 reset        - xoa kho DuckDB"
        Write-Host "    .\run.ps1 clean        - xoa kho + log + thu muc tam"
        Write-Host "    .\run.ps1 dbt-test     - chay dbt test"
        Write-Host "    .\run.ps1 seed-extra   - sinh du lieu bai mo rong"
        Write-Host "    .\run.ps1 explain      - do rows scanned cua dashboard"
        Write-Host "    .\run.ps1 plan         - explain + in cay EXPLAIN ANALYZE"
        Write-Host "    .\run.ps1 compact      - chay tools/compact.py"
        Write-Host "    .\run.ps1 crash-test   - kich ban consumer bi crash`n"
    }
}
