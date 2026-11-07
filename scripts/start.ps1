param([int]$Port=8765)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $projectRoot
$pythonExe = Join-Path $projectRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $pythonExe)) {
    python -m venv .venv
    & $pythonExe -m pip install -r requirements.txt
    if ($LASTEXITCODE -ne 0) { throw 'Python dependency installation failed' }
}
if (-not (Test-Path -LiteralPath 'node_modules\vite\bin\vite.js')) {
    $pnpmCmd = Get-Command pnpm -ErrorAction SilentlyContinue
    if ($pnpmCmd) { & $pnpmCmd.Source install } else { npm install }
    if ($LASTEXITCODE -ne 0) { throw 'Install Node dependencies with npm install or pnpm install first' }
}
node node_modules/typescript/bin/tsc --noEmit
if ($LASTEXITCODE -ne 0) { throw 'TypeScript check failed' }
node node_modules/vite/bin/vite.js build
if ($LASTEXITCODE -ne 0) { throw 'Frontend build failed' }
Write-Output "FleetOps: http://127.0.0.1:$Port"
& $pythonExe -m uvicorn backend.app:app --host 127.0.0.1 --port $Port
