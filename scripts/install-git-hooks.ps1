param()

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

git config core.hooksPath .githooks
Write-Host "core.hooksPath=.githooks"
Write-Host "Installed hooks:"
Write-Host "  post-merge -> scripts/sync-submodules.sh"
Write-Host "  post-checkout -> scripts/sync-submodules.sh"
