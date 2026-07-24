<#
.SYNOPSIS
    WAL-E Installer for Windows (PowerShell).

.DESCRIPTION
    Installs WAL-E as a CLI tool, Cursor skill, Claude Code skill, or MCP server.
    This is the native Windows counterpart to install.sh. Run it from PowerShell:

        powershell -ExecutionPolicy Bypass -File .\install.ps1 --cli

.NOTES
    Requires Python 3.10+ (https://python.org) on PATH or the 'py' launcher.
#>

[CmdletBinding()]
param(
    [switch]$cursor,
    [switch]$claude,
    [switch]$mcp,
    [switch]$cli,
    [switch]$all,
    [switch]$uninstall,
    [Alias("h")][switch]$help
)

$ErrorActionPreference = "Stop"
$ScriptDir = $PSScriptRoot
$WaleVersion = "0.1.0"

function Write-Banner {
    Write-Host ""
    Write-Host " __        __           ||       |_____"  -ForegroundColor Cyan
    Write-Host " \ \      / /    / \    ||        | ____|" -ForegroundColor Cyan
    Write-Host "  \ \ /\ / /    / _ \   ||        | |__"   -ForegroundColor Cyan
    Write-Host "   \ V  V /    / ___ \  ||        |  __|"  -ForegroundColor Cyan
    Write-Host "    \_/\_/    /_/   \_\ ||____    |_|____"  -ForegroundColor Cyan
    Write-Host "    Well-Architected Lakehouse Evaluator" -ForegroundColor DarkGray
    Write-Host ""
}

function Write-Info    { param([string]$Msg) Write-Host "[i] $Msg" -ForegroundColor Blue }
function Write-Success { param([string]$Msg) Write-Host "[ok] $Msg" -ForegroundColor Green }
function Write-Warn    { param([string]$Msg) Write-Host "[!] $Msg" -ForegroundColor Yellow }
function Write-Err     { param([string]$Msg) Write-Host "[x] $Msg" -ForegroundColor Red }

function Show-Usage {
    Write-Banner
    Write-Host "Usage: .\install.ps1 [OPTION]"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  --cursor        Install WAL-E as a Cursor AI skill (copies rules + installs pip package)"
    Write-Host "  --claude        Install WAL-E as a Claude Code skill (copies SKILL.md to `$HOME\.codex\skills\)"
    Write-Host "  --mcp           Register WAL-E as an MCP server for Claude Code / AI Dev Kit"
    Write-Host "  --cli           Install WAL-E CLI only (auto-detects Python 3.10+, uses python -m pip)"
    Write-Host "  --all           Install everything: CLI + Cursor + Claude + MCP"
    Write-Host "  --uninstall     Remove WAL-E from all integration points"
    Write-Host "  -h, --help      Show this help message"
    Write-Host ""
    Write-Host "Examples:"
    Write-Host "  .\install.ps1 --cli          # Just the wal-e command"
    Write-Host "  .\install.ps1 --cursor       # Cursor users: skill + CLI"
    Write-Host "  .\install.ps1 --all          # Everything"
    Write-Host ""
}

# ============================================================================
# Find a Python interpreter that satisfies WAL-E's >=3.10 requirement.
#
# Prefer the 'py' launcher (the Windows standard) pinned to specific versions,
# then fall back to python / python3 on PATH. Returns the ABSOLUTE interpreter
# path (sys.executable) so every later step uses the same 3.10+ interpreter and
# never a stray/older 'pip'.
# ============================================================================
function Find-Python {
    $candidates = @(
        @("py", "-3.13"), @("py", "-3.12"), @("py", "-3.11"), @("py", "-3.10"),
        @("py", "-3"), @("python"), @("python3")
    )
    foreach ($candidate in $candidates) {
        $exe = $candidate[0]
        if (-not (Get-Command $exe -ErrorAction SilentlyContinue)) { continue }
        $verArgs = @()
        if ($candidate.Count -gt 1) { $verArgs = $candidate[1..($candidate.Count - 1)] }
        try {
            & $exe @verArgs -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" 2>$null
            if ($LASTEXITCODE -eq 0) {
                $resolved = (& $exe @verArgs -c "import sys; print(sys.executable)" 2>$null)
                if ($resolved) { return $resolved.Trim() }
            }
        } catch { }
    }
    return $null
}

function Install-Cli {
    Write-Info "Installing WAL-E CLI..."

    $py = Find-Python
    if (-not $py) {
        Write-Err "No Python >=3.10 found (WAL-E requires Python 3.10+)."
        Write-Err "Install it from https://python.org (check 'Add python.exe to PATH'), then re-run install.ps1"
        exit 1
    }
    Write-Info "Using $(& $py --version 2>&1) at $py"

    & $py -m pip --version *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Warn "pip is not available for $py; bootstrapping it with ensurepip..."
        & $py -m ensurepip --upgrade *> $null
        if ($LASTEXITCODE -ne 0) {
            Write-Err "Could not bootstrap pip. Run manually: `"$py`" -m ensurepip --upgrade"
            exit 1
        }
    }

    & $py -m pip install -e "$ScriptDir"
    if ($LASTEXITCODE -ne 0) {
        Write-Err "Failed to install WAL-E. Try manually: `"$py`" -m pip install -e `"$ScriptDir`""
        return
    }
    Write-Success "WAL-E CLI installed successfully."

    if (Get-Command wal-e -ErrorAction SilentlyContinue) {
        Write-Success "wal-e command is available: $((Get-Command wal-e).Source)"
    } else {
        Write-Warn "wal-e isn't on your PATH yet. Run it directly with: `"$py`" -m wal_e"
        Write-Warn "Or add your user Scripts directory to PATH (find it with: `"$py`" -m site --user-base)"
    }
}

function Install-Cursor {
    Write-Info "Installing WAL-E Cursor skill..."
    Install-Cli

    $targetDir = if ($env:CURSOR_PROJECT_DIR) { $env:CURSOR_PROJECT_DIR } else { (Get-Location).Path }
    $rulesDir = Join-Path $targetDir ".cursor\rules"
    New-Item -ItemType Directory -Force -Path $rulesDir | Out-Null

    $ruleSrc = Join-Path $ScriptDir ".cursor\rules\wal-e-assessment.md"
    $ruleDst = Join-Path $rulesDir "wal-e-assessment.md"

    if (Test-Path $ruleSrc) {
        Copy-Item -Force $ruleSrc $ruleDst
        Write-Success "Cursor rule installed: $ruleDst"
    } else {
        Write-Warn "Rule source not found at $ruleSrc; skipping rule copy."
    }

    Write-Host ""
    Write-Success "WAL-E Cursor skill installed!"
    Write-Host "  Open Cursor in $targetDir and ask:"
    Write-Host "    `"Run a Well-Architected Lakehouse assessment on my workspace`""
    Write-Host ""
}

function Install-Claude {
    Write-Info "Installing WAL-E Claude Code skill..."
    Install-Cli

    $codexHome = if ($env:CODEX_HOME) { $env:CODEX_HOME } else { Join-Path $HOME ".codex" }
    $skillDir = Join-Path $codexHome "skills\wal-e"
    New-Item -ItemType Directory -Force -Path $skillDir | Out-Null

    $skillContent = @'
# WAL-E: Well-Architected Lakehouse Evaluator

## What is WAL-E?

WAL-E is an automated assessment tool that evaluates a Databricks workspace against the Well-Architected Lakehouse Framework. It scores best practices across 7 pillars and generates executive readout decks.

## When to Use

Use WAL-E when the user asks to:
- Assess a Databricks workspace
- Run a Well-Architected Lakehouse review
- Generate assessment reports or readout decks
- Evaluate governance, security, performance, or cost posture

## Commands

```
wal-e assess --profile <PROFILE> --output ./assessment-results --format all
wal-e validate --profile <PROFILE>
wal-e assess --interactive --profile <PROFILE>
wal-e report --input ./assessment-results --format pptx html csv
```

## Prerequisites

- Databricks CLI configured with a profile (`databricks configure --profile <name>`)
- Read-only workspace access (admin recommended for full coverage)
'@
    Set-Content -Path (Join-Path $skillDir "SKILL.md") -Value $skillContent -Encoding UTF8
    Write-Success "Claude Code skill installed: $(Join-Path $skillDir 'SKILL.md')"
    Write-Host ""
}

function Install-Mcp {
    Write-Info "Registering WAL-E as MCP server..."
    Install-Cli

    # Resolve the interpreter so the MCP server launches with the same 3.10+
    # Python WAL-E was installed into. Use forward slashes so the path is valid
    # inside the JSON payload regardless of shell quoting.
    $py = Find-Python
    if (-not $py) { $py = "python" }
    $pyJson = $py -replace '\\', '/'
    $serverPath = (Join-Path $ScriptDir "mcp\server.py") -replace '\\', '/'
    $mcpJson = "{`"command`": `"$pyJson`", `"args`": [`"$serverPath`"]}"

    if (-not (Get-Command claude -ErrorAction SilentlyContinue)) {
        Write-Warn "Claude CLI not found. Manual MCP registration:"
        Write-Host "  claude mcp add-json wal-e '$mcpJson'" -ForegroundColor Cyan
        return
    }

    claude mcp add-json wal-e "$mcpJson" 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Success "WAL-E registered as MCP server."
    } else {
        Write-Warn "MCP registration may require manual setup. Run:"
        Write-Host "  claude mcp add-json wal-e '$mcpJson'" -ForegroundColor Cyan
    }
    Write-Host ""
}

function Uninstall-Wale {
    Write-Info "Uninstalling WAL-E..."

    $py = Find-Python
    if ($py) {
        & $py -m pip uninstall -y wal-e 2>$null
        if ($LASTEXITCODE -eq 0) { Write-Success "WAL-E pip package removed." }
        else { Write-Warn "WAL-E pip package not found or already removed." }
    } else {
        Write-Warn "No Python found; skipping pip uninstall."
    }

    $cursorRule = Join-Path (Get-Location).Path ".cursor\rules\wal-e-assessment.md"
    if (Test-Path $cursorRule) {
        Remove-Item -Force $cursorRule
        Write-Success "Cursor rule removed: $cursorRule"
    }

    $codexHome = if ($env:CODEX_HOME) { $env:CODEX_HOME } else { Join-Path $HOME ".codex" }
    $skillDir = Join-Path $codexHome "skills\wal-e"
    if (Test-Path $skillDir) {
        Remove-Item -Recurse -Force $skillDir
        Write-Success "Claude Code skill removed: $skillDir"
    }

    if (Get-Command claude -ErrorAction SilentlyContinue) {
        claude mcp remove wal-e 2>$null
        Write-Success "MCP server deregistered (if it was registered)."
    }

    Write-Success "WAL-E uninstalled."
}

# ============================================================================
# Main
# ============================================================================
$actionCount = @($cursor, $claude, $mcp, $cli, $all, $uninstall).Where({ $_ }).Count

if ($help -or $actionCount -eq 0) {
    Show-Usage
    exit 0
}
if ($actionCount -gt 1) {
    Write-Err "Choose a single action (e.g. --cli OR --all), not several at once."
    Show-Usage
    exit 1
}

Write-Banner

if ($all) {
    Install-Cli; Write-Host ""; Install-Cursor; Write-Host ""; Install-Claude; Write-Host ""; Install-Mcp
}
elseif ($cursor)    { Install-Cursor }
elseif ($claude)    { Install-Claude }
elseif ($mcp)       { Install-Mcp }
elseif ($cli)       { Install-Cli }
elseif ($uninstall) { Uninstall-Wale }

Write-Host ""
Write-Host "Done! WAL-E v$WaleVersion" -ForegroundColor Green
