#!/usr/bin/env bash
# ============================================================================
# WAL-E Installer
# Installs WAL-E as a CLI tool, Cursor skill, Claude Code skill, or MCP server
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WALE_VERSION="0.1.0"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
RESET='\033[0m'

banner() {
  echo -e "${CYAN}"
  echo " __        __     _   ||  ___   _____"
  echo " \\ \\      / /    / \\   ||        | ____|"
  echo "  \\ \\ /\\ / /    / _ \\  ||        | |__"
  echo "   \\ V  V /    / ___ \\ ||        |  __|"
  echo "    \\_/\\_/    /_/   \\_\\||____ ___|_|____"
  echo -e "${RESET}    ${DIM}Well-Architected Lakehouse Evaluator${RESET}"
  echo ""
}

info()    { echo -e "${BLUE}ℹ${RESET} $*"; }
success() { echo -e "${GREEN}✓${RESET} $*"; }
warn()    { echo -e "${YELLOW}⚠${RESET} $*"; }
error()   { echo -e "${RED}✗${RESET} $*" >&2; }

usage() {
  banner
  echo -e "${BOLD}Usage:${RESET} ./install.sh [OPTIONS]"
  echo ""
  echo -e "${BOLD}Options:${RESET}"
  echo "  --cursor        Install WAL-E as a Cursor AI skill (copies rules + installs pip package)"
  echo "  --claude        Install WAL-E as a Claude Code skill (copies SKILL.md to ~/.codex/skills/)"
  echo "  --mcp           Register WAL-E as an MCP server for Claude Code / AI Dev Kit"
  echo "  --cli           Install WAL-E CLI only (pip install -e .)"
  echo "  --all           Install everything: CLI + Cursor + Claude + MCP"
  echo "  --uninstall     Remove WAL-E from all integration points"
  echo "  -h, --help      Show this help message"
  echo ""
  echo -e "${BOLD}Examples:${RESET}"
  echo "  ./install.sh --cursor       # Cursor users: skill + CLI"
  echo "  ./install.sh --claude       # Claude Code users: skill + CLI"
  echo "  ./install.sh --cli          # Just the wal-e command"
  echo "  ./install.sh --all          # Everything"
  echo ""
}

# ============================================================================
# Install CLI (pip install -e .)
# ============================================================================
install_cli() {
  info "Installing WAL-E CLI via pip..."
  if command -v pip3 &> /dev/null; then
    PIP=pip3
  elif command -v pip &> /dev/null; then
    PIP=pip
  else
    error "pip not found. Please install Python 3.10+ first."
    exit 1
  fi

  # Check Python version
  PYTHON_VERSION=$($PIP --version 2>/dev/null | grep -oP 'python \K[0-9]+\.[0-9]+' || echo "unknown")

  # Try standard install first, fall back to --break-system-packages
  if $PIP install -e "$SCRIPT_DIR" 2>/dev/null; then
    success "WAL-E CLI installed successfully."
  elif $PIP install --break-system-packages -e "$SCRIPT_DIR" 2>/dev/null; then
    success "WAL-E CLI installed successfully (system packages mode)."
  else
    error "Failed to install WAL-E. Try: pip install -e $SCRIPT_DIR"
    return 1
  fi

  # Verify
  if command -v wal-e &> /dev/null; then
    success "wal-e command is available: $(which wal-e)"
  else
    warn "wal-e may not be on your PATH yet. Try: python -m wal_e"
  fi
}

# ============================================================================
# Install Cursor Skill
# ============================================================================
install_cursor() {
  info "Installing WAL-E Cursor skill..."

  # 1. Install CLI first
  install_cli

  # 2. Find the target Cursor project directory
  # If running from inside a Cursor project, install there; otherwise use CWD
  TARGET_DIR="${CURSOR_PROJECT_DIR:-$(pwd)}"
  CURSOR_RULES_DIR="$TARGET_DIR/.cursor/rules"

  mkdir -p "$CURSOR_RULES_DIR"

  # 3. Copy the Cursor rule file
  RULE_SRC="$SCRIPT_DIR/.cursor/rules/wal-e-assessment.md"
  RULE_DST="$CURSOR_RULES_DIR/wal-e-assessment.md"

  if [[ -f "$RULE_SRC" ]]; then
    cp "$RULE_SRC" "$RULE_DST"
    success "Cursor rule installed: $RULE_DST"
  else
    # Generate the rule inline if source not found
    warn "Rule source not found at $RULE_SRC, generating..."
    cat > "$RULE_DST" << 'RULEEOF'
---
description: WAL-E Databricks Well-Architected Lakehouse assessment tool usage and integration
globs: "**/*.py, **/wal_e/**/*"
alwaysApply: false
---

# WAL-E Assessment Tool Rule

## When to Use WAL-E

Apply this rule and suggest WAL-E when the user:

- Asks about **Databricks assessment**, **architecture review**, or **Well-Architected Lakehouse (WAL) framework**
- Wants to **evaluate a Databricks workspace** against best practices
- Needs an **automated scan** of governance, security, cost, or operational maturity
- Asks for **readout decks**, **assessment reports**, or **pillar scores** for a Databricks lakehouse

## How to Run WAL-E

```bash
# Full assessment (collect → score → report)
wal-e assess

# With options
wal-e assess --profile customer-workspace --output ./results --format md csv pptx audit

# Validate workspace access
wal-e validate --profile customer-workspace

# Show setup guide
wal-e setup --guide
```

## Interpreting Results

- **Score 0:** Not implemented (Critical)
- **Score 1:** Partially implemented (Action needed)
- **Score 2:** Fully implemented (Maintain)
- **Maturity:** Beginning < 0.5 | Developing 0.5–1.25 | Established 1.25–1.75 | Optimized > 1.75
RULEEOF
    success "Cursor rule generated: $RULE_DST"
  fi

  echo ""
  success "WAL-E Cursor skill installed!"
  echo ""
  echo -e "  ${BOLD}How to use in Cursor:${RESET}"
  echo -e "    Open Cursor in ${CYAN}$TARGET_DIR${RESET} and ask:"
  echo -e "    ${DIM}\"Run a Well-Architected Lakehouse assessment on my workspace\"${RESET}"
  echo ""
}

# ============================================================================
# Install Claude Code Skill
# ============================================================================
install_claude() {
  info "Installing WAL-E Claude Code skill..."

  # 1. Install CLI first
  install_cli

  # 2. Set up the skill directory
  CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
  SKILL_DIR="$CODEX_HOME/skills/wal-e"
  mkdir -p "$SKILL_DIR"

  # 3. Create SKILL.md for Claude Code
  cat > "$SKILL_DIR/SKILL.md" << SKILLEOF
# WAL-E: Well-Architected Lakehouse Evaluator

## What is WAL-E?

WAL-E is an automated assessment tool that evaluates a Databricks workspace against the Well-Architected Lakehouse Framework. It scores 129 best practices across 7 pillars and generates executive readout decks.

## When to Use

Use WAL-E when the user asks to:
- Assess a Databricks workspace
- Run a Well-Architected Lakehouse review
- Generate assessment reports or readout decks
- Evaluate governance, security, performance, or cost posture

## Commands

\`\`\`bash
# Run full assessment
wal-e assess --profile <PROFILE> --output ./assessment-results --format all

# Validate access first
wal-e validate --profile <PROFILE>

# Interactive mode (for customer sessions)
wal-e assess --interactive --profile <PROFILE>

# Re-generate reports from cached data
wal-e report --input ./assessment-results --format pptx html csv
\`\`\`

## Prerequisites

- Databricks CLI configured with a profile (\`databricks configure --profile <name>\`)
- Read-only workspace access (admin recommended for full coverage)

## Output Files

- \`WAL_Assessment_Readout.md\` - Full detailed report
- \`WAL_Assessment_Scores.csv\` - Scored best practices
- \`WAL_Assessment_Presentation.pptx\` - Executive deck
- \`WAL_Assessment_Presentation.html\` - Browser presentation
- \`WAL_Assessment_Audit_Report.md\` - Evidence trail
SKILLEOF

  success "Claude Code skill installed: $SKILL_DIR/SKILL.md"
  echo ""
  echo -e "  ${BOLD}How to use in Claude Code:${RESET}"
  echo -e "    1. Open Claude Code inside the wal-e/ project directory"
  echo -e "    2. Ask naturally (no slash command needed):"
  echo -e "       ${DIM}\"Run a WAL-E assessment on my Databricks workspace\"${RESET}"
  echo -e "    Claude Code reads CLAUDE.md from the project root automatically."
  echo ""
}

# ============================================================================
# Install MCP Server
# ============================================================================
install_mcp() {
  info "Registering WAL-E as MCP server..."

  # 1. Install CLI first
  install_cli

  # 2. Check for claude CLI
  if ! command -v claude &> /dev/null; then
    warn "Claude CLI not found. Providing manual registration instructions."
    echo ""
    echo -e "  ${BOLD}Manual MCP registration:${RESET}"
    echo -e "  ${CYAN}claude mcp add-json wal-e '{\"command\": \"python3\", \"args\": [\"$SCRIPT_DIR/mcp/server.py\"]}'${RESET}"
    echo ""
    return 0
  fi

  # 3. Register with Claude
  MCP_JSON="{\"command\": \"python3\", \"args\": [\"$SCRIPT_DIR/mcp/server.py\"]}"
  claude mcp add-json wal-e "$MCP_JSON" 2>/dev/null && \
    success "WAL-E registered as MCP server." || \
    warn "MCP registration may require manual setup. Run:"

  echo ""
  echo -e "  ${BOLD}Available MCP tools:${RESET}"
  echo "    wal_e_assess    - Run full assessment"
  echo "    wal_e_collect   - Collect workspace data"
  echo "    wal_e_score     - Score against framework"
  echo "    wal_e_report    - Generate reports"
  echo "    wal_e_validate  - Validate workspace access"
  echo ""
}

# ============================================================================
# Uninstall
# ============================================================================
uninstall() {
  info "Uninstalling WAL-E..."

  # Remove pip package
  if pip3 uninstall -y wal-e 2>/dev/null || pip uninstall -y wal-e 2>/dev/null; then
    success "WAL-E pip package removed."
  else
    warn "WAL-E pip package not found or already removed."
  fi

  # Remove Cursor rule (from CWD)
  CURSOR_RULE="$(pwd)/.cursor/rules/wal-e-assessment.md"
  if [[ -f "$CURSOR_RULE" ]]; then
    rm "$CURSOR_RULE"
    success "Cursor rule removed: $CURSOR_RULE"
  fi

  # Remove Claude Code skill
  CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
  SKILL_DIR="$CODEX_HOME/skills/wal-e"
  if [[ -d "$SKILL_DIR" ]]; then
    rm -rf "$SKILL_DIR"
    success "Claude Code skill removed: $SKILL_DIR"
  fi

  # Deregister MCP
  if command -v claude &> /dev/null; then
    claude mcp remove wal-e 2>/dev/null && \
      success "MCP server deregistered." || true
  fi

  success "WAL-E uninstalled."
}

# ============================================================================
# Main
# ============================================================================
main() {
  if [[ $# -eq 0 ]]; then
    usage
    exit 0
  fi

  banner

  case "${1:-}" in
    --cursor)
      install_cursor
      ;;
    --claude)
      install_claude
      ;;
    --mcp)
      install_mcp
      ;;
    --cli)
      install_cli
      ;;
    --all)
      install_cli
      echo ""
      install_cursor
      echo ""
      install_claude
      echo ""
      install_mcp
      ;;
    --uninstall)
      uninstall
      ;;
    -h|--help)
      usage
      ;;
    *)
      error "Unknown option: $1"
      usage
      exit 1
      ;;
  esac

  echo ""
  echo -e "${GREEN}${BOLD}Done!${RESET} WAL-E v${WALE_VERSION}"
}

main "$@"
