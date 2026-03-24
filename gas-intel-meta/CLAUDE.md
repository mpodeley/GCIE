# gas-intel-meta — Orchestration & Dashboard

## Role
Meta-project for orchestration, weekly reporting, and dashboard.

## Contents
- scripts/run_overnight.sh — tmux automation for nightly AutoResearch sessions
- scripts/weekly_report.sh — Calls Qwen 122B (port 8081) with all results.tsv for executive summary
- scripts/check_datalake.sh — Verifies Data Lake snapshot freshness before running engines
- dashboard/ — HTML dashboard with consolidated results
- docker/ — Container configs if needed

## Agent model assignment
- Port 8080: Qwen 3.5 35B-A3B — Fast iterations, coding agent (Cline/aider)
- Port 8081: Qwen 3.5 122B-A10B — Weekly report generation, complex analysis

## Nightly session flow
1. check_datalake.sh — verify fresh snapshot
2. Verify program.md is current
3. Start llama-server port 8080 (Qwen 35B)
4. tmux: launch Cline/aider pointing to target engine repo
5. Agent iterates autonomously: read → modify model.py → execute → evaluate → commit/revert → repeat
6. Morning: review results.tsv + git log, decide what to merge to main

## Guardrails
- Max 200 experiments per session
- Scope lock: agent ONLY modifies model.py
- No real execution: simulation only
- Human checkpoint every 50 experiments OR >10% improvement
- Fallback naive models in each engine if optimized model degrades
