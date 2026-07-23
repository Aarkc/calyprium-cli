"""Bundled Claude Code agent skills for the Calyprium CLI.

The skill sources live in the ``skills/`` subdirectory and are shipped as
package data. The CLI copies them into ``~/.claude/skills/`` on first run
(see ``calyprium skills`` / the auto-install in ``calyprium.py``).
"""
from pathlib import Path

SKILLS_DIR = Path(__file__).resolve().parent / "skills"

__all__ = ["SKILLS_DIR"]
