# Automated-Ticket-Booking-AI-Agent

# Commands

- `python -m venv .venv`
- `powershell.exe -noprofile -executionpolicy bypass -file .\.venv\Scripts\activate.ps1`
- `pip install -r requirements.txt`

## Utility Commands

- Linux : `find . | grep -E "(__pycache__|\.pyc$)" | xargs rm -rf`
- Windows : `Get-ChildItem -Path . -Recurse -Include '__pycache__', '*.pyc' | Remove-Item -Recurse -Force`

## Initial Plan

- Develop an API Wrapper for TNSTC
- Define Natural Language Conversational requirement and User interaction flows
- Identify security constraints, Mode of payment etc
- Gather Detailed User preferences
- Assess the Implementation points for Human-in-the-loop

## Architecture

- Design Architecture covering AI, backend booking engine and UI
- Create Data models for User, Booking, and Payment
- Implement Error handling and logging mechanisms
- Develop natural language parsing with LLM to convert ambiguous requests into structured queries.
- Implement multi-turn context persistence for smooth conversational flow.
