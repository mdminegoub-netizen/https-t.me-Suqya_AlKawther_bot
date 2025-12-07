Suqya_AlKawther_bot
====================

## Development setup

1. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set the required environment variables (bot token, webhook host, admin IDs, and any Firebase credentials).
4. Run the bot locally:
   ```bash
   python bot.py
   ```

Use `.env` or your hosting provider's secret store to keep sensitive values out of version control. Temporary files, IDE settings, and caches are now ignored by default via `.gitignore`.
