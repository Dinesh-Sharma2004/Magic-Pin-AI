# Vera Deterministic Composer

This submission implements Vera as a deterministic FastAPI bot in `bot.py`. It stores judge-pushed category, merchant, customer, and trigger contexts, then dispatches by `trigger.kind` to produce concise WhatsApp messages with a specific hook, category-appropriate voice, one CTA, and the original suppression key.

The bot does not call an external LLM, so it is fast, repeatable, and safe for synthetic merchant/customer payloads. It includes seed-data fallbacks for local replay, but judge-pushed versions replace those contexts through `/v1/context`.

Key choices:
- Trigger-specific templates for research digests, recall reminders, performance spikes/dips, competitor openings, festivals, dormancy, milestones, reviews, renewals, supply alerts, and customer lifecycle nudges.
- Clinical categories use cited sources and avoid hype/guarantees; salons/gyms/restaurants/pharmacies use practical service+price framing where available.
- `/v1/reply` handles opt-outs, WhatsApp Business auto-replies, explicit commitment/intent handoff, and out-of-scope questions.
- Repetition protection checks prior Vera bodies in merchant history and adjusts the final line when needed.

Run locally:

```bash
python -m uvicorn bot:app --host 0.0.0.0 --port 8080
```

On this Windows machine the working interpreter is:

```powershell
& 'C:\Program Files\Python313\python.exe' -m uvicorn bot:app --host 0.0.0.0 --port 8080
```

Deploy on Render:

1. Push this `D:\MagicPin` folder to a GitHub repo.
2. In Render, create a new Blueprint or Web Service from that repo.
3. Render will use `render.yaml`; the public base URL will expose:
   - `GET /v1/healthz`
   - `POST /v1/context`
   - `POST /v1/tick`
   - `POST /v1/reply`
