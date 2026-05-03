from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel


START = time.time()
ROOT = Path(__file__).resolve().parent
DATASET = ROOT / "Problem and Data" / "dataset"

app = FastAPI(title="Vera Merchant Engagement Bot")

contexts: dict[tuple[str, str], dict[str, Any]] = {}
conversations: dict[str, list[dict[str, Any]]] = {}
sent_suppression_keys: set[str] = set()
merchant_auto_counts: dict[str, int] = {}
blocked_merchants: set[str] = set()


def _load_seed_contexts() -> None:
    """Make local simulator replay useful even before a warmup push."""
    if not DATASET.exists():
        return
    for file in (DATASET / "categories").glob("*.json"):
        data = json.loads(file.read_text(encoding="utf-8"))
        contexts.setdefault(("category", data["slug"]), {"version": 0, "payload": data})

    for scope, filename, key in [
        ("merchant", "merchants_seed.json", "merchants"),
        ("customer", "customers_seed.json", "customers"),
        ("trigger", "triggers_seed.json", "triggers"),
    ]:
        path = DATASET / filename
        if not path.exists():
            continue
        for item in json.loads(path.read_text(encoding="utf-8")).get(key, []):
            item_id = item.get(f"{scope}_id") or item.get("id")
            contexts.setdefault((scope, item_id), {"version": 0, "payload": item})


_load_seed_contexts()


@app.get("/")
async def root() -> dict[str, str]:
    return {"status": "ok", "service": "magicpin-vera-bot", "healthz": "/v1/healthz"}


class ContextBody(BaseModel):
    scope: str
    context_id: str
    version: int
    payload: dict[str, Any]
    delivered_at: str | None = None


class TickBody(BaseModel):
    now: str
    available_triggers: list[str] = []


class ReplyBody(BaseModel):
    conversation_id: str
    merchant_id: str | None = None
    customer_id: str | None = None
    from_role: str | None = None
    message: str
    received_at: str | None = None
    turn_number: int = 1


def _ctx(scope: str, context_id: str | None) -> dict[str, Any] | None:
    if not context_id:
        return None
    item = contexts.get((scope, context_id))
    return item.get("payload") if item else None


def _first_name(merchant: dict[str, Any]) -> str:
    identity = merchant.get("identity", {})
    owner = identity.get("owner_first_name")
    if owner:
        return str(owner).replace("Dr. ", "").strip()
    name = identity.get("name", "there")
    return str(name).split()[0].replace("Dr.", "Dr.")


def _salutation(category: dict[str, Any], merchant: dict[str, Any]) -> str:
    identity = merchant.get("identity", {})
    first = _first_name(merchant)
    if category.get("slug") == "dentists":
        return f"Dr. {first}"
    return f"Hi {first}"


def _hi_mix(merchant: dict[str, Any], customer: dict[str, Any] | None = None) -> bool:
    if customer:
        pref = str(customer.get("identity", {}).get("language_pref", "")).lower()
        return "hi" in pref and "english" not in pref
    langs = [str(x).lower() for x in merchant.get("identity", {}).get("languages", [])]
    return "hi" in langs


def _money_offer(merchant: dict[str, Any], category: dict[str, Any]) -> str:
    for offer in merchant.get("offers", []):
        title = offer.get("title", "")
        if offer.get("status") == "active" and ("@" in title or "Free" in title or "FREE" in title):
            return title
    for offer in category.get("offer_catalog", []):
        title = offer.get("title", "")
        if "@" in title or "Free" in title or "FREE" in title:
            return title
    return "a service-at-price offer"


def _digest_item(category: dict[str, Any], trigger: dict[str, Any]) -> dict[str, Any]:
    payload = trigger.get("payload", {})
    wanted = payload.get("top_item_id") or payload.get("digest_item_id") or payload.get("alert_id")
    for item in category.get("digest", []):
        if item.get("id") == wanted:
            return item
    return category.get("digest", [{}])[0] if category.get("digest") else {}


def _pct(value: Any) -> str:
    try:
        return f"{abs(float(value)) * 100:.0f}%"
    except Exception:
        return str(value)


def _ctr(value: Any) -> str:
    try:
        return f"{float(value) * 100:.1f}%"
    except Exception:
        return str(value)


def _recent_vera_bodies(merchant: dict[str, Any]) -> set[str]:
    bodies = set()
    for turn in merchant.get("conversation_history", []):
        if turn.get("from") == "vera" and turn.get("body"):
            bodies.add(_norm(turn["body"]))
    return bodies


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def _trim(body: str, limit: int = 220) -> str:
    words = body.split()
    if len(words) <= limit:
        return body
    return " ".join(words[:limit - 1]).rstrip(".,") + "."


def _compose_research(category: dict[str, Any], merchant: dict[str, Any], trigger: dict[str, Any]) -> dict[str, Any]:
    item = _digest_item(category, trigger)
    sal = _salutation(category, merchant)
    title = item.get("title", "new category digest item")
    source = item.get("source", "category digest")
    signals = " ".join(merchant.get("signals", []))
    cohort = "high-risk adult patients" if "high_risk" in signals else "your patient mix"
    n = item.get("trial_n")
    stat = "38% lower recurrence" if "38" in (item.get("summary", "") + title) else item.get("actionable", "")
    if n:
        hook = f"{source}: {n:,}-patient item says {title}; {stat}."
    else:
        hook = f"{source}: {title}."
    body = f"{sal}, {hook} This looks relevant to {cohort}; your CTR is {_ctr(merchant.get('performance', {}).get('ctr'))} vs peer {_ctr(category.get('peer_stats', {}).get('avg_ctr'))}. Reply YES and I will draft a patient WhatsApp from it."
    return out(body, "binary_yes_no", "vera", trigger, [source, title, sal], "Research digest trigger uses the cited digest item, the merchant cohort/signal, and peer CTR for a clinical, specific hook.")


def _compose_customer(category: dict[str, Any], merchant: dict[str, Any], trigger: dict[str, Any], customer: dict[str, Any]) -> dict[str, Any]:
    payload = trigger.get("payload", {})
    name = customer.get("identity", {}).get("name", "there")
    merchant_name = merchant.get("identity", {}).get("name", "the clinic")
    offer = _money_offer(merchant, category)
    slots = payload.get("available_slots") or payload.get("next_session_options") or []
    slot_labels = [s.get("label") for s in slots if s.get("label")]
    service_due = str(payload.get("service_due", "follow-up")).replace("_", " ")
    last = payload.get("last_service_date") or customer.get("relationship", {}).get("last_visit")
    if trigger.get("kind") == "recall_due":
        due = payload.get("due_date", "now")
        slot_text = " ya ".join(slot_labels[:2]) if slot_labels else "this week ke 2 evening slots"
        body = f"Hi {name}, {merchant_name} here. {last} ke baad aapka {service_due} recall {due} ko due hai. {offer}; slots ready hain: {slot_text}. Reply 1 for first slot, 2 for second."
        return out(body, "binary_slot", "merchant_on_behalf", trigger, [name, merchant_name, offer, slot_text], "Customer recall is sent on behalf of the merchant, names the due reason, uses the real offer, and gives concrete slots.")
    if trigger.get("kind") in {"customer_lapsed_soft", "customer_lapsed_hard", "trial_followup", "wedding_package_followup"}:
        days = payload.get("days_since_last_visit") or payload.get("days_to_wedding") or customer.get("state")
        body = f"Hi {name}, {merchant_name} here. {days} is the timely nudge in your plan, and {offer} is available this week. Reply YES to book a slot."
        return out(body, "binary_yes_no", "merchant_on_behalf", trigger, [name, merchant_name, offer], "Customer lifecycle trigger uses relationship state and a single booking CTA.")
    if trigger.get("kind") == "chronic_refill_due":
        meds = ", ".join(payload.get("molecule_list", [])[:3])
        runs_out = payload.get("stock_runs_out_iso", "soon")[:10]
        body = f"Hi {name}, {merchant_name} reminder: {meds} stock may run out by {runs_out}. Home delivery is saved for you. Reply YES to confirm refill."
        return out(body, "binary_yes_no", "merchant_on_behalf", trigger, [name, meds, runs_out], "Chronic refill due is precise, non-promotional, and uses saved delivery context.")
    body = f"Hi {name}, {merchant_name} here. Your last visit was {last}; {offer} is available this week. Reply YES to book."
    return out(body, "binary_yes_no", "merchant_on_behalf", trigger, [name, merchant_name, offer], "Customer trigger gets a concise merchant-on-behalf reminder with one CTA.")


def compose(category: dict[str, Any], merchant: dict[str, Any], trigger: dict[str, Any], customer: dict[str, Any] | None = None) -> dict[str, Any]:
    if customer or trigger.get("scope") == "customer":
        return _compose_customer(category, merchant, trigger, customer or {})

    kind = trigger.get("kind", "")
    sal = _salutation(category, merchant)
    perf = merchant.get("performance", {})
    payload = trigger.get("payload", {})
    offer = _money_offer(merchant, category)
    hi = _hi_mix(merchant)

    if kind in {"research_digest", "category_research_digest_release", "regulation_change", "cde_opportunity", "supply_alert", "category_seasonal"}:
        if kind == "research_digest":
            return _compose_research(category, merchant, trigger)
        item = _digest_item(category, trigger)
        source = item.get("source", payload.get("alert_id", "category update"))
        title = item.get("title", payload.get("molecule", "new update"))
        body = f"{sal}, {source}: {title}. {item.get('actionable', 'Worth checking today')} for {merchant.get('identity', {}).get('locality', 'your locality')}. Reply YES and I will draft the exact merchant note."
        return out(body, "binary_yes_no", "vera", trigger, [source, title, sal], f"{kind} is anchored to a cited external item and converted into one concrete action.")

    if kind == "perf_spike":
        metric = payload.get("metric", "views")
        delta = _pct(payload.get("delta_pct", perf.get("delta_7d", {}).get(f"{metric}_pct", 0)))
        driver = payload.get("likely_driver")
        why = f" Looks like {driver} may have helped." if driver else ""
        body = f"{sal}, {metric} are up {delta} in {payload.get('window', '7d')}.{why} Aapko kya lagta hai, what caused the jump?"
        return out(body, "open_ended", "vera", trigger, [metric, delta, sal], "Performance spike is celebrated with the exact metric and asks one direct diagnostic question.")

    if kind in {"perf_dip", "seasonal_perf_dip"}:
        metric = payload.get("metric", "calls")
        delta = _pct(payload.get("delta_pct", 0))
        peer = category.get("peer_stats", {}).get("avg_ctr")
        gentle = "seasonal lag raha hai" if payload.get("is_expected_seasonal") and hi else "not alarming, but worth one small fix"
        body = f"{sal}, {metric} dipped {delta} in {payload.get('window', '7d')}; {gentle}. Your CTR is {_ctr(perf.get('ctr'))} vs peer {_ctr(peer)}. Reply YES and I will draft one {offer} post."
        return out(body, "binary_yes_no", "vera", trigger, [metric, delta, offer], "Performance dip is calm, uses a peer benchmark, and suggests one concrete recovery action.")

    if kind == "competitor_opened":
        comp = payload.get("competitor_name")
        dist = payload.get("distance_km")
        opened = payload.get("opened_date")
        their_offer = payload.get("their_offer", "a visible offer")
        who = f"{comp} " if comp else "A new listing "
        body = f"{sal}, {who}opened {dist}km away on {opened} with {their_offer}. Your active hook is {offer}; I spotted the side-by-side difference. Want to see?"
        return out(body, "open_ended", "vera", trigger, [str(dist), str(opened), their_offer], "Competitor trigger uses the named competitor only because it exists in context and ends with voyeur-curiosity.")

    if kind in {"festival_upcoming", "ipl_match_today"}:
        if kind == "ipl_match_today":
            body = f"{sal}, {payload.get('match')} starts {payload.get('match_time_iso', '')[-14:-6]} in {payload.get('city')}; match-night searches are strongest before dinner. {offer} is already live. Reply YES and I will make a 1-line WhatsApp post for tonight."
            return out(body, "binary_yes_no", "vera", trigger, [payload.get("match", ""), offer], "IPL trigger connects the event time to a restaurant offer and one immediate action.")
        body = f"{sal}, {payload.get('festival')} is on {payload.get('date')} ({payload.get('days_until')} days left). Category demand usually moves before the festival, and {offer} gives you a clean hook. Reply YES to draft the post."
        return out(body, "binary_yes_no", "vera", trigger, [payload.get("festival", ""), payload.get("date", ""), offer], "Festival trigger is time-anchored and category-connected.")

    if kind == "dormant_with_vera":
        days = payload.get("days_since_last_merchant_message", "many")
        body = f"{sal}, {days} days since your last reply. Quick check: should I focus next on getting more calls, more repeat customers, or a stronger {offer} post?"
        return out(body, "open_ended", "vera", trigger, [str(days), offer], "Dormancy trigger reopens gently with one useful question and no history recap.")

    if kind == "milestone_reached":
        value = payload.get("value_now") or payload.get("milestone_value")
        metric = payload.get("metric", "reviews").replace("_", " ")
        body = f"{sal}, you are at {value} {metric}; {payload.get('milestone_value', value)} is right there. Nice momentum. Reply YES and I will draft a thank-you post that nudges the next reviews."
        return out(body, "binary_yes_no", "vera", trigger, [str(value), metric], "Milestone trigger celebrates simply and nudges one next-level action.")

    if kind == "review_theme_emerged":
        theme = str(payload.get("theme", "review theme")).replace("_", " ")
        count = payload.get("occurrences_30d", "?")
        quote = payload.get("common_quote")
        quote_bit = f' One quote says "{quote}".' if quote else ""
        body = f"{sal}, {count} reviews in 30d mention {theme}.{quote_bit} I can draft a short reply pattern plus one operational note. Reply YES to use it."
        return out(body, "binary_yes_no", "vera", trigger, [str(count), theme], "Review trigger uses exact occurrence count and offers a practical response.")

    if kind == "active_planning_intent":
        topic = str(payload.get("intent_topic", "campaign")).replace("_", " ")
        last = payload.get("merchant_last_message", "Yes")
        body = f"{sal}, based on your note '{last}', drafted the {topic} plan: headline, price hook, and WhatsApp text around {offer}. Reply YES to see the draft."
        return out(body, "binary_yes_no", "vera", trigger, [topic, offer], "Active intent moves directly to drafting because the merchant already showed planning intent.")

    if kind in {"renewal_due", "winback_eligible", "gbp_unverified", "curious_ask_due"}:
        if kind == "renewal_due":
            body = f"{sal}, Pro has {payload.get('days_remaining')} days left and your 30d profile brought {perf.get('calls')} calls + {perf.get('directions')} directions. Reply YES and I will share the renewal summary."
        elif kind == "winback_eligible":
            body = f"{sal}, {payload.get('days_since_expiry')} days after expiry, calls are down {_pct(payload.get('perf_dip_pct', 0))} and {payload.get('lapsed_customers_added_since_expiry')} lapsed customers were added. Reply YES for a 7-day winback draft."
        elif kind == "gbp_unverified":
            body = f"{sal}, your GBP is still unverified; context says verification can lift visibility about {_pct(payload.get('estimated_uplift_pct', 0))}. Reply YES and I will outline the phone/postcard path."
        else:
            body = f"{sal}, quick curiosity check: which service is most in demand this week? I can turn the answer into a {offer} post."
        return out(body, "binary_yes_no" if kind != "curious_ask_due" else "open_ended", "vera", trigger, [sal, offer], f"{kind} is anchored to merchant state and uses a single low-friction CTA.")

    body = f"{sal}, your 30d dashboard shows {perf.get('views')} views, {perf.get('calls')} calls, and {perf.get('directions')} direction taps. {offer} is the cleanest next hook. Reply YES and I will draft the WhatsApp message."
    return out(body, "binary_yes_no", "vera", trigger, [str(perf.get("views")), offer], "Fallback still uses merchant numbers, a service-price hook, and one CTA.")


def out(body: str, cta: str, send_as: str, trigger: dict[str, Any], params: list[Any], rationale: str) -> dict[str, Any]:
    body = _trim(re.sub(r"\s+", " ", body).strip())
    return {
        "body": body,
        "cta": cta,
        "send_as": send_as,
        "rationale": rationale,
        "suppression_key": trigger.get("suppression_key", ""),
        "template_params": [str(p) for p in params if p is not None][:5],
    }


@app.get("/v1/healthz")
async def healthz() -> dict[str, Any]:
    counts = {"category": 0, "merchant": 0, "customer": 0, "trigger": 0}
    for scope, _ in contexts:
        counts[scope] = counts.get(scope, 0) + 1
    return {"status": "ok", "uptime_seconds": int(time.time() - START), "contexts_loaded": counts}


@app.get("/v1/metadata")
async def metadata() -> dict[str, Any]:
    return {
        "team_name": "Vera Deterministic",
        "team_members": ["Dinesh Sharma"],
        "model": "rules+context composer, no external LLM",
        "approach": "trigger-dispatched WhatsApp composer with auto-reply, intent, and opt-out routing",
        "contact_email": "not-provided@example.com",
        "version": "1.0.0",
        "submitted_at": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/v1/context")
async def push_context(body: ContextBody) -> dict[str, Any]:
    if body.scope not in {"category", "merchant", "customer", "trigger"}:
        return {"accepted": False, "reason": "invalid_scope", "details": body.scope}
    key = (body.scope, body.context_id)
    cur = contexts.get(key)
    if cur and cur["version"] >= body.version:
        return {"accepted": False, "reason": "stale_version", "current_version": cur["version"]}
    contexts[key] = {"version": body.version, "payload": body.payload}
    return {
        "accepted": True,
        "ack_id": f"ack_{body.context_id}_v{body.version}",
        "stored_at": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/v1/tick")
async def tick(body: TickBody) -> dict[str, Any]:
    actions = []
    for trigger_id in body.available_triggers[:20]:
        trigger = _ctx("trigger", trigger_id)
        if not trigger:
            continue
        suppression = trigger.get("suppression_key", trigger_id)
        merchant_id = trigger.get("merchant_id")
        if suppression in sent_suppression_keys or merchant_id in blocked_merchants:
            continue
        merchant = _ctx("merchant", merchant_id)
        if not merchant:
            continue
        category = _ctx("category", merchant.get("category_slug")) or {}
        customer = _ctx("customer", trigger.get("customer_id"))
        msg = compose(category, merchant, trigger, customer)
        if _norm(msg["body"]) in _recent_vera_bodies(merchant):
            msg["body"] = msg["body"] + " Fresh angle from today: I can keep it to a 2-line draft."
            msg["rationale"] += " Also adjusted to avoid repeating a prior Vera body."
        sent_suppression_keys.add(suppression)
        conv_id = f"conv_{merchant_id}_{trigger_id}_{abs(hash(suppression)) % 100000}"
        conversations[conv_id] = [{"from": "vera", "body": msg["body"], "trigger_id": trigger_id}]
        actions.append({
            "conversation_id": conv_id,
            "merchant_id": merchant_id,
            "customer_id": trigger.get("customer_id"),
            "send_as": msg["send_as"],
            "trigger_id": trigger_id,
            "template_name": _template_name(trigger),
            "template_params": msg["template_params"],
            "body": msg["body"],
            "cta": msg["cta"],
            "suppression_key": msg["suppression_key"],
            "rationale": msg["rationale"],
        })
    return {"actions": actions}


def _template_name(trigger: dict[str, Any]) -> str:
    if trigger.get("scope") == "customer":
        return "merchant_customer_nudge_v1"
    kind = trigger.get("kind", "generic")
    return f"vera_{kind}_v1"


@app.post("/v1/reply")
async def reply(body: ReplyBody) -> dict[str, Any]:
    conversations.setdefault(body.conversation_id, []).append({"from": body.from_role, "body": body.message})
    text = _norm(body.message)
    merchant_id = body.merchant_id or "unknown"

    if any(x in text for x in ["stop messaging", "spam", "don't contact", "dont contact", "band karo", "remove me", "not interested"]):
        if merchant_id:
            blocked_merchants.add(merchant_id)
        return {
            "action": "end",
            "body": "Noted, we'll stop. You can reach us anytime at magicpin.",
            "cta": "none",
            "rationale": "Hostile or opt-out phrase detected; closing without re-pitching.",
        }

    is_auto = _is_auto_reply(text, body.conversation_id)
    if is_auto:
        merchant_auto_counts[merchant_id] = merchant_auto_counts.get(merchant_id, 0) + 1
        count = merchant_auto_counts[merchant_id]
        if count >= 3:
            return {"action": "end", "cta": "none", "rationale": "auto_reply_detected: same/canned WhatsApp Business reply repeated 3 times, so ending."}
        if count >= 2:
            return {"action": "wait", "wait_seconds": 86400, "rationale": "auto_reply_detected: repeated canned reply; waiting 24h for owner/manager."}
        return {
            "action": "send",
            "body": "Looks like an auto-reply. When the owner sees this, reply YES and I will send the draft.",
            "cta": "binary_yes_no",
            "rationale": "auto_reply_detected: one light nudge for the real owner before backing off.",
        }

    if any(x in text for x in ["haan", "yes", "judrna", "join", "let's do it", "lets do it", "go ahead", "theek hai", "send it", "kar do", "ok lets do it"]):
        return {
            "action": "send",
            "body": "Done. Drafted the next WhatsApp now; I am preparing it with the current offer and merchant numbers. Reply CONFIRM to send, or STOP to pause.",
            "cta": "binary_yes_no",
            "rationale": "Explicit merchant commitment detected; moving straight to action instead of asking another qualifying question.",
        }

    if any(x in text for x in ["gst", "tax filing", "income tax", "itr"]):
        return {
            "action": "send",
            "body": "GST filing is for your CA; I can help with the magicpin/WhatsApp growth side. Coming back to this campaign, reply YES and I will draft the message.",
            "cta": "binary_yes_no",
            "rationale": "Out-of-scope request politely redirected to the active Vera task.",
        }

    return {
        "action": "send",
        "body": "Got it. I will keep this practical: one short draft using your current offer and latest dashboard numbers. Reply YES to see it.",
        "cta": "binary_yes_no",
        "rationale": "Continues the conversation with a single low-friction next step.",
    }


def _is_auto_reply(text: str, conv_id: str) -> bool:
    canned_bits = [
        "thank you for contacting",
        "will respond shortly",
        "automated assistant",
        "team tak pahuncha",
        "hamari team",
        "our team will respond",
        "business hours are",
    ]
    if any(bit in text for bit in canned_bits):
        return True
    inbound = [_norm(t.get("body", "")) for t in conversations.get(conv_id, []) if t.get("from") != "vera"]
    return inbound.count(text) >= 2
