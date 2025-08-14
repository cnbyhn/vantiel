# hybridgm_bundle.py — Single-file HybridGM engine (strict schemas, fixed journal validation)
# Build marker for preflight version checks:
__BUILD__ = "2025-08-14-class-onboarding-v2"

from __future__ import annotations
import os, re, json, hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

# ------------------------
# Paths & constants
# ------------------------
SAVE_PATH = "/mnt/data/save.json"
JOURNAL_PATH = "/mnt/data/saves/journal.ndjson"
SAVES_DIR = Path("/mnt/data/saves"); SAVES_DIR.mkdir(parents=True, exist_ok=True)

SCHEMA_PATH = Path("/mnt/data/save_schema.v1.2.json")             # external, required
JOURNAL_SCHEMA_PATH = Path("/mnt/data/journal_schema.v1.0.json")  # external, required

DEFAULT_FILES_BASE_URL = ""  # optionally set env FILES_BASE_URL to publish links

# ------------------------
# Helpers (hash + basic validate)
# ------------------------
def _sorted_json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def compute_save_hash(save: dict) -> str:
    """Stable hash ignoring the hash field itself."""
    try:
        clone = json.loads(json.dumps(save))
        flags = clone.get("flags", {})
        integ = flags.get("integrity", {})
        if "save_hash" in integ:
            integ["save_hash"] = ""
            flags["integrity"] = integ
            clone["flags"] = flags
        return hashlib.sha256(_sorted_json(clone).encode("utf-8")).hexdigest()
    except Exception:
        return hashlib.sha256(repr(save).encode("utf-8")).hexdigest()

def basic_validate(save: dict) -> List[str]:
    """Strictly require external save_schema.v1.2.json and validate top-level presence order list."""
    issues: List[str] = []
    if not SCHEMA_PATH.exists():
        raise RuntimeError("Save schema missing: " + str(SCHEMA_PATH))
    try:
        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        required = schema["schema"]["top_level_order"]
        for key in required:
            if key not in save:
                issues.append(f"missing:{key}")
    except Exception as e:
        raise RuntimeError(f"Save schema invalid or unreadable: {e}")
    return issues

# ------------------------
# Save manager
# ------------------------
def ensure_dirs() -> None:
    Path(SAVE_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(JOURNAL_PATH).parent.mkdir(parents=True, exist_ok=True)

def _dump_json(obj: Dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)

def _load_json(path: str) -> Dict[str, Any]:
    p = Path(path)
    return json.loads(p.read_text(encoding="utf-8"))

def export_save(SAVE: Dict[str, Any]) -> Tuple[str, List[str]]:
    warnings: List[str] = []
    try:
        SAVE.setdefault("flags", {}).setdefault("integrity", {})
        SAVE["flags"]["integrity"]["save_hash"] = compute_save_hash(SAVE)
    except Exception as e:
        warnings.append(f"warn:hash_failed:{e}")
    blob = _dump_json(SAVE)
    return blob, warnings

def import_save_merge(path: str, current: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    warnings: List[str] = []
    incoming = _load_json(path)
    warnings.extend(basic_validate(incoming))
    warnings.extend(basic_validate(current))

    turn_in = int(incoming.get("turn", 0))
    turn_cur = int(current.get("turn", 0))

    def _concat_trim(log: list, cap: int = 10) -> list:
        if not isinstance(log, list): return []
        return log[-cap:] if len(log) > cap else log

    if turn_in > turn_cur:
        merged = incoming
        warnings.append(f"info:incoming_newer:{turn_in}>{turn_cur}")
    elif turn_in < turn_cur:
        merged = dict(current)
        for k, v in incoming.items():
            if k not in merged:
                merged[k] = v
        warnings.append(f"info:current_newer:{turn_cur}>{turn_in}")
    else:
        merged = dict(current)
        for k, v in incoming.items():
            merged[k] = v
        for k in ("dialogue_log", "turn_log"):
            dl = (current.get(k) or []) + (incoming.get(k) or [])
            merged[k] = _concat_trim(dl, cap=10 if k == "dialogue_log" else 50)
        warnings.append("info:merged_equal_turn")

    try:
        merged.setdefault("flags", {}).setdefault("integrity", {})
        merged["flags"]["integrity"]["save_hash"] = compute_save_hash(merged)
    except Exception as e:
        warnings.append(f"warn:hash_failed:{e}")
    return merged, warnings

def init_new_game_from_dropin(dropin_path: str) -> Tuple[Dict[str, Any], List[str]]:
    SAVE = _load_json(dropin_path)
    warnings: List[str] = basic_validate(SAVE)
    SAVE.setdefault("dialogue_log", [])
    SAVE.setdefault("turn_log", [])
    SAVE.setdefault("turn_tags", [])
    SAVE.setdefault("flags", {}).setdefault("integrity", {})
    return SAVE, warnings

def write_save_file(SAVE: Dict[str, Any], snapshot: bool = False) -> Tuple[str, List[str]]:
    ensure_dirs()
    blob, warnings = export_save(SAVE)
    Path(SAVE_PATH).write_text(blob, encoding="utf-8")
    if snapshot:
        snap = SAVES_DIR / f"snapshot-turn-{SAVE.get('turn','0')}.json"
        snap.write_text(blob, encoding="utf-8")
    return SAVE_PATH, warnings

def load_latest_save_or_none() -> Dict[str, Any] | None:
    p = Path(SAVE_PATH)
    if not p.exists(): return None
    return _load_json(SAVE_PATH)

# ------------------------
# Post-turn routine (with fixed journal validation)
# ------------------------
def _now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def _ensure_lists(obj: Dict[str, Any]) -> None:
    obj.setdefault("dialogue_log", [])
    obj.setdefault("turn_log", [])
    obj.setdefault("turn_tags", [])
    obj.setdefault("flags", {}).setdefault("integrity", {})

def end_turn(
    SAVE: Dict[str, Any],
    scene_ref: Optional[str],
    dialogue_lines: List[Dict[str, Any]],
    scene_tags: Optional[List[str]] = None,
    choices: Optional[List[str]] = None,
    choice_taken: Optional[int] = None,
    mode: str = "GM",
) -> Dict[str, Any]:
    _ensure_lists(SAVE)
    if mode.upper() != "IC":
        try:
            SAVE["turn"] = int(SAVE.get("turn", 0)) + 1
        except Exception:
            SAVE["turn"] = 1
    entry = {
        "turn": SAVE.get("turn", 0),
        "scene": scene_ref or "",
        "lines": dialogue_lines or [],
        "choice": choice_taken if (choice_taken is not None) else None,
        "tags": list(scene_tags or []),
    }
    SAVE["dialogue_log"].append(entry)
    if len(SAVE["dialogue_log"]) > 10:
        SAVE["dialogue_log"] = SAVE["dialogue_log"][-10:]
    SAVE["turn_log"].append({"turn": SAVE.get("turn", 0), "ref": scene_ref or ""})
    if scene_tags:
        known = set(SAVE.get("turn_tags", []) or [])
        for t in scene_tags:
            if t not in known:
                SAVE["turn_tags"].append(t); known.add(t)
    return SAVE

def write_save(SAVE: Dict[str, Any], snapshot: bool = True) -> str:
    ensure_dirs()
    p = Path(SAVE_PATH)
    try:
        SAVE.setdefault("flags", {}).setdefault("integrity", {})
        SAVE["flags"]["integrity"]["save_hash"] = compute_save_hash(SAVE)
    except Exception:
        pass
    blob = json.dumps(SAVE, ensure_ascii=False, indent=2)
    with p.open("w", encoding="utf-8") as f:
        f.write(blob); f.flush(); os.fsync(f.fileno())
    if snapshot:
        snap = SAVES_DIR / f"snapshot-turn-{SAVE.get('turn','0')}.json"
        with snap.open("w", encoding="utf-8") as f:
            f.write(blob)
    return str(p)

def append_journal(
    SAVE: Dict[str, Any],
    scene_ref: Optional[str],
    dialogue_lines: List[Dict[str, Any]],
    scene_tags: Optional[List[str]] = None,
    choices: Optional[List[str]] = None,
    choice_taken: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> str:
    # Ensure directory exists
    jpath = Path(JOURNAL_PATH); jpath.parent.mkdir(parents=True, exist_ok=True)

    # Build entry
    entry: Dict[str, Any] = {
        "turn": int(SAVE.get("turn", 0)),
        "timestamp": _now_iso(),
        "location": SAVE.get("loc"),
        "time": SAVE.get("time"),
        "scene_ref": scene_ref or None,
        "scene_tags": list(scene_tags or []),
        "dialogue": dialogue_lines or [],
        "choices": list(choices or []),
        "choice_taken": choice_taken if choice_taken is not None else None,
        "relationship_changes": [],
        "inventory_changes": [],
        "money_change": {},
        "hooks_added": [],
        "flags_set": [],
        "notes": "",
        "promises": [],
    }
    if extra: entry.update(extra)

    # Strict validation using your journal schema:
    if not JOURNAL_SCHEMA_PATH.exists():
        raise RuntimeError("Journal schema missing: " + str(JOURNAL_SCHEMA_PATH))

    journal_schema = json.loads(JOURNAL_SCHEMA_PATH.read_text(encoding="utf-8"))

    # 1) Root-level required only
    root_required = list(journal_schema.get("required", []))
    missing = [k for k in root_required if k not in entry or entry[k] in (None, "")]
    if missing:
        raise RuntimeError("Journal entry missing required fields per schema: " + ", ".join(missing))

    # 2) Dialogue item-level required (if defined)
    dlg_req = (journal_schema.get("properties", {})
               .get("dialogue", {})
               .get("items", {})
               .get("required", []) )
    if dlg_req:
        for i, line in enumerate(entry.get("dialogue") or []):
            for key in dlg_req:
                if key not in line or line[key] in (None, ""):
                    raise RuntimeError(f"Journal dialogue line {i} missing '{key}' per schema")

    # Write as NDJSON
    line = json.dumps(entry, ensure_ascii=False)
    with jpath.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    return str(jpath)

# ------------------------
# Footer helpers
# ------------------------
def _nonempty(p: Path) -> bool:
    try:
        return p.exists() and p.stat().st_size > 0
    except Exception:
        return False

def _build_url_from_base(path: str) -> Optional[str]:
    base = (os.getenv("FILES_BASE_URL", "") or DEFAULT_FILES_BASE_URL).rstrip("/")
    if not base: return None
    pth = Path(path)
    try:
        rel = pth.relative_to("/mnt/data")
    except Exception:
        rel = pth.name
    return f"{base}/{rel.as_posix()}"

def compose_footer() -> str:
    save_ok = _nonempty(Path(SAVE_PATH))
    j_ok = _nonempty(Path(JOURNAL_PATH))
    save_url = _build_url_from_base(SAVE_PATH) or f"sandbox:{SAVE_PATH}"
    journal_url = _build_url_from_base(JOURNAL_PATH) or f"sandbox:{JOURNAL_PATH}"
    lines: list[str] = []
    if save_url.startswith("http"):
        lines.append(f"[Download Save]({save_url})")
    else:
        lines.append(f"Download: {save_url}  <!-- set FILES_BASE_URL for public link -->")
    if journal_url.startswith("http"):
        lines.append(f"[Download Journal]({journal_url})")
    else:
        lines.append(f"Download Journal: {journal_url}  <!-- set FILES_BASE_URL for public link -->")
    if not (save_ok and j_ok):
        lines.append("**Save/Journal not written**")
    return "\n\n" + "\n".join(lines)

# ------------------------
# Engine glue with hard failure surfacing
# ------------------------
def persist_turn_and_footer(
    SAVE: Dict[str, Any],
    *, scene_ref: Optional[str],
    dialogue_lines: List[Dict[str, Any]],
    scene_tags: Optional[List[str]] = None,
    choices: Optional[List[str]] = None,
    choice_taken: Optional[int] = None,
    mode: str = "GM",
) -> str:
    # Wrap persistence to surface the real cause if something fails
    try:
        end_turn(SAVE, scene_ref=scene_ref, dialogue_lines=dialogue_lines, scene_tags=scene_tags or [], choices=choices or [], choice_taken=choice_taken, mode=mode)
        write_save(SAVE, snapshot=True)
        append_journal(SAVE, scene_ref=scene_ref, dialogue_lines=dialogue_lines, scene_tags=scene_tags or [], choices=choices or [], choice_taken=choice_taken)
    except Exception as e:
        raise RuntimeError(f"PERSISTENCE_FAILED: {e}")
    footer = compose_footer()
    assert "**Save/Journal not written**" not in footer, "PERSISTENCE_FAILED: files missing/empty after write"
    return footer

def prologue_turn(SAVE: Dict[str, Any], *, scene_ref: str, narration: str, choices: List[str], tags: Optional[List[str]] = None) -> str:
    dialogue_lines = [{"speaker":"Narrator","text": narration}]
    footer = persist_turn_and_footer(SAVE, scene_ref=scene_ref, dialogue_lines=dialogue_lines, scene_tags=tags or ["Prologue"], choices=choices, choice_taken=None, mode="GM")
    return footer

# ------------------------
# Import facade + New Game flow
# ------------------------
def ensure_engine_imports(reload: bool = False) -> Dict[str, Any]:
    mods = {
        "save_manager": {"export_save": export_save, "import_save_merge": import_save_merge,
                         "init_new_game_from_dropin": init_new_game_from_dropin,
                         "write_save_file": write_save_file, "load_latest_save_or_none": load_latest_save_or_none,
                         "SAVE_PATH": SAVE_PATH, "JOURNAL_PATH": JOURNAL_PATH},
        "post_turn_routine": {"end_turn": end_turn, "write_save": write_save, "append_journal": append_journal},
        "gm_output_helpers": {"compose_footer": compose_footer},
        "hybridgm_helpers": {"compute_save_hash": compute_save_hash, "basic_validate": basic_validate},
        "hybridgm_engine": {"persist_turn_and_footer": persist_turn_and_footer, "prologue_turn": prologue_turn},
        "end_turn": end_turn, "write_save": write_save, "append_journal": append_journal, "compose_footer": compose_footer,
    }
    return mods

def _pg_get(d: Dict[str, Any], path: str, default: Any=None) -> Any:
    cur: Any = d
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur

# --- Minimal SAVE template (Vantiel / Greyfen Marches / Ridgehaven) ---
def _minimal_save() -> Dict[str, Any]:
    return {
        "schema": "save.v1.2",
        "turn": 0,
        "time": "Morning",
        "loc": "Greyfen Forest Edge",
        "world": "Vantiel",
        "region": "Greyfen Marches",
        "town": "Ridgehaven",
        "obj": [],
        "party": {
            "You": {"name": "", "class": "", "LV": 1, "HP": 20, "STA": 10, "MaxHP": 20, "MaxSTA": 10, "XP": 0, "XP_to_next": 100},
            "Appa": {"present": None, "name": "Appa", "HP": 10, "STA": 10, "MaxHP": 10, "MaxSTA": 10},
            "members": [], "marching_order": ["You","Appa"]
        },
        "inventory": [],
        "money": {"gold": 0, "silver": 0, "copper": 0},
        "inv_delta": {"found": [], "spent": [], "consumed": [], "dropped": [], "equipped": [], "notes": []},
        "quests": [],
        "promises": [],
        "relationships": {},
        "hooks": [],
        "flags": {
            "origin": "Earth",
            "prologue": {"city": "", "attacker": "", "death": True, "completed": False},
            "gate_party_meet": False,
            "romance_intensity": "Cautious",
            "guild": {"rank":"Copper","rank_points":0,"rp_pending":0},
            "integrity": {"schema_migration":"v1.2","save_hash":""}
        },
        "crystals": {"I":0,"II":0,"III":0,"IV":0,"V":0},
        "position": {"town":"Ridgehaven","area":"Outskirts","node":"Greyfen Forest Edge"},
        "weather": "",
        "light": "",
        "since_short_rest": 0,
        "since_long_rest": 0,
        "day_count": 1,
        "turn_tags": [],
        "dialogue_log": [],
        "prev_turn": {"turn": 0, "ref": ""},
        "turn_log": [],
        "motifs": {"running_jokes": [], "motifs_summary": ""},
        "promises_summary": ""
    }

# --- Natural-language profile parsing (EN + TR) — "class" wording ---
_keyval_re = re.compile(r"^\s*(NAME|CLASS|DOG|CITY|CAUSE|ADIM|İSİM|ISIM|SINIF|ROL|KÖPEK|SEHIR|ŞEHİR|SEHIR|SEBEP|NEDEN)\s*:\s*(.+?)\s*$",
                        flags=re.IGNORECASE | re.MULTILINE)

def parse_profile_from_text(text: str) -> Dict[str, Any]:
    text = text or ""
    found: Dict[str, Any] = {}

    # 1) key:value blocks (EN + TR)
    for m in _keyval_re.finditer(text):
        k = m.group(1).lower()
        v = m.group(2).strip()
        if k in ("name","adim","isim","i̇sim","isım"):
            found["name"] = v
        elif k in ("class","sinif","rol","sınıf"):
            found["class"] = v
        elif k in ("dog","köpek"):
            lv = v.lower()
            found["appa_present"] = True if lv in ("yes","y","evet","true","1","var") else False if lv in ("no","n","hayır","hayir","false","0","yok") else None
        elif k in ("city","sehir","şehir","sehir"):
            found["city"] = v
        elif k in ("cause","sebep","neden"):
            found["attacker"] = v

    # 2) free-form English/Turkish
    # name
    m = re.search(r"\b(my name is|call me|i'm|i am)\s+([A-ZÇĞİÖŞÜ][\wçğıöşü'\-]+)", text, flags=re.IGNORECASE)
    if m and "name" not in found:
        found["name"] = m.group(2).strip()
    # class
    m = re.search(r"\b(my class is|i am|i\'m)\s+(a\s+)?([a-zçğıöşü\-\s]{3,40})\b", text, flags=re.IGNORECASE)
    if m and "class" not in found:
        cand = m.group(3).strip()
        found["class"] = cand
    # dog
    if "appa_present" not in found:
        if re.search(r"\b(with|along with|and)\s+my\s+dog\b|\bAppa\b", text, flags=re.IGNORECASE):
            found["appa_present"] = True
        elif re.search(r"\bno\s+dog\b|\b(I'?m|I am)\s+alone\b|yaln[ıi]z[ıi]m", text, flags=re.IGNORECASE):
            found["appa_present"] = False
    # city
    m = re.search(r"\bfrom\s+([A-ZÇĞİÖŞÜ][\wçğıöşü\-\s]+)", text, flags=re.IGNORECASE)
    if m and "city" not in found:
        found["city"] = m.group(1).strip()
    # cause
    lc = text.lower()
    if "attacker" not in found:
        if re.search(r"sokak köpe|stray dog|strays?", lc):
            found["attacker"] = "Strays"
        elif re.search(r"saldır|bıçak|stab|mugger|attacker|attack", lc):
            found["attacker"] = "Attacker"
        elif re.search(r"kaza|accident|crash|truck|car", lc):
            found["attacker"] = "Accident"

    return found

def apply_profile(save: Dict[str, Any], *, name: str, klass: str, appa_present: bool, city: str, attacker: str) -> Dict[str, Any]:
    save.setdefault("party", {}).setdefault("You", {})
    you = save["party"]["You"]
    if name is not None: you["name"] = str(name)
    if not klass: klass = you.get("class") or you.get("klass") or you.get("role") or ""
    you["class"] = str(klass)
    save["party"].setdefault("Appa", {})
    save["party"]["Appa"]["present"] = bool(appa_present) if appa_present is not None else save["party"]["Appa"].get("present", False)
    save.setdefault("flags", {}).setdefault("prologue", {})
    save["flags"]["prologue"]["city"] = city
    save["flags"]["prologue"]["attacker"] = attacker
    save["flags"]["prologue"]["death"] = True
    return save

# --- Onboarding & Prologue
def _cause_to_narration(save: Dict[str, Any]) -> str:
    city = _pg_get(save, "flags.prologue.city", "your city")
    cause = (_pg_get(save, "flags.prologue.attacker", "") or "").lower()
    has_dog = bool(_pg_get(save, "party.Appa.present", False))
    dog_line = " Your dog, Appa, stays glued to your side, hackles raised." if has_dog else ""
    if cause.startswith("stray"):
        return (f"The night air of {city} is thin and cold. The alley reeks of damp paper and iron. "
                f"You hear the first growl before you see the shapes—four, then six—eyes catching streetlamp light. "
                f"Strays circle in, ribs like wire. You raise your hands, back to the brick, the world shrinking to breath and teeth.{dog_line} "
                f"When they surge, you shove the closest away and feel the tearing heat at your calf. You stumble, the ground rushing up, "
                f"shouts far away. The last thing you know is the hot press of bodies and the distant wail of a siren.")
    if cause.startswith("attack") or cause.startswith("attacker"):
        return (f"{city} hums under neon and rain. A shadow peels from a doorway, steps matching yours. "
                f"You cross the light; he doesn’t. The glint at his hip blooms into a blade. "
                f"You run—shoulder to shoulder with fear—boots slapping, breath burning.{dog_line} "
                f"In the tunnel under the tracks, the world narrows to echo and steel. A shove; a flash; wet heat along your ribs. "
                f"You try to keep pressure, to breathe, to stay standing. The lights smear into stars.")
    if cause.startswith("accid"):
        return (f"Morning rush in {city}: a spill of horns and white lines. The crosswalk tick counts down. "
                f"You step out with the crowd. Screams split the air—a truck fishtails, metal shrieking. "
                f"You pivot to pull someone back and the world becomes glass and thunder.{dog_line} "
                f"Weightless for a heartbeat, then the ground takes you. You taste copper; everything fades to a far-off siren.")
    return (f"In {city}, the day ends strangely. A feeling of being watched trails you from the station to your door. "
            f"You double-check the lock, then the window.{dog_line} "
            f"Something is wrong—too quiet, too hollow. When the world tilts, it’s like a film jump: "
            f"the room slides, your stomach drops, and the dark closes in as if called.")

def _prologue_choices() -> List[str]:
    return [
        "Scan your body and surroundings. 【Info】",
        "Call out—\"Hello?\"—and listen. 【Info】",
        "Test your footing and pick a direction along the treeline. 【Move】",
        "Whistle softly for Appa and keep close. 【Bond】",
    ]

def render_onboarding_and_persist(SAVE: Dict[str, Any]) -> str:
    # Diegetic onboarding (uses 'class' wording)
    narration = (
        "Cold earth against your palms. Pine and loam — Greyfen air — and far off, gulls over the river.\n"
        "You blink up through fir needles. The canopy breathes, blue dusk pooled between boughs. Somewhere beyond, a road will wind toward Ridgehaven.\n"
        "Memory shivers: sirens, steel, or teeth. Whatever tore you from Earth still hums in your bones, and the world that answers has a different sky — Vantiel.\n\n"
        "A breath steadies. A thought takes shape.\n"
        "— *What do they call you?*\n"
        "— *What **class** do you claim?* (healer, sword-hand, archer — say it your way)\n"
        "— *Did a dog walk at your heel, or do you wake alone?*\n"
        "— *Which city did you leave behind?*\n"
        "— *What tore you away?*\n\n"
        "Answer naturally — a sentence or two is enough. For example: “My name is Can; my class is katana user; my dog is with me; I’m from İzmir; an attacker.”\n"
    )
    dialogue_lines = [{"speaker":"Narrator","text": narration}]
    footer = persist_turn_and_footer(
        SAVE,
        scene_ref="onboarding.profile",
        dialogue_lines=dialogue_lines,
        scene_tags=["Onboarding","Diegetic"],
        choices=["Answer in your own words."],
        choice_taken=None,
        mode="GM",
    )
    return narration + footer

def start_prologue_now(SAVE: Dict[str, Any]) -> str:
    SAVE.setdefault("flags", {}).setdefault("prologue", {})
    SAVE["flags"]["prologue"]["death"] = True
    SAVE["flags"]["prologue"]["completed"] = False
    narration = _cause_to_narration(SAVE)
    footer = prologue_turn(
        SAVE,
        scene_ref="prologue.death",
        narration=narration,
        choices=_prologue_choices(),
        tags=["Prologue","Earth","Death"]
    )
    SAVE["flags"]["prologue"]["completed"] = True
    write_save(SAVE, snapshot=True)
    return narration + "\n\nA breath later, the smell of pine replaces exhaust, and cold soil presses your shoulder. The world that answers is not your own.\n" + footer

def init_new_game(dropin_path: str = "/mnt/data/save.v1.2.dropin.upgraded.json", profile: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], List[str]]:
    SAVE, warnings = init_new_game_from_dropin(str(dropin_path)) if Path(dropin_path).exists() else (_minimal_save(), [])
    if profile:
        apply_profile(
            SAVE,
            name=profile.get("name", SAVE.get("party", {}).get("You", {}).get("name", "")),
            klass=profile.get("class", SAVE.get("party", {}).get("You", {}).get("class", "")),
            appa_present=profile.get("appa_present", _pg_get(SAVE, "party.Appa.present", True)),
            city=profile.get("city", _pg_get(SAVE, "flags.prologue.city", "")),
            attacker=profile.get("attacker", _pg_get(SAVE, "flags.prologue.attacker", "")),
        )
    return SAVE, warnings

def auto_new_game(user_text: str) -> Optional[Tuple[str, Dict[str, Any]]]:
    if not re.match(r"^\s*new\s*game\b", user_text or "", flags=re.I):
        return None
    dropin = Path("/mnt/data/save.v1.2.dropin.upgraded.json")
    if dropin.exists():
        try:
            SAVE, _ = init_new_game_from_dropin(str(dropin))
        except Exception:
            SAVE = json.loads(dropin.read_text(encoding="utf-8"))
    else:
        SAVE = _minimal_save()
    prof = parse_profile_from_text(user_text or "")
    if prof:
        SAVE = apply_profile(
            SAVE,
            name=prof.get("name",""),
            klass=prof.get("class",""),
            appa_present=prof.get("appa_present", None),
            city=prof.get("city",""),
            attacker=prof.get("attacker","")
        )
    # If profile incomplete, render onboarding (persists)
    required = ["party.You.name","party.You.class","party.Appa.present","flags.prologue.city","flags.prologue.attacker"]
    def _missing(save: Dict[str, Any]) -> List[str]:
        def _get(d: Dict[str, Any], path: str, default=None):
            cur = d
            for part in path.split("."):
                if isinstance(cur, dict) and part in cur:
                    cur = cur[part]
                else:
                    return default
            return cur
        miss = []
        for k in required:
            v = _get(save, k, None)
            if k.endswith("Appa.present"):
                if v is None: miss.append(k)
            elif v in (None, ""):
                miss.append(k)
        return miss
    if _missing(SAVE):
        text = render_onboarding_and_persist(SAVE)
        return (text, SAVE)
    # Else start prologue immediately (persists)
    scene = start_prologue_now(SAVE)
    return (scene, SAVE)


    # ===== Public API: Single-Scene Onboarding Flow =====
    
# === Single-Scene Onboarding API (public) =====================================
def run_single_scene_onboarding(USER_TEXT: str):
    """
    Public entry: single-scene onboarding + immediate prologue.
    Returns (scene_text, SAVE). Always persists before sending text.
    Behavior:
      - If USER_TEXT starts a New Game: initialize save/journal, persist an onboarding prompt turn, return prompt+footer.
      - Else if USER_TEXT contains a profile reply: persist profile capture as next turn, then start_prologue_now(SAVE) and return that scene (already persisted + footer).
      - Else: return (None, SAVE) so caller can continue normal loop.
    """
    import re
    # Local helpers from this module
    try:
        _parse = parse_profile_from_text
    except NameError:
        raise RuntimeError("ENGINE_MISSING: parse_profile_from_text")
    try:
        _apply = apply_profile
        _start_prologue = start_prologue_now
        _end = end_turn
        _ws = write_save
        _aj = append_journal
        _footer = compose_footer
        _load = load_latest_save_or_none
    except NameError as e:
        raise RuntimeError(f"ENGINE_MISSING: {e}")
    
    lowered = (USER_TEXT or "").strip().lower()
    is_new = "new game" in lowered or lowered == "start" or lowered == "-new game" or lowered == "newgame"

    # Ensure a SAVE exists (fresh for New Game)
    SAVE = _load() or {}

    def _ensure_minimal_save():
        nonlocal SAVE
        if not SAVE or not isinstance(SAVE, dict) or not SAVE.get("party"):
            # Use the module's minimal initializer if present; otherwise create a small template.
            # (We avoid external imports per engine policy.)
            SAVE = {
                "schema": "save.v1.2",
                "turn": 0,
                "time": "Morning",
                "loc": "Greyfen Forest Edge",
                "world": "Vantiel",
                "region": "Greyfen Marches",
                "town": "Ridgehaven",
                "obj": [],
                "party": {
                    "You": {"name":"", "class":"", "LV":1, "HP":20, "MP":5, "STA":10, "MaxHP":20, "MaxMP":5, "MaxSTA":10,
                            "stats":{"Might":1,"Agility":1,"Grit":1,"Focus":1,"Insight":1,"Presence":1},
                            "cooldowns":{}, "conditions":[], "skills":[], "XP":0, "XP_to_next":100, "last_level_up_turn":0},
                    "Appa": {"present": None, "name":"Appa", "HP":10, "STA":10, "MaxHP":10, "MaxSTA":10,
                             "stats":{"Might":1,"Agility":1,"Grit":1,"Focus":0,"Insight":0,"Presence":1},
                             "conditions":[], "moves":["Bark","Bite","Guard"], "XP":0, "XP_to_next":50},
                    "members": [], "marching_order": ["You","Appa"]
                },
                "inventory": [], "money":{"gold":0,"silver":0,"copper":0},
                "inv_delta":{"found":[],"spent":[],"consumed":[],"dropped":[],"equipped":[],"notes":[]},
                "quests": [], "promises": [], "relationships": {}, "hooks": [],
                "flags":{"origin":"Earth","prologue":{"city":"","attacker":"","death":True,"completed":False},
                         "gate_party_meet":False,"romance_intensity":"Cautious",
                         "guild":{"rank":"Copper","rank_points":0,"rp_pending":0},
                         "reputation":{},"preferences":{"tone":"","romance":"","nsfw":False,"formatting":True},
                         "integrity":{"schema_migration":"v1.2","save_hash":""}},
                "crystals":{"I":0,"II":0,"III":0,"IV":0,"V":0},
                "position":{"town":"Ridgehaven","area":"Outskirts","node":"Greyfen Forest Edge"},
                "weather":"","light":"",
                "since_short_rest":0,"since_long_rest":0,"day_count":1,"turn_tags":[],
                "dialogue_log":[],"prev_turn":{"turn":0,"ref":""},
                "motifs":{"running_jokes":[],"motifs_summary":""},
                "promises_summary":"","turn_log":[]
            }

    def _guard_persistence():
        # Minimal on-disk assert (mirror of external guard)
        from pathlib import Path as _P
        s = _P("/mnt/data/save.json"); j = _P("/mnt/data/saves/journal.ndjson")
        assert s.exists() and s.stat().st_size > 0, "PERSISTENCE_GUARD: save.json missing/empty"
        assert j.exists() and j.stat().st_size > 0, "PERSISTENCE_GUARD: journal.ndjson missing/empty"

    def _persist(scene_ref, dialogue_lines=None, scene_tags=None, choices=None, choice_taken=None, extra=None):
        nonlocal SAVE
        _end(SAVE, scene_ref, dialogue_lines or [], scene_tags or [], choices or [], choice_taken)
        _ws(SAVE, snapshot=True)
        _aj(SAVE, scene_ref, dialogue_lines or [], scene_tags or [], choices or [], choice_taken, extra)
        _guard_persistence()

    def _compose_footered(text):
        try:
            return text + "

" + _footer()
        except Exception:
            return text

    def _onboarding_prompt_text():
        return (
            "You drift between worlds, memory fraying to threads of light.
"
            "A voice—your own, distant—tries to anchor you.

"
            "What was your **name**?
"
            "Where did it **happen**—which **city** held your last day?
"
            "How did you **die**—what was the **cause**?
"
            "Was your dog **Appa** with you? (yes/no)
"
            "When you awaken in Vantiel, which **class** will your hands remember?

"
            "*(Reply in one line: e.g., “Can, katana user, İzmir, war, Appa yes”)*"
        )

    # Branch A: New Game → initialize + prompt
    if is_new:
        _ensure_minimal_save()
        # Reset turn to 0 and write immediately so links exist
        try:
            # Persist an onboarding prompt turn
            prompt_text = _onboarding_prompt_text()
            dialogue = [{"speaker": None, "text": "Onboarding prompt issued."}]
            _persist("onboarding:prompt", dialogue, ["onboarding","profile"])
            return (_compose_footered(prompt_text), SAVE)
        except Exception as e:
            raise RuntimeError(f"PERSISTENCE_FAILED:new_game:{e}")

    # Branch B: Profile reply?
    prof = _parse(USER_TEXT or "")
    if prof and any(k in prof for k in ("name","class","appa_present","city","attacker")):
        _ensure_minimal_save()
        # Apply parsed fields
        SAVE = _apply(
            SAVE,
            name=prof.get("name",""),
            klass=prof.get("class",""),
            appa_present=prof.get("appa_present", None),
            city=prof.get("city",""),
            attacker=prof.get("attacker","")
        )
        # Validate completeness
        missing = []
        if not (SAVE.get("party",{}).get("You",{}).get("name")): missing.append("name")
        if not (SAVE.get("party",{}).get("You",{}).get("class")): missing.append("class")
        if (SAVE.get("party",{}).get("Appa",{}).get("present") is None): missing.append("Appa present (yes/no)")
        if not (SAVE.get("flags",{}).get("prologue",{}).get("city")): missing.append("city")
        if not (SAVE.get("flags",{}).get("prologue",{}).get("attacker")): missing.append("death cause")
        if missing:
            # Persist clarify + re-prompt
            try:
                msg = "Profile incomplete: missing " + ", ".join(missing) + "."
                dialogue = [{"speaker":"System","text":msg}]
                _persist("onboarding:clarify", dialogue, ["onboarding","clarify"])
                text = msg + "

" + _onboarding_prompt_text()
                return (_compose_footered(text), SAVE)
            except Exception as e:
                raise RuntimeError(f"PERSISTENCE_FAILED:onboarding_clarify:{e}")
        # Persist profile capture at start of next turn
        try:
            # Log all five fields into journal extra
            extra = {"profile_captured": {
                "name": SAVE["party"]["You"]["name"],
                "class": SAVE["party"]["You"]["class"],
                "city": SAVE["flags"]["prologue"]["city"],
                "attacker": SAVE["flags"]["prologue"]["attacker"],
                "appa_present": SAVE["party"]["Appa"]["present"],
            }}
            dialogue = [{"speaker":"System","text":"Profile captured."}]
            _persist("onboarding:capture", dialogue, ["onboarding","profile","persist"], extra=extra)
        except Exception as e:
            raise RuntimeError(f"PERSISTENCE_FAILED:profile_capture:{e}")
        # Immediately run prologue (engine handles persistence + footer)
        prologue_text = _start_prologue(SAVE)
        # _start_prologue should have persisted and appended footer; return as-is
        return (prologue_text, SAVE)

    # Not an onboarding-related message
    return (None, SAVE)


                "What was your **name**? What **class** will you awaken as? **Where** were you when it happened? "
                "How did you **die**? Was your dog **Appa** with you?

"
                "*(Reply in one line, e.g.: "Can, katana user, İzmir, war, Appa yes")*"
            )
            return txt + "\n" + compose_footer()

        # Branch 1: New Game requested
        if USER_TEXT and "new game" in USER_TEXT.lower():
            SAVE = _init_empty_save()
            return _onboarding_text(), SAVE

        # Branch 2: Handle profile reply
        prof = parse_profile_from_text(USER_TEXT or "")
        # Determine completeness
        def _is_complete(p):
            return bool(p) and all(k in p and (p[k] is not None and p[k] != "") for k in ("name","class","city","attacker")) and ("appa_present" in p and p["appa_present"] is not None)

        if _is_complete(prof):
            SAVE = load_latest_save_or_none() or _init_empty_save()
            # Apply and persist at start-of-next-turn
            SAVE = apply_profile(SAVE,
                                 name=prof.get("name",""),
                                 klass=prof.get("class",""),
                                 appa_present=prof.get("appa_present", None),
                                 city=prof.get("city",""),
                                 attacker=prof.get("attacker","Strays"))
            # Persist the profile turn
            # Summarize choices in dialogue for the journal
            lines = [
                {"speaker": None, "text": f"Profile set → name={prof.get('name','')}, class={prof.get('class','')}, "
                                           f"city={prof.get('city','')}, attacker={prof.get('attacker','Strays')}, "
                                           f"appa_present={prof.get('appa_present',None)}"}
            ]
            end_turn(SAVE, "onboarding:profile", dialogue_lines=lines,
                     scene_tags=["onboarding","profile"], choices=[], choice_taken=None)
            write_save(SAVE, snapshot=True)
            append_journal(SAVE, "onboarding:profile", dialogue_lines=lines,
                           scene_tags=["onboarding","profile"], choices=[], choice_taken=None,
                           extra={"save_snapshot": SAVE, "profile_complete": True})
            _guard()
            # Now immediately trigger the prologue (this should persist and append its own footer)
            scene_text = start_prologue_now(SAVE)
            _guard()
            return scene_text, SAVE

        # Branch 3: Partial or no parse → re-ask single-scene prompt, persist clarify turn
        SAVE = load_latest_save_or_none() or _init_empty_save()
        end_turn(SAVE, "onboarding:clarify",
                 dialogue_lines=[{"speaker": None, "text": "Clarification requested: profile incomplete."}],
                 scene_tags=["onboarding","clarify"], choices=["(awaiting profile)"], choice_taken=None)
        write_save(SAVE, snapshot=True)
        append_journal(SAVE, "onboarding:clarify",
                       dialogue_lines=[{"speaker": None, "text": "Re-asking single-scene profile prompt."}],
                       scene_tags=["onboarding","clarify"], choices=["(awaiting profile)"], choice_taken=None)
        _guard()
        return _onboarding_text(), SAVE

