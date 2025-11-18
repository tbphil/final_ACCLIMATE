# File: backend/routers/hbom_write.py

import json
import datetime
import io

from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from fastapi.responses import JSONResponse

from models       import HBOMDefinition, HBOMComponent
from hbomLoader   import parse_excel, diff, _compute_path, _build_parent_map, DuplicateItem
from database     import hbom_components, hbom_fragilities, hbom_definitions
from crud_hbom    import upsert_component, insert_definition

from fuzzywuzzy import fuzz

router = APIRouter(prefix="/api/hbom", tags=["hbom"])

# ────────────────────────────────────────────────────────────────
# Dummy auth until real user system arrives
def get_current_username():
    return "INL"

# ────────────────────────────────────────────────────────────────
# Utility to flatten a tree into (uuid, label, parent_uuid)
def _flatten(node: HBOMComponent, parent_uuid: str | None = None):
    yield node.uuid, node.label, parent_uuid
    for child in node.subcomponents or []:
        yield from _flatten(child, node.uuid)

# ────────────────────────────────────────────────────────────────
# 1) PREVIEW
# ────────────────────────────────────────────────────────────────
@router.post("/preview")
async def preview_hbom(
    file:    UploadFile = File(...),
    sector:  str        = Form(...),
):
    # 1a) parse the upload with no self-duplicates
    contents = await file.read()
    buf      = io.BytesIO(contents)
    try:
        incoming: HBOMDefinition = parse_excel(
            buf,
            default_sector=sector,
            return_duplicates=False,
            return_hazards=False,
        )
    except Exception as e:
        raise HTTPException(400, f"Failed to parse workbook: {e}")

    # ── 1b) load the f root components for THIS sector ────────────────
    def_doc = await hbom_definitions.find_one({"sector": sector})
    existing: HBOMDefinition | None = None
    if def_doc and def_doc.get("root_ids"):
        roots: list[HBOMComponent] = []
        for root_uuid in def_doc["root_ids"]:
            comp_doc = await hbom_components.find_one({"uuid": root_uuid})
            if comp_doc:
                roots.append(HBOMComponent(**comp_doc))
        if roots:
            existing = HBOMDefinition(sector=sector, components=roots)

    # 2) structural diff
    diff_result = diff(existing, incoming)
    diff_json = {
        "added":   diff_result.added,
        "removed": diff_result.removed,
        "changed": [
            {
                "uuid":   ci.uuid,
                "path":   ci.path,
                "changes":[
                    {"field": ch.field, "old": ch.old, "new": ch.new}
                    for ch in ci.changes
                ]
            }
            for ci in diff_result.changed
        ],
    }

      #── 3) fuzzy‐match incoming vs. existing for Mongo‐based duplicates ────
    duplicates: list[dict] = []
    if existing:
        # a) Index every incoming node for path computation
        node_lookup: dict[str, HBOMComponent] = {}
        def index_in(node: HBOMComponent):
            node_lookup[node.uuid] = node
            for ch in node.subcomponents or []:
                index_in(ch)
        for root in incoming.components:
            index_in(root)

        parent_map_new = _build_parent_map(incoming.components)

        # b) Index every existing node for path computation
        existing_lookup: dict[str, HBOMComponent] = {}
        def index_ex(node: HBOMComponent):
            existing_lookup[node.uuid] = node
            for ch in node.subcomponents or []:
                index_ex(ch)
        for root in existing.components:
            index_ex(root)

        existing_parent_map = _build_parent_map(existing.components)

        # c) Flatten both trees into (uuid, label, parent_uuid) lists
        inc_nodes: list[tuple[str, str, str | None]] = []
        for root in incoming.components:
            inc_nodes.extend(_flatten(root))

        ex_nodes: list[tuple[str, str, str | None]] = []
        for root in existing.components:
            ex_nodes.extend(_flatten(root))

        # d) Cross‐compare every incoming label to every existing label
        for inc_uuid, inc_lbl, _ in inc_nodes:
            for ex_uuid, ex_lbl, _ in ex_nodes:
                sim = fuzz.ratio(inc_lbl.lower(), ex_lbl.lower())
                if sim >= 80:  # your similarity threshold
                    # compute both paths
                    incoming_path = _compute_path(inc_uuid, node_lookup, parent_map_new)
                    existing_path = _compute_path(ex_uuid, existing_lookup, existing_parent_map)
                    duplicates.append({
                        "incomingPath":   incoming_path,
                        "existingPath":   existing_path,
                        "incomingUuid": inc_uuid,
                        "originalUuid":   ex_uuid,
                        "originalLabel":  ex_lbl,
                        "candidateLabel": inc_lbl,
                        "similarity":     sim,
                    })
    catalog_matches: list[dict] = []
    if existing:
        for root in existing.components:
            catalog_matches.append({
                "uuid": root.uuid,
                "label": root.label
            })
    # 4) return preview payload
    return JSONResponse({
        "hbom":           incoming.model_dump(),      # for front-end tree rendering
        "diff":           diff_json,
        "duplicates":     duplicates,
        "catalogMatches": catalog_matches,                         
        "hazards":        [],                         
    })


# ────────────────────────────────────────────────────────────────
# 2) COMMIT
# ────────────────────────────────────────────────────────────────
@router.post("/commit")
async def commit_hbom(
    file:      UploadFile = File(...),
    sector:    str        = Form(...),
    decisions: str        = Form(...),
    user:      str        = Depends(get_current_username),
):
    decision_map = json.loads(decisions or "{}")

    # parse upload
    contents = await file.read()
    buffer   = io.BytesIO(contents)
    hbom, haz_list = parse_excel(
        buffer,
        default_sector=sector,
        return_hazards=True
    )

    # existing roots for this sector (to decide if root is NEW)
    def_doc = await hbom_definitions.find_one({"sector": sector})
    existing_root_ids = set(def_doc.get("root_ids", [])) if def_doc else set()

    # working root
    root = hbom.components[0]

    # 1) apply root choice
    root_choice = (decision_map.get("root") or "").strip()
    if root_choice:
        root.uuid = root_choice

    # 2) index all incoming nodes so we can rewrite uuids for duplicates
    node_lookup: dict[str, HBOMComponent] = {}
    def index(node: HBOMComponent):
        node_lookup[node.uuid] = node
        for ch in node.subcomponents or []:
            index(ch)
    index(root)

    # apply all per-row duplicate decisions: {inc: <incomingUuid>, use: <existingUuid>}
    for pair in decision_map.get("duplicates", []):
        inc = pair.get("inc")
        use = pair.get("use")
        if inc and use and inc in node_lookup:
            node_lookup[inc].uuid = use

    # 3) rebuild hazards from the now-mutated tree (UUIDs are final)
    haz_list_final: list[tuple[str, str, dict]] = []
    if haz_list:
        for orig_uuid, hz, det in haz_list:
            final_uuid = node_lookup.get(orig_uuid, None).uuid if orig_uuid in node_lookup else orig_uuid
            haz_list_final.append((final_uuid, hz, det or {}))
    else:
        def _gather_hazards(n: HBOMComponent):
            for hz, det in (getattr(n, "hazards", {}) or {}).items():
                haz_list_final.append((n.uuid, hz, det or {}))
            for ch in (n.subcomponents or []):
                _gather_hazards(ch)
        _gather_hazards(root)

    print(f"[commit] hazards found: {len(haz_list_final)}")
    for comp_uuid, hz, det in haz_list_final[:8]:
        print("   →", hz, "on", comp_uuid, "model=", det.get("fragility_model"))
    
    # 4) upsert component tree
    await upsert_component(root, added_by=user)

    # 5) upsert fragilities on the FINAL component uuids
   # Build a uuid -> label map once from the final (mutated) tree
    _uuid_to_label: dict[str, str] = {}
    def _walk(n: HBOMComponent):
        _uuid_to_label[n.uuid] = n.label
        for ch in (n.subcomponents or []):
            _walk(ch)
    _walk(root)

    # Upsert fragilities using FINAL component UUIDs
    for comp_uuid, hazard, details in haz_list_final:
        # normalize field names coming from the parser
        model  = (
            details.get("fragility_model")
            or details.get("model")
            or "inherit"
        )
        params = (
            details.get("fragility_params")
            or details.get("fragilty_params")   # tolerate legacy misspelling
            or details.get("params")
            or {}
        )
        payload = {
            "component_uuid": comp_uuid,
            "label": _uuid_to_label.get(comp_uuid, ""),  # ← real component label
            "hazard": hazard,
            "fragility_model": model,
            "fragility_params": params,
            "added_by": user,
            "added_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        await hbom_fragilities.replace_one(
            {"component_uuid": comp_uuid, "hazard": hazard},
            payload,
            upsert=True,
        )

    # 6) add root to definition only if it’s NEW for this sector
    if root.uuid not in existing_root_ids:
        await insert_definition(sector, [root.uuid], added_by=user)

    return {"status": "ok", "root_uuid": root.uuid}