import os
import json
import logging
from datetime import datetime, timezone

import azure.functions as func
import pyodbc
import requests

# ✅ アプリ全体のデフォルト認証レベルを anonymous に
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# ===== DB 接続ヘルパー =====

# Service Connector などで設定した接続文字列
CONN_STR = os.environ.get("FABRIC_SQL_CONNECTIONSTRING")
FABRIC_API_BASE = os.environ.get("FABRIC_API_BASE", "https://api.fabric.microsoft.com")
FABRIC_WORKSPACE_ID = os.environ.get("FABRIC_WORKSPACE_ID")
FABRIC_ARTIFACT_ID = os.environ.get("FABRIC_ARTIFACT_ID")
FABRIC_TENANT_ID = os.environ.get("FABRIC_TENANT_ID")
FABRIC_CLIENT_ID = os.environ.get("FABRIC_CLIENT_ID")
FABRIC_CLIENT_SECRET = os.environ.get("FABRIC_CLIENT_SECRET")

if not CONN_STR:
    # 起動時に気づけるようにしておく
    logging.warning("FABRIC_SQL_CONNECTIONSTRING is not set. DB access will fail.")

def get_connection():
    if not CONN_STR:
        raise RuntimeError("FABRIC_SQL_CONNECTIONSTRING is not configured.")
    # 必要に応じて timeout を追加してもOK
    return pyodbc.connect(CONN_STR)

def query_all(sql: str, params: tuple = ()):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        cols = [c[0] for c in cursor.description]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
    return rows

def execute_non_query(sql: str, params: tuple = ()) -> int:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        conn.commit()
        return cursor.rowcount


def _clamp_limit(value, default: int = 200, max_limit: int = 2000) -> int:
    try:
        num = int(value)
    except Exception:
        return default
    return max(1, min(max_limit, num))


def _fetch_table_rows(
    table_name: str, req: func.HttpRequest, filterable: dict | None = None
) -> dict:
    """
    Generic helper to return rows and column metadata from a table.
    Applies optional filters only if the column exists, so it will not fail when
    a requested column is absent.
    """
    limit = _clamp_limit(req.params.get("limit", 200))
    filterable = filterable or {}

    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute(f"SELECT TOP (0) * FROM {table_name}")
        available_columns = [c[0] for c in cursor.description]

        where_parts = []
        params = []
        for query_param, column_name in filterable.items():
            value = req.params.get(query_param)
            if value is None:
                continue
            if column_name not in available_columns:
                logging.warning(
                    "Ignoring filter '%s' for table %s because column is missing",
                    column_name,
                    table_name,
                )
                continue
            where_parts.append(f"{column_name} = ?")
            params.append(value)

        order_column = None
        for candidate in ("evaluated_at", "updated_at", "created_at"):
            if candidate in available_columns:
                order_column = candidate
                break

        where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
        order_clause = f" ORDER BY {order_column} DESC" if order_column else ""
        sql = f"SELECT TOP ({limit}) * FROM {table_name}{where_clause}{order_clause};"

        cursor.execute(sql, tuple(params))
        cols = [c[0] for c in cursor.description]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]

    return {"columns": cols, "rows": rows}

# ===== GET /api/getFeatureCandidates =====

DEFAULT_TYPE = "behavior_feature"
DEFAULT_STATUS = "new"

@app.route(route="getFeatureCandidates", methods=["GET"])
def get_feature_candidates(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("getFeatureCandidates called")

    type_param = req.params.get("type", DEFAULT_TYPE)
    status_param = req.params.get("status", DEFAULT_STATUS)

    try:
        sql = """
            SELECT TOP (100)
              candidate_id,
              type,
              source,
              name_proposed,
              description_proposed,
              logic_proposed,
              status,
              created_at
            FROM feature_candidates
            WHERE type = ? AND status = ?
            ORDER BY created_at DESC;
        """

        rows = query_all(sql, (type_param, status_param))

        return func.HttpResponse(
            body=json.dumps(rows, default=str),
            mimetype="application/json",
            status_code=200,
        )
    except Exception as e:
        logging.exception("getFeatureCandidates error")
        return func.HttpResponse(
            body=json.dumps(
                {
                    "message": str(e),
                    "endpoint": "getFeatureCandidates",
                },
                default=str,
            ),
            mimetype="application/json",
            status_code=500,
        )

# ===== POST /api/updateFeatureCandidate =====

VALID_ACTIONS = ("adopt", "reject")

@app.route(route="updateFeatureCandidate", methods=["POST"])
def update_feature_candidate(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("updateFeatureCandidate called")

    # --- validate payload ---
    try:
        payload = req.get_json()
    except:
        return func.HttpResponse(
            json.dumps({"message": "Invalid JSON"}),
            mimetype="application/json",
            status_code=400,
        )

    candidate_id = payload.get("candidate_id")
    action = payload.get("action")
    if not candidate_id or action not in ("adopt", "reject"):
        return func.HttpResponse(
            json.dumps({"message": "candidate_id and action are required."}),
            mimetype="application/json",
            status_code=400,
        )

    next_status = "adopted" if action == "adopt" else "rejected"
    now = datetime.now(tz=timezone.utc)

    try:
        # --- 1) Update status ---
        sql = "UPDATE feature_candidates SET status = ?, updated_at = ? WHERE candidate_id = ?;"
        execute_non_query(sql, (next_status, now, candidate_id))

        if next_status != "adopted":
            return func.HttpResponse(
                json.dumps({"candidate_id": candidate_id, "status": next_status}),
                mimetype="application/json",
                status_code=200,
            )

        # --- 2) SELECT adopted candidate ---
        candidate_sql = """
            SELECT candidate_id, type, name_proposed, description_proposed
            FROM feature_candidates
            WHERE candidate_id = ?;
        """
        rows = query_all(candidate_sql, (candidate_id,))
        if not rows:
            return func.HttpResponse(
                json.dumps({"message": "Candidate not found."}),
                mimetype="application/json",
                status_code=404,
            )
        c = rows[0]

        # --- 3) Insert into proper master table ---
        if c["type"] == "tag":
            insert_sql = """
                INSERT INTO tag_definitions
                  (tag_id, tag_code, tag_name, description, value_type, source_type,
                   is_multi_valued, is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            params = (
                c["candidate_id"],
                c["name_proposed"],
                c["name_proposed"],
                c["description_proposed"],
                "string",
                "llm",
                0,
                1,
                now,
                now,
            )

            execute_non_query(insert_sql, params)

        elif c["type"] == "score":
            insert_sql = """
                INSERT INTO score_definitions
                  (score_id, score_code, score_name, description, min_value, max_value,
                   direction, source_type, refresh_interval, is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            params = (
                c["candidate_id"],
                c["name_proposed"],
                c["name_proposed"],
                c["description_proposed"],
                None,
                None,
                "higher_is_better",
                "llm",
                None,
                1,
                now,
                now,
            )

            execute_non_query(insert_sql, params)

        # --- 4) return response ---
        return func.HttpResponse(
            json.dumps({"candidate_id": candidate_id, "status": next_status}),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.exception("updateFeatureCandidate error")
        return func.HttpResponse(
            json.dumps({"message": str(e)}),
            mimetype="application/json",
            status_code=500,
        )

# ===== GET /api/getTagDefinitions =====

@app.route(route="getTagDefinitions", methods=["GET"])
def get_tag_definitions(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("getTagDefinitions called")

    include_inactive = (
        str(req.params.get("include_inactive", "false")).lower() in ("1", "true", "yes")
    )
    limit = _clamp_limit(req.params.get("limit", 200))

    try:
        where_clause = "" if include_inactive else "WHERE is_active = 1"
        sql = f"""
            SELECT TOP ({limit})
              tag_id,
              tag_name,
              description,
              created_at
            FROM tag_definitions
            {where_clause}
            ORDER BY created_at DESC;
        """
        rows = query_all(sql)
        return func.HttpResponse(
            body=json.dumps(rows, default=str),
            mimetype="application/json",
            status_code=200,
        )
    except Exception as e:
        logging.exception("getTagDefinitions error")
        return func.HttpResponse(
            json.dumps({"message": str(e)}),
            mimetype="application/json",
            status_code=500,
        )

# ===== GET /api/getScoreDefinitions =====

@app.route(route="getScoreDefinitions", methods=["GET"])
def get_score_definitions(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("getScoreDefinitions called")

    include_inactive = (
        str(req.params.get("include_inactive", "false")).lower() in ("1", "true", "yes")
    )
    limit = _clamp_limit(req.params.get("limit", 200))

    try:
        where_clause = "" if include_inactive else "WHERE is_active = 1"
        sql = f"""
            SELECT TOP ({limit})
              score_id,
              score_code,
              score_name,
              description,
              min_value,
              max_value,
              direction,
              source_type,
              refresh_interval,
              is_active,
              created_at,
              updated_at
            FROM score_definitions
            {where_clause}
            ORDER BY updated_at DESC, created_at DESC;
        """
        rows = query_all(sql)
        return func.HttpResponse(
            body=json.dumps(rows, default=str),
            mimetype="application/json",
            status_code=200,
        )
    except Exception as e:
        logging.exception("getScoreDefinitions error")
        return func.HttpResponse(
            json.dumps({"message": str(e)}),
            mimetype="application/json",
            status_code=500,
        )

# ===== GET /api/getAccountTags =====

@app.route(route="getAccountTags", methods=["GET"])
def get_account_tags(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("getAccountTags called")

    try:
        limit = _clamp_limit(req.params.get("limit", 200))
        account_id = req.params.get("account_id")
        tag_id = req.params.get("tag_id")

        where_parts = []
        params = []
        if account_id:
            where_parts.append("at.account_id = ?")
            params.append(account_id)
        if tag_id:
            where_parts.append("at.tag_id = ?")
            params.append(tag_id)

        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        sql = f"""
            SELECT TOP ({limit})
              at.account_id,
              at.tag_id,
              at.tag_value,
              at.confidence_score,
              at.created_at,
              COALESCE(a.company_name, a.account_name, a.name) AS account_name,
              td.tag_name
            FROM account_tags AS at
            LEFT JOIN tag_definitions AS td ON at.tag_id = td.tag_id
            LEFT JOIN accounts AS a ON at.account_id = a.account_id
            {where_clause}
            ORDER BY at.created_at DESC;
        """

        rows = query_all(sql, tuple(params))
        payload = {
            "columns": [
                "account_name",
                "tag_name",
                "tag_value",
                "confidence_score",
                "created_at",
                "account_id",
                "tag_id",
            ],
            "rows": rows,
        }
        return func.HttpResponse(
            body=json.dumps(payload, default=str),
            mimetype="application/json",
            status_code=200,
        )
    except Exception as e:
        logging.exception("getAccountTags error")
        return func.HttpResponse(
            json.dumps({"message": str(e)}),
            mimetype="application/json",
            status_code=500,
        )

# ===== GET /api/getAccountScores =====

@app.route(route="getAccountScores", methods=["GET"])
def get_account_scores(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("getAccountScores called")

    try:
        payload = _fetch_table_rows(
            "account_scores",
            req,
            filterable={"account_id": "account_id", "score_id": "score_id"},
        )
        return func.HttpResponse(
            body=json.dumps(payload, default=str),
            mimetype="application/json",
            status_code=200,
        )
    except Exception as e:
        logging.exception("getAccountScores error")
        return func.HttpResponse(
            json.dumps({"message": str(e)}),
            mimetype="application/json",
            status_code=500,
        )

# ===== POST /api/generateTagCandidates =====

DEFAULT_SAMPLE_SIZE = 1000
DEFAULT_MAX_CANDIDATES = 10
DEFAULT_MIN_CANDIDATES = 3

def _get_fabric_access_token() -> str:
    """
    Azure AD (Entra ID) の client_credentials フローで
    Fabric API 用のアクセストークンを取得するヘルパー。
    """
    if not FABRIC_TENANT_ID:
        raise RuntimeError("FABRIC_TENANT_ID is not set.")
    if not FABRIC_CLIENT_ID:
        raise RuntimeError("FABRIC_CLIENT_ID is not set.")
    if not FABRIC_CLIENT_SECRET:
        raise RuntimeError("FABRIC_CLIENT_SECRET is not set.")

    token_url = f"https://login.microsoftonline.com/{FABRIC_TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": FABRIC_CLIENT_ID,
        "client_secret": FABRIC_CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "https://api.fabric.microsoft.com/.default",
    }

    resp = requests.post(token_url, data=data, timeout=10)
    if not resp.ok:
        logging.error(
            "Failed to obtain Fabric access token. status=%s, body=%s",
            resp.status_code,
            resp.text,
        )
        raise RuntimeError(
            f"Failed to obtain Fabric access token. status={resp.status_code}"
        )

    try:
        token_json = resp.json()
        access_token = token_json.get("access_token")
    except Exception:
        logging.exception("Failed to parse token response JSON.")
        raise RuntimeError("Failed to parse token response JSON.")

    if not access_token:
        raise RuntimeError("access_token is missing in token response.")

    return access_token

def _call_fabric_notebook(payload: dict):
    if not FABRIC_WORKSPACE_ID:
        raise RuntimeError("FABRIC_WORKSPACE_ID is not set.")
    if not FABRIC_ARTIFACT_ID:
        raise RuntimeError("FABRIC_ARTIFACT_ID is not set.")

    access_token = _get_fabric_access_token()

    trigger_url = (
        f"{FABRIC_API_BASE.rstrip('/')}/v1/workspaces/{FABRIC_WORKSPACE_ID}"
        f"/items/{FABRIC_ARTIFACT_ID}/jobs/instances?jobType=RunNotebook"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    resp = requests.post(trigger_url, json=payload, headers=headers, timeout=30)
    return resp


@app.route(route="generateTagCandidates", methods=["POST"])
def generate_tag_candidates(req: func.HttpRequest) -> func.HttpResponse:
    """
    Trigger Fabric Notebook to generate tag candidates.
    The notebook side should read account/appointment tables and insert records into feature_candidates.
    """
    logging.info("generateTagCandidates called")

    try:
        payload = req.get_json() or {}
    except Exception:
        payload = {}

    try:
        sample_size = max(1, min(5000, int(payload.get("sample_size", DEFAULT_SAMPLE_SIZE))))
        max_candidates = max(1, min(10, int(payload.get("max_candidates", DEFAULT_MAX_CANDIDATES))))
        min_candidates = max(1, min(max_candidates, int(payload.get("min_candidates", DEFAULT_MIN_CANDIDATES))))
    except Exception:
        return func.HttpResponse(
            json.dumps({"message": "sample_size, max_candidates, min_candidates must be numbers."}),
            mimetype="application/json",
            status_code=400,
        )

    notebook_payload = {
        "sample_size": sample_size,
        "max_candidates": max_candidates,
        "min_candidates": min_candidates,
    }
    # Allow pass-through of any other parameters to the notebook
    for k, v in payload.items():
        if k not in notebook_payload:
            notebook_payload[k] = v

    try:
        resp = _call_fabric_notebook(notebook_payload)
        if resp.status_code not in (200, 201, 202):
            return func.HttpResponse(
                json.dumps(
                    {
                        "message": "Failed to trigger Fabric notebook.",
                        "status_code": resp.status_code,
                        "response": resp.text,
                    }
                ),
                mimetype="application/json",
                status_code=502,
            )

        return func.HttpResponse(
            json.dumps(
                {
                    "message": "Fabric notebook triggered.",
                    "upstream_status": resp.status_code,
                    "upstream_response": resp.text,
                }
            ),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.exception("generateTagCandidates error")
        return func.HttpResponse(
            json.dumps({"message": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
