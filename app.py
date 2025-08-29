import os
import io
import json
import traceback
from typing import Tuple, List

import pandas as pd
from flask import Flask, jsonify, request
import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIG PADRÃO (ajuste se quiser)
# =========================
DEFAULT_SPREADSHEET_ID = "1NPP1K8335plGHaJtSUM3ag6FZkbONEgGxISONXW3KDQ"
DEFAULT_SHEET_TITLE = "Cursos Gratuitos"

# Escopos recomendados para Sheets + Drive (gspread)
GSHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# (Opcional) Proteção simples por API key nas rotas de escrita:
# Defina no Render uma env: API_KEY=segredo-123
REQUIRE_API_KEY = False  # mude para True se quiser exigir X-API-KEY
API_KEY_ENV_NAME = "API_KEY"


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["JSON_SORT_KEYS"] = False

    # =========================
    # Rotas básicas
    # =========================
    @app.get("/")
    def index():
        return jsonify({
            "message": "OK",
            "endpoints": ["/", "/health", "/sheets/preview", "/send-to-sheets"]
        })

    @app.get("/health")
    def health():
        return jsonify({"status": "ok"}), 200

    # =========================
    # Helpers
    # =========================
    def _require_api_key_if_enabled() -> Tuple[bool, str]:
        """
        Se REQUIRE_API_KEY=True, exige header X-API-KEY igual à env API_KEY.
        Retorna (ok, motivo_erro_ou_vazio).
        """
        if not REQUIRE_API_KEY:
            return True, ""

        expected = os.getenv(API_KEY_ENV_NAME, "").strip()
        provided = request.headers.get("X-API-KEY", "").strip()
        if not expected:
            return False, f"SERVER_MISCONFIG: defina {API_KEY_ENV_NAME} no ambiente."
        if provided != expected:
            return False, "UNAUTHORIZED: X-API-KEY inválido ou ausente."
        return True, ""

    def _load_service_account_info() -> dict:
        """
        Lê GOOGLE_CREDENTIALS_JSON de duas formas:
        - Secret File (valor da env é um path existente)
        - JSON inline (valor é o próprio JSON)
        """
        raw = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if not raw:
            raise RuntimeError("Falta a env GOOGLE_CREDENTIALS_JSON no Render.")

        if os.path.exists(raw):
            # Secret File → raw é caminho de arquivo
            with open(raw, "r", encoding="utf-8") as f:
                content = f.read()
        else:
            # JSON inline
            content = raw

        try:
            info = json.loads(content)
        except Exception as e:
            raise RuntimeError(f"GOOGLE_CREDENTIALS_JSON inválido (não é JSON). Detalhe: {e}")
        return info

    def _gspread_client():
        """
        Cria cliente gspread autorizado com as credenciais lidas.
        """
        info = _load_service_account_info()
        credentials = Credentials.from_service_account_info(info, scopes=GSHEETS_SCOPES)
        return gspread.authorize(credentials)

    # =========================
    # Rotas Google Sheets
    # =========================
    @app.get("/sheets/preview")
    def sheets_preview():
        """
        Lê as primeiras 10 linhas da aba solicitada.
        Query params:
          - spreadsheet_id (opcional) => default DEFAULT_SPREADSHEET_ID
          - sheet (opcional)          => default DEFAULT_SHEET_TITLE
        """
        sheet_title = request.args.get("sheet", DEFAULT_SHEET_TITLE)
        spreadsheet_id = request.args.get("spreadsheet_id", DEFAULT_SPREADSHEET_ID)

        try:
            client = _gspread_client()
            sh = client.open_by_key(spreadsheet_id)
            ws = sh.worksheet(sheet_title)
            data: List[List[str]] = ws.get_all_values()
            header = data[0] if data else []
            rows = data[1:11] if len(data) > 1 else []
            out = [dict(zip(header, r)) for r in rows] if header else rows
            return jsonify({"sheet": sheet_title, "preview": out}), 200

        except gspread.exceptions.APIError as e:
            # Erros da API do Google (inclui auth/escopos/perm)
            return jsonify({
                "error": "Falha ao ler Google Sheets (APIError)",
                "detail": str(e)
            }), 500
        except Exception as e:
            return jsonify({
                "error": "Falha ao ler Google Sheets",
                "detail": str(e),
                "trace": traceback.format_exc()
            }), 500

    @app.post("/send-to-sheets")
    def send_to_sheets():
        """
        Envie multipart/form-data com key 'file' apontando para um .xlsx
        Ex.: curl -X POST '.../send-to-sheets?sheet=MinhaAba' -F "file=@/caminho/arquivo.xlsx"
        Query params:
          - spreadsheet_id (opcional) => default DEFAULT_SPREADSHEET_ID
          - sheet (opcional)          => default DEFAULT_SHEET_TITLE
        """
        # (Opcional) checagem de API Key
        ok, reason = _require_api_key_if_enabled()
        if not ok:
            return jsonify({"error": reason}), 401

        if "file" not in request.files:
            return jsonify({"error": "Envie um arquivo Excel no campo 'file' (multipart/form-data)."}), 400

        sheet_title = request.args.get("sheet", DEFAULT_SHEET_TITLE)
        spreadsheet_id = request.args.get("spreadsheet_id", DEFAULT_SPREADSHEET_ID)

        file = request.files["file"]
        content = file.read()

        try:
            # pandas precisa do openpyxl para .xlsx
            # (garanta openpyxl~=3.1.5 no requirements.txt)
            df = pd.read_excel(io.BytesIO(content))
        except Exception as e:
            return jsonify({"error": f"Falha ao ler Excel", "detail": str(e)}), 400

        try:
            client = _gspread_client()
            sh = client.open_by_key(spreadsheet_id)
            try:
                ws = sh.worksheet(sheet_title)
            except gspread.WorksheetNotFound:
                ws = sh.add_worksheet(title=sheet_title, rows="100", cols="20")

            ws.clear()
            if df.empty:
                ws.update("A1", [["(vazio)"]])
                rows = 0
            else:
                ws.update("A1", [df.columns.tolist()])
                ws.update("A2", df.values.tolist())
                rows = len(df)

            return jsonify({"status": "ok", "rows": int(rows), "sheet": sheet_title}), 200

        except gspread.exceptions.APIError as e:
            return jsonify({
                "error": "Falha ao escrever no Google Sheets (APIError)",
                "detail": str(e)
            }), 500
        except Exception as e:
            return jsonify({
                "error": "Falha ao escrever no Google Sheets",
                "detail": str(e),
                "trace": traceback.format_exc()
            }), 500

    # Handlers globais
    @app.errorhandler(404)
    def not_found(_):
        return jsonify({"error": "Not Found"}), 404

    @app.errorhandler(500)
    def internal_err(_):
        return jsonify({"error": "Internal Server Error"}), 500

    return app


# Exporte o app para o Gunicorn
app = create_app()

if __name__ == "__main__":
    # Execução local (dev). No Render use o Gunicorn (Start Command).
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
