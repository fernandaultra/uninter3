import os
import io
import json
import pandas as pd
from flask import Flask, jsonify, request
import gspread
from google.oauth2.service_account import Credentials

# ---- CONFIG BÁSICA ----
DEFAULT_SPREADSHEET_ID = "1NPP1K8335plGHaJtSUM3ag6FZkbONEgGxISONXW3KDQ"
DEFAULT_SHEET_TITLE = "Cursos Gratuitos"

def create_app() -> Flask:
    app = Flask(__name__)
    app.config["JSON_SORT_KEYS"] = False

    @app.get("/")
    def index():
        return jsonify({"message": "OK", "endpoints": ["/", "/health", "/sheets/preview", "/send-to-sheets"]})

    @app.get("/health")
    def health():
        return jsonify({"status": "ok"}), 200

    # ------- Helpers Google Sheets -------
    def _gspread_client():
        creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if not creds_json:
            raise RuntimeError("Falta a env GOOGLE_CREDENTIALS_JSON no Render.")
        info = json.loads(creds_json)
        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        credentials = Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(credentials)

    # ------- Rotas Google Sheets -------
    @app.get("/sheets/preview")
    def sheets_preview():
        sheet_title = request.args.get("sheet", DEFAULT_SHEET_TITLE)
        spreadsheet_id = request.args.get("spreadsheet_id", DEFAULT_SPREADSHEET_ID)
        try:
            client = _gspread_client()
            sh = client.open_by_key(spreadsheet_id)
            ws = sh.worksheet(sheet_title)
            data = ws.get_all_values()
            header = data[0] if data else []
            rows = data[1:11] if len(data) > 1 else []
            out = [dict(zip(header, r)) for r in rows] if header else rows
            return jsonify({"sheet": sheet_title, "preview": out}), 200
        except Exception as e:
            return jsonify({"error": f"Falha ao ler Google Sheets: {e}"}), 500

    @app.post("/send-to-sheets")
    def send_to_sheets():
        """
        Envie multipart/form-data com key 'file' apontando para um .xlsx
        Ex.: curl -X POST '.../send-to-sheets?sheet=Cursos%20Gratuitos' -F "file=@/caminho/arquivo.xlsx"
        """
        if "file" not in request.files:
            return jsonify({"error": "Envie um arquivo Excel no campo 'file' (multipart/form-data)."}), 400

        sheet_title = request.args.get("sheet", DEFAULT_SHEET_TITLE)
        spreadsheet_id = request.args.get("spreadsheet_id", DEFAULT_SPREADSHEET_ID)

        file = request.files["file"]
        content = file.read()
        try:
            # pandas precisa do openpyxl instalado para .xlsx
            df = pd.read_excel(io.BytesIO(content))
        except Exception as e:
            return jsonify({"error": f"Falha ao ler Excel: {e}"}), 400

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
        except Exception as e:
            return jsonify({"error": f"Falha ao escrever no Google Sheets: {e}"}), 500

        return jsonify({"status": "ok", "rows": int(rows), "sheet": sheet_title}), 200

    # ------- Handlers globais -------
    @app.errorhandler(404)
    def not_found(_):
        return jsonify({"error": "Not Found"}), 404

    @app.errorhandler(500)
    def internal_err(_):
        return jsonify({"error": "Internal Server Error"}), 500

    return app

app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
