import os
import io
import csv
import json
import html
import traceback
from typing import Tuple, List

import pandas as pd
from flask import Flask, jsonify, request, Response, send_file
import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIG PADRÃO
# =========================
DEFAULT_SPREADSHEET_ID = "1NPP1K8335plGHaJtSUM3ag6FZkbONEgGxISONXW3KDQ"
DEFAULT_SHEET_TITLE = "Cursos Gratuitos"

# Escopos recomendados para Sheets + Drive (gspread)
GSHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# (Opcional) Proteção por API key na rota de escrita
# Defina no Render: API_KEY=segredo-123 e mude para True abaixo:
REQUIRE_API_KEY = False
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
            "endpoints": [
                "/",
                "/health",
                "/sheets/preview",
                "/sheets/export.csv",
                "/sheets/export.xlsx",
                "/sheets/view",
                "/send-to-sheets",
            ]
        })

    @app.get("/health")
    def health():
        return jsonify({"status": "ok"}), 200

    # =========================
    # Helpers
    # =========================
    def _require_api_key_if_enabled() -> Tuple[bool, str]:
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
        raw = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if not raw:
            raise RuntimeError("Falta a env GOOGLE_CREDENTIALS_JSON no Render.")
        if os.path.exists(raw):  # Secret File → path
            with open(raw, "r", encoding="utf-8") as f:
                content = f.read()
        else:  # JSON inline
            content = raw
        try:
            return json.loads(content)
        except Exception as e:
            raise RuntimeError(f"GOOGLE_CREDENTIALS_JSON inválido (não é JSON). Detalhe: {e}")

    def _gspread_client():
        info = _load_service_account_info()
        credentials = Credentials.from_service_account_info(info, scopes=GSHEETS_SCOPES)
        return gspread.authorize(credentials)

    # =========================
    # Rotas Google Sheets (JSON)
    # =========================
    @app.get("/sheets/preview")
    def sheets_preview():
        """
        Lê linhas da planilha com paginação.
        Params:
          - spreadsheet_id (opcional)
          - sheet (opcional)
          - limit (opcional, padrão 10, máx 1000)
          - offset (opcional, padrão 0)
        """
        sheet_title = request.args.get("sheet", DEFAULT_SHEET_TITLE)
        spreadsheet_id = request.args.get("spreadsheet_id", DEFAULT_SPREADSHEET_ID)
        try:
            limit = int(request.args.get("limit", 10))
            offset = int(request.args.get("offset", 0))
            limit = max(0, min(limit, 1000))
            offset = max(0, offset)
        except ValueError:
            return jsonify({"error": "Parâmetros limit/offset devem ser inteiros."}), 400

        try:
            client = _gspread_client()
            sh = client.open_by_key(spreadsheet_id)
            ws = sh.worksheet(sheet_title)
            data: List[List[str]] = ws.get_all_values()
            header = data[0] if data else []
            body = data[1:] if len(data) > 1 else []
            sliced = body[offset: offset + limit] if limit > 0 else body[offset:]
            out = [dict(zip(header, r)) for r in sliced] if header else sliced
            return jsonify({
                "sheet": sheet_title,
                "total_rows": len(body),
                "limit": limit,
                "offset": offset,
                "preview": out
            }), 200

        except gspread.exceptions.APIError as e:
            return jsonify({"error": "Falha ao ler Google Sheets (APIError)", "detail": str(e)}), 500
        except Exception as e:
            return jsonify({
                "error": "Falha ao ler Google Sheets",
                "detail": str(e),
                "trace": traceback.format_exc()
            }), 500

    @app.get("/sheets/export.csv")
    def sheets_export_csv():
        sheet_title = request.args.get("sheet", DEFAULT_SHEET_TITLE)
        spreadsheet_id = request.args.get("spreadsheet_id", DEFAULT_SPREADSHEET_ID)
        try:
            client = _gspread_client()
            sh = client.open_by_key(spreadsheet_id)
            ws = sh.worksheet(sheet_title)
            data = ws.get_all_values()
            if not data:
                return Response("", mimetype="text/csv")

            output = io.StringIO()
            writer = csv.writer(output, lineterminator="\n")
            for row in data:
                writer.writerow(row)
            output.seek(0)
            return Response(
                output.read(),
                mimetype="text/csv",
                headers={"Content-Disposition": f'attachment; filename="{sheet_title}.csv"'}
            )
        except Exception as e:
            return jsonify({"error": f"Falha ao exportar CSV: {e}"}), 500

    @app.get("/sheets/export.xlsx")
    def sheets_export_xlsx():
        sheet_title = request.args.get("sheet", DEFAULT_SHEET_TITLE)
        spreadsheet_id = request.args.get("spreadsheet_id", DEFAULT_SPREADSHEET_ID)
        try:
            client = _gspread_client()
            sh = client.open_by_key(spreadsheet_id)
            ws = sh.worksheet(sheet_title)
            data = ws.get_all_values()
            df = pd.DataFrame(data[1:], columns=data[0]) if data else pd.DataFrame()

            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name=sheet_title)
            buf.seek(0)
            return send_file(
                buf,
                as_attachment=True,
                download_name=f"{sheet_title}.xlsx",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            return jsonify({"error": f"Falha ao exportar XLSX: {e}"}), 500

    # =========================
    # Rota Google Sheets (HTML para humanos)
    # =========================
    @app.get("/sheets/view")
    def sheets_view():
        """
        Exibe a aba da planilha como TABELA HTML.
        Params:
          - spreadsheet_id (opcional)
          - sheet (opcional)
          - limit (opcional, padrão 50; 0 = tudo)
          - offset (opcional, padrão 0)
        """
        sheet_title = request.args.get("sheet", DEFAULT_SHEET_TITLE)
        spreadsheet_id = request.args.get("spreadsheet_id", DEFAULT_SPREADSHEET_ID)
        try:
            limit = int(request.args.get("limit", 50))
            offset = int(request.args.get("offset", 0))
            limit = max(0, min(limit, 2000))
            offset = max(0, offset)
        except ValueError:
            return Response("<p>Parâmetros limit/offset devem ser inteiros.</p>", mimetype="text/html", status=400)

        try:
            client = _gspread_client()
            sh = client.open_by_key(spreadsheet_id)
            ws = sh.worksheet(sheet_title)
            data = ws.get_all_values()
            header = data[0] if data else []
            body = data[1:] if len(data) > 1 else []
            rows = body[offset: offset + limit] if limit > 0 else body[offset:]

            # HTML minimalista
            html_out = []
            html_out.append("<!doctype html><html lang='pt-br'><head><meta charset='utf-8'>")
            html_out.append(f"<title>{html.escape(sheet_title)}</title>")
            html_out.append("""
            <style>
              :root { color-scheme: dark; }
              body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; background:#0b0b0b; color:#e9e9e9;}
              h1 { margin: 0 0 8px; font-size: 20px;}
              .meta { margin: 0 0 16px; opacity:.8; font-size: 13px;}
              table { width:100%; border-collapse:collapse; border:1px solid #2b2b2b; }
              th, td { border:1px solid #2b2b2b; padding:8px 10px; vertical-align:top; }
              th { position:sticky; top:0; background:#151515; z-index:1; text-transform:uppercase; letter-spacing:.02em; font-size:12px; }
              tr:nth-child(even) td { background:#111; }
              a { color:#8ecbff; text-decoration:none; }
              a:hover { text-decoration:underline; }
            </style>
            """)
            html_out.append("</head><body>")
            html_out.append(f"<h1>{html.escape(sheet_title)}</h1>")
            html_out.append(f"<p class='meta'>Total linhas: {len(body)} • Exibindo: {len(rows)} • offset={offset} • limit={limit}</p>")

            html_out.append("<table><thead><tr>")
            for col in header:
                html_out.append(f"<th>{html.escape(col)}</th>")
            html_out.append("</tr></thead><tbody>")
            for r in rows:
                html_out.append("<tr>")
                for cell in r:
                    val = "" if cell is None else str(cell)
                    if val.startswith(("http://", "https://")):
                        esc = html.escape(val, quote=True)
                        html_out.append(f"<td><a href='{esc}' target='_blank' rel='noopener'>{esc}</a></td>")
                    else:
                        html_out.append(f"<td>{html.escape(val)}</td>")
                html_out.append("</tr>")
            html_out.append("</tbody></table>")
            html_out.append("</body></html>")
            return Response("".join(html_out), mimetype="text/html")

        except Exception as e:
            return Response(f"<pre>Erro ao renderizar tabela:\n{e}\n\n{traceback.format_exc()}</pre>", mimetype="text/html", status=500)

    # =========================
    # Upload de Excel → escreve na planilha
    # =========================
    @app.post("/send-to-sheets")
    def send_to_sheets():
        """
        Upload de Excel (.xlsx) e escrita na aba.
        Params (query):
          - spreadsheet_id (opcional)
          - sheet (opcional)
        Body (multipart/form-data):
          - file = arquivo .xlsx
        Header opcional (se habilitar API key):
          - X-API-KEY: <valor>
        """
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
            df = pd.read_excel(io.BytesIO(content))  # requer openpyxl
        except Exception as e:
            return jsonify({"error": "Falha ao ler Excel", "detail": str(e)}), 400

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
            return jsonify({"error": "Falha ao escrever no Google Sheets (APIError)", "detail": str(e)}), 500
        except Exception as e:
            return jsonify({
                "error": "Falha ao escrever no Google Sheets",
                "detail": str(e),
                "trace": traceback.format_exc()
            }), 500

    # =========================
    # Handlers globais
    # =========================
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
