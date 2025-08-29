import os
from flask import Flask, jsonify

def create_app():
    app = Flask(__name__)
    app.config["JSON_SORT_KEYS"] = False

    @app.route("/")
    def index():
        return jsonify({"message": "OK", "endpoints": ["/", "/health"]})

    @app.route("/health")
    def health():
        return jsonify({"status": "ok"}), 200

    return app

app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
