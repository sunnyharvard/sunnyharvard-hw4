from flask import Flask, request
import api.county_data as county_data

app = Flask(__name__)

@app.route("/county_data", methods=["POST"])
def run_handler():
    return county_data.handler(request)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
