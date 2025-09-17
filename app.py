from flask import Flask, render_template, request, send_file
import pandas as pd
import os
from data_utils import hitung_data_tidak_lengkap, transform_data

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

DAPIL_FILE = "dapil_dprd.csv"

@app.route("/")
def home():
    try:
        df_dapil = pd.read_csv(DAPIL_FILE, dtype=str)
        provinsi_list = df_dapil["name"].dropna().unique().tolist()
    except Exception as e:
        provinsi_list = []
        print("Gagal load dapil:", e)

    return render_template("index.html", provinsi_list=provinsi_list)

@app.route("/cek", methods=["POST"])
def cek():
    file = request.files.get("file")
    if not file:
        return "Tidak ada file diunggah", 400

    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(file_path)

    kolom_awal = "dapil_id"
    kolom_akhir = "nomor_urut"
    total, data_tidak_lengkap = hitung_data_tidak_lengkap(file_path, kolom_awal, kolom_akhir)
    tabel_html = data_tidak_lengkap.to_html(classes="table table-bordered table-striped table-hover", index=False)

    return render_template("result.html", total=total, table_html=tabel_html)

@app.route("/transform", methods=["POST"])
def transform():
    file = request.files.get("file")
    provinsi = request.form.get("provinsi")

    if not file:
        return "Tidak ada file diunggah", 400

    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(file_path)

    df_transformed = transform_data(file_path, provinsi, dapil_file=DAPIL_FILE)
    output_path = os.path.join(UPLOAD_FOLDER, "transformed.csv")
    df_transformed.to_csv(output_path, index=False, encoding="utf-8-sig")

    tabel_html = df_transformed.to_html(classes="table table-bordered table-striped table-hover", index=False)

    return render_template("transform.html", table_html=tabel_html, download_file="transformed.csv", provinsi=provinsi)

@app.route("/download/<filename>")
def download(filename):
    return send_file(os.path.join(UPLOAD_FOLDER, filename), as_attachment=True)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5001, debug=True)
