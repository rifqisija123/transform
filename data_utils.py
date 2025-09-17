import pandas as pd
import re

def load_dapil_mapping(file_path):
    """Load file referensi dapil (name -> dapil_id)."""
    df = pd.read_csv(file_path, dtype=str)
    mapping = dict(zip(df["name"], df["dapil_id"]))
    return mapping

def hitung_data_tidak_lengkap(file_path, kolom_awal, kolom_akhir):
    df = pd.read_csv(file_path)
    kolom_range = df.loc[:, kolom_awal:kolom_akhir]
    mask_tidak_lengkap = kolom_range.isnull().any(axis=1) | (
        kolom_range.astype(str).replace(['nan', ''], pd.NA).isna().any(axis=1)
    )
    baris_tidak_lengkap = df[mask_tidak_lengkap]
    total_tidak_lengkap = mask_tidak_lengkap.sum()
    return total_tidak_lengkap,
    
def bersihkan_nama(nama):
    if pd.isna(nama):
        return ""
    return re.split(r",", str(nama))[0].strip()

def transform_data(file_path, provinsi, dapil_file="dapil_dprd.csv"):
    df = pd.read_csv(file_path, dtype=str)

    mapping = load_dapil_mapping(dapil_file)
    dapil_id = mapping.get(provinsi, "")
    if "nama_lengkap" in df.columns:
        idx = df.columns.get_loc("nama_lengkap")
        df.insert(idx, "dapil_id", dapil_id)
        
    if "nama_lengkap" in df.columns:
        df["nama_lengkap"] = df["nama_lengkap"].apply(bersihkan_nama)

    if "tanggal_lahir" in df.columns:
        df["tanggal_lahir"] = df["tanggal_lahir"].astype(str).str.strip()
        df["tanggal_lahir"] = df["tanggal_lahir"].str.replace(r"\s*00:00:00", "", regex=True)
        df["tanggal_lahir"] = pd.to_datetime(
            df["tanggal_lahir"],
            errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    if "usia" in df.columns:
        df["usia"] = df["usia"].astype(str).str.replace("tahun", "", case=False).str.strip()
        df["usia"] = df["usia"].replace(["nan", "NaT", "None"], "")

    if "jenis_kelamin" in df.columns:
        df["jenis_kelamin"] = df["jenis_kelamin"].replace({
            "LAKI-LAKI": "L",
            "Laki-laki": "L",
            "laki-laki": "L",
            "PEREMPUAN": "P",
            "Perempuan": "P",
            "perempuan": "P",
        })

    if "status_perkawinan" in df.columns:
        df["status_perkawinan"] = df["status_perkawinan"].replace({
            "P": "Pernah"
        })

    if "agama" in df.columns:
        mapping_agama = {
            "Islam": "1001001",
            "Kristen Protestan": "1001002",
            "Kristen Katholik": "1001005",
            "Budha": "1001004",
            "Hindu": "1001003",
            "Konghucu": "1001006",
        }
        df["kode_agama"] = df["agama"].map(mapping_agama)

        kolom = list(df.columns)
        if "agama" in kolom:
            idx = kolom.index("agama") + 1
            kolom.remove("kode_agama")
            kolom.insert(idx, "kode_agama")
            df = df[kolom]

    for col in ["rt_", "rw"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r"\.0$", "", regex=True)
            df[col] = df[col].replace("nan", "")

    if "provinsi" in df.columns:
        mapping_provinsi = {
            "ACEH": "11",
            "SUMATERA UTARA": "12",
            "SUMATERA BARAT": "13",
            "RIAU": "14",
            "JAMBI": "15",
            "SUMATERA SELATAN": "16",
            "BENGKULU": "17",
            "LAMPUNG": "18",
            "KEPULAUAN BANGKA BELITUNG": "19",
            "KEPULAUAN RIAU": "21",
            "DKI JAKARTA": "31",
            "JAWA BARAT": "32",
            "JAWA TENGAH": "33",
            "DAERAH ISTIMEWA YOGYAKARTA": "34",
            "JAWA TIMUR": "35",
            "BANTEN": "36",
            "BALI": "51",
            "NUSA TENGGARA BARAT": "52",
            "NUSA TENGGARA TIMUR": "53",
            "KALIMANTAN BARAT": "61",
            "KALIMANTAN TENGAH": "62",
            "KALIMANTAN SELATAN": "63",
            "KALIMANTAN TIMUR": "64",
            "KALIMANTAN UTARA": "65",
            "SULAWESI UTARA": "71",
            "SULAWESI TENGAH": "72",
            "SULAWESI SELATAN": "73",
            "SULAWESI TENGGARA": "74",
            "GORONTALO": "75",
            "SULAWESI BARAT": "76",
            "MALUKU": "81",
            "MALUKU UTARA": "82",
            "P A P U A": "91",
            "PAPUA BARAT": "92",
            "PAPUA SELATAN": "93",
            "PAPUA TENGAH": "94",
            "PAPUA PEGUNUNGAN": "95",
            "PAPUA BARAT DAYA": "96",
        }

        df["provinsi"] = df["provinsi"].astype(str).str.strip().str.upper()

        df["kode_provinsi"] = df["provinsi"].map(mapping_provinsi)

        df["provinsi"] = df["provinsi"].replace(["nan", "NaT", "None", "NAN"], "").fillna("")
        df["kode_provinsi"] = df["kode_provinsi"].replace(["nan", "NaT", "None", "NAN"], "").fillna("")

        kolom = list(df.columns)
        if "provinsi" in kolom:
            idx = kolom.index("provinsi") + 1
            kolom.remove("kode_provinsi")
            kolom.insert(idx, "kode_provinsi")
            df = df[kolom]

    if "pekerjaan" in df.columns:
        mapping_pekerjaan = {
            "SWASTA/WIRASWASTA/LAINNYA": "3015",
            "ANGGOTA DPRD KAB/KOTA": "3063",
            "ANGGOTA DPRD PROVINSI": "3062",
            "ADVOKAT": "3067",
            "ANGGOTA DPR": "3048",
            "PERANGKAT DESA": "3085",
            "PEJABAT/KARYAWAN BUMN/BUMD": "3016",
            "PEJABAT/KARYAWAN PADA BADAN LAIN YANG BERSUMBER DARI KEUANGAN NEGARA": "3016",
            "KEPALA DESA": "3086",
            "PNS/ASN": "3005",
            "Anggota DPD": "3049",
            "NOTARIS": "3068",
            "TNI": "3006",
            "POLRI": "3007",
        }

        df["pekerjaan"] = df["pekerjaan"].astype(str).str.strip().str.upper()

        df["kode_pekerjaan"] = df["pekerjaan"].map(mapping_pekerjaan)

        df["pekerjaan"] = df["pekerjaan"].replace(["nan", "NaT", "None", "NAN"], "").fillna("")
        df["kode_pekerjaan"] = df["kode_pekerjaan"].replace(["nan", "NaT", "None", "NAN"], "").fillna("")

        kolom = list(df.columns)
        if "pekerjaan" in kolom:
            idx = kolom.index("pekerjaan") + 1
            kolom.remove("kode_pekerjaan")
            kolom.insert(idx, "kode_pekerjaan")
            df = df[kolom]

    if "partai" in df.columns:
        mapping_partai = {
            "Partai Kebangkitan Bangsa": "100001",
            "Partai Gerakan Indonesia Raya": "100002",
            "Partai Demokrasi Indonesia Perjuangan": "100003",
            "Partai Golongan Karya": "100004",
            "Partai NasDem": "100005",
            "Partai Buruh": "100006",
            "Partai Gelombang Rakyat Indonesia": "100007",
            "Partai Keadilan Sejahtera": "100008",
            "Partai Kebangkitan Nusantara": "100009",
            "Partai Hati Nurani Rakyat": "100010",
            "Partai Garda Republik Indonesia": "100011",
            "Partai Amanat Nasional": "100012",
            "Partai Bulan Bintang": "100013",
            "Partai Demokrat": "100014",
            "Partai Solidaritas Indonesia": "100015",
            "PARTAI PERINDO": "100016",
            "Partai Persatuan Pembangunan": "100017",
            "Partai Nanggroe Aceh": "100018",
            "Partai Generasi Atjeh Beusaboh Tha`at Dan Taqwa": "100019",
            "Partai Darul Aceh": "100020",
            "Partai Aceh": "100021",
            "Partai Adil Sejahtera Aceh": "100022",
            "PARTAI SIRA (SOLIDITAS INDEPENDEN RAKYAT ACEH)": "100023",
            "Partai Ummat": "100024",
            "Partai Beringin Karya": "100025",
            "Partai Karya Peduli Bangsa": "100026",
            "Partai Pengusaha dan Pekerja Indonesia": "100027",
            "Partai Peduli Rakyat Nasional": "100028",
            "Partai Barisan Nasional": "100029",
            "Partai Keadilan dan Persatuan Indonesia": "100030",
            "Partai Perjuangan Indonesia Baru": "100031",
            "Partai Kedaulatan": "100032",
            "Partai Persatuan Daerah": "100033",
            "Partai Pemuda Indonesia": "100034",
            "Partai Nasional Indonesia Marhaenisme": "100035",
            "Partai Demokrasi Pembaruan": "100036",
            "Partai Karya Perjuangan": "100037",
            "Partai Matahari Bangsa": "100038",
            "Partai Penegak Demokrasi Indonesia": "100039",
            "Partai Demokrasi Kebangsaan": "100040",
            "Partai Republika Nusantara": "100041",
            "Partai Pelopor": "100042",
            "Partai Damai Sejahtera": "100043",
            "Partai Nasional Benteng Kerakyatan Indonesia": "100044",
            "Partai Bintang Reformasi": "100045",
            "Partai Patriot": "100046",
            "Partai Kasih Demokrasi Indonesia": "100047",
            "Partai Indonesia Sejahtera": "100048",
            "Partai Kebangkitan Nasional Ulama": "100049",
            "Partai Sarikat Indonesia": "100050",
        }

        df["partai"] = df["partai"].astype(str).str.strip()

        df["kode_partai"] = df["partai"].map(mapping_partai)

        df.loc[df["partai"].str.upper() == "PARTAI PERINDO", "kode_partai"] = "100016"

        kolom = list(df.columns)
        if "partai" in kolom:
            idx = kolom.index("partai") + 1
            kolom.remove("kode_partai")
            kolom.insert(idx, "kode_partai")
            df = df[kolom]

    return df
