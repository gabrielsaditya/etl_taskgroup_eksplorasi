from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import requests
import json
import os
import logging
import csv

FOLDER_DATA = "/opt/airflow/dags/data"  # folder buat naro data hasil ambilan API

argumen_default = {
    "owner": "gabriel",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

def siapin_folder():
    os.makedirs(FOLDER_DATA, exist_ok=True)

def ambil_dan_simpan(url: str, nama_file: str, **context):
    siapin_folder()
    logging.info("🚀 Lagi ambil data dari %s", url)
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    lokasi_file = os.path.join(FOLDER_DATA, nama_file)
    with open(lokasi_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info("✅ Nyimpen %d record ke %s", (len(data) if isinstance(data, list) else 1), lokasi_file)
    return lokasi_file

def gabungin_file(posts_path: str, comments_path: str, nama_output: str = "gabungan.json", **context):
    siapin_folder()
    with open(posts_path, "r", encoding="utf-8") as f:
        posts = json.load(f)
    with open(comments_path, "r", encoding="utf-8") as f:
        comments = json.load(f)
    hasil_gabungan = {
        "jumlah_post": len(posts) if isinstance(posts, list) else 1,
        "jumlah_komentar": len(comments) if isinstance(comments, list) else 1,
        "postingan": posts,
        "komentar": comments,
    }
    lokasi_output = os.path.join(FOLDER_DATA, nama_output)
    with open(lokasi_output, "w", encoding="utf-8") as f:
        json.dump(hasil_gabungan, f, ensure_ascii=False, indent=2)
    logging.info("🎉 File gabungan udah jadi di %s", lokasi_output)
    return lokasi_output

def json_ke_csv(path_json: str, nama_csv: str = "data_api_gabungan.csv", **context):
    siapin_folder()
    lokasi_csv = os.path.join(FOLDER_DATA, nama_csv)

    with open(path_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Flatten data jadi baris2 CSV sederhana
    with open(lokasi_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        # header
        writer.writerow(["tipe", "id", "isi"])
        # posts
        for p in data["postingan"]:
            writer.writerow(["post", p.get("id"), p.get("title")])
        # komentar
        for c in data["komentar"]:
            writer.writerow(["komentar", c.get("id"), c.get("body")])

    logging.info("📂 CSV berhasil dibuat di %s", lokasi_csv)
    return lokasi_csv

with DAG(
    dag_id="contoh_taskgroup_api_santai",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    default_args=argumen_default,
    catchup=False,
    tags=["belajar", "taskgroup", "api", "santai"],
) as dag:

    mulai = PythonOperator(
        task_id="siapin_folder",
        python_callable=siapin_folder,
    )

    with TaskGroup("grup_ambil", tooltip="Ambil posts & komentar barengan") as grup_ambil:
        ambil_posts = PythonOperator(
            task_id="ambil_posts",
            python_callable=ambil_dan_simpan,
            op_kwargs={
                "url": "https://jsonplaceholder.typicode.com/posts",
                "nama_file": "posts.json",
            },
        )

        ambil_komentar = PythonOperator(
            task_id="ambil_komentar",
            python_callable=ambil_dan_simpan,
            op_kwargs={
                "url": "https://jsonplaceholder.typicode.com/comments",
                "nama_file": "komentar.json",
            },
        )

    gabung = PythonOperator(
        task_id="gabung_posts_komentar",
        python_callable=gabungin_file,
        op_kwargs={
            "posts_path": "{{ ti.xcom_pull(task_ids='grup_ambil.ambil_posts') }}",
            "comments_path": "{{ ti.xcom_pull(task_ids='grup_ambil.ambil_komentar') }}",
            "nama_output": "data_api_gabungan.json",
        },
    )

    selesai = PythonOperator(
        task_id="selesai_bikin_csv",
        python_callable=json_ke_csv,
        op_kwargs={
            "path_json": "{{ ti.xcom_pull(task_ids='gabung_posts_komentar') }}",
            "nama_csv": "data_api_gabungan.csv",
        },
    )

    mulai >> grup_ambil >> gabung >> selesai
