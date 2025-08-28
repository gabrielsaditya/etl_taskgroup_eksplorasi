ETL eksplorasi taskgroup, dockerfile, dan versi airflow v2.x.x

Eksperimen ETL Airflow pakai TaskGroup buat ambil data API paralel, gabungin, terus export ke CSV.

- DAG: `contoh_taskgroup_api_santai`
- Output CSV: `/opt/airflow/dags/data/data_api_gabungan.csv`
- build Docker → docker-compose up → buka Airflow → trigger DAG
