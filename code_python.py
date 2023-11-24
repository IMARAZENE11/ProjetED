from datetime import datetime
import os
import pandas as pd
import sys
import subprocess
subprocess.check_call([sys.executable, "-m", "pip", "install", "apache-airflow"])

import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "apache-airflow"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "sqlalchemy"])

def extract_and_transform(**kwargs):
    try:
        # Créer le répertoire '/data' s'il n'existe pas
        data_dir = os.path.expandvars("${AIRFLOW_HOME}/data")
        os.makedirs(data_dir, exist_ok=True)

        df_urgences = pd.read_csv(
            os.path.join(data_dir, 'donnees-urgences-SOS-medecins.csv'),
            sep="|",
            dtype="unicode",
            names=["dep", "date_de_passage", "sursaud_cl_age_corona", "nbre_pass_corona", "nbre_pass_tot",
                   "nbre_hospit_corona", "nbre_pass_corona_h", "nbre_pass_corona_f", "nbre_pass_tot_h", "nbre_pass_tot_f",
                   "nbre_hospit_corona_h", "nbre_hospit_corona_f", "nbre_acte_corona", "nbre_acte_tot",
                   "nbre_acte_corona_h", "nbre_acte_corona_f", "nbre_acte_tot_h", "nbre_acte_tot_f"])
    
        

        # Supprimer les lignes avec des valeurs manquantes
        df_urgences = df_urgences.dropna()

        # Supprimer les lignes en double
        df_urgences = df_urgences.drop_duplicates()

        # Vérifier les données manquantes
        print(df_urgences.isnull().sum())

        # Afficher le DataFrame
        print(df_urgences)

        # Sauvegarder le DataFrame nettoyé dans un nouveau fichier CSV
        df_urgences.to_csv('/data/donnees_transformees.csv', index=False)

    except Exception as e:
        # Imprimer l'erreur pour le débogage
        print(f"An error occurred: {e}")
        # Lever à nouveau l'exception pour indiquer que la tâche a échoué
        raise
def data_transform_and_load(**kwargs):
    try:
        # Connexion à la base de données PostgreSQL
        conn_id = 'postgres_connexion'  
        engine = create_engine(conn_id)

        # Charger les données transformées depuis le fichier CSV
        
        df_transformed = pd.read_csv('/data/donnees_transformees.csv')

        
        # Charger les données dans la base de données PostgreSQL
        table_name_faits = 'urgences_covid_faits'
        df_transformed.to_sql('urgences_covid_faits', con=engine, index=False, if_exists='replace')
# Liste des tables de dimension à charger
        tables_dimensions = ['departements', 'tranche_age', 'regions', 'sexe']
        
        for table_name_dimension in tables_dimensions:
            # Charger les données dans chaque table de dimension
            df_dimension = pd.read_csv(f'/data/donnees_transformees.csv')  # Remplacez par le chemin vers vos données
            df_dimension.to_sql(table_name_dimension, engine, index=False, if_exists='replace')

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

# Définir le DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}
dag = DAG(
    'ETL_Urgences',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2022, 12, 26),
    catchup=False,
)

# Tâche pour effectuer l'extraction et la transformation
etl_task = PythonOperator(
    task_id='Extract_Transform_Load',
    python_callable=extract_and_transform,
    provide_context=True,
    dag=dag,
)
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_connexion',
    sql='C:\\Users\\Mpscomputer\\Desktop\\projet\\dags\\create_table.sql'
)
transform_and_load = PythonOperator(
    task_id = 'transform_and_load',
    python_callable = data_transform_and_load
    )

etl_task >> create_table >> transform_and_load