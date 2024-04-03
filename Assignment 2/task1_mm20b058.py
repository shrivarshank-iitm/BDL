from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import re
import random
from zipfile import ZipFile
import shutil
from bs4 import BeautifulSoup
import requests

# Define default_args and DAG parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2000, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id = 'climatological_data_fetch_page',
    default_args=default_args,
    description='Fetch location-wise datasets page for a specific year',
    schedule_interval=None,
) as dag:
    year = '2000'
    num_stations = 25 
    required_fields = ['DATE','HourlyWindSpeed'] 
    root = '/opt/airflow/dags/weatherdata/' 

    url = f'https://www.ncei.noaa.gov/data/local-climatological-data/access/'


    fetch_page_task = BashOperator(
            task_id=f'fetch_page_{year}',
            bash_command=f"curl -f {url}{year}/  -o {root}{year} && echo 'Page fetched successfully'",
        )
                
    def extract_links_from_html(html_file_path,output_file_path):
        with open(html_file_path, 'r') as file:
            html_content = file.read()

        soup = BeautifulSoup(html_content, 'html.parser')

        links = [link.get('href') for link in soup.find_all('a', href=True) if link.get('href').endswith('.csv')]

        links = links[:num_stations]

        with open(output_file_path, 'w') as output_file:
            for link in links:
                output_file.write(f"{link}\n")


    select_files_task = PythonOperator(
        task_id = f'select_files_{year}',
        python_callable = extract_links_from_html,
        op_kwargs = {'html_file_path':f'{root}{year}','output_file_path':f'{root}{year}csvfilelist'},
    )

            
    def fetch_datafiles(filelist_path,output_dirpath):

        with open(filelist_path, 'r') as file:
            csv_files = file.read().splitlines()

        for csv_file_url in csv_files:
            total_csv_url = f"{url}{year}/{csv_file_url}"
            response = requests.get(total_csv_url)

            if response.status_code == 200:
                csv_filename = csv_file_url.split('/')[-1]
                csv_file_path = f"{output_dirpath}{csv_filename[:-4]}_{year}.csv"

                with open(csv_file_path, 'w', newline='') as csvfile:
                    csvfile.write(response.text)
                print(f"Saved data from {csv_file_url} to {csv_file_path}")
            else:
                print(f"Failed to fetch data from {csv_file_url}")


    fetch_data_task = PythonOperator(
        task_id = f'fetch_data_{year}',
        python_callable = fetch_datafiles,
        op_kwargs = {'filelist_path':f'{root}{year}csvfilelist','output_dirpath':root}
    )

        
    def zip_csv_files(input_dir, output_zip_file):
        csv_files = [f for f in os.listdir(input_dir) if f.endswith(f'{year}.csv')]
        with ZipFile(output_zip_file, 'w') as zipf:
            for csv_file in csv_files:
                file_path = os.path.join(input_dir, csv_file)
                zipf.write(file_path, arcname=csv_file)

    zip_files_task = PythonOperator(
        task_id = f'zip_files_{year}',
        python_callable = zip_csv_files,
        op_kwargs = {'input_dir':root,'output_zip_file':f'{root}zip_{year}.zip'}
    )

    def move_archive(destination_path, **kwargs):
        ti = kwargs.get('ti')
        try:
            # Retrieving the zip_path from XCom
            zip_path = ti.xcom_pull(task_ids="zip_files", key="zip_path")
            
            # Log statements for debugging
            print(f"Moving archive from {zip_path} to {destination_path}")

            # Moving the archive
            shutil.move(zip_path, destination_path)

            print("Move successful!")
        except Exception as e:
            print(f"Error moving archive: {e}")

    move_archive_task = PythonOperator(
        task_id='move_archive',
        python_callable=move_archive,
        op_args=['/tmp/loc.zip'],  # Destination path
        provide_context=True,
        dag=dag,  # Make sure to add the DAG instance
    )

    fetch_page_task >> select_files_task >> fetch_data_task >> zip_files_task >> zip_files_task >> move_archive_task