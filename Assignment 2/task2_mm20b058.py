from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import zipfile
import os
import pandas as pd
import apache_beam as beam
#from apache_beam.options.pipeline_options import PipelineOptions
#import apache_beam.runners.interactive.interactive_beam as ib
import json
import pickle
import matplotlib.pyplot as plt
import geopandas

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


dag = DAG(
    'geomaps_pipeline',
    default_args=default_args,
    description='Airflow pipeline for processing and visualizing geospatial data',
    schedule_interval=None,
)
year = '2000'
num_stations = 25 
required_fields = ['DATE','HourlyWindSpeed'] 
root = '/opt/airflow/dags/weatherdata/' 

url = f'https://www.ncei.noaa.gov/data/local-climatological-data/access/'

p_zip = '/tmp/loc.zip'
p_unzip = '/tmp/loc1'
wait_for_archive = FileSensor(
    task_id='wait_for_archive',
    filepath= p_zip,
    fs_conn_id = 'bdl_2',
    timeout=5,
    poke_interval = 1,  # Adjust this value according to your requirements
    mode='poke',
    dag=dag,
)

wait_for_archive_task = FileSensor(
task_id=f'wait_for_archive_{year}',
poke_interval = 1,
timeout=60,      # Timeout after 300 seconds (5 minutes)
mode = 'poke',
filepath=f'{root}zip_{year}.zip',
)

def unzip_file():
    zip_file_path = f'{root}zip_{year}.zip'
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        is_valid = zip_ref.testzip() is None
        if is_valid:
            os.mkdir(f'{root}extracted_{year}')
            zip_ref.extractall(f'{root}extracted_{year}')
            print("ZIP file extracted successfully")
        else:
            raise Exception("Invalid ZIP file")


unzip_archive_task = PythonOperator(
task_id = f'unzip_archive_{year}',
python_callable = unzip_file,
)

def process_with_beam():

    def process_csv(csv_file):
        df = pd.read_csv(csv_file)
        filtered_df = df[required_fields].copy()
        lat,lon = df['LATITUDE'].iloc[0],df['LONGITUDE'].iloc[0]
        filtered_df['DATE'] = pd.to_datetime(filtered_df['DATE'], format='%Y-%m-%dT%H:%M:%S')
        filtered_df['DATE'] = filtered_df['DATE'].dt.strftime('%m-%Y')
        for field in required_fields:
            if field!='DATE':
                filtered_df[field] = pd.to_numeric(filtered_df[field], errors='coerce')
        filtered_df = filtered_df.groupby(['DATE']).mean()
        filtered_df['DATE'] = filtered_df.index
        filtered_df = filtered_df.reset_index(drop=True)
        data_tuple = (lat,lon,filtered_df.values.tolist())
        file_name = csv_file.split('/')[-1]
        with open(f'{root}extracted_{year}/{file_name[:-4]}.pickle', 'wb') as f:
            pickle.dump(data_tuple, f)


    csv_files = []
    for r,dir,files in os.walk(f'{root}extracted_{year}/'):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(r,file))

    with beam.Pipeline() as pipeline:
        grouped_data = (
            pipeline
            | 'Create'>> beam.Create(csv_files)
            | 'Process'>> beam.Map(process_csv)
        )


process_csv_task = PythonOperator(
task_id = f'process_csv_{year}',
python_callable = process_with_beam,
)

def group_with_beam():

    def extract_df(file):
        with open(file, 'rb') as f:
            loaded_tuple = pickle.load(f)
        lat = loaded_tuple[0]
        lon = loaded_tuple[1]
        data = loaded_tuple[2]
        col_list = []
        for field in required_fields:
            if field!='DATE':
                col_list.append(field)
        col_list.append('DATE')
        df = pd.DataFrame(data,columns=col_list)
        df['Latitude'] = lat
        df['Longitude'] = lon
        print(f"Extracted DataFrame: {df.head()}")
        return df


    def group_dfs(dfs):
        dfs_list = []

        for df in dfs:    
            dfs_list.append(df)
        merged_df = pd.concat(dfs_list,ignore_index=True)
        grouped = merged_df.groupby('DATE')
        for name, grp in grouped:
            df = grp
            with open(f'{root}extracted_{year}/{name}_grouped.pickle', 'wb') as f:
                pickle.dump(df, f)
        return merged_df

    pickle_files = []
    for r,dir,files in os.walk(f'{root}extracted_{year}/'):
        for file in files:
            if file.endswith('.pickle'):
                pickle_files.append(os.path.join(r,file))

    with beam.Pipeline() as pipeline:
        group_df = (
            pipeline
            | 'Create'>> beam.Create(pickle_files)
            | 'Process1'>> beam.Map(extract_df)
            | 'Process2'>> beam.CombineGlobally(group_dfs)
        )

group_csv_task = PythonOperator(
task_id = f'group_csv_{year}',
python_callable = group_with_beam,
)
    
def create_plots_with_beam():

    def create_plots(file):
        with open(file, 'rb') as f:
            df = pickle.load(f)
        gdf = geopandas.GeoDataFrame(
            df, geometry=geopandas.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326"
        )
        
        world = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))
        ax = world.clip([-100, 25, 100, 90]).plot(color="green", edgecolor="black")
        # We can now plot our `GeoDataFrame`.
        
        
        for field in required_fields:
            if field!='DATA':
                file_name = file.split('/')[-1]
    #                        plt.figure()
                gdf.plot(column=field, ax=ax, cmap='coolwarm')
    #                        cbar = plt.colorbar(ax.get_children()[1], ax=ax, orientation='vertical')
    #                        cbar.set_label(f'{field}')
                plt.xlabel('Longitude')
                plt.ylabel('Latitude')
                plt.title(f'{field} : Period {file_name[:-15]}')
                plt.savefig(f'{file[:-15]}.png',bbox_inches="tight")


    grp_pickles = []
    for r,dir,files in os.walk(f'{root}extracted_{year}/'):
        for file in files:
            if file.endswith('grouped.pickle'):
                grp_pickles.append(os.path.join(r,file))

    with beam.Pipeline() as pipeline:
        plotfigs = (
            pipeline
            | 'Create'>> beam.Create(grp_pickles)
            | 'Plot'>> beam.Map(create_plots)
        )

create_plots_task = PythonOperator(
task_id=f'create_plots_{year}',
python_callable=create_plots_with_beam,
)

wait_for_archive_task >> unzip_archive_task >> process_csv_task >> group_csv_task >> create_plots_task