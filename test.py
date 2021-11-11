from concurrent.futures import ThreadPoolExecutor
from time import time
from tqdm import tqdm
import os
from datetime import datetime as dt
import xarray as xr
import pandas as pd
from functools import partial
import pprint


# Download parralellization part

def setup_directories():
    folders = ['assets', 'csv']
    for folder in folders:
        try:
            if not os.path.exists(folder):
                # Faz pasta relativo ao diretório atual
                os.makedirs(os.path.join(os.getcwd(), folder))
        except OSError:
            print("Erro ao criar o diretório")

def make_folder(year, month, day, hour):
    """
    Function makes  folder and return the  path to the folder  formatted idealy
    it's executed on assets and csv folder, please take care it can make a mess
    :year: int
    :month: int
    :day: int
    :hour: int
    :returns: string
    """
    curr_dir = os.getcwd()

    # Cria estrutura de diretórios organizada
    for param in [str(year), str(month), str(day), str(hour)]:
        try:
            if not os.path.exists(param):
                os.makedirs(param)
            os.chdir(os.path.join(os.getcwd(), param))
        except OSError:
            print("Erro ao criar o diretório", param)
        pass
    # Path pra pasta com nome formatado
    folder_str = os.getcwd()

    # Retorna ao diretório de origem
    os.chdir(curr_dir)
    return folder_str


def download_and_move(year, day_in_year, hour, folder_str, d_vars=['event_id', 'event_energy', 'event_lat', 'event_lon', 'event_time_offset', 'event_parent_group_id']):
    from google.cloud import storage

    bucket_name = 'gcp-public-data-goes-16'
    storage_client = storage.Client.create_anonymous_client()
    bucket = storage_client.bucket(bucket_name)
    blobs = [blob.name for blob in bucket.list_blobs(prefix=f'GLM-L2-LCFA/{year}/{day_in_year}/{hour}')]


    def download(file):
      blob = bucket.blob(file)
      print(f'downloading: {file}\n')
      localfilename = file.removeprefix(f'GLM-L2-LCFA/{year}/{day_in_year}/{hour}/')
      blob.download_to_filename(os.path.join(folder_str, localfilename))
      pprint.pprint(f'file: {os.path.join(folder_str, localfilename)}')
      xr_dset = xr.open_dataset(os.path.join(folder_str, localfilename))
      xr_dset.drop_vars(names=d_vars)
      xr_dset.to_netcdf(os.path.join(folder_str, localfilename))
      xr_dset.close()

    with ThreadPoolExecutor() as executor:
      executor.map(download, blobs)


def assets_download(dic_start_params, dic_end_params):
    """
    Function gets two dictionaries, one for input data, the other for output
    data. For now the year from the start and end dicts have to be the same.
    """

    # import s3fs
    setup_directories()
    curr_path = os.getcwd()
    os.chdir('assets')

    # Inicializa o fs com credenciais anônimas p/ acessar dados públicos
    #fs = s3fs.S3FileSystem(anon=True)

    # ano/mês/dia/hora início e fim
    try:
        if dic_start_params['year'] == dic_end_params['year'] and \
           dic_start_params['month'] == dic_end_params['month']:
            year = int(dic_start_params['year'])
            month = int(dic_start_params['month'])
        else:
            raise ValueError
    # Limitando por hora p/ manter controle do backend
    except ValueError:
        print('Ano ou mês de início e de fim diferem')
    day_s = int(dic_start_params['day'])
    day_e = int(dic_end_params['day'])
    hour_s = int(dic_start_params['hour'])
    hour_e = int(dic_end_params['hour'])

    # Preparando iterables p/ for
    if day_s != day_e:
        days = range(day_s, day_e + 1)
    else:
        days = [day_s]
    hours = range(hour_s, hour_e + 1)

    for day in days:
        today = dt(year, month, day, 0, 0)
        day_in_year = (today - dt(year, 1, 1)).days + 1

        for hour in hours:
            folder_str = make_folder(year, month, day, hour)
            print(folder_str)
            download_and_move(year,str(day_in_year).zfill(3), str(hour).zfill(2), folder_str)

    # Retornando p/ diretório de origem
    os.chdir(curr_path)


# Processing parralellization part


def merge_csv(tmp_dir, csv_dir, categories, csv_time):
    """
    Merges csvs in a folder

    :dir: TODO
    :category: TODO
    :returns: TODO

    """
    from glob import glob
    # root_dir = os.getcwd()

    for category in categories:
        all_filenames = [i for i in glob(os.path.join(tmp_dir, f'{category}*'))]
        # combine all files in the list
        combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames],
                                 ignore_index=False)
        # export to csv
        # Remove coluna 'Unnamed'
        combined_csv = combined_csv.loc[:, ~combined_csv.
                                            columns.str.contains('^Unnamed')]
        # Sorting do dataframe pelo id
        combined_csv = combined_csv.sort_values(f'{category}_id')
        # Remove coluna de índices
        combined_csv.to_csv(os.path.join(csv_dir, f'{category}_{csv_time}.csv'), index=False)
    # delete tmp files
    for tmp_csvs in os.listdir(tmp_dir):
        os.remove(os.path.join(tmp_dir,tmp_csvs))
    # windows e ntfs...
    # Retorna ao diretório de origem
    # os.chdir(root_dir)
    # remove tmp directory
    os.rmdir(tmp_dir)
    # os.chdir(csv_dir)


def flash_csv(file, file_idx, tmp_dir, root_dir):
    """
    Function generates csv of flash data based on netCDF4.Dataset given

    :df: netCDF4.Dataset
    :returns: TODO

    """
    # Abre o arquivo
    glm_data = xr.open_dataset(file)

    # Organizando variáveis de tempo
    time_offsets = glm_data.flash_time_offset_of_first_event
    # Cria Dataframe com as listas p/ exportar em .csv
    flashes = pd.DataFrame.from_dict({
        'flash_id': glm_data.flash_id,
        'flash_lat': glm_data.flash_lat,
        'flash_lon': glm_data.flash_lon,

        'Ano': time_offsets.dt.year,
        'Mes': time_offsets.dt.month,
        'Dia': time_offsets.dt.day,
        'Hora': time_offsets.dt.hour,
        'Minuto': time_offsets.dt.minute,
        'Segundo': time_offsets.dt.second,

        'flash_time_offset_of_first_event': glm_data.flash_time_offset_of_first_event.dt.microsecond,
        'flash_time_offset_of_last_event': glm_data.flash_time_offset_of_last_event.dt.microsecond,
        'flash_area': glm_data.flash_area,
        'flash_energy': glm_data.flash_energy,
        'flash_quality_flag': glm_data.flash_quality_flag
    })
    # Fecha o Dataset
    glm_data.close()
    flashes.to_csv(os.path.join(tmp_dir, f'flash_{file_idx}.csv'))
    # Retorna ao diretório raiz
    os.chdir(root_dir)


def group_csv(file, file_idx, tmp_dir, root_dir):
    """
    Function generates csv of flash data based on netCDF4.Dataset given

    :df: netCDF4.Dataset
    :returns: TODO

    """
    # Abre arquivo
    glm_data = xr.open_dataset(file)
    # Organizando variáveis de tempo
    time_offsets = glm_data.group_time_offset
    # Cria Dataframe com as listas p/ exportar em .csv
    groups = pd.DataFrame.from_dict({
        'group_id': glm_data.group_id,
        'group_lat': glm_data.group_lat,
        'group_lon': glm_data.group_lon,

        'Ano': time_offsets.dt.year,
        'Mes': time_offsets.dt.month,
        'Dia': time_offsets.dt.day,
        'Hora': time_offsets.dt.hour,
        'Minuto': time_offsets.dt.minute,
        'Segundo': time_offsets.dt.second,

        'group_time_offset': time_offsets.dt.microsecond,
        'group_energy': glm_data.group_energy,
        'group_parent_flash_id': glm_data.group_parent_flash_id,
        'group_quality_flag': glm_data.group_quality_flag,
    })
    # Fecha dataset
    glm_data.close()
    groups.to_csv(os.path.join(tmp_dir,f'group_{file_idx}.csv'))
    # Retorna ao diretório raiz
    os.chdir(root_dir)


def event_csv(file, file_idx, tmp_dir, root_dir):
    """
    Function generates csv of flash data based on netCDF4.Dataset given

    :df: netCDF4.Dataset
    :returns: None

    """
    # Abre arquivo
    glm_data = xr.open_dataset(file)

    # Organizando variáveis de tempo
    time_offsets = glm_data.event_time_offset
    # Cria Dataframe com as listas p/ exportar em .csv
    events = pd.DataFrame.from_dict({
        'event_id': glm_data.event_id,
        'event_lat': glm_data.event_lat,
        'event_lon': glm_data.event_lon,

        'Ano': time_offsets.dt.year,
        'Mes': time_offsets.dt.month,
        'Dia': time_offsets.dt.day,
        'Hora': time_offsets.dt.hour,
        'Minuto': time_offsets.dt.minute,
        'Segundo': time_offsets.dt.second,

        'event_time_offset': time_offsets.dt.offset,
        'event_energy': glm_data.event_energy,
        'event_parent_group_id': glm_data.event_parent_group_id,
        'event_count': glm_data.event_count,
    })
    glm_data.close()
    events.to_csv(os.path.join(tmp_dir, f'event{file_idx}.csv'))
    # Retorna ao diretório raiz
    os.chdir(root_dir)

def csv_filter(csv_path, csv_time, categories,
               lat1=-79, lat2=-45, lon1=-14, lon2=8, rm_orig=False):
    """
    uses data to filter out csv

    :csv_path: todo
    :coordinates: todo
    :returns: todo

    """
    if not os.path.exists(csv_path):
        print('files does\'nt exist')
        return
    for category in categories:
        # colunas:
        # category_id,
        # category_lat,
        # category_lon,
        # ...
        orig_csv = os.path.join(csv_path, f'{category}_{csv_time}.csv')
        filtered_csv = os.path.join(csv_path, f'{category}-filtered_{csv_time}.csv')
        data = pd.read_csv(orig_csv)
        filter_mask = (lat1 < data[f'{category}_lat']) & (data[f'{category}_lat'] < lat2) & (lon1 < data[f'{category}_lon']) & (data[f'{category}_lon'] < lon2)
        filtered_data = data[filter_mask]
        filtered_data.to_csv(filtered_csv)

        if rm_orig:
            os.remove(orig_csv)

def parallel(categories, file_path, file_idx, tmp_dir, root_dir):
    '''
    highly specific function for handling parralellization
    '''
    csv_functions = {
            'flash': flash_csv,
            'group': group_csv,
            'event': event_csv
    }
    for category in categories:
        with ThreadPoolExecutor() as executor:
            func = partial(csv_functions[category], tmp_dir=tmp_dir, root_dir=root_dir)
            args = zip(file_path, file_idx)
            executor.map(lambda arg: func(arg[0], arg[1]), args)
            executor.shutdown(wait=True)
    return True


def create_csv(year, month, day, hour, categories, root_dir=os.getcwd(),
               lat1=-14, lat2=8, lon1=-79, lon2=-45):
    """
    Function manipulates .nc files located at args and creates a .csv
    based on it's parameters

    :year: int
    :moth: int
    :day: int
    :hour: int
    :radius: float
    :category: string
    :center: tuple
    :root_dir: string
    :returns: TODO

    """

    # Pasta com os .nc
    assets_dir = os.path.join(root_dir, 'assets',
                              str(year), str(month), str(day), str(hour))

    if not os.path.exists(assets_dir):
        print('assets_dir não existe')
        return

    assets = os.listdir(assets_dir)
    # Pasta de destino
    csv_time = f'{year}_{month}_{day}_{hour}'
    folder_name = f'{year}_{month}_{day}'
    csv_dir = os.path.join(root_dir, 'csv', folder_name)
    # Pasta temporária
    tmp_dir = os.path.join(csv_dir, 'tmp')
    if not os.path.exists(csv_dir):
        os.mkdir(csv_dir)
    if not os.path.exists(tmp_dir):
        os.mkdir(tmp_dir)

    # Cria os csvs
    file_path = [os.path.join(assets_dir, file) for file in assets]
    file_idx = [i for i in range(len(file_path))]

    done = parallel(categories, file_path, file_idx, tmp_dir, root_dir)
    print('finished csv creation')
    while not done:
        parallel(categories, file_path, file_idx, tmp_dir, root_dir)
    merge_csv(tmp_dir, csv_dir, categories, csv_time)
    print('finished csv merging')
    csv_filter(csv_dir, csv_time, categories,lat1=lat1, lat2=lat2, lon1=lon1,
               lon2=lon2)
    print('finished csv filtering')


def data_acces(dic_start_params, dic_end_params, categories,
        dic_coordinates={
            'lat1': -79,
            'lat2': -45,
            'lon1': -14,
            'lon2': 8
            }):
    """
    Utilizes create_csv to access data on assets folder and generate csv
    files

    :params: dictionary
    :returns: nothing

    """
    try:
        if dic_start_params['year'] == dic_end_params['year'] and \
           dic_start_params['month'] == dic_end_params['month']:
            year = int(dic_start_params['year'])
            month = int(dic_start_params['month'])
        else:
            raise ValueError
    # Limitando por hora p/ manter controle do backend
    except ValueError:
        print('Ano ou mês de início e de fim diferem')
    day_s = int(dic_start_params['day'])
    day_e = int(dic_end_params['day'])
    hour_s = int(dic_start_params['hour'])
    hour_e = int(dic_end_params['hour'])

    # Preparando iterables p/ for
    if day_s != day_e:
        days = range(day_s, day_e + 1)
    else:
        days = [day_s]
    hours = range(hour_s, hour_e + 1)

    # Coodenadas p/ recorte:
    lat1 = dic_coordinates['lat1']
    lat2 = dic_coordinates['lat2']
    lon1 = dic_coordinates['lon1']
    lon2 = dic_coordinates['lon2']
    for day in days:
        for hour in hours:
            create_csv(year, month, day, hour, categories,
                       lat1=lat1, lat2=lat2, lon1=lon1, lon2=lon2)


start_params = {
    'hour': '1',
    'day': '24',
    'month': '2',
    'year': '2019'
}

end_params = {
    'hour': '1',
    'day': '24',
    'month': '2',
    'year': '2019'
}

dic_coordinates={
    'lat1': -45,
    'lat2': -79,
    'lon1': -14,
    'lon2': 8
}

categories = ['flash', 'group']

tic = time()
assets_download(start_params, start_params)
# data_acces(start_params, end_params, categories, dic_coordinates)
tac = time()
print(f'elapsed: {tac - tic}')
