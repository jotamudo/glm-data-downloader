"""

TODO: Implementar geração de mapa baseado em csvs gerado
"""

import os
import numpy as np
import shutil
from datetime import datetime
import pandas as pd
import math
from datetime import timedelta


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


def assets_download(dic_start_params, dic_end_params):
    """
    Function gets two dictionaries, one for input data, the other for output
    data. For now the year from the start and end dicts have to be the same.

    """
    import s3fs
    setup_directories()
    curr_path = os.getcwd()
    os.chdir('assets')

    # Inicializa o fs com credenciais anônimas p/ acessar dados públicos
    fs = s3fs.S3FileSystem(anon=True)

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
        today = datetime(year, month, day, 0, 0)
        day_in_year = (today - datetime(year, 1, 1)).days + 1

        for hour in hours:
            # Seleciona o diretório da pool
            files = np.array(fs.ls(
                'noaa-goes16/GLM-L2-LCFA/{0}/{1:03d}/{2:02d}/'.format(
                    year, day_in_year, hour)))

            # Cria a pasta relacionada aos parâmetros
            folder_srt = make_folder(year, month, day, hour)
            cnt = 0
            quant = len(files)

            for file in files:
                # Download dos arquivos
                fs.get(file, file.split('/')[-1])
                cnt += 1
                print(file, f'{(cnt/quant)*(100):.2f}% done')

                # Move o arquivo p/ pasta designada no loop
                ls = os.listdir(os.getcwd())
                for file in ls:
                    if file.endswith('.nc'):
                        shutil.move(
                                os.path.join(os.getcwd(), file),
                                os.path.join(os.getcwd(), folder_srt, file))

    # Retornando p/ diretório de origem
    os.chdir(curr_path)


def csv_folders(year, month, day, hour, root_dir=os.getcwd()):
    """
    Function creates a folder in the csv directory

    :year: TODO
    :month: TODO
    :day: TODO
    :hour: TODO
    :returns: TODO

    """
    folder_name = f'{year}_{month}_{day}_{hour}'
    try:
        if not os.path.exists(os.path.join(root_dir, folder_name)):
            os.mkdir(folder_name)
    except FileExistsError:
        print('Pasta já existe, utilizando existente')
    return os.path.join(root_dir, folder_name)


def merge_csv(tmp_dir, csv_dir, categories):
    """
    Merges csvs in a folder

    :dir: TODO
    :category: TODO
    :returns: TODO

    """
    from glob import glob
    root_dir = os.getcwd()
    os.chdir(tmp_dir)

    for category in categories:
        all_filenames = [i for i in glob(f'{category}*')]
        # combine all files in the list
        combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames],
                                 ignore_index=False)
        # export to csv
        os.chdir(csv_dir)
        # Remove coluna 'Unnamed'
        combined_csv = combined_csv.loc[:, ~combined_csv.columns.str.contains('^Unnamed')]
        # Sorting do dataframe pelo id
        combined_csv = combined_csv.sort_values(f'{category}_id')
        # Remove coluna de índices
        combined_csv.to_csv(f'{category}.csv', index=False)
        os.chdir(tmp_dir)
    # delete tmp files
    for tmp_csvs in os.listdir():
        os.remove(tmp_csvs)
    # windows e ntfs...
    os.chdir(csv_dir)
    # remove tmp directory
    os.rmdir(tmp_dir)

    # Retorna ao diretório de origem
    os.chdir(root_dir)


def flash_csv(file, file_idx, tmp_dir, root_dir):
    """
    Function generates csv of flash data based on netCDF4.Dataset given

    :df: netCDF4.Dataset
    :returns: TODO

    """
    from netCDF4 import Dataset

    # Abre o arquivo
    glm_data = Dataset(file)

    # Organizando variáveis de tempo
    # Time format : YYYY-MM-DDTHH:MM:SS.S
    tempo_inicio = glm_data.getncattr('time_coverage_start')
    time_offsets = glm_data.variables['flash_time_offset_of_first_event'][:]
    ano = int(tempo_inicio[0:4])
    anos = []
    mes = int(tempo_inicio[5:7])
    meses = []
    dia = int(tempo_inicio[8:10])
    dias = []
    hora = int(tempo_inicio[11:13])
    horas = []
    minuto = int(tempo_inicio[14:16])
    minutos = []
    segundo = int(tempo_inicio[17:19])
    segundos = []
    data = datetime(ano, mes, dia, hora, minuto, segundo)
    for offset in time_offsets:
        microsseconds = int(offset * 1000000)
        conv_offset = timedelta(microseconds=microsseconds)
        time = data + conv_offset
        anos.append(time.year)
        meses.append(time.month)
        dias.append(time.day)
        horas.append(time.hour)
        minutos.append(time.minute)
        segundos.append(time.second + (time.microsecond / 1000000))

    # Cria Dataframe com as listas p/ exportar em .csv
    flashes = pd.DataFrame.from_dict({
        'flash_id': glm_data.variables['flash_id'][:],

        'Ano': anos,
        'Mes': meses,
        'Dia': dias,
        'Hora': horas,
        'Minuto': minutos,
        'Segundo': segundos,

        'flash_time_offset_of_first_event':
        glm_data.variables['flash_time_offset_of_first_event'][:],

        'flash_time_offset_of_last_event':
        glm_data.variables['flash_time_offset_of_last_event'][:],

        'flash_lat': glm_data.variables['flash_lat'][:],
        'flash_lon': glm_data.variables['flash_lon'][:],
        'flash_area': glm_data.variables['flash_area'][:],
        'flash_energy': glm_data.variables['flash_energy'][:],
        'flash_quality_flag': glm_data.variables['flash_quality_flag'][:],
    })
    # Fecha o Dataset
    glm_data.close()
    os.chdir(tmp_dir)
    flashes.to_csv(f'flash_{file_idx}.csv')
    # Retorna ao diretório raiz
    os.chdir(root_dir)


def group_csv(file, file_idx, tmp_dir, root_dir):
    """
    Function generates csv of flash data based on netCDF4.Dataset given

    :df: netCDF4.Dataset
    :returns: TODO

    """
    from netCDF4 import Dataset

    # Abre o arquivo
    glm_data = Dataset(file)

    # Organizando variáveis de tempo
    tempo_inicio = glm_data.getncattr('time_coverage_start')
    time_offsets = glm_data.variables['group_time_offset'][:]
    ano = int(tempo_inicio[0:4])
    anos = []
    mes = int(tempo_inicio[5:7])
    meses = []
    dia = int(tempo_inicio[8:10])
    dias = []
    # Convertendo p/ nanossegundos
    hora = int(tempo_inicio[11:13])
    horas = []
    minuto = int(tempo_inicio[14:16])
    minutos = []
    segundo = int(tempo_inicio[17:19])
    segundos = []
    data = datetime(ano, mes, dia, hora, minuto, segundo)
    for offset in time_offsets:
        microsseconds = int(offset * 1000000)
        conv_offset = timedelta(microseconds=microsseconds)
        time = data + conv_offset
        anos.append(time.year)
        meses.append(time.month)
        dias.append(time.day)
        horas.append(time.hour)
        minutos.append(time.minute)
        segundos.append(time.second + (time.microsecond / 1000000))
    # Coleta dados
    # Cria Dataframe com as listas p/ exportar em .csv
    groups = pd.DataFrame.from_dict({
        'group_id': glm_data.variables['group_id'][:],
        'Ano': anos,
        'Mes': meses,
        'Dia': dias,
        'Hora': horas,
        'Minuto': minutos,
        'Segundo': segundos,
        'group_time_offset': glm_data.variables['group_time_offset'][:],
        'group_lat': glm_data.variables['group_lat'][:],
        'group_lon': glm_data.variables['group_lon'][:],
        'group_area': glm_data.variables['group_area'][:],
        'group_energy': glm_data.variables['group_energy'][:],
        'group_parent_flash_id': glm_data.variables['group_parent_flash_id'][:],
        'group_quality_flag': glm_data.variables['group_quality_flag'][:],
    })
    # Fecha dataset
    glm_data.close()
    os.chdir(tmp_dir)
    groups.to_csv(f'group_{file_idx}.csv')
    # Retorna ao diretório raiz
    os.chdir(root_dir)


def event_csv(assets_dir, csv_dir, root_dir):
    """
    Function generates csv of flash data based on netCDF4.Dataset given

    :df: netCDF4.Dataset
    :returns: TODO

    """
    from netCDF4 import Dataset

    if os.path.exists(assets_dir):
        os.chdir(assets_dir)
    else:
        print('assets_dir não existe')
        return

    # Cria pasta tmp p/ guardar arquivos temporários
    tmp_dir = os.path.join(csv_dir, 'tmp')
    if not os.path.exists(tmp_dir):
        os.mkdir(tmp_dir)

    files = os.listdir()
    file_idx = 0
    file_cnt = 0
    file_quant = len(files)
    for file in files:

        # Printa o diretório e arquivo sendo lido
        print('event')
        print(os.getcwd())
        file_cnt += 1
        print(file, f'{(file_cnt/file_quant)*(100):.2f}% done')
        # Abre o arquivo
        glm_data = Dataset(file)

        # Organizando variáveis de tempo
        time_len = len(glm_data.variables['event_time_offset'])
        tempo_inicio = glm_data.getncattr('time_coverage_start')
        temp = glm_data.variables['event_time_offset'][:]
        # Fazendo lista de tamanho time_len
        dia_formatado = [int(tempo_inicio[0:4] +
                         tempo_inicio[5:7] +
                         tempo_inicio[8:10])] * time_len
        # Convertendo p/ nanossegundos
        hora_n = int(tempo_inicio[11:13]) * 60 * 60 * (10**9)
        minuto_n = int(tempo_inicio[14:16]) * 60 * (10**9)
        segundo_n = int(tempo_inicio[17:19]) * (10**9)
        nano_times = []
        for idx in range(time_len):
            nano_times.append(hora_n + minuto_n + segundo_n + float(temp[idx]))
        # Inserindo o número corretamente convertido na lista
        tempo_exato = []
        for nanos in nano_times:
            # Converte p/ timestamp (divisão inteira vai em floor)
            times = timedelta(seconds=nanos//1000000000)
            # Adiciona precisão do nano ao fim
            times = str(times) + '.' + str(int(nanos % 1000000000)).zfill(9)
            # Divide nos ":"
            times = times.split(':')
            time_string = ''
            for time in times:
                time_string += time
            # formato: HHMMSS.SS
            tempo_exato.append(float(time_string))

        # Coleta dados

        # Cria Dataframe com as listas p/ exportar em .csv
        events = pd.DataFrame.from_dict({
            'event_id': glm_data.variables['event_id'][:],
            'event_time_offset': glm_data.variables['event_time_offset'][:],
            'event_lat': glm_data.variables['event_lat'][:],
            'event_lon': glm_data.variables['event_lon'][:],
            'event_energy': glm_data.variables['event_energy'][:],
            'event_parent_group_id': glm_data.variables['event_parent_group_id'][:],
            'event_count': glm_data.variables['event_count'][:],
            'Data': dia_formatado,
            'Tempo_exato': tempo_exato,
        })
        os.chdir(tmp_dir)
        events.to_csv(f'event{file_idx}.csv')
        file_idx += 1
        os.chdir(assets_dir)
    merge_csv(tmp_dir, csv_dir, 'event')
    # Retorna ao diretório raiz
    os.chdir(root_dir)


def create_csv(year, month, day, hour, root_dir=os.getcwd()):
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
    file_quant = len(os.listdir(assets_dir))
    # Pasta de destino
    csv_dir = os.path.join(root_dir, 'csv', f'{year}_{month}_{day}_{hour}')
    # Pasta temporária
    tmp_dir = os.path.join(csv_dir, 'tmp')
    if not os.path.exists(csv_dir):
        os.mkdir(csv_dir)
    if not os.path.exists(tmp_dir):
        os.mkdir(tmp_dir)

    # Cria os csvs
    file_cnt = 0
    file_idx = 0
    for file in assets:
        file_path = os.path.join(assets_dir, file)
        file_cnt += 1
        file_idx += 1
        flash_csv(file_path, file_idx, tmp_dir, root_dir)
        group_csv(file_path, file_idx, tmp_dir, root_dir)
        print(file, f'{(file_cnt/file_quant)*(100):.2f}% done')
        pass

    categories = ['flash', 'group']
    merge_csv(tmp_dir, csv_dir, categories)

    # Disabled for the time being
    # event_csv(assets_dir, csv_dir, root_dir)


def data_acces(dic_start_params, dic_end_params):
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
    for day in days:
        for hour in hours:
            create_csv(year, month, day, hour)


def generate_map(dic_start_params, dic_end_params, radius, center,
                 category='flash'):
    """
    Uses csv data to generate map

    :start_params: dictionary
    :end_params: dictionary
    :radius: float
    :center: tuple
    :category: string
    :returns: nothing

    """
    categories = ['event', 'flash', 'group']
    if category not in categories:
        print('categoria inválida')
        return

    root_dir = os.getcwd()
    csv_dir = os.path.join(root_dir, 'csv')
    from cartopy import crs as ccrs
    from matplotlib import pyplot as plt

    # Garante que ano e mês são iguais
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

    if hour_s != hour_e:
        hours = range(hour_s, hour_e + 1)
    else:
        hours = [hour_s]

    # Listas de latitudes e longitudes
    lats = []
    lons = []
    # Lista de latitudes e longitudes desejadas
    w_lats = []
    w_lons = []
    (central_lat, central_lon) = center
    # Deixando espaço p/ salvar maior raio
    maior_raio = 0
    for day in days:
        for hour in hours:
            # Acessa csv
            csv_folder = f'{year}_{month}_{day}_{hour}'
            data = pd.read_csv(os.path.join(csv_dir, csv_folder, f'{category}.csv'))
            # Extrai dados e transforma p/ lista
            lats = list(data[f'{category}_lat'])
            lons = list(data[f'{category}_lon'])
            # Filtrar latitudes e longitudes desejadas
            for idx in data.index:
                Lat1, Lon1 = central_lat * np.pi / 180,\
                    central_lon * np.pi / 180
                Lat2, Lon2 = lats[idx] * np.pi / 180,\
                    lons[idx] * np.pi / 180
                AD_d = (6378137.00 * math.acos(
                    math.cos(Lat1) * math.cos(Lat2) * math.cos(Lon2 - Lon1) +
                    math.sin(Lat1) * math.sin(Lat2))) / 1000
                if AD_d <= radius:
                    w_lats.append(lats[idx])
                    w_lons.append(lons[idx])
                # Guarda maior_raio distância
                if AD_d > maior_raio:
                    maior_raio = AD_d
    # Cria uma variável p/ delimitar extensão do mapa
    border = (maior_raio * 2000) / 6378137.00
    extent = [central_lon - border, central_lon + border,
              central_lat - border, central_lat - border]
    ax = plt.axes(projection=ccrs.NearsidePerspective(
                  central_latitude=central_lat, central_longitude=central_lon))
    ax.plot(lons, lats, markersize=5,marker='o',linestyle='',color='#3b3b3b',transform=ccrs.PlateCarree())
    ax.gridlines()
    ax.stock_img()
    ax.coastlines(resolution='50m')
    # ax.set_extent(extent)
    plt.savefig('mapa.png')
    plt.show()
