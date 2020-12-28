"""

TODO: Reorganizar a estrutura de downloads, o volume vai sobrecarregar a lista
      gem de arquivos (os.listdir), nova estrutura de pastas:
            {ano}
                {mes}
                    {dia}
                        {hora}
                            {arquivo}

"""

import os
import numpy as np
import shutil
from datetime import datetime
import pandas as pd
import math


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

            for file in files:
                # Download dos arquivos
                fs.get(file, file.split('/')[-1])

                # Move o arquivo p/ pasta designada no loop
                ls = os.listdir(os.getcwd())
                for file in ls:
                    if file.endswith('.nc'):
                        shutil.move(
                                os.path.join(os.getcwd(), file),
                                os.path.join(os.getcwd(), folder_srt, file))

    # Retornando p/ diretório de origem
    os.chdir(curr_path)


def data_manipulation(year, month, day, hour, radius, category, center=(0, 0)):
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
    :returns: TODO

    """
    categories = ['event', 'flash', 'group']

    if category not in categories:
        print('categoria inválida')
        return

    from netCDF4 import Dataset

    # Guarda o diretório atual
    curr_dir = os.getcwd()

    # Pasta com os .nc
    assets_dir = os.path.join('assets',
             str(year), str(month), str(day), str(hour))

    # Todos os arquivos tem extensão .nc
    files = os.listdir(assets_dir)

    # Laço p/filtrar cada arquivo
    for file in files:
        glm_data = Dataset(file)

        tempo_inicio = glm_data.getncattr('time_coverage_start')
        tempo_final = glm_data.getncattr('time_coverage_end')

        # Time output:
        # 'YYYY-MM-DDThh:mm:ss.ss'
        dia_formatado = tempo_inicio[0:4] + ',' + \
            tempo_inicio[5:7] + ',' + tempo_inicio[8:10]
        tempo_inicio_formatado = tempo_inicio[11:13] + \
            ',' + tempo_inicio[14:16] + ',' + tempo_inicio[17:21]
        tempo_final_formatado = tempo_final[11:13] + \
            ',' + tempo_final[14:16] + ',' + tempo_final[17:21]

        # Número de eventos
        lenght = len(glm_data.variables['flash_id'][:])
        # Adquirir dados e filtrar
        for idx in range(lenght):

            var = str(int(tempo_inicio[17:19]) + float(glm_data.variables['flash_time_offset_of_first_event'][idx]))
            tempo_exato = tempo_inicio[11:13] + ',' + \
                tempo_inicio[14:16] + ',' + var[0:8]

            # Distância do flash ao centro
            Lat1, Lon1 = center * np.pi / 180
            Lat2, Lon2 = glm_data.variables['flash_lat'][idx] * np.pi / 180,\
                         glm_data.variables['flash_lon'][idx] * np.pi / 180
            AD_d = (6378137.00 * math.acos(
                math.cos(Lat1) * math.cos(Lat2) * math.cos(Lon2 - Lon1) +
                math.sin(Lat1) * math.sin(Lat2))) / 1000

            if AD_d <= radius:
                flashes = pd.DataFrame({
                'flash_id': glm_data.variables['flash_id'][idx],
                'Data': dia_formatado,
                'Tempo_inicio': tempo_inicio_formatado,
                'Tempo_final': tempo_final_formatado,
                'Tempo_exato': tempo_exato,
                'Distância': AD_d,

                'flash_time_offset_of_first_event': glm_data.variables[
                'flash_time_offset_of_first_event'][idx],

                'flash_time_offset_of_last_event': glm_data.variables[
                'flash_time_offset_of_last_event'][idx],

                'flash_frame_time_offset_of_first_event': glm_data.variables[
                'flash_frame_time_offset_of_first_event'][idx],

                'flash_frame_time_offset_of_last_event': glm_data.variables[
                'flash_frame_time_offset_of_last_event'][idx],

                'flash_lat': glm_data.variables['flash_lat'][idx],

                'flash_lon': glm_data.variables['flash_lon'][idx],

                'flash_area': glm_data.variables['flash_area'][idx],

                'flash_energy': glm_data.variables['flash_energy'][idx],

                'flash_quality_flag': glm_data.variables['flash_quality_flag'][idx],

                'product_time': glm_data.variables['product_time'][idx]
                }
                )
                csv_name = os.path.join(curr_dir, 'csv',
                                        f'{year}_{month}_{day}_{hour}.csv')
                flashes.to_csv(csv_name, index=False)
