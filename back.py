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
from netCDF4 import Dataset


def setup_directories():
    folders = ['assets']
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
    it's executed on assets folder, please take care it can make a mess

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
        except FileExistsError:
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
    day_s  = int(dic_start_params['day'])
    day_e  = int(dic_end_params['day'])
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
                        shutil.move(os.path.join(os.getcwd(), file),
                                    os.path.join(os.getcwd(), folder_srt, file))

    # Retornando p/ diretório de origem
    os.chdir(curr_path)


def data_manipulation():
    pass
