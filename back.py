import s3fs
import os
import numpy as np
import shutil
import datetime

def setup_directories():
    folders = ['assets']
    for folder in folders:
        try:
            if not os.path.exists(folder):
                # Faz pasta relativo ao diretório atual
                os.makedirs(os.path.join(os.getcwd(), folder))
        except OSError:
            print("Erro ao criar o diretório")

def mk_dir():
    pass


def assets_download(dic_start_params, dic_end_params):
    """
    A função recebe dois dicionários, um  p/ dados do início, outro p/ dados
    do fim. O ano de início e de fim deve ser o mesmo entre os dicionários por
    hora.

    """
    setup_directories()
    curr_path = os.getcwd()
    # os.chdir(os.path.join(curr_path, 'assets'))
    os.chdir('assets')
    # Inicializa o fs com credenciais anônimas p/ acessar dados públicos
    fs = s3fs.S3FileSystem(anon=True)

    # ano/mês/dia início e fim
    try:
        if dic_start_params['year'] == dic_end_params['year'] and \
           dic_start_params['month'] == dic_end_params['month']:
            year = int(dic_start_params['year'])
            month = int(dic_start_params['month'])
        else:
            raise ValueError
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
    # today = datetime.datetime(year, month, day_s, 00, 00)
    # day_in_year = (today - datetime.datetime(year, 1, 1)).days + 1

    for day in days:
        today = datetime.datetime(year, month, day, 0, 0)
        day_in_year = (today - datetime.datetime(year, 1, 1)).days + 1

        for hour in hours:
            # Seleciona os diretórios da pool
            files = np.array(fs.ls('noaa-goes16/GLM-L2-LCFA/{0}/{1:03d}/{2:02d}/'.format(year, day_in_year, hour)))

            # Cria a pasta relacionada aos parâmetros
            folder_srt = '{0:02d}-{1:02d}-{2}#T-{3}h00m'.format(day, month, year, hour)
            os.mkdir(os.path.join(os.getcwd(), folder_srt))

            for file in files:
                # Download dos arquivos
                fs.get(file, file.split('/')[-1])

                # Move o arquivo p/ pasta designada no loop
                lista_arquivos_fonte = os.listdir(os.getcwd())
                for file in lista_arquivos_fonte:
                    if file.endswith('.nc'):
                        shutil.move(os.path.join(os.getcwd(), file),
                                    os.path.join(os.getcwd(), folder_srt, file))

    # Retornando p/ diretório de origem
    os.chdir(curr_path)
