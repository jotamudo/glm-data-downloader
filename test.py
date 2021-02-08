import back
from time import time


start_params = {
    'hour': '14',
    'day': '24',
    'month': '2',
    'year': '2019'
}

end_params = {
    'hour': '15',
    'day': '24',
    'month': '2',
    'year': '2019'
}

special_params = {
    'hour': '19',
    'day': '1',
    'month': '9',
    'year': '2019'
}

# Dicionário p/ as coordenadas do filtro, os arquivos originais não serão apagados
dic_coordinates={
    'lat1': -45,
    'lat2': -79,
    'lon1': -14,
    'lon2': 8
}

# Categorias a serem analisadas, válidas: 'flash'; 'group'; 'event';
# 'event' ainda não implementado
categories = ['flash', 'group']

# Os csvs gerados possuem colunas ordenadas da esqueda p/ direita:
# category_id
# category_lat
# category_lon
# Ano
# Mes
# Dia
# Hora
# Minuto
# Segundo
# Time offset de início
# (flash) Time offset de fim
# category_area
# category_energy
# (flash) quality_flag (group) parent_flash_id
# (group) quality flag

tic = time()

back.assets_download(start_params, end_params)
back.data_acces(start_params, end_params, categories, dic_coordinates)
# back.generate_map(start_params, start_params, 1000, (-1.474012, -48.457615))
tac = time()

print(f'tempo:{tac-tic}')
