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
    'lat1': -14,
    'lat2': 8,
    'lon1': -45,
    'lon2':-79
}

# Categorias a serem analisadas, válidas: 'flash'; 'group'; 'event';
# 'event' ainda não implementado
categories = ['flash', 'group']

tic = time()

back.assets_download(start_params, end_params)
back.data_acces(start_params, end_params, categories, dic_coordinates)
# back.generate_map(start_params, start_params, 1000, (-1.474012, -48.457615))
tac = time()

print(f'tempo:{tac-tic}')
