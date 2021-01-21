import back
from time import time


start_params = {
    'hour': '14',
    'day': '23',
    'month': '2',
    'year': '2019'
}

end_params = {
    'hour': '15',
    'day': '23',
    'month': '2',
    'year': '2019'
}

special_params = {
    'hour': '19',
    'day': '1',
    'month': '9',
    'year': '2019'
}

tic = time()

# back.assets_download(start_params, end_params)
back.data_acces(start_params, end_params)
# back.generate_map(start_params, start_params, 1000, (-1.474012, -48.457615))
tac = time()

print(f'tempo:{tac-tic}')
