import pytest
import os
import zstandard as zstd
from pg_shuffle import check_page, get_page, shuffle_page, page_size
from pg_get_table_files import get_table_files
from tabulate import tabulate


@pytest.mark.parametrize("base_name", ['custom', 'postgres', 'sportdb' ]) #
def test(base_name):
    print(f'-----Processing base {base_name}-----')
    table_files = get_table_files(user='postgres', password='12345', db=base_name)
    out = []
    for k in table_files.keys():
        v = table_files.get(k)
        c = main(k, v)
        if c[2] != 0:
            out.append(c)
    total = []
    total.append('---Total---')
    total_cr_new = 0
    total_cr_old = 0
    total_n_shuffled = 0
    total_n_pages = 0
    for k in out:
        total_cr_new += k[1]
        total_cr_old += k[2]
        total_n_shuffled += k[4]
        total_n_pages += k[5]
    total.append(total_cr_new)
    total.append(total_cr_old)
    total_pct = (total_cr_new / total_cr_old - 1) * 100
    total.append(total_pct)
    total.append(total_n_shuffled)
    total.append(total_n_pages)
    out.append(total)
    headers = ["name", "cr_new", "cr_old", "pct", "n_shuffled", "n_pages"]
    print(tabulate(out, tablefmt='orgtbl', headers=headers, floatfmt=".2f"))


def main(name, path):
    #print(name)
    fpath = path
    tname = name
    #print(f'Processing file {tname} {fpath}')
    #fname = os.path.join('data', 'new', str(number))
    with open(fpath, 'rb') as f:
        data = f.read()
        data_new = bytearray(data)  # преобразуем в изменяемый формат (из bytes в bytearray)

    n_pages = len(data_new) // page_size
    n_shuffled = 0
    for i in range(n_pages):  # i - номер страницы
        p = get_page(data_new, i)
        if check_page(p) == True:  # при равных длинах элементов
            p_new = shuffle_page(p)  # получаем измененную страницу
            n_shuffled += 1
        else:
            p_new = p  # иначе неизмененную
        data_new[i*page_size: (i+1)*page_size] = p_new

    data_new = bytes(data_new)  # конвертируем из bytearray обратно в bytes

    cctx = zstd.ZstdCompressor()  # сжимаем данные
    compressed_new = cctx.compress(data_new)
    cr_new = len(data_new) / len(compressed_new)  # во сколько раз сжалось после изменения
    #print(f'CR после преобразования: {cr_new:.2f}')

    compressed = cctx.compress(data)
    cr_old = len(data) / len(compressed)  # во сколько раз сжалось до изменения
    #print(f'CR до преобразования: {cr_old:.2f}')
    if cr_old != 0:
        pct = (cr_new/cr_old - 1)*100
    else:
        pct = 0
    #print(f'CR improved: {pct:.2f}%')
    #print(f'Преобразовано страниц: {n_shuffled} из {n_pages}')


    c = [name, cr_new, cr_old, pct, n_shuffled, n_pages]
    return c



