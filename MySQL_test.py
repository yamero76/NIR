import pytest
import os
import zstandard as zstd
from MySQL_shuffle import check_page, get_page, shuffle_page, page_size
from tabulate import tabulate


@pytest.mark.parametrize("base_name", ["sakila", "world"])
def test(base_name):
    print(f'-----Processing base {base_name}-----')

    out = []
    len_data = 0
    len_data_new = 0
    len_compressed = 0
    len_compressed_new = 0

    dirname = os.path.join('MySQL_data', str(base_name))
    for filename in os.listdir(dirname):
        c = main(base_name, filename, dirname)
        if c[2] != 0:
            out.append(c[:6])
            len_data += c[6]
            len_data_new += c[8]
            len_compressed += c[7]
            len_compressed_new += c[9]

    total = []
    total.append('---Total---')
    total_cr_new = len_data_new / len_compressed_new
    total_cr_old = len_data / len_compressed
    total_n_shuffled = 0
    total_n_pages = 0
    for k in out:
        #total_cr_new += k[1]
        #total_cr_old += k[2]
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


def main(base_name, filename, dirname):

    with open(os.path.join(dirname, filename), 'rb') as f:
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
    pct = (cr_new/cr_old - 1)*100
    #print(f'CR improved: {pct:.2f}%')
    #print(f'Преобразовано страниц: {n_shuffled} из {n_pages}')

    c = [filename, cr_new, cr_old, pct, n_shuffled, n_pages, len(data), len(compressed), len(data_new), len(compressed_new)]

    return c
