import pytest
import os
import zstandard as zstd
from pg_shuffle import check_page, get_page, shuffle_page, page_size


@pytest.mark.parametrize("number", [16572, 16583, 16590, 16597, 16609, 16613, 16624, 16631, 16638, 16655, 16662, 16674,
                                    16680, 16692, 16703])
def test(number):
    print(f'Processing file n {number}')
    fname = os.path.join('data', 'dvdrental', str(number))
    with open(fname, 'rb') as f:
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
    print(f'CR после преобразования: {cr_new:.2f}')

    compressed = cctx.compress(data)
    cr_old = len(data) / len(compressed)  # во сколько раз сжалось до изменения
    print(f'CR до преобразования: {cr_old:.2f}')
    pct = (cr_new/cr_old - 1)*100
    print(f'CR improved: {pct:.2f}%')
    print(f'Преобразовано страниц: {n_shuffled} из {n_pages}')

    return
