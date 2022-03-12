import pytest
import os
import zstandard as zstd
from pg_shuffle import check_page, get_page, shuffle_page, page_size


@pytest.mark.parametrize("number", [16512, 16485, 16519, 16428])
def test(number):
    print(f'Processing file n {number}')
    fname = os.path.join('data', str(number))
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
    dctx = zstd.ZstdDecompressor()    #разжимаем данные
    decompressed = dctx.decompress(compressed)
    #if decompressed == data:               #нужно для проверки. Оставить в итоговом варианте или удалить?
        #print('decomp_cool')               #пока закомментила
    decompressed_new = dctx.decompress(compressed_new)
    #if decompressed_new == data_new:
        #print('decomp_new_cool')

    data_old = bytearray(data_new)
    for i in range(len(data_old) // 8192):  # i - номер страницы
        #     print(f'{i})')
        if check_page(get_page(i)) == True:  # при равных длинах элементов
            p_old = unshuffle_page(data_new[i * 8192:(
                i + 1) * 8192])  # получаем измененную страницу, которая должна быть равна странице из data
        else:
            p_old = data_new[i * 8192:(i + 1) * 8192]  # иначе неизмененную

        for j in range(i * 8192, (i + 1) * 8192):  # j - номер байта в data_old
            data_old[j] = p_old[j - i * 8192]  # записываем в data_old страницы, к-е мы получили
    data_old = bytes(data_old)
    if data_old == data:
        print('Успешно восстановлено')