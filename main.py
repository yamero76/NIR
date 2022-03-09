import os
import zstandard as zstd
from bitarray import bitarray
from bitarray.util import ba2int

fname = os.path.join('data', '16512') #16428 - с одинаковой длиной эл-в, остальные (прим.: 16512) с разными
page_size = 8192 #у всех страниц длина фиксированная

with open(fname, 'rb') as f:
    data = f.read()
    data_new=bytearray(data) #преобразуем в изменяемый формат (из bytes в bytearray)
    #print(len(data)/8192) #число страниц (для проверки)


def b2n(b):
    """ bytes => integer (little-endian) """
    res = 0
    for bb in b[::-1]:
        res = res * 256 + int(bb)
    return res


def b2s(b, n=32):       #Это не используется, лучше удалить или оставить?
    """ bytes => string """
    s = []
    for i in range(0, len(b), n):
        bb = b[i:i + n]
        s.append(' '.join(f'{x:02x}' for x in bb))
    return '\n'.join(s)


def get_page(k):
    return data[k * page_size: (k + 1) * page_size]


def check_page(p):

    # header = p[:24]
    pd_lower = b2n(p[12:14])  # читаем заголовок
    pd_upper = b2n(p[14:16])
    pd_special = b2n(p[16:18])
    n_items = (pd_lower - 24) // 4  # получаем количество элементов

    res = True

    len_item = [0 for i in range(n_items)]
    lp_off = [0 for i in range(n_items)]  # offset to tuple (from start of page)
    check_sum = 0
    for i in range(n_items):
        x = 24 + i * 4
        y = 24 + (i + 1) * 4
        # print(f'конец ид-ров из цикла: {y}, конец ид-ров из pd_lower: {pd_lower}')
        bitarr = bitarray(endian='little')
        bitarr.frombytes(bytes(p[x:y]))
        len_item[i] = ba2int(bitarr[0:15])
        lp_off[i] = ba2int(bitarr[0:15])
        if i == 0:
            len_item[i] = pd_special - lp_off[i]
        else:
            len_item[i] = lp_off[i - 1] - lp_off[i]
        if i > 0 and len_item[i - 1] != len_item[i]:
            res = False
            break

        #check_sum += len_item[i]
        # print(f'длина айтема {i}: {len_item[i]}')

    # print(f'Сумма реальная (pd_special - pd_upper) = {(pd_special - pd_upper)}, сумма после цикла = {check_sum}')
    #print(f'res = {res}')
    return res


def shuffle_page(k):
    p = get_page(k)  # получаем страницу для преобразования

    # header = p[:24]
    pd_lower = b2n(p[12:14])  # читаем заголовок
    pd_upper = b2n(p[14:16])
    pd_special = b2n(p[16:18])
    n_items = (pd_lower - 24) // 4  # получаем количество и размер элементов
    item_size = (pd_special - pd_upper) // n_items

    # print(f'кол-во айтемов: {n_items}')
    # print(f'размер айтемов: {item_size}')

    p_new = bytearray(p)
    k = 0
    for j in range(item_size):  # j - номер байта в элементе
        for i in range(n_items):  # i - номер элемента
            p_new[pd_upper + k] = p[pd_upper + j + i * item_size]  # меняем байты
            k += 1
    return p_new


def unshuffle_page(new):
    # p = get_page(k) #получаем страницу изначальных данных

    # header = p[:24]
    pd_lower = b2n(new[12:14])  # читаем заголовок
    pd_upper = b2n(new[14:16])
    pd_special = b2n(new[16:18])
    n_items = (pd_lower - 24) // 4  # получаем количество и размер элементов
    item_size = (pd_special - pd_upper) // n_items

    old = new
    old = bytearray(old)
    k = 0
    for j in range(item_size):  # j - номер байта в элементе
        for i in range(n_items):  # i - номер элемента
            old[pd_upper + j + i * item_size] = new[pd_upper + k]  # меняем байты
            k += 1
    return old


# добавили проверку:
shuffled_pages = 0
total_pages = len(data_new)
for i in range(len(data_new) // 8192):  # i - номер страницы
    #    print(f'{i})')
    if check_page(get_page(i)) == True:  # при равных длинах элементов
        p_new = shuffle_page(i)  # получаем измененную страницу
        shuffled_pages += 1
    else:
        p_new = get_page(i)  # иначе неизмененную
    for j in range(i * 8192, (i + 1) * 8192):  # j - номер байта в data_new
        data_new[j] = p_new[j - i * 8192]  # записываем в data_new страницы, к-е мы получили

data_new = bytes(data_new)  # конвертируем из bytearray обратно в bytes

cctx = zstd.ZstdCompressor()  # сжимаем данные
compressed_new = cctx.compress(data_new)
cr = len(data_new) / len(compressed_new)  # во сколько раз сжалось после изменения
print(f'CR после преобразования: {cr:.2f}')

compressed = cctx.compress(data)
cr = len(data) / len(compressed)  # во сколько раз сжалось до изменения
print(f'CR до преобразования: {cr:.2f}')

print(f'Преобразовано страниц: {shuffled_pages} из {total_pages}')

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