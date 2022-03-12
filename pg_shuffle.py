
from bitarray import bitarray
from bitarray.util import ba2int


page_size = 8192 #у всех страниц длина фиксированная


def b2n(b):
    """ bytes => integer (little-endian) """
    res = 0
    for bb in b[::-1]:
        res = res * 256 + int(bb)
    return res


def get_page(data, k):
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


def shuffle_page(p):
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


