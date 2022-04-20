from bitarray import bitarray
from bitarray.util import ba2int

page_size = 16384  # у всех страниц длина фиксированная


def b2n(b):
    """ bytes => integer (little-endian) """
    res = 0
    for bb in b[::-1]:
        res = res * 256 + int(bb)
    return res


def big_b2n(b):
    """ bytes => integer (big-endian) """
    res = 0
    for bb in b:
        res = res * 256 + int(bb)
    return res


def get_page(data, k):
    return data[k * page_size: (k + 1) * page_size]


def check_page(p):
    # Fil header = p[:38]
    fil_page_type = p[24:26]
    if big_b2n(fil_page_type) != 0x45bf:
        res = False
    else:
        # print(f'YES fil_page_type = {big_b2n(fil_page_type)}')
        # page header = p[38:94]
        page_n_recs = big_b2n(p[54:56])  # number of user records (items)
        #print(f' page_n_recs = b2n(p[54:56]) = {page_n_recs},')

        ind = 94

        items = []
        while ind < page_size:
            item_len = big_b2n(p[ind + 3: ind + 5])
            items.append(p[ind: ind + item_len])
            ind += item_len

        items.pop(-1)

        #print(f'len(items[1:] = {len(items[1:])}, page_n_recs = {page_n_recs}')

        if (len(set(map(len, items[1:]))) == 1):  # and (len(items[1:]) == page_n_recs):
            res = True
        else:
            res = False
        #print(f'res = {res}')

    return res


def shuffle_page(p):

    page_heap_top = b2n(p[40:42])
    page_n_recs = b2n(p[54:56])
    ind = 94
    items = []
    while ind < page_size:
        item_len = big_b2n(p[ind + 3: ind + 5])
        items.append(p[ind: ind + item_len])
        ind += item_len
    items.pop(-1)


    n_items = len(items[1:])
    item_size = len(items[1])


    #print(f'кол-во айтемов: {n_items}')
    #print(f'размер айтемов: {item_size}')

    p_new = bytearray(p)
    k = 0
    for j in range(item_size):  # j - номер байта в элементе
        for i in range(n_items):  # i - номер элемента
            p_new[94 + k] = p[94 + j + i * item_size]  # меняем байты
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
