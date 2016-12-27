#-*- coding: utf-8 -*-#
__author__ = "ar3b"

from functools import partial
import multiprocessing
import time
from ctypes import c_int
from db import DB

# Функция, которая будет выполнятся во множистве потоков
# Первая часть аргументов - служебная (см. далее)
# Вторая - свои, какие необходимо
def insert(lock, counter, (number, wait)):
    ## Делаем что-то, что не требует блокирования
    time.sleep(wait)

    ## Начинаем блокирующие операции
    # Блокируем все потоки
    lock.acquire()

    # Выполняем операции требующие блокировки
    db = DB()
    db.execute(
        "INSERT INTO test_table (id, name, size) VALUES "+
        "(null, 'ascascasc', "+str(number)+");"
    )
    db.commit()
    db.close()

    # Снимаем блокировку
    lock.release()

    # Увеличиваем значение общей для всех потоков переменной
    counter.value += 1
    print "counter == %d\n\t\twith number == %d" % (counter.value, number)

    # Возвращаем результат для callback
    return number*2

# Функция, куда будет передан результат выполнения операций
# Важно - вызывается один раз по завершению выполнения всех процессов пулла задач
# В аргумент будет передан массив результатов в том же порядке. в котором задачи
# отправлялись в пулл
def callback(result):
    print "\n-------- R E S U L T ---------"
    print result

def main():

    # Просто подготовка БД
    db = DB()
    db.execute(
        "DELETE FROM test_table"
    )
    db.execute("VACUUM")
    db.commit()
    db.close()

    # Готовим манагер, который разруливает данные между потоками
    manager = multiprocessing.Manager()

    # Готовим пулл процессов, указывая, что одновременно процессов будет не более
    # чем кол-во ядер минус 2 (один - это этот поток, один - ещё один, судя по всему, манагерский)
    pool = multiprocessing.Pool(
        processes=multiprocessing.cpu_count()-2
    )
    # Готовим объект для ручного управления блокировкой
    lock = manager.Lock()

    # Готовим объект для общего использования
    # Агрументы - тип и начальное значение
    # Тип может быть только поддерживаемый библиотекой ctypes (или структурой из этой же библиотеки)
    counter = manager.Value(
        typecode=c_int,
        value=0
    )

    # Готовим спец-объект, который будет содержать всё, что надо для нашей функции
    func = partial(
        insert,
        lock,
        counter
    )

    # Готовим массив с агрументам, с которыми будет вызываться функция
    data = list()
    for num in range(12):
        data.append(
            (num, 1)
        )

    # Задаём пуллу набор работы
    # функция (спец. объект) и массив с аргументами для каждого её вызова
    tasks = pool.map_async(
        func=func,
        iterable=data,
        callback=callback
    )

    # Ждём, пока пулл работ не закончит
    tasks.wait()

if __name__ == '__main__':
    main()