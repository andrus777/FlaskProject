import pandas as pd
from datetime import datetime

from brom import БромКлиент
from sqlalchemy import create_engine, update, table, column, func
import time

def update_sys_data(tbname):
    # Создаём engine для подключения к MySQL
    engine = create_engine("mysql+pymysql://master_logist:!StE1q2w3e2w1q@192.168.2.228:3306/sppr")
    # Обновляем данные по таблице
    # Описываем таблицу (без ORM-модели)
    svs_update_info = table(
        'svs_update_info',
        column('table_name'),
        column('update_date'),
        column('state_update')
    )
    with engine.begin() as conn:
        stmt = (
            update(svs_update_info)
            .where(svs_update_info.c.table_name == tbname)
            .values(
                update_date=func.now(),
                state_update='updated'
            )
        )
        conn.execute(stmt)

def start_update_tbl(table_name):
    if table_name == "src_store":
        # Создаём DataFrame
        print('Соответствие таблице MySQL src_store: ', table_name)
        data = getOne()
        print(data)
        df = pd.DataFrame(data)

        # Создаём engine для подключения к MySQL
        engine = create_engine("mysql+pymysql://master_logist:!StE1q2w3e2w1q@192.168.2.228:3306/sppr")

        # Загружаем данные в таблицу
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        df.to_sql(
            name=f"{table_name}",  # Название таблицы в БД
            con=engine,  # Движок подключения
            if_exists="replace",  # Действия, если таблица существует:
            #   - "fail" — ошибка
            #   - "replace" — перезаписать
            #   - "append" — добавить строки
            index=False,  # Не сохранять индекс DataFrame
            method="multi",  # Оптимизация: множественная вставка
            chunksize=1000  # Размер пакета (для больших данных)
        )
        # Закрываем соединение
        engine.dispose()
        print("Данные из 1С загружены")
        update_sys_data(table_name)

        return 1


    sql_txt = "UPDATE *  FROM sppr.view_store_counts;"
    engine = create_engine("mysql+pymysql://master_logist:!StE1q2w3e2w1q@192.168.2.228:3306/sppr")
    try:
        connection = engine.connect()
        print("Подключение успешно!")
        connection.close()
    except Exception as e:
        print(f"Ошибка подключения: {e}")

    tables = {"src_demand": "z:/_logist/___LOGBASE/Integration/_SOURCE/1С_ЗАКАЗЫ.xlsx",
              "src_store": "z:/_logist/___LOGBASE/Integration/_SOURCE/1С_ОСТАТКИ_СКЛАД.xlsx",
              "src_current_sales": "z:/_logist/___LOGBASE/Integration/_SOURCE/1С_ОТГРУЗКИ_2026.xlsx",
              "src_prod_receipt": "z:/_logist/___LOGBASE/Integration/_SOURCE/1С_ПОСТУПЛЕНИЯ.xlsx",
              "src_price": "z:/_logist/___LOGBASE/Integration/_SOURCE/1С_ЦЕНЫ.xlsx",
              "src_fgos": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_ФГОС.xlsx",
              "src_autor_contracts": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_Авторские_договоры.xlsx",
              "src_configurations": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_Конфигурации.xlsx",
              "src_projects": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_Проекты.xlsx",
              "src_tk": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_ТК.xlsx",
              "src_tp": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_ТП.xlsx",
              "src_sales_statistic": "z:/_logist/___LOGBASE/Integration/_SOURCE/СТАТИСТИКА_ПРОДАЖ.xlsx"}



    result = "ok"
    key = table_name
    value = tables.get(key, '')
    if value != '':
        df = pd.read_excel(f"{value}", sheet_name="Лист_1")
        df.to_sql(
            name=f"{key}",  # Название таблицы в БД
            con=engine,  # Движок подключения
            if_exists="replace",  # Действия, если таблица существует:
            #   - "fail" — ошибка
            #   - "replace" — перезаписать
            #   - "append" — добавить строки
            index=False,  # Не сохранять индекс DataFrame
            method="multi",  # Оптимизация: множественная вставка
            chunksize=1000  # Размер пакета (для больших данных)
        )

    update_sys_data(key)

    print(result)
    return result

def getOne():
    result = ''
    клиент = БромКлиент("http://192.168.1.2:8000/UT_Current/ws/brom_api/", "brom_user", "StEazsxdcfv11")

    # Создаем запрос
    текЗапрос = клиент.СоздатьЗапрос("""
            ВЫБРАТЬ
                Прав(СокрЛП(ТоварыНаСкладахОстатки.Номенклатура.Артикул), 5) КАК КодПроекта,
                СокрЛП(ТоварыНаСкладахОстатки.Номенклатура.Артикул) КАК Артикул,
                ТоварыНаСкладахОстатки.Номенклатура.Наименование КАК Номенклатура,
                Цел(ТоварыНаСкладахОстатки.ВНаличииОстаток) КАК В_наличии,
                ЕСТЬNULL(Цел(ЗапасыИПотребностиОстатки.РезервироватьНаСкладеОстаток), 0) КАК В_резерве
            ИЗ
                РегистрНакопления.ТоварыНаСкладах.Остатки КАК ТоварыНаСкладахОстатки
                ЛЕВОЕ СОЕДИНЕНИЕ РегистрНакопления.ЗапасыИПотребности.Остатки КАК ЗапасыИПотребностиОстатки
                ПО ТоварыНаСкладахОстатки.Склад = ЗапасыИПотребностиОстатки.Склад
                И ТоварыНаСкладахОстатки.Номенклатура = ЗапасыИПотребностиОстатки.Номенклатура
                ГДЕ ТоварыНаСкладахОстатки.Склад.Наименование = "ИЦ Академия"
        """)


    # Выполняем запрос и выводим результат на экран
    результат = текЗапрос.Выполнить()
    # Извлекаем строки результата
    rows = []
    for стр in результат:
        #print(стр.Артикул, стр.Номенклатура, стр.В_наличии)
        row_dict = {
            "КодПроекта": стр.КодПроекта,
            "Артикул": стр.Артикул,
            "Номенклатура": стр.Номенклатура,
            "В_наличии": стр.В_наличии,
            "В_резерве": стр.В_резерве
        }
        rows.append(row_dict)

    return rows


def start_update():

    engine = create_engine("mysql+pymysql://master_logist:!StE1q2w3e2w1q@192.168.2.228:3306/sppr")
    try:
        connection = engine.connect()
        print("Подключение успешно!")
        connection.close()
    except Exception as e:
        print(f"Ошибка подключения: {e}")

    tables = {"src_demand": "z:/_logist/___LOGBASE/Integration/_SOURCE/1С_ЗАКАЗЫ.xlsx",
     "src_store": "z:/_logist/___LOGBASE/Integration/_SOURCE/1С_ОСТАТКИ_СКЛАД.xlsx",
     "src_current_sales": "z:/_logist/___LOGBASE/Integration/_SOURCE/1С_ОТГРУЗКИ_2026.xlsx",
     "src_prod_receipt": "z:/_logist/___LOGBASE/Integration/_SOURCE/1С_ПОСТУПЛЕНИЯ.xlsx",
     "src_price": "z:/_logist/___LOGBASE/Integration/_SOURCE/1С_ЦЕНЫ.xlsx"}

     # "src_fgos": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_ФГОС.xlsx",
     # "src_autor_contracts": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_Авторские_договоры.xlsx",
     # "src_configurations": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_Конфигурации.xlsx",
     # "src_projects": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_Проекты.xlsx",
     # "src_tk": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_ТК.xlsx",
     # "src_tp": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_ТП.xlsx",
     # "src_sales_statistic": "z:/_logist/___LOGBASE/Integration/_SOURCE/СТАТИСТИКА_ПРОДАЖ.xlsx"}




    result = ""

    for key, value in tables.items():
        print(f"{key}: {value}")
        start_time = time.perf_counter()
        df = pd.read_excel(f"{value}", sheet_name="Лист_1")

        df.to_sql(
                name=f"{key}",  # Название таблицы в БД
                con=engine,  # Движок подключения
                if_exists="replace",  # Действия, если таблица существует:
                                        #   - "fail" — ошибка
                                        #   - "replace" — перезаписать
                                        #   - "append" — добавить строки
                index=False,  # Не сохранять индекс DataFrame
                method="multi",  # Оптимизация: множественная вставка
                chunksize=1000  # Размер пакета (для больших данных)
        )
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        print (f"Данные в таблицу {key} из файла {value} загружены! Время выполнения: {execution_time:.4f} секунд")
        result = result + "\n" + f"Данные в таблицу {key} из файла {value} загружены! Время выполнения: {execution_time:.4f} секунд"


    print(result)
    return result