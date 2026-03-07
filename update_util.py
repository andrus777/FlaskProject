import pandas as pd
from datetime import datetime

from brom import БромКлиент
from httplib2.auth import params
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
    result = -1

    if table_name == "src_store":
        # Создаём DataFrame
        print('Соответствие таблице MySQL: ', table_name)
        txt_sql = """
            ВЫБРАТЬ
                Прав(СокрЛП(ТоварыНаСкладахОстатки.Номенклатура.Артикул), 5) КАК КодПроекта,
                СокрЛП(ТоварыНаСкладахОстатки.Номенклатура.Артикул) КАК Артикул,
                ТоварыНаСкладахОстатки.Номенклатура.Наименование КАК Номенклатура,
                Цел(ТоварыНаСкладахОстатки.ВНаличииОстаток) КАК В_наличии,
                ЕСТЬNULL(Цел(ЗапасыИПотребностиОстатки.РезервироватьНаСкладеОстаток), 0) КАК В_резерве,
                ЕСТЬNULL(Цел(ТоварыНаСкладахОстатки.КОтгрузкеОстаток), 0) КАК В_отгрузке
            ИЗ
                РегистрНакопления.ТоварыНаСкладах.Остатки КАК ТоварыНаСкладахОстатки
                ЛЕВОЕ СОЕДИНЕНИЕ РегистрНакопления.ЗапасыИПотребности.Остатки КАК ЗапасыИПотребностиОстатки
                ПО ТоварыНаСкладахОстатки.Склад = ЗапасыИПотребностиОстатки.Склад
                И ТоварыНаСкладахОстатки.Номенклатура = ЗапасыИПотребностиОстатки.Номенклатура
                ГДЕ ТоварыНаСкладахОстатки.Склад.Наименование = "ИЦ Академия"
        """
        fields = ["КодПроекта", "Артикул", "Номенклатура","В_наличии","В_резерве","В_отгрузке"]
        data = getOne(txt_sql, fields)
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
        result = 1

    if table_name == "src_sale_current":
        # Создаём DataFrame
        print('Соответствие таблице MySQL: ', table_name)
        txt_sql = """
            ВЫБРАТЬ
                Прав(СокрЛП(РеализацияТовары.Номенклатура.Артикул), 5) КАК Проект,
                СокрЛП(РеализацияТовары.Номенклатура.Артикул) КАК Артикул,
                РеализацияТовары.Номенклатура.Наименование КАК Номенклатура,
                Цел(РеализацияТовары.Количество) КАК Количество,
                РеализацияТовары.Сумма КАК Сумма
            ИЗ
                Документ.РеализацияТоваровУслуг КАК Реализация
                    ЛЕВОЕ СОЕДИНЕНИЕ Документ.РеализацияТоваровУслуг.Товары КАК РеализацияТовары
                    ПО Реализация.Ссылка = РеализацияТовары.Ссылка
            ГДЕ
                Реализация.Дата >= ДАТАВРЕМЯ(2026, 1, 1, 0, 0, 0)
        """
        fields = ["Проект", "Артикул", "Номенклатура","Количество","Сумма"]
        data = getOne(txt_sql, fields)
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
        result = 1

    if table_name == "src_realisation":
        # Создаём DataFrame
        print('Соответствие таблице MySQL: ', table_name)
        txt_sql = """
            ВЫБРАТЬ
                Реализация.ЗаказКлиента.Номер КАК НомерЗаказа, 
                МАКСИМУМ(Реализация.Дата) КАК ДатаРеализации,
                СУММА(Реализация.СуммаДокумента) КАК СуммаДокумента
            ИЗ
                Документ.РеализацияТоваровУслуг КАК Реализация  
            ГДЕ
                Реализация.ПометкаУдаления = ЛОЖЬ
                
            СГРУППИРОВАТЬ ПО
                Реализация.ЗаказКлиента.Номер
        """
        fields = ["НомерЗаказа", "ДатаРеализации", "СуммаДокумента"]
        data = getOne(txt_sql, fields)
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
        result = 1

    if table_name == "src_demands":
        # Создаём DataFrame
        print('Соответствие таблице MySQL: ', table_name)
        txt_sql = """
            ВЫБРАТЬ
                ЗаказКлиента.Номер КАК Номер,
                ЕСТЬNULL(ЗаказКлиента.Ссылка.Дата, "-") КАК ДатаЗаказа,
                ЕСТЬNULL(ЗаказКлиента.Академия_Проект.Родитель.Представление, "") КАК ПроектРодитель,
                ЕСТЬNULL(ЗаказКлиента.Академия_Проект.Наименование, "-") КАК ПроектИмя,
                ЕСТЬNULL(ЗаказКлиента.Академия_СтатусЗаказа.Наименование, "-") КАК СтатусЗаказа,
                ЗаказКлиента.Комментарий КАК Комментарий,
                ЗаказКлиента.Менеджер.Наименование КАК Менеджер,
                ЗаказКлиента.Автор.Наименование КАК Автор,
                ЗаказКлиента.Контрагент.ИНН КАК ИНН,
                ЗаказКлиента.Контрагент.КПП КАК КПП,
                ЗаказКлиента.Партнер.БизнесРегион.Наименование КАК БизнесРегион,
                ЕСТЬNULL(ЗаказКлиентаДополнительныеРеквизиты.Значение.Наименование, "-") КАК ОтвМен,
                ЗаказКлиента.Соглашение.Наименование КАК Соглашение,
                ЗаказКлиента.СуммаДокумента КАК СуммаЗаказа,
                ЕСТЬNULL(РеализацияПоЗаказам.ДатаРеализации, "-") КАК ДатаРеализации,
                ЕСТЬNULL(РеализацияПоЗаказам.СуммаДокумента, "-") КАК СуммаРеализации
            ИЗ
                Документ.ЗаказКлиента КАК ЗаказКлиента
                    ЛЕВОЕ СОЕДИНЕНИЕ Документ.ЗаказКлиента.ДополнительныеРеквизиты КАК ЗаказКлиентаДополнительныеРеквизиты
                    ПО (ЗаказКлиента.Ссылка = ЗаказКлиентаДополнительныеРеквизиты.Ссылка)
                    ЛЕВОЕ СОЕДИНЕНИЕ (ВЫБРАТЬ
                        Реализация.ЗаказКлиента.Номер КАК ЗаказКлиентаНомер,
                        МАКСИМУМ(Реализация.Дата) КАК ДатаРеализации,
                        СУММА(Реализация.СуммаДокумента) КАК СуммаДокумента
                    ИЗ
                        Документ.РеализацияТоваровУслуг КАК Реализация
                    
                    СГРУППИРОВАТЬ ПО
                        Реализация.ЗаказКлиента.Номер) КАК РеализацияПоЗаказам
                    ПО (ЗаказКлиента.Номер = РеализацияПоЗаказам.ЗаказКлиентаНомер)
            ГДЕ
                ЗаказКлиента.ПометкаУдаления = ЛОЖЬ
        """
        fields = ["Номер", "ДатаЗаказа", "ПроектРодитель","ПроектИмя","СтатусЗаказа","Комментарий","Менеджер","Автор","ИНН","КПП","БизнесРегион","ОтвМен","Соглашение","СуммаЗаказа","ДатаРеализации", "СуммаРеализации"]
        data = getOne(txt_sql, fields)
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
        result = 1

    if table_name == "dt_all_projects":
        # Создаём engine для подключения к MySQL
        engine = create_engine("mysql+pymysql://master_logist:!StE1q2w3e2w1q@192.168.2.228:3306/sppr")

        # Выполняем SQL‑запрос и получаем DataFrame
        query = "SELECT * FROM view_projects_all"
        df = pd.read_sql(query, con=engine)

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
        print("Данные загружены")
        update_sys_data(table_name)
        result = 1



    tables = {"src_fgos": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_ФГОС.xlsx",
              "src_autor_contracts": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_Авторские_договоры.xlsx",
              "src_configurations": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_Конфигурации.xlsx",
              "src_projects": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_Проекты.xlsx",
              "src_tk": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_ТК.xlsx",
              "src_tp": "z:/_logist/___LOGBASE/Integration/_SOURCE/АХ_ТП.xlsx",
              "src_sale_statistic": "z:/_logist/___LOGBASE/Integration/_SOURCE/СТАТИСТИКА_ПРОДАЖ.xlsx",
              "src_sale_statistic_all": "z:/_logist/___LOGBASE/Integration/_SOURCE/СТАТИСТИКА_ПРОДАЖ_ПО_2025.xlsx"}
    if table_name in tables:
        engine = create_engine("mysql+pymysql://master_logist:!StE1q2w3e2w1q@192.168.2.228:3306/sppr")

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
        result = 1

    return result

def getOne(txt_sql, fields):
    result = ''
    клиент = БромКлиент("http://192.168.1.2:8000/UT_Current/ws/brom_api/", "brom_user", "StEazsxdcfv11")

    # Создаем запрос
    текЗапрос = клиент.СоздатьЗапрос(txt_sql)


# Выполняем запрос и выводим результат на экран
    результат = текЗапрос.Выполнить()
    # Извлекаем строки результата
    rows = []
    for стр in результат:
        row_dict = {}
        for field in fields:
            if hasattr(стр, field):
                row_dict[field] = getattr(стр, field)
            else:
                row_dict[field] = None
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