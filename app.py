# https://proglib.io/p/samouchitel-po-python-dlya-nachinayushchih-chast-23-osnovy-veb-razrabotki-na-flask-2023-06-27

from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from paste.auth import form

from flask.cli import ScriptInfo

from models import Artist, Album, Song, db
import os
import sys
import json

from update_util import *



app = Flask(__name__, static_folder="static")
app.secret_key = 'your-secret-key-here'

# Настройка Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'index'

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///music.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# связываем приложение и экземпляр SQLAlchemy
db.init_app(app)

# Пример пользователя (в реальном приложении используйте БД)
USERS = {
    'admin': {
        'password_hash': generate_password_hash('adminpass'),
        'is_admin': True
    },
    'user': {
        'password_hash': generate_password_hash('userpass'),
        'is_admin': False
    }
}

class User(UserMixin):
    def __init__(self, username, is_admin):
        self.id = username
        self.username = username
        self.is_admin = is_admin

@login_manager.user_loader
def load_user(user_id):
    if user_id in USERS:
        return User(user_id, USERS[user_id]['is_admin'])
    return None




@app.route('/')
def index():  # put application's code here
    return render_template('base_new.html')

@app.route('/adminp/<admcode>', methods=['GET','POST'])
def adminp(admcode):
    print("Код в url: ", admcode)

    if admcode == 'main':
        return render_template('admin_main.html')
    elif admcode == 'about':
        return render_template('admin_about.html')
    elif admcode == 'update':
        # 1. Получаем JSON из запроса
        data = request.get_json()
        print('data: ', data)
        if len(data['tbname']) > 1:
            table_name = data['tbname']
            print(f"Начинаем обновление таблицы: {table_name}")
            start_update_tbl(table_name)

        con = mysql.connector.connect(host='192.168.2.228', user='master_logist', password='!StE1q2w3e2w1q',
                                      database='sppr')
        cursor = con.cursor()
        sql_txt = "SELECT * FROM sppr.svs_update_info;"
        cursor.execute(sql_txt)
        results = cursor.fetchall()
        cursor.close()
        con.close()
        return render_template('admin_update.html', results=results)

    else:
        pass  # Заглушка для будущей реализации

@app.route('/update', methods=['GET','POST'])
def update():  # put application's code here
    start_update()
    return render_template('base_new.html')

@app.route('/eis/sale/<izd>', methods=['GET','POST'])
def eis_sale(izd):
    print("Код в url: ", izd)
    data = request.get_json()
    print('data: ', data)
    if len(data['tbname']) > 1:
        id = data['tbname']
        print(f"Артикула: {id}")

    if len(izd) == 9:
        con = mysql.connector.connect(host='192.168.2.228', user='master_logist', password='!StE1q2w3e2w1q',
                                      database='sppr')
        cursor = con.cursor()
        sql_txt = f"select Заказ, Дата_накладной, Краткое_название, Количество, Сумма_накладной from sppr.src_sale_statistic_all where полный_номер='{izd}' and Год={id} order by Заказ;"

        cursor.execute(sql_txt)
        results = cursor.fetchall()
        cursor.close()
        con.close()
        return render_template('eis_sale.html', results=results)

@app.route('/eis/<prcode>', methods=['GET','POST'])
def eis(prcode):
    print("Код в url: ", prcode)

    if prcode == 'projects':
        con = mysql.connector.connect(host='192.168.2.228', user='master_logist', password='!StE1q2w3e2w1q',
                                      database='sppr')
        cursor = con.cursor()
        # sql_txt = "SELECT Проект, Проект_Имя, Примечание, Статус FROM sppr.tbl_projects ORDER BY Проект_Имя;"
        sql_txt = "select КодПроекта, Проект_Имя, Уровень_образования, Статус, Примечание, В_резерве, Доступно, pr2023, epr2023, pr2024, epr2024, pr2025, epr2025, pr2026, epr2026 from sppr.dt_all_projects order by КодПроекта;"

        cursor.execute(sql_txt)
        results = cursor.fetchall()
        cursor.close()
        con.close()
        return render_template('eis_projects.html', results=results)
    elif prcode == 'main':
        return render_template('eis_main.html')
    elif prcode == 'about_eis':
        return render_template('eis_about.html')
    elif prcode == 'store':
        con = mysql.connector.connect(host='192.168.2.228', user='master_logist', password='!StE1q2w3e2w1q',
                                      database='sppr')

        cursor = con.cursor()
        sql_txt = "SELECT *  FROM sppr.view_store_counts_new;"
        cursor.execute(sql_txt)
        results = cursor.fetchall()
        cursor.close()
        con.close()
        return render_template('eis_store.html', results=results)

    elif prcode == 'rez':
        results = ""
        # 1. Получаем JSON из запроса
        data = request.get_json()
        print('data: ', data)
        if len(data['tbname']) > 1:
            id = data['tbname']
            print(f"Артикула: {id}")

            # Создаём DataFrame
            txt_sql = f"""
                             ВЫБРАТЬ
                                ЗапасыИПотребностиОстатки.Номенклатура.Артикул КАК Артикул,
                                ЗапасыИПотребностиОстатки.Номенклатура.Наименование КАК Номенклатура,
                                ЦЕЛ(ЗапасыИПотребностиОстатки.РезервироватьНаСкладеОстаток) КАК Резерв,
                                ЗапасыИПотребностиОстатки.Заказ.Номер КАК ЗаказНомер, 
                                ЗапасыИПотребностиОстатки.Заказ.Дата КАК ЗаказДата,
                                ЗапасыИПотребностиОстатки.Заказ.Партнер.Наименование КАК Партнер,
                                ЗапасыИПотребностиОстатки.Заказ.Менеджер.Наименование КАК Менеджер
                            ИЗ
                                РегистрНакопления.ЗапасыИПотребности.Остатки КАК ЗапасыИПотребностиОстатки
                            ГДЕ
                                ЗапасыИПотребностиОстатки.Склад.Наименование = "ИЦ Академия"
                                И
                                СокрЛп(ЗапасыИПотребностиОстатки.Номенклатура.Артикул) = "{id}"
                        """
            fields = ["Артикул", "Номенклатура", "Резерв", "ЗаказНомер", "ЗаказДата", "Партнер", "Менеджер"]
            data = getOne(txt_sql, fields)
            # print(data)
            # results = pd.DataFrame(data)
            results = [[row[field] for field in fields] for row in data]
            print("Данные из 1С загружены")
            print(results)

        return render_template('eis_rezerv.html', results=results)

    else:
        if len(prcode)>3:
            con = mysql.connector.connect(host='192.168.2.228', user='master_logist', password='!StE1q2w3e2w1q',
                                          database='sppr')
            cursor = con.cursor()

            if prcode.isdigit():
                sql_txt = "SELECT * FROM sppr.view_eis_pr WHERE ПроектКод LIKE '%" + prcode + "';"
            else:
                sql_txt = "SELECT * FROM sppr.view_eis_pr WHERE ПроектКод LIKE '%" + prcode + "%';"

            print(sql_txt)

            cursor.execute(sql_txt)
            results = cursor.fetchall()
            cursor.close()
            con.close()
            return render_template('eis_projects_tb1.html', results=results)
        else:
            results = {'', '', '', '', '', '', '', '', ''}
            return render_template('eis_projects.html', results=results)

@app.route('/eis_ext/<prcode>', methods=['POST'])
def eis_ext(prcode):

    con = mysql.connector.connect(host='192.168.2.228', user='master_logist', password='!StE1q2w3e2w1q',
                                          database='sppr')
    cursor = con.cursor()
    if prcode.isdigit():
        sql_txt = "SELECT * FROM sppr.view_configuration_all WHERE left(Артикул,1)='1' and  КодПроекта = '" + prcode + "';"
        print(sql_txt)

        cursor.execute(sql_txt)
        results = cursor.fetchall()
        cursor.close()
        con.close()
        return render_template('eis_projects_tb2.html', results=results)
    else:
        results = {'', '', '', '', '', '', '', '', ''}
        return render_template('eis_projects_tb2.html', results=results)

@app.route('/demands/<demtag>', methods=['GET','POST'])
def demands(demtag):
    print("Код в url: ", demtag)
    if demtag == "about":
        return render_template('demands_about.html')
    elif demtag == "dem":

        con = mysql.connector.connect(host='192.168.2.228', user='master_logist', password='!StE1q2w3e2w1q',
                                  database='sppr')
        cursor = con.cursor()
        sql_txt = "SELECT * FROM view_all_demands;"
        print(sql_txt)
        cursor.execute(sql_txt)
        results = cursor.fetchall()

        cursor.close()
        con.close()
        return render_template('demands_all.html', results=results)
    else:
        return render_template('demands_main.html')
@app.route('/game')
def game():  # put application's code here
    return render_template('game.html')

from brom import *
@app.route('/onec/<nom>')
def one_c(nom):
    result = ''
    клиент = БромКлиент("http://192.168.1.2:8000/UT_Test/ws/brom_api/", "brom_user", "StEazsxdcfv11")

    # Создаем запрос
    текЗапрос = клиент.СоздатьЗапрос("""
        ВЫБРАТЬ
       	    ТоварыНаСкладах.Номенклатура.Артикул КАК НоменклатураАртикул,
       	    ТоварыНаСкладах.Номенклатура.Наименование КАК НоменклатураНаименование,
       	    ТоварыНаСкладах.ВНаличии КАК ВНаличии
        ИЗ
       	    РегистрНакопления.ТоварыНаСкладах КАК ТоварыНаСкладах
       	ГДЕ
       	    СокрЛП(ТоварыНаСкладах.Номенклатура.Артикул) ПОДОБНО &Артикул
          """)

    # Устанавливаем значение параметра запроса
    текЗапрос.УстановитьПараметр("Артикул", "%" + nom.strip()[-5:])

    # Выполняем запрос и выводим результат на экран
    результат = текЗапрос.Выполнить()
    result = ""
    nom_name = ""
    for стр in результат:
        art = str(стр.НоменклатураАртикул).strip()
        print(art[-5:], nom.strip()[-5:])
        if art[-5:] == nom.strip()[-5:]:
            nom_name = стр.НоменклатураНаименование
            result = result + "Артикул: {0} Остаток: {1}".format(стр.НоменклатураАртикул, стр.ВНаличии) + "\n"
    if len(nom_name) > 0:
        result = nom_name + "\n" + result
    else:
        result = "Проект " + nom + " не найден"

    return result

@app.route('/demands_onec')
def demands_onec():
    result = ''
    клиент = БромКлиент("http://192.168.1.2:8000/UT_Test/ws/brom_api/", "brom_user", "StEazsxdcfv11")

    # Создаем запрос
    текЗапрос = клиент.СоздатьЗапрос("""
        ВЫБРАТЬ
            ЗаказКлиента.Номер КАК Номер,
            ЗаказКлиента.Дата КАК Дата
        ИЗ
            Документ.ЗаказКлиента КАК ЗаказКлиента
        
    """)

    # Устанавливаем значение параметра запроса
    # текЗапрос.УстановитьПараметр("Артикул", "%" + nom.strip()[-5:])

    # Выполняем запрос и выводим результат на экран
    результат = текЗапрос.Выполнить()
    result = ""

    for стр in результат:
        result = result + стр.Номер + "\n"

    if result == "":
        result = "Запрос вернул пусто"

    return result

@app.route('/songs')
def songs():
    songs_list = Song.query.all()
    return render_template('songs.html', songs=songs_list)

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
@app.route('/demandsgoogle')
def demands_google():
    # Подсоединение к Google Таблицам
    scope = ['https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive"]

    credentials = ServiceAccountCredentials.from_json_keyfile_name("static/gs_credentials.json", scope)
    client = gspread.authorize(credentials)
    #sheet = client.create("NewDatabase2")
    #sheet.share('kotov.aa.logist@gmail.com', perm_type='user', role='writer')

    # Откройте таблицу
    # sheet = client.open("NewDatabase2").sheet1
    # Прочитайте csv с помощью pandas
    # df = pd.read_csv('static/football_news.csv')
    # Экспортируйте df в таблицу
    # sheet.update([df.columns.values.tolist()] + df.values.tolist())

    sheet = client.open("Заказы ИЦ Академия").sheet1
    data = sheet.get_all_records()
    return render_template('demands.html', results=data)

@app.route('/demandsupdate')
def demands_update():
    fs_g = {'date_zakaz' : 'Дата',
            'manager' : 'Менеджер',
            'fo' : 'ФО',
            'region' : 'Регион',
            'vedom' : 'Ведомственная принадлежность',
            'uchred' : 'Учредитель',
            'inn' : 'ИНН',
            'kpp' : 'КПП',
            'id_zakaz' : 'Заказ',
            'client_name' : 'Клиент',
            'summa' : 'Сумма',
            'comment' : 'Комментарий',
            'status' : 'Статус',
            'project' : 'Проект',
            'proj_detail' : 'Детализация',
            'kvartal' : 'Квартал',
            'typ_nom' : 'ЭБ/СЭО/книги',
            'date_otgr' : 'Дата отгрузки (дата документов)',
            'type_dog' : 'Тип договора',
            'date_pay_plan' : 'Плановая дата оплаты',
            'status_pay' : 'Статус оплаты',
            'original_dog' : 'Оригинал получен'}
    fg_s = dict(zip(fs_g.values(), fs_g.keys()))
    #print(fg_s)

    # Подсоединение к Google Таблицам
    scope = ['https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name("static/gs_credentials.json", scope)
    client = gspread.authorize(credentials)
    sheet = client.open("Заказы ИЦ Академия").sheet1
    data = sheet.get_all_records()

    con = mysql.connector.connect(host='31.31.196.159', user='u0350632_ipadmin', password='StE1q2w3e4r5t',
                                  database='u0350632_ipbase')
    cursor = con.cursor()
    cursor.execute("DELETE FROM u0350632_ipbase.all_demands;")

    cursor = con.cursor()
    #print(data)
    for d in data:
        sql_txt="INSERT INTO u0350632_ipbase.all_demands ("
        values = ""
        fields = ""
        #print(d)
        if d['Заказ'] != "":
            for x in d.items():
                if x[0] in fg_s.keys():
                    value = x[1]
                    if fg_s[x[0]] == 'summa':
                        #print( x[1].replace(" ",""))
                        value = to_float(x[1])
                    #print(x[0], value, fg_s[x[0]])
                    fields = fields + fg_s[x[0]] + ", "
                    values = values + "'" + value + "', "
            sql_txt = sql_txt + fields[:-2] + ') VALUES (' + values[:-2] + ')'
            #print(sql_txt)
            #cursor = con.cursor()
            cursor.execute(sql_txt)
            con.commit()

    cursor = con.cursor()
    cursor.execute("SELECT * FROM `u0350632_ipbase`.`all_demands`;")
    results = cursor.fetchall()
    print(results)

    cursor.close()
    con.close()

    return render_template('demands.html', results=results)

def to_float(s):
    l = ('1','2','3','4','5','6','7','8','9','0',',','.')
    tmp_s = ""
    for sg in s:
        if sg in l:
            if sg == ',':
                tmp_s = tmp_s + '.'
            else:
                tmp_s = tmp_s + sg
    if tmp_s == "":
        return '0.0'
    else:
        return tmp_s
@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/store')
def store():
    return render_template('eis_main.html')

@app.route('/blog')
def blog():
    return 'Это блог с заметками о работе и увлечениях.'

import mysql.connector
@app.route('/eis_base')
def eis_base():

    con = mysql.connector.connect(host='31.31.196.159', user='u0350632_dev_use', password='StE171336777', database='u0350632_devs')

    # Создайте курсор для выполнения запросов
    cursor = con.cursor()

    # Выполните SQL-запрос
    cursor.execute("SELECT * FROM rate LIMIT 5")

    # Получите результаты
    results = cursor.fetchall()

    # for row in results:
    #    print(row)

    # Закройте соединение
    cursor.close()
    con.close()
    return render_template('nom.html', results=results)


@app.route('/user/<username>')
def user_profile(username):
    return f"Это профиль пользователя {username}"


@app.route('/login', methods=['POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        # проверка логина и пароля
        return 'Вы вошли в систему!'
    else:
        return render_template('login.html')

@app.route('/submit-form', methods=['GET', 'POST'])
def submit_form():
    if request.method == 'POST':
        List_code = request.form.get('textarea-1747820476136')
        print(List_code)


    return render_template('dem_analis.html', form=form)  # Отображение шаблона с формой

if __name__ == '__main__':
    from waitress import serve
    #host = os.getenv('FLASK_HOST', '192.168.2.228')
    #port = os.getenv('FLASK_PORT', '8080')
    #app.run(host=host, port=int(port), debug=False)

    #serve(app, host="192.168.2.228", port=8080)

    #http_server = WSGIServer(('192.168.2.228', 8080), app)
    #http_server.serve_forever()

    # Запуск на конкретном IP и порту
    app.run(
        host='192.168.2.228',  # IP-адрес
        port=8080,  # Порт
        debug=False  # Включить режим отладки
    )