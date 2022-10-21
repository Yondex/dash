from multiprocessing import connection
import os
import io
import csv

from flask import Flask, redirect, url_for, render_template, request, flash
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
import psycopg2
import pandas as pd
import numpy as np
from config import user, password, host, port, database, SQLALCHEMY_DATABASE_URI, secret_key
from openpyxl import load_workbook, Workbook
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash



ALLOWED_EXTENSIONS = {'xlsx'}


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False 
app.config["SQLALCHEMY_POOL_RECYCLE"] = 299

db = SQLAlchemy (app)
migrate = Migrate(app, db)

arr = []




@app.route('/add')
def base():
    return render_template('base.html')

@app.route('/autho')
def autho():
    return render_template('login.html')

@app.route('/login',methods = ['POST', 'GET'])
def login():
    password = 0
    email = request.form.get('user')
    password = str(request.form.get('pass'))
    hash = generate_password_hash(password)
    print(hash)
    try:
        #Создание конекшена
        bd = connect()
         # Курсор для выполнения операций с базой данных
        cursor = bd.cursor()
         # Выполнение SQL-запроса
        cursor.execute('Select * from dbo.specification_sbs order by period;')
        bd.commit()
        record = cursor.fetchall()
    except (Exception, IOError) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if bd:
            cursor.close()
            bd.close()
        print("Соединение с PostgreSQL закрыто")

    return render_template('login.html')

@app.route('/spec')
def spec():
    return render_template('spec.html')

@app.route('/test')
def test():
    return render_template('spec.html', message = "hello")

@app.route('/dict')
def dict():
    return render_template('dict.html')

@app.route('/index')
@app.route('/')
def hello():
    return render_template('index.html')

# Изменения словаря Спецификации
@app.route('/dict_update')
#def dict_update():
#    try:
#       connection = connect()
        # Курсор для выполнения операций с базой данных
#        cursor = connection.cursor()
        # Выполнение SQL-запроса
#        sql_update_query = ("""update dbo.specification_sbs set summa = %s, obj_count = %s, central_sign = %s  where id = %s""")
#        cursor.execute(sql_update_query, (summa, obj_count, central_sign, ids))
#        connection.commit()
#    except (Exception, IOError) as error:
#        print("Ошибка при работе с PostgreSQL", error)
 #   finally:
 #       if connection:
 #           cursor.close()
 #           connection.close()
 #       print("Соединение с PostgreSQL закрыто")

 #       return redirect(url_for('main'))

 #   return render_template('dict_update.html')

@app.route('/fop')
def fop():
    return render_template('fop.html')

@app.route('/result',methods = ['POST'])
def result():
    #summa1 = request.form.get('summa')
    #obj_count = request.form.get('kolvo')
    #ids = request.form.get('ids')
    #central_sign = request.form.get('status')


    data = request.get_json()
    count = data['entry']['count']
    summa = data['entry']['summa']
    status = data['entry']['status']
    ids = data['entry']['ids']



    try:
        connection = connect()
        # Курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        # Выполнение SQL-запроса
        sql_update_query = ("""update dbo.specification_sbs set summa = %s, obj_count = %s, central_sign = %s  where id = %s""")
        cursor.execute(sql_update_query, (summa, count, status, ids))
        connection.commit()
    except (Exception, IOError) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
        print("Соединение с PostgreSQL закрыто")

    #return redirect(url_for('main'))
    return render_template('main.html', message=error)




# функция выбора спеки по фильтрам (ТБ, услуга, период)
@app.route('/selected',methods = ['POST', 'GET'])
def db_selected():
    period = request.form.get('period')
    tb = request.form.get('tb')
    it_service = request.form.get('it_service')
    if tb == 'all':
        tb = r'%%'
    if it_service == 'all':
        it_service = r'%%'
    try:
        connection = connect()
        cursor = connection.cursor()
        sql_update_query = ("""select * from dbo.specification_sbs where period=%s and tb like %s and it_service like %s """)
        cursor.execute(sql_update_query, (period, tb, it_service))
        connection.commit()
        record = cursor.fetchall()
    except (Exception, IOError) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
        print("Соединение с PostgreSQL закрыто")
    if bool(record) is False:
        record = 'Нет данных в базе'
        return render_template('Error.html', message=record)
    return render_template('main.html', message=record)
    
   
@app.route('/main')
def main():
    try:
        #Создание конекшена
        connection = connect()
         # Курсор для выполнения операций с базой данных
        cursor = connection.cursor()
         # Выполнение SQL-запроса
        cursor.execute('Select * from dbo.specification_sbs order by period;')
        connection.commit()
        record = cursor.fetchall()
    except (Exception, IOError) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
        print("Соединение с PostgreSQL закрыто")
    return render_template('main.html', message = record)


# Удаляет задвоенные значения в БД.
def double_conn():
    try:
    # Подключение к существующей базе данных
        connection = connect()
    # Курсор для выполнения операций с базой данных
        cursor = connection.cursor()
    # Выполнение SQL-запроса
        cursor.execute('CALL dbck_nsk.dbo.remove_duplication();')
        connection.commit()
    except (Exception, IOError) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
        print("Соединение с PostgreSQL закрыто")


@app.route('/upload', methods=['GET', 'POST'])
def process_xlsx():
    filepath = upload()
    try:
        connection = connect()
        # Курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        df_reverse = pd.read_excel(filepath)
        df_reverse['model'] = 'Отсутствует'
        df_reverse['norm'] = 'Отсутствует'
        df_reverse['exclusion'] = 0
        df_reverse_new = df_reverse.filter(
            ['ID_SM', 'Вид движения_1С', 'Дата проводки_1С', 'Тербанк_SAP', 'Номенклатура\Краткий текст материала_1С',
             'Завод_1С', 'exclusion', 'Материал_1С', 'norm', 'Вид актива_1С', 'model', 'ПВХ код_1С', 'ПВХ имя_1С',
             'Адрес КЭ_1С', 'Статус актива_SAP', 'ID актива_SAP', 'Номер накладной_SAP', 'Сер номер_1С', 'Инв номер_1С',
             'Количество_1С'], axis=1)
        df_reverse_new.rename(
            columns={'ID_SM': 'id', 'Вид движения_1С': 'movement_type', 'Дата проводки_1С': 'doc_date',
                     'Тербанк_SAP': 'tb', 'Номенклатура\Краткий текст материала_1С': 'nomenclature', 'Завод_1С': 'gosb',
                     'Материал_1С': 'material', 'Вид актива_1С': 'typeeq', 'ПВХ код_1С': 'numpvh',
                     'ПВХ имя_1С': 'namepvh', 'Адрес КЭ_1С': 'sity', 'Статус актива_SAP': 'stateeq',
                     'ID актива_SAP': 'ideqsap', 'Номер накладной_SAP': 'invoicenumber', 'Сер номер_1С': 'serialnum',
                     'Инв номер_1С': 'invnum', 'Количество_1С': 'counteq'}, inplace=True)

        # Получаем названия полей
        headers = df_reverse_new.columns
        # Получаем лист значений
        data = df_reverse_new.values.tolist()
        # Создаем в памяти CSV файл
        string_buffer = io.StringIO()
        csv_writer = csv.writer(string_buffer)
        csv_writer.writerows(data)
       # Возвращаем buffer на первую строку
        string_buffer.seek(0)
        # Загружаем данные в базу
        cursor.copy_expert('''COPY dbo.reverse_flow_sbs FROM STDIN WITH (FORMAT CSV)''', string_buffer)
        # cursor.copy_from(string_buffer, 'reverse_flow_sbs', sep = ',', null = 'nan', columns = headers)
        cursor.execute("""UPDATE dbo.reverse_flow_sbs set material = REPLACE(material, 'nan', ''), sity = REPLACE(sity, 'nan', ''), ideqsap = REPLACE(ideqsap, 'nan', ''),
                                      invoicenumber = REPLACE(invoicenumber, 'nan', ''), serialnum = REPLACE(serialnum, 'nan', ''), invnum = REPLACE(invnum, 'nan', '')""")
        connection.commit()
        double_conn()
    except (Exception, IOError) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
        print("Соединение с PostgreSQL закрыто")
    record = 'Данные добавлены в базу'
    print("1")
    return render_template('about.html', message=record)


def connect():
    connection = psycopg2.connect(user=user,
                                      # пароль, который указали при установке PostgreSQL
                                      password=password,
                                      host=host,
                                      port=port,
                                      database=database)
    return connection


@app.route('/upload_dict', methods=['GET', 'POST'])
def upload_dict():
    filepath = upload()
    try:
        connection = connect()
        # Курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        df_speca_dict = pd.read_excel(filepath)
        df_speca_dict_new = df_speca_dict.filter(
            ['it_service', 'TB', 'level', 'days', 'connection_time', 'execution_time', 'tarif', 'object_names', 'raschet'], axis=1)
        # Получаем названия полей
        headers = df_speca_dict_new.columns
        # Получаем лист значений
        data = df_speca_dict_new.values.tolist()
        # Создаем в памяти CSV файл
        string_buffer = io.StringIO()
        csv_writer = csv.writer(string_buffer)
        csv_writer.writerows(data)
        # Возвращаем buffer на первую строку
        string_buffer.seek(0)
        # Загружаем данные в базу
        cursor.execute("""DELETE from dbo.specification_tarif""")
        connection.commit()
        cursor.copy_expert('''COPY dbo.specification_tarif FROM STDIN WITH (FORMAT CSV)''', string_buffer)
        # cursor.copy_from(string_buffer, 'reverse_flow_sbs', sep = ',', null = 'nan', columns = headers)
        connection.commit()
    except (Exception, IOError) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
        print("Соединение с PostgreSQL закрыто")
    record = 'Данные добавлены в базу'
    return record


@app.route('/upload_spec', methods=['GET', 'POST'])
def upload_spec():
    filepath = upload()
    try:
        connection = connect()
        # Курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        df_speca = pd.read_excel(filepath, sheet_name = 'Спецификация')
        print(0)
        df_speca['fimishstate'] = 0
        df_speca['central_sign'] = 0
        print(00)
        print(df_speca.head(3))
        df_speca['Объекты обслуживания'] = df_speca['Объекты обслуживания'].astype(str) + ' | '
        print(1)
        df_speca['Сегмент лок. сети'] = ' | ' + df_speca['Сегмент лок. сети'].astype(str)
        print(2)
        df_speca.loc[df_speca['Сегмент лок. сети'] == ' | Не учитывается', 'Сегмент лок. сети'] = ''
        print(3)
        df_speca['object_names'] = df_speca['Объекты обслуживания'].astype(str) + df_speca['Категория площадки Заказчика'].astype(str) + df_speca['Сегмент лок. сети'].astype(str)
        print(4)
        df_speca_new = df_speca.filter(
            ['Услуга', 'Месяц', 'ТБ', 'Уровень', 'Дней под-ки', 'Срок исполнения', 'Тариф', 'object_names', 'Кол-во Объектов', 'Сумма', 'fimishstate', 'central_sign'], axis=1)
        print(5)
        df1 = df_speca_new.head(3)
        print(df1)
        df_speca_new.to_excel("output.xlsx", sheet_name='Sheet_name_1')
        # Получаем названия полей
        headers = df_speca_new.columns
        # Получаем лист значений
        data = df_speca_new.values.tolist()
        # Создаем в памяти CSV файл
        string_buffer = io.StringIO()
        csv_writer = csv.writer(string_buffer)
        csv_writer.writerows(data)
        # Возвращаем buffer на первую строку
        string_buffer.seek(0)
         #Загружаем данные в базу
        cursor.execute("""DELETE from dbo.specification_tarif""")
        connection.commit()
        cursor.copy_expert('''COPY dbo.specification_tarif FROM STDIN WITH (FORMAT CSV)''', string_buffer)
        cursor.copy_from(string_buffer, 'reverse_flow_sbs', sep=',', null='nan', columns=headers)
        connection.commit()
    except (Exception, IOError) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
        print("Соединение с PostgreSQL закрыто")
    record = 'Данные добавлены в базу'
    return record

@app.route('/tarif_sbs')
def tarif_sbs():
    try:
        connection = connect()
        cursor = connection.cursor()
        cursor.execute('Select * from dbo.tarif_sbs;')
        connection.commit()
        record = cursor.fetchall()
        print(record)
    except (Exception, IOError) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
        print("Соединение с PostgreSQL закрыто")
    if not record:
        const_message = "Нет данных"
        return render_template('about.html', message = const_message)
    return render_template ('dict_update.html', message = record)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def upload():
    if request.method != 'POST':
        return render_template('index.html', error='unsupported HTTP method to upload')
    if 'file' not in request.files:
        return render_template('index.html', error='file not provided')
    file = request.files['file']
    if file.filename == '':
        return render_template('index.html', error='filename is empty')
    if not allowed_file(file.filename):
        return render_template('index.html', error='file extension not supported')
    if file:
        filename = secure_filename(file.filename)
        filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)) + r'/tmp', filename)
        file.save(filepath)

    return filepath




if __name__ == "__main__":
    app.run()