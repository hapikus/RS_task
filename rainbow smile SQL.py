import pandas as pd
import sqlite3
from pandas.io.excel import ExcelWriter
import os


def create_tables():
    file = 'SQL.xlsx'

    shops = pd.read_excel(file, sheet_name='SHOPS')
    goods = pd.read_excel(file, sheet_name='GOODS')
    sales = pd.read_excel(file, sheet_name='SALES')

    # print(list(shops), list(goods), list(sales))

    conn = sqlite3.connect('rainbow_smile')
    c = conn.cursor()

    c.execute('CREATE TABLE IF NOT EXISTS SHOPS (SHOPNUMBER, CITY, ADDRESS)')
    c.execute('CREATE TABLE IF NOT EXISTS GOODS (ID_GOOD, CATEGORY, GOOD_NAME, PRICE)')
    c.execute('CREATE TABLE IF NOT EXISTS SALES (DATE, SHOPNUMBER, ID_GOOD, QTY)')
    conn.commit()

    shops.to_sql('SHOPS', conn, if_exists='replace', index=False)
    goods.to_sql('GOODS', conn, if_exists='replace', index=False)
    sales.to_sql('SALES', conn, if_exists='replace', index=False)


def tasks(lst):
    conn = sqlite3.connect('rainbow_smile')
    c = conn.cursor()

    # SHOPS ['SHOPNUMBER', 'CITY', 'ADDRESS']
    # GOODS ['ID_GOOD', 'CATEGORY', 'GOOD_NAME', 'PRICE']
    # SALES ['DATE', 'SHOPNUMBER', 'ID_GOOD', 'QTY']

    # task1
    # Необходимо получить все возможные варианты магазин-товар (без использования таблицы SALES)
    if 1 in lst:
        c.execute('''  
                SELECT SHOPS.SHOPNUMBER, SHOPS.CITY,
                       GOODS.ID_GOOD, GOODS.CATEGORY
                FROM SHOPS 
                CROSS JOIN GOODS        
                 ''')

        task1 = pd.DataFrame(c.fetchall())
        task1.to_excel('task1.xlsx')

    # task2
    # Условие: выборка по продажам за 2.01.2016
    if 2 in lst:
        c.execute('''
                SELECT SALES.SHOPNUMBER, 
                       SHOPS.CITY, SHOPS.ADDRESS,
                       sum(SALES.QTY) as QTY_sum,
                       sum(SALES.QTY * GOODS.PRICE) as Price_goods
                FROM SALES, GOODS, SHOPS
                WHERE SALES.ID_GOOD = GOODS.ID_GOOD AND 
                      DATE = '2016-01-02 00:00:00' AND
                      SALES.SHOPNUMBER = SHOPS.SHOPNUMBER
                GROUP BY SALES.SHOPNUMBER
                 ''')

        task2 = pd.DataFrame(c.fetchall())
        task2.to_excel('task2.xlsx')

    # task3
    # Условие: выборка только по товарам направления ЧИСТОТА
    if 3 in lst:
        c.execute('''     
                SELECT SALES.DATE, SHOPS.CITY,
                       round(CAST(SUM(SALES.QTY * GOODS.PRICE) * 100 AS FLOAT)/
                       SUM(SUM(SALES.QTY * GOODS.PRICE)) over(PARTITION BY SALES.DATE), 4) as Percentage 
                FROM SALES, SHOPS, GOODS
                WHERE SALES.SHOPNUMBER = SHOPS.SHOPNUMBER AND
                      SALES.ID_GOOD = GOODS.ID_GOOD AND
                      GOODS.CATEGORY = "ЧИСТОТА"
                GROUP BY SALES.DATE, SHOPS.CITY   
                 ''')

        task3 = pd.DataFrame(c.fetchall())
        task3.to_excel('task3.xlsx')

    # task4
    # Условие: информация о топ-3 товарах по продажам в штуках в каждом магазине в каждую дату
    if 4 in lst:
        c.execute('''   
                WITH Top_all AS (SELECT SALES.DATE, SALES.SHOPNUMBER, GOODS.GOOD_NAME, SALES.QTY,
                       COUNT(SHOPNUMBER) OVER(PARTITION BY SHOPNUMBER, DATE
                       ORDER BY SALES.DATE ASC, SALES.SHOPNUMBER ASC, SALES.QTY DESC 
                       rows between unbounded preceding and current row) 
                       AS Count_val
                FROM SALES, GOODS 
                WHERE SALES.ID_GOOD = GOODS.ID_GOOD)
                
                SELECT DATE, SHOPNUMBER, GOOD_NAME
                FROM Top_all
                WHERE Count_val <=3
                ORDER BY DATE ASC, SHOPNUMBER ASC     
                ''')

        task4 = pd.DataFrame(c.fetchall())
        task4.to_excel('task4.xlsx')

    # task5
    # Условие: только магазины СПб
    if 5 in lst:
        # Сумма в руб за предыдущую дату - на 2 число показывем продажи за первое и т.д.
        c.execute('''   
                SELECT datetime(SALES.DATE,'+1 day') as Date_new, 
                       SALES.SHOPNUMBER, GOODS.CATEGORY, 
                       sum(SALES.QTY * GOODS.PRICE) as Price_goods
                FROM SALES, GOODS, SHOPS
                WHERE SALES.SHOPNUMBER = SHOPS.SHOPNUMBER AND
                      SALES.ID_GOOD = GOODS.ID_GOOD AND
                      SHOPS.CITY = "СПб"
                GROUP BY SALES.DATE, SALES.SHOPNUMBER, GOODS.CATEGORY
                ''')

        task5_v1 = pd.DataFrame(c.fetchall())

        # Сумма в руб за предыдущую дату - сумма нарастающим итогом, 2 - за 1 и 2, 3 - за 1, 2, 3 и т.д.
        c.execute('''
                with day_sum_cat as (SELECT DISTINCT SALES.DATE, SHOPS.SHOPNUMBER, GOODS.CATEGORY,
                      0 as Price_goods
                FROM SHOPS, SALES
                CROSS JOIN GOODS 
                WHERE SHOPS.SHOPNUMBER = SALES.SHOPNUMBER AND
                      SHOPS.CITY = "СПб"
                
                UNION ALL
           
                SELECT SALES.DATE, SALES.SHOPNUMBER, GOODS.CATEGORY, 
                       sum(SALES.QTY * GOODS.PRICE) as Price_goods
                FROM SALES, GOODS, SHOPS
                WHERE SALES.SHOPNUMBER = SHOPS.SHOPNUMBER AND
                      SALES.ID_GOOD = GOODS.ID_GOOD AND
                      SHOPS.CITY = "СПб" 
                GROUP BY SALES.DATE, SALES.SHOPNUMBER, GOODS.CATEGORY)
                
                SELECT DATE, SHOPNUMBER, CATEGORY,
                       sum(Sum(Price_goods)) OVER(PARTITION BY SHOPNUMBER, CATEGORY
                       ORDER BY DATE ASC, SHOPNUMBER ASC, CATEGORY ASC
                       rows between unbounded preceding and current row)
                       AS Sum_total       
                FROM day_sum_cat
                GROUP BY DATE, SHOPNUMBER, CATEGORY
                
                ''')

        # print(len(c.fetchall()))
        # for row in c.fetchall():
        #     print(row)

        task5_v2 = pd.DataFrame(c.fetchall())
        with ExcelWriter('task5.xlsx') as writer:
            task5_v1.to_excel(writer, sheet_name="task5_v1", index=False)
            task5_v2.to_excel(writer, sheet_name="task5_v2", index=False)


if not os.path.exists('rainbow_smile'):
    assert(os.path.exists('SQL.xlsx'))
    create_tables()

assert(os.path.exists('rainbow_smile'))
tasks(list(i for i in range(1, 6)))
