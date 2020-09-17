import psycopg2

def citysort(data):
    city = data[0][1]
    has_sold = 0
    soldlist = []
    all = {}
    for i in data:
        if i[1] == city:
            has_sold += int(i[4])
        else:
            all[has_sold] = city
            soldlist.append(has_sold)
            has_sold = 0
            city = i[1]
    soldlist.sort(reverse=True)
    city_sort = {}
    for i in soldlist:
        city_sort[all.get(i)] = i
    print(city_sort)

def marketsort(data):
    market = data[0][1]
    has_sold = 0
    soldlist = []
    all = {}
    for i in data:
        if i[1] == market:
            has_sold += int(i[3])
        else:
            all[has_sold] = market
            soldlist.append(has_sold)
            has_sold = 0
            market = i[1]
    soldlist.sort(reverse=True)
    market_sort = {}
    for i in soldlist:
        market_sort[all.get(i)] = i
    print(market_sort)

#connencting to fpdb
connection = psycopg2.connect(user = "postgres",
                                  password = "",
                                  host = "localhost",
                                  port = "5432",
                                  database = "fpdb")
cursor = connection.cursor()

#processing fp_city_aggregation data
create_table_query = '''select * from fp_city_aggregation order by city ;'''
cursor.execute(create_table_query)
data = cursor.fetchall()
citysort(data)

#processing fp_store _aggregation data
create_table_query = '''select * from fp_store_aggregation order by market_id ;'''
cursor.execute(create_table_query)
data = cursor.fetchall()
marketsort(data)


connection.commit()
connection.close()