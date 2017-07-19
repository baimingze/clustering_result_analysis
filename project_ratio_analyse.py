import pymysql.cursors

connection = None

project_id = "PXD000383"

def connect_to_db():
    connection = pymysql.connect(host='localhost',
                                     user='pride_cluster_t',
                                     password='pride123',
                                     db='pride_cluster_t',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
    print("Opened database successfully")




    select_sql = "SELECT cluster_id FROM `201504_test` WHERE spec_prj_id = `" + project_id + "`"

    try:
        with connection.cursor() as cursor:
            cursor.execute(select_sql)
            result = cursor.fetchAll()
            connection.commit()

            print(str(len(result)))
         
    finally:
        print("end of select")

connect_to_db()
