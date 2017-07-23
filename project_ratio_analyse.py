import pymysql.cursors
import urllib3


cluster_props_dict = {}

get_id_peptides_url_pre = "http://www.ebi.ac.uk:80/pride/ws/archive/peptide/count/project/"

http = urllib3.PoolManager()

def get_id_peptides_no(project_id):

    get_id_peptides_url = get_id_peptides_url_pre + project_id
    try:
        response = http.request('GET', get_id_peptides_url, timeout=1000.0, retries=1000)
        if response.status == 200:
            return int(response.data)
        else:
            print("Failed to get the number of identified peptides for project !", project_id)
            return -1
    except:
        print("Failed to get the number of identified peptides for project !", project_id)
        return -2 

def connect_to_db():
    connection = pymysql.connect(host='localhost',
                                     user='pride_cluster_t',
                                     password='pride123',
                                     db='pride_cluster_t',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
    print("Opened database successfully")
    return connection

def get_projects(table_name, connection):
    select_sql = "SELECT project_id FROM `" + table_name + "`;"  
    projects = set() 
    try:
        with connection.cursor() as cursor:
            cursor.execute(select_sql)
            results = cursor.fetchall()
            connection.commit()
            for result in results:
                project_id = result.get("project_id")
                print(project_id)
                projects.add(project_id)
    finally:
        print("Got " + str(len(projects)) + "projects")
    projects.add('PRD000478')
    return projects

def get_clusters(project_id, table_name, connection):
    global cluster_props_dict
    #only get the identified spec
    select_spec_sql = "SELECT spectrum_title, cluster_fk FROM `" + table_name + "_spec` WHERE " + \
                 "spec_prj_id = '" + project_id + "' AND " +\
                 "is_spec_identified = 1" 
    N_spec_clustered = 0
    N_spec_above_clus_ratio = 0
    N_spec_lower_clus_ratio = 0

    try:
        with connection.cursor() as cursor:
            cursor.execute(select_spec_sql)
            results = cursor.fetchall()
            connection.commit()
            for result in results:
                spectrum_title = result.get("spectrum_title")
                cluster_fk = result.get("cluster_fk")
                if cluster_fk not in cluster_props_dict :
                    select_cluster_sql = "SELECT cluster_ratio, n_spec FROM `" + table_name + "` WHERE " + \
                       "id = '" + str(cluster_fk) + "'"
                    cursor.execute(select_spec_sql)
                    result = cursor.fetchone()
                    connection.commit()
                    cluster_ratio = result.get("cluster_ratio")
                    n_spec = result.get("n_spec")
                    cluster_props = [cluster_ratio, n_spec] 
                    cluster_props_dict.update( {cluster_fk:cluster_props})
                else:
                    cluster_props = cluster_props_dict.get(cluster_fk)
                    cluster_ratio = cluster_props[0]
                    n_spec = cluster_props[1]
                
                N_spec_clustered += 1
                if cluster_ratio >= 0.618:
                    N_spec_above_clus_ratio += 1
                else:
                    N_spec_lower_clus_ratio += 1
   finally:
        pass

    """
    cluster_dict = {}
    empty_spectra_list = [] 
    N_spec_clustered = 0
    N_spec_above_clus_ratio = 0
    N_spec_lower_clus_ratio = 0
    try:
        with connection.cursor() as cursor:
            cursor.execute(select_sql)
            results = cursor.fetchall()
            connection.commit()
            for result in results:
                cluster_id = result.get("cluster_id")
                cluster_ratio = result.get("cluster_ratio")
                spectrum_title = result.get("spectrum_title")
                spectra_list = cluster_dict.get(cluster_id, [])
                spectra_list.append(spectrum_title)
                cluster_dict.update({cluster_id: spectra_list})
                N_spec_clustered += 1
                if cluster_ratio >= 0.618:
                    N_spec_above_clus_ratio += 1
                else:
                    N_spec_lower_clus_ratio += 1
    """
    return (N_spec_clustered, N_spec_above_clus_ratio, N_spec_lower_clus_ratio)

def print_head():
    print("spectra:")
    line = "project_id\t" + "identified\t" + "clustered\t" +  "high_ratio\t" + \
            "low_ratio\t" + "clustered_percent\t" + "high_ratio_percent\t" + "low_ratio_percent"
    print(line)
    print("")

def main():
    table_name = "201504_3"
    connection = connect_to_db()
    projects = get_projects(table_name + "_projects", connection)
    print_head()
    for project_id in projects:
        N_peptides_identified = get_id_peptides_no(project_id)
        (N_spec_clustered, N_spec_above_clus_ratio, N_spec_lower_clus_ratio) = get_clusters(project_id, table_name, connection)
        print(project_id, end='\t\t')
        print(str(N_peptides_identified), end='\t\t')
        print(str(N_spec_clustered), end='\t\t')
        print(str(N_spec_above_clus_ratio), end='\t\t')
        print(str(N_spec_lower_clus_ratio), end='\t\t')
        if N_peptides_identified > 0:
            print("%5.2f"%(100 * N_spec_clustered/N_peptides_identified) + "%", end='\t\t')
        else:
            print("Null", end='\t\t')
        if N_spec_clustered > 0:
            print("%5.2f"%(100 * N_spec_above_clus_ratio/N_spec_clustered) + "%", end='\t\t')
            print("%5.2f"%(100 * N_spec_lower_clus_ratio/N_spec_clustered) + "%", end='\t\t')
        print('')
        break 
    return  
    
    print(projects)
    for cluster_id in clusters.keys():
        spectra = clusters[cluster_id]
        print('\n\n\n------------------------------')
        print('This cluster has ' + str(len(spectra)) + 'spectra')
        for spectrum in spectra:
            print(spectrum)

if __name__ == "__main__":
    main()
