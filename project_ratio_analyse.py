import pymysql.cursors
import urllib3



get_id_peptides_url_pre = "http://www.ebi.ac.uk:80/pride/ws/archive/peptide/count/project/"

http = urllib3.PoolManager()

def get_id_peptides_no(project_id):

    get_id_peptides_url = get_id_peptides_url_pre + project_id
    response = http.request('GET', get_id_peptides_url)
    if response.status == 200:
        return int(response.data)
    else:
        raise Exception("Failed to get the number of identified peptides for project !", project_id)




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
    projects = []
    try:
        with connection.cursor() as cursor:
            cursor.execute(select_sql)
            results = cursor.fetchall()
            connection.commit()
            for result in results:
                project_id = result.get("project_id")
                projects.append(project_id)
    finally:
        pass

    return projects

def get_clusters(project_id, connection):
    #only get the identified spec
    select_sql = "SELECT cluster_id, cluster_ratio, spectrum_title FROM `201504_test` WHERE " + \
                 "spec_prj_id = '" + project_id + "' AND " +\
                 "is_spec_identified = 1" 
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
    finally:
        pass
    return (N_spec_clustered, N_spec_above_clus_ratio, N_spec_lower_clus_ratio, cluster_dict)

def print_head():
    print("spectra:")
    line = "project_id\t" + "identified\t" + "clustered\t" +  "high_ratio\t" + "high_ratio_percent\t" \
            + "low_ratio\t" + "low_ratio_percent"
    print(line)
    print("")

def main():
    table_name = "201504_test"
    connection = connect_to_db()
    projects = get_projects(table_name + "_projects", connection)
    
    print_head()
    for project_id in projects:
        N_peptides_identified = get_id_peptides_no(project_id)
        (N_spec_clustered, N_spec_above_clus_ratio, N_spec_lower_clus_ratio, clusters) = get_clusters(project_id, connection)
        print(project_id, end='\t')
        print(str(N_peptides_identified), end='\t')
        print(str(N_spec_clustered), end='\t')
        print(str(N_spec_above_clus_ratio), end='\t')
        print("%5.2f"%(100 * N_spec_above_clus_ratio/N_spec_clustered) + "%", end='\t')
        print(str(N_spec_lower_clus_ratio), end='\t')
        print("%5.2f"%(100 * N_spec_lower_clus_ratio/N_spec_clustered) + "%", end='\t')
        print('')
        return
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
