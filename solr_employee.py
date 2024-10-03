from SolrClient import SolrClient
import csv


solr = SolrClient('http://localhost:8983/solr')


def indexData(p_collection_name, p_exclude_column):
    with open('Employee Sample Data 1.csv', newline='', encoding='ISO-8859-1') as csvfile:

        reader = csv.DictReader(csvfile)
        for row in reader:
            if p_exclude_column in row:
                del row[p_exclude_column]  
            solr.index(p_collection_name, [row])
    solr.commit(p_collection_name)
    print(f"Data indexed into collection: {p_collection_name}")


def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = f"{p_column_name}:{p_column_value}"
    results = solr.query(p_collection_name, {'q': query, 'wt': 'json'})
    print(f"Search results for {p_column_name} = {p_column_value}:")
    for doc in results.docs:
        print(doc)


def getEmpCount(p_collection_name):
    results = solr.query(p_collection_name, {'q': '*:*', 'rows': 0, 'wt': 'json'})
    print(f"Total number of employees in the collection: {results.get_num_found()}")


def delEmpById(p_collection_name, p_employee_id):
    solr.delete_doc_by_id(p_collection_name, p_employee_id)  
    solr.commit(p_collection_name)
    print(f"Employee with ID {p_employee_id} has been deleted from collection: {p_collection_name}")


def getDepFacet(p_collection_name):
    facet_query = {
        'q': '*:*',
        'facet': 'true',
        'facet.field': 'Country',  
        'rows': 0,
        'wt': 'json'
    }
    
    
    results = solr.query(p_collection_name, facet_query)
    
    
    if 'facet_fields' in results.get_facets() and 'Country' in results.get_facets()['facet_fields']:
        facet_counts = results.get_facets()['facet_fields']['Country']
        print("Employee count by department:")
        for i in range(0, len(facet_counts), 2):
            print(f"{facet_counts[i]}: {facet_counts[i + 1]} employees")
    else:
        print("No facets found for the 'Country' field or field is missing.")


if __name__ == "__main__":
    collection_name = 'Employee'  
    indexData(collection_name, 'Age')
    searchByColumn(collection_name, 'Gender', 'Male')
    getEmpCount(collection_name)
    delEmpById(collection_name, 'E02002') 
    getDepFacet(collection_name)

