import pysolr
import csv

solr_url = 'http://localhost:8983/solr/Employee'  
solr = pysolr.Solr(solr_url, always_commit=True)

v_nameCollection = 'Hash_YourName'  
v_phoneCollection = 'Hash_1234'  

def createCollection(collection_name):
    try:
        print(f"Collection '{collection_name}' is ready to use (ensure it exists in Solr).")
    except Exception as e:
        print(f"Error creating collection: {e}")
        
def getEmpCount(collection_name):
    try:
        results = solr.search('*:*', **{'facet': 'false', 'rows': 0})
        count = results.hits
        print(f"Employee count in '{collection_name}': {count}")
        print(" ")
        return count
    except Exception as e:
        print(f"Error fetching employee count: {e}")

def indexData(collection_name, column_name):
    try:
        with open('Employee Sample Data 1.csv', newline='', encoding='ISO-8859-1') as csvfile:
            reader = csv.DictReader(csvfile)
            documents = []
            for row in reader:
                document = {
                    'id': row['Employee ID'],  
                    'name': row['Name'],
                    'age': row['Age'],
                    'department': row['Department'],  
                    'gender': row['Gender'],  
                    'country': row['Country']  
                }
                documents.append(document)

            
            solr.add(documents)
            print(f"Data indexed in '{collection_name}' for column '{column_name}'.")
    except Exception as e:
        print(f"Error indexing data: {e}")
        print(" ")

def delEmpById(collection_name, employee_id):
    try:
        solr.delete(id=employee_id)
        print(f"Employee with ID '{employee_id}' deleted from '{collection_name}'.")
        print(" ")
    except Exception as e:
        print(f"Error deleting employee by ID: {e}")

def searchByColumn(collection_name, column_name, value):
    try:
        query = f"{column_name}:{value}"
        results = solr.search(query)
        print(f"Searched in '{collection_name}' by '{column_name}' for value '{value}':")
        print(" ")
        for result in results:
            print(result)
        print(" ")
    except Exception as e:
        print(f"Error searching by column: {e}")

def getDepFacet(collection_name):
    try:
        facet_query = {
            'q': '*:*',
            'facet': 'true',
            'facet.field': 'Department',
            'rows': 0,
            'wt': 'json'
        }
        results = solr.search(**facet_query)

        if 'facet_fields' in results.facets and 'Department' in results.facets['facet_fields']:
            facet_counts = results.facets['facet_fields']['Department']
            print("Employee count by department:")
            for i in range(0, len(facet_counts), 2):
                print(f"{facet_counts[i]}: {facet_counts[i + 1]} employees")
                print(" ")
        else:
            print(f"No facets found for the 'Department' field in '{collection_name}'.")
    except Exception as e:
        print(f"Error retrieving department facets: {e}")


createCollection(v_nameCollection)
createCollection(v_phoneCollection)
getEmpCount(v_nameCollection)
indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')
delEmpById(v_nameCollection, 'E02003')
getEmpCount(v_nameCollection)
searchByColumn(v_nameCollection, 'Department', 'IT')
searchByColumn(v_nameCollection, 'Gender', 'Male')
searchByColumn(v_phoneCollection, 'Department', 'IT')
getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)
