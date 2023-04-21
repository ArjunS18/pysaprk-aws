import os

# Set Environment Variables
#print(os.environ['PYSPARK_PYTHON'])
os.environ['envn'] = 'TEST'  #as we are working in test environment, set the value to TEST
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

# Get Environment Variables
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

# Set Other Variables
appName = "USA Prescriber Research Report"
current_path = os.getcwd()
print(current_path)

staging_dim_city = r'C:\Users\ismar\PycharmProjects\pyspark\src\python\staging\dimension_city'
staging_fact = r'C:\Users\ismar\PycharmProjects\pyspark\src\python\staging\fact'

#print(current_path)






