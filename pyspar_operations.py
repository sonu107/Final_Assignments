def pyspark_code(file_name):
    import pandas as pd
    import os

    # Create data
    data = {'Name': ['John', 'Anna', 'Peter', 'Linda'],
        'Age': [28, 35, 42, 29],
        'City': ['New York', 'Paris', 'London', 'Sydney']}
    
    print("current user is:")
    print(os.system("whoami"))

    # Create DataFrame
    df = pd.DataFrame(data)
    print(df.head())

#    df_1 = pd.read_csv('/Users/sonushah/Desktop/aws-mwaa-local-runner/dags/output.csv')

#    print(df_1.head())

    print(file_name)
    return 'abcd'

print(pyspark_code('filename'))