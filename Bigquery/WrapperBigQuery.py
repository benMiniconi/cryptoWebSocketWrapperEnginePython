from google.cloud import bigquery
import pandas as pd
import pandas_gbq


def implicit():
    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
    # , SUM(number) as total_people
    # WHERE state = 'TX'
    # GROUP BY name, state
    # ORDER BY total_people DESC
    # LIMIT 20
    client = bigquery.Client()
    query = """
        SELECT * 
        FROM `heartbeat-001.crytpoQuotes.deribit`   
    """
    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    for row in query_job:
        # Row values can be accessed by field name or index.
        print("name={}".format(row))


def writeQuotes(jsonToWrite, plateforme):
    df = pd.DataFrame(jsonToWrite)
    job = pandas_gbq.to_gbq(df, "crytpoQuotes"+plateforme, project_id="heartbeat-001", if_exists="append")
    print(job)


#implicit()