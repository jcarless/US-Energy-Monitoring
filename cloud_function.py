from google.cloud import bigquery
def load_to_bigquery(data, context):
    print("Starting Job...")
    sperator = '/'
    data_split = data["name"].split(sperator)
    client = bigquery.Client()
    if data_split[0] == "traffic":
        print("Processing Traffic Folder...")
        dataset_id = "real_time_traffic"
        dataset_ref = client.dataset(dataset_id)
        job_config = bigquery.LoadJobConfig()
        if data_split[1] == "weather":
            print("Processing Weather File...")
            table = "weather_current"
            job_config.schema = [
                bigquery.SchemaField("ts", "TIMESTAMP", "REQUIRED"),
                bigquery.SchemaField("timezone", "STRING", "REQUIRED"),
                bigquery.SchemaField("location", "GEOGRAPHY", "REQUIRED"),
                bigquery.SchemaField("summary", "STRING"),
                bigquery.SchemaField("nearest_storm_distance", "FLOAT"),
                bigquery.SchemaField("precip_intensity", "FLOAT"),
                bigquery.SchemaField("precip_intensity_error", "FLOAT"),
                bigquery.SchemaField("precip_prob", "FLOAT"),
                bigquery.SchemaField("precip_type", "STRING"),
                bigquery.SchemaField("apparent_temp", "FLOAT"),
                bigquery.SchemaField("temp", "FLOAT"),
                bigquery.SchemaField("wind_speed", "FLOAT"),
                bigquery.SchemaField("wind_gust", "FLOAT"),
                bigquery.SchemaField("cloud_cover", "FLOAT"),
                bigquery.SchemaField("uv_index", "INTEGER"),
                bigquery.SchemaField("visibility", "FLOAT")
            ]
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            path = sperator.join([data_split[0], data_split[1], data_split[2], data_split[3], data_split[4]])
            uri = f"gs://real-time-traffic/{path}"
            load_job = client.load_table_from_uri(
                uri, dataset_ref.table(table), job_config=job_config
            )  # API request
            print("Starting job {}".format(load_job.job_id))
            load_job.result()  # Waits for table load to complete.
            print("Job finished.")
            destination_table = client.get_table(dataset_ref.table(table))
            print("Loaded {} rows.".format(destination_table.num_rows))

        if data_split[1] == "incident_details":
            print("Processing Incedent Details File...")
            table = "incident_details"
            job_config.schema = [
                bigquery.SchemaField("traffic_model_id", "INTEGER", "REQUIRED"),
                bigquery.SchemaField("incident_id", "STRING", "REQUIRED"),
                bigquery.SchemaField("ts", "TIMESTAMP", "REQUIRED"),
                bigquery.SchemaField("location", "GEOGRAPHY", "REQUIRED"),
                bigquery.SchemaField("category", "STRING", "REQUIRED"),
                bigquery.SchemaField("magnitude", "STRING", "REQUIRED"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("estimated_end", "TIMESTAMP"),
                bigquery.SchemaField("cause", "STRING"),
                bigquery.SchemaField("from_street", "STRING"),
                bigquery.SchemaField("to_street", "STRING"),
                bigquery.SchemaField("length", "INTEGER"),
                bigquery.SchemaField("delay", "INTEGER"),
                bigquery.SchemaField("road", "STRING")
            ]
            job_config.skip_leading_rows = 1
            job_config.source_format = bigquery.SourceFormat.CSV
            path = sperator.join([data_split[0], data_split[1], data_split[2]])
            uri = f"gs://real-time-traffic/{path}"
            load_job = client.load_table_from_uri(
                uri, dataset_ref.table(table), job_config=job_config
            )  # API request
            print("Starting job {}".format(load_job.job_id))
            load_job.result()  # Waits for table load to complete.
            print("Job finished.")
            destination_table = client.get_table(dataset_ref.table(table))
            print("Loaded {} rows.".format(destination_table.num_rows))


if __name__ == "__main__":


    # data="real-time-traffic/traffic/weather/New_York/current/2020-01-25T19:40:00.jsonl.gz/1579981203567911"
    # sperator = '/'
    # data_split = data.split(sperator)
    # path = sperator.join([data_split[1], data_split[2], data_split[3], data_split[4], data_split[5]])
    # print(data_split)
    # print("BUCKET: ", data_split[0])
    # print("PATH: ", path)

    json = {
        "test": 'me'
    }
