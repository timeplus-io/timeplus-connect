import timeplus_connect

if __name__ == '__main__':
    client = timeplus_connect.get_client()
    query = 'SELECT number, to_string(number) AS number_as_str FROM system.numbers LIMIT 5'
    fmt = 'CSVWithNames'  # or any other format, see https://clickhouse.com/docs/en/interfaces/formats
    stream = client.raw_stream(query=query, fmt=fmt)
    with open("output.csv", "wb") as f:
        for chunk in stream:
            f.write(chunk)
