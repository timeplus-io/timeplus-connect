import timeplus_connect


def main():
    print(f'\nTimeplus Connect installed version: {timeplus_connect.version()}')
    client = timeplus_connect.get_client(host='play.clickhouse.com',
                                         username='play',
                                         password='clickhouse',
                                         port=443)
    print(f'Timeplus Play current version and timezone: {client.server_version} ({client.server_tz})')
    result = client.query('SHOW DATABASES')
    print('Timeplus play Databases:')
    for row in result.result_set:
        print(f'  {row[0]}')
    client.close()


if __name__ == '__main__':
    main()
