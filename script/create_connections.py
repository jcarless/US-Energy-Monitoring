import subprocess
import json


def main():
    with open('credentials/connections.json', 'r') as infile:
        connections = json.load(infile)

    airflow_call = ['airflow', 'connections', '--add', '--conn_id']

    for connection, parameters in connections.items():
        additional_params = list()
        for param, value in parameters.items():
            additional_params.append(f'--{param}')
            additional_params.append(value)
        call = airflow_call + [connection] + additional_params
        subprocess.call(call)


if __name__ == "__main__":
    main()
