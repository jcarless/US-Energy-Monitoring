import io
import gzip
import json


def zip_json(data):
    print("Zipping data...")
    try:
        gz_body = io.BytesIO()
        gz = gzip.GzipFile(None, 'wb', 9, gz_body)
        gz.write(json.dumps(data).encode('utf-8'))
        gz.close()
        return gz_body.getvalue()

    except BaseException as e:
        print("Zip failed!")
        raise e
