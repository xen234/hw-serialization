from flask import Flask, request
from timeit import timeit, default_timer
import os
from main import xml_serialization, xml_deserialization, avro_serialization, avro_deserialization, json_serialization, json_deserialization, mpk_serialization, mpk_deserialization, yml_serialization, yml_deserialization, pickle_serialization, pickle_deserialization, proto_serialization, proto_deserialization

app = Flask(__name__)

serialization = {"xml": (xml_serialization, xml_deserialization),
                 "avro": (avro_serialization, avro_deserialization),
                 "json": (json_serialization, json_deserialization),
                 "mpk": (mpk_serialization, mpk_deserialization),
                 "yml": (yml_serialization, yml_deserialization),
                 "pickle": (pickle_serialization, pickle_deserialization),
                 "proto": (proto_serialization, proto_deserialization)}

@app.route('/get_result/<stype>', methods=['GET', 'POST'])
def get_result(stype: str):
    if stype not in serialization.keys():
        return "Unknown type"

    serialization_func, deserialization_func = serialization[stype]

    s_time = round(timeit(serialization_func, default_timer, number=100), 3)
    d_time = round(timeit(deserialization_func, default_timer, number=100), 3)

    size = os.path.getsize(f'test.{stype}')

    result = f"<p><strong>{stype}</strong>"\
             f"<p><strong>Size:</strong> {size}</p>" \
             f"<p><strong>Serialization:</strong> {s_time}ms</p>" \
             f"<p><strong>Deserialization:</strong> {d_time}ms</p>"

    return result


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=800)