import avro
import xmltodict
import msgpack
import yaml
import pickle
import json
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import data_pb2


serialization_data = dict(
        str='abcteststring+++124testtest' * 55,
        int=1111111111111111111,
        float=3.1415647377468212532,
        dict={"key": "value",
              "key2": "value2",
              "key3": "value3"},
        list=['a', 'b', 'c', 'list_data'] * 55
    )

xml_serialization_data = dict(root=dict(
        str='abcteststring+++124testtest' * 55,
        int=1111111111111111111,
        float=3.1415647377468212532,
        dict={"key": "value",
              "key2": "value2",
              "key3": "value3"},
        list=['a', 'b', 'c', 'list_data'] * 55
    ))
def pickle_serialization():
    with open("test.pickle", "wb") as f:
        pickle.dump(serialization_data, f)


def pickle_deserialization():
    with open("test.pickle", "rb") as f:
        pickle.load(f)

def avro_serialization():
    schema = avro.schema.parse(open("schema.avsc", "rb").read())
    writer = DataFileWriter(open("test.avro", "wb"), DatumWriter(), schema)
    writer.append(serialization_data)
    writer.close()

def avro_deserialization():
    DataFileReader(open("test.avro", "rb"), DatumReader())

def xml_serialization():
    with open("test.xml", "w") as f:
        xmltodict.unparse(xml_serialization_data, f)

def xml_deserialization():
    with open("test.xml", "r") as f:
        xmltodict.parse(''.join(f.readlines()))


def json_serialization():
    with open("test.json", "w") as f:
        json.dump(serialization_data, f)

def json_deserialization():
    with open("test.json", "r") as f:
        json.load(f)


def yml_serialization():
    with open("test.yml", "w") as f:
        yaml.dump(serialization_data, f)

def yml_deserialization():
    with open("test.yml", "r") as f:
        yaml.load(f, Loader=yaml.FullLoader)


def mpk_serialization():
    with open("test.mpk", "wb") as f:
        msgpack.dump(serialization_data, f, use_bin_type=True)


def mpk_deserialization():
    with open("test.mpk", "rb") as f:
        msgpack.load(f)

def proto_serialization():
    serialized = data_pb2.Data()
    serialized.str = serialization_data['str']
    serialized.int = serialization_data['int']
    serialized.float = serialization_data['float']
    serialized.dict.update(serialization_data['dict'])
    serialized.list.extend(serialization_data['list'])
    with open("test.proto", "wb") as f:
        f.write(serialized.SerializeToString())


def proto_deserialization():
    serialized = data_pb2.Data()
    with open("test.proto", "rb") as f:
        serialized.ParseFromString(f.read())

