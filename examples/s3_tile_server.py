import avro
import avro.io
import io
import numpy as np
import sys

from flask import Flask, make_response
from geopyspark.avroserializer import AvroSerializer
from PIL import Image
from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import AutoBatchedSerializer
from geopyspark.avroregistry import AvroRegistry


app = Flask(__name__)
app.reader = None

@app.route("/<int:zoom>/<int:x>/<int:y>.png")
def tile(x, y, zoom):
    # fetch tile

    tup = value_reader.readSpatialSingleband(layer_name, zoom, x, y)
    (serialized_tile, schema) = (tup._1(), tup._2())

    if app.reader == None:
        app.reader = avro.io.DatumReader(avro.schema.Parse(schema))

    tile_buffer = io.BytesIO(serialized_tile)
    decoder = avro.io.BinaryDecoder(tile_buffer)
    string_data = app.reader.read(decoder).get('cells')

    data = np.int16(string_data).reshape(256, 256)
    blank = np.empty((256, 256), dtype='int8')

    np.floor_divide(data, [256], blank)

    # display tile
    size = 256, 256

    bio = io.BytesIO()
    im = Image.fromarray(blank, mode='L').resize(size, Image.NEAREST)
    im.save(bio, 'PNG')

    response = make_response(bio.getvalue())
    response.headers['Content-Type'] = 'image/png'
    response.headers['Content-Disposition'] = 'filename=%d.png' % 0

    return response


if __name__ == "__main__":
    #bucket = "azavea-datahub"
    #root = "catalog"
    #layer_name = "nlcd-zoomed"

    bucket = "geopyspark-test"
    root = "srtm"
    layer_name = "srtm-test"

    sc = SparkContext(appName="s3-flask")
    store_factory = sc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreFactory
    store = store_factory.buildS3(bucket, root)
    value_reader_factory = sc._gateway.jvm.geopyspark.geotrellis.io.ValueReaderFactory
    value_reader = value_reader_factory.buildS3(store)

    '''
    z = 9
    x = 495
    y = 289

    output = "ouput_{}_{}_{}.tif".format(z, x, y)

    tup = value_reader.readSpatialSingleband(layer_name, z, x, y)
    (serialized_tile, schema) = (tup._1(), tup._2())
    app.reader = avro.io.DatumReader(avro.schema.Parse(schema))
    tile_buffer = io.BytesIO(serialized_tile)
    decoder = avro.io.BinaryDecoder(tile_buffer)
    string_data = app.reader.read(decoder).get('cells')

    data = np.int16(string_data).reshape(256, 256)
    blank = np.empty((256, 256), dtype='int8')
    np.floor_divide(data, [256], blank)

    size = 128, 128

    bio = io.BytesIO()
    im = Image.fromarray(blank).resize(size, Image.NEAREST)
    im.save(output)
    '''

    app.run()
