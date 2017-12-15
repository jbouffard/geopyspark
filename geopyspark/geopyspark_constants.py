"""GeoPySpark package constants."""
from os import path

"""GeoPySpark version."""
VERSION = '0.3.0'

"""Backend jar name."""
GEOTRELLIS_JAR = 'geotrellis-backend-assembly-' + VERSION + '.jar'

VECTORPIPE_JAR = 'vectorpipe-' + VERSION + '.jar'

"""The current location of this file."""
CWD = path.abspath(path.dirname(__file__))
