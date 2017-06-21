"""A wrapper for ``SparkContext`` that provides extra functionality for GeoPySpark."""
from geopyspark.geotrellis.protobufserializer import ProtoBufSerializer
from geopyspark.geopyspark_utils import check_environment
import geopyspark.geotrellis.converters

check_environment()

from pyspark import RDD, SparkContext

from py4j.java_gateway import java_import

class GeoPyContext(object):
    """A wrapper of ``SparkContext``.
    This wrapper provides extra functionality by providing methods that help with sending/recieving
    information to/from python.

    Args:
        pysc (pypspark.SparkContext, optional): An existing ``SparkContext``.
        **kwargs: ``GeoPyContext`` can create a ``SparkContext`` if given its constructing
            arguments.

    Note:
        If both ``pysc`` and ``kwargs`` are set the ``pysc`` will be used.

    Attributes:
        pysc (pyspark.SparkContext): The wrapped ``SparkContext``.
        sc (org.apache.spark.SparkContext): The scala ``SparkContext`` derived from the python one.

    Raises:
        TypeError: If neither a ``SparkContext`` or its constructing arguments are given.

    Examples:
        Creating ``GeoPyContext`` from an existing ``SparkContext``.

        >>> sc = SparkContext(appName="example", master="local[*]")
        >>> SparkContext
        >>> geopysc = GeoPyContext(sc)
        >>> GeoPyContext

        Creating ``GeoPyContext`` from the constructing arguments of ``SparkContext``.

        >>> geopysc = GeoPyContext(appName="example", master="local[*]")
        >>> GeoPyContext

    """

    def __init__(self, pysc=None, **kwargs):
        if pysc:
            self.pysc = pysc
        elif kwargs:
            self.pysc = SparkContext(**kwargs)
        else:
            raise TypeError(("Either a SparkContext or its constructing"
                             " parameters must be given,"
                             " but none were found"))

        self.sc = self.pysc._jsc.sc()
        self._jvm = self.pysc._gateway.jvm
        self.pysc._gateway.start_callback_server()

        java_import(self._jvm, 'geopyspark.geotrellis.SpatialTiledRasterRDD')
