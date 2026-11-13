"""Compile the PoC catalog against the actual Dataproc Spark runtime and upload its JAR."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import zipfile

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test06_build_catalog").getOrCreate()
jvm = spark._jvm
source = Path("BigQueryReadOnlyCatalog.java").resolve()
if not source.exists():
    from pyspark import SparkFiles
    source = Path(SparkFiles.get("BigQueryReadOnlyCatalog.java"))
with tempfile.TemporaryDirectory() as tmp:
    subprocess.run([
        os.path.join(os.environ["JAVA_HOME"], "bin", "javac"),
        "-classpath", jvm.java.lang.System.getProperty("java.class.path"),
        "-d", tmp, str(source),
    ], check=True)
    jar = Path(tmp) / "bigquery-readonly-catalog.jar"
    with zipfile.ZipFile(jar, "w", zipfile.ZIP_DEFLATED) as archive:
        for cls in Path(tmp).rglob("*.class"):
            archive.write(cls, cls.relative_to(tmp).as_posix())
    target = jvm.org.apache.hadoop.fs.Path(sys.argv[1])
    fs = target.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    fs.copyFromLocalFile(False, False, jvm.org.apache.hadoop.fs.Path(str(jar)), target)
    print("CATALOG_JAR=" + sys.argv[1], flush=True)
