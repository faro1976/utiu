package it.utiu.bioinf
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.DenseVector

object PCA2 {
def main(args: Array[String]): Unit = {
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors

val data = Array(
  Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
  Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
  Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
)

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("PCAExample")
      .getOrCreate()

val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
df.show()
val pca = new PCA()
  .setInputCol("features")
  .setOutputCol("pcaFeatures")
  .setK(3)
  .fit(df)

val result = pca.transform(df).select("pcaFeatures")
result.show(false)
println(pca.transform(df).select("pcaFeatures").first().get(0).asInstanceOf[DenseVector].size)

}  
}