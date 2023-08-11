import com.bdec.training.javasparkl2.DBSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;

public class DBSaprkTest {

    private static SparkSession spark;

    @BeforeClass
    public static void setUp() {
        spark = SparkSession
                .builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();
    }


    @Test
   public  void testcalculateTotalProfitAndTotalQuantity() {

        List<Row> rows = Arrays.asList(
                RowFactory.create("CA-2014-AB10015140-41954",   "11/11/2014",    "First Class", "Consumer", "Oklahoma City", "Oklahoma", "United States", "Technology", "Phones", "$221.98", 2, 0.0, "$62.15", 40.77, "No"),
                RowFactory.create("IN-2014-JR162107-41675",     "2/5/2014",      "Second Class", "Corporate", "Wollongong", "New South Wales", "Australia", "Furniture", "Chairs", "$3,709.40", 9, 0.1, "-$288.77", 923.63, "No"),
                RowFactory.create("IN-2013-TH2155027-41409",    "5/15/2013",     "Same Day", "Home Office", "Shuangyashan", "Heilongjiang", "China", "Office Supplies", "Envelopes", "$61.08", 2, 0.0, "$26.22", 1.11, "No"),
                RowFactory.create("NI-2014-VT1170095-41908",    "9/26/2014",     "Standard Class", "Home Office", "Kaduna", "Kaduna", "Nigeria", "Office Supplies", "Paper", "$4.46", 1, 0.7, "-$7.15", 1.11, "No"),
                RowFactory.create("MX-2013-AF1087051-41489",    "8/3/2013",     "Standard Class", "Consumer", "Mixco", "Guatemala", "Guatemala", "Office Supplies", "Fasteners", "$18.80", 2, 0.0, "$0.16", 1.109, "No"),
                RowFactory.create("US-2015-SG2008054-42366",    "12/28/2015",    "Standard Class", "Consumer", "Carrefour", "Ouest", "Haiti", "Technology", "Accessories", "$31.85", 2, 0.4, "-$6.39", 1.109, "No")
        );


        List<Row> returnsRows = Arrays.asList(
                RowFactory.create("CA-2014-AB10015140-41954","Yes"),
                RowFactory.create("MX-2013-AF1087051-41489","Yes")
        );

        StructType schema = DataTypes.createStructType(
                new StructField[]{

                        DataTypes.createStructField("Order ID", DataTypes.StringType, false),
                        DataTypes.createStructField("Order Date", DataTypes.StringType, false),
                        DataTypes.createStructField("Ship Mode", DataTypes.StringType, false),
                        DataTypes.createStructField("Segment", DataTypes.StringType, false),
                        DataTypes.createStructField("City", DataTypes.StringType, false),
                        DataTypes.createStructField("State", DataTypes.StringType, false),
                        DataTypes.createStructField("Country", DataTypes.StringType, false),
                        DataTypes.createStructField("Category", DataTypes.StringType, false),
                        DataTypes.createStructField("Sub-Category", DataTypes.StringType, false),
                        DataTypes.createStructField("Sales", DataTypes.StringType, false),
                        DataTypes.createStructField("Quantity", DataTypes.IntegerType, false),
                        DataTypes.createStructField("Discount", DataTypes.DoubleType, false),
                        DataTypes.createStructField("Profit", DataTypes.StringType, false),
                        DataTypes.createStructField("Shipping Cost", DataTypes.DoubleType, false),
                        DataTypes.createStructField("Returns", DataTypes.StringType, false)
                });


        StructType returnSchema = DataTypes.createStructType(
                new StructField[]{

                        DataTypes.createStructField("Order ID", DataTypes.StringType, false),
                        DataTypes.createStructField("Returned", DataTypes.StringType, false)
                });
        final Dataset<Row> salesDataSet = spark.createDataFrame(rows, schema);
        final Dataset<Row> returnsDataSet = spark.createDataFrame(returnsRows, returnSchema);

        final Dataset<Row> finalReturnsDataSet  = DBSpark.calculateTotalProfitAndTotalQuantity(salesDataSet,returnsDataSet);
        Assert.assertEquals(26.22,finalReturnsDataSet.select("Total Profit").first().get(0));
    }
}
