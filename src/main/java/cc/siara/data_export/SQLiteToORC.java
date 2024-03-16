package cc.siara.data_export;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.OrcFile.EncodingStrategy;

import java.io.File;
import java.io.IOException;
import java.sql.*;

/**
 * Modifiedj example from
 * https://orc.apache.org/docs/core-java.html
 *
 * Created by mreyes on 6/5/16.
 */
public class SQLiteToORC {

     public static void main(String [ ] args) throws Exception {

        if (args.length == 0) {
            System.out.println("Usage: java -jar SqliteToORC <sqlite database> <table name> <storage types>");
            return;
        }

        // Load SQLite JDBC driver
        Class.forName("org.sqlite.JDBC");

        // JDBC connection URL
        String connectionUrl = "jdbc:sqlite:" + args[0];

        // SQL query to fetch data
        String query = "SELECT * FROM " + args[1];

        // Connect to SQLite database
        try (Connection connection = DriverManager.getConnection(connectionUrl);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Define schema with column encodings
            TypeDescription schema = TypeDescription.createStruct();
            for (int i = 1; i <= columnCount; i++) {
                //String enc = getEncoding(metaData.getColumnType(i));
                switch (metaData.getColumnType(i)) {
                  case Types.INTEGER:
                      schema.addField(metaData.getColumnName(i), TypeDescription.createInt());
                   break;
                  case Types.VARCHAR:
                      schema.addField(metaData.getColumnName(i), TypeDescription.createString());
                   break;
                  case Types.FLOAT:
                      schema.addField(metaData.getColumnName(i), TypeDescription.createFloat());
                   break;
              }
            }

            // Output ORC file path
            File file = new File("data.orc");
            file.delete();
            Path outputPath = new Path("data.orc");

            // Create ORC writer with specific encodings
            Configuration conf = new Configuration();
            OrcFile.WriterOptions options = OrcFile.writerOptions(conf)
                    .encodingStrategy(EncodingStrategy.SPEED)
                    .setSchema(schema);
            Writer writer = OrcFile.createWriter(outputPath, options);

            // Iterate over the result set and write data to ORC file
            VectorizedRowBatch batch = schema.createRowBatch();
            while (resultSet.next()) {
               int row = batch.size++;
               for (int i = 1; i <= columnCount; i++) {
                   switch (metaData.getColumnType(i)) {
                      case Types.INTEGER:
        				LongColumnVector intVector = (LongColumnVector) batch.cols[i - 1];
                        intVector.vector[row] = resultSet.getInt(i);
                        break;
                      case Types.VARCHAR:
                        BytesColumnVector stringVector = (BytesColumnVector) batch.cols[i - 1];
		        		String s = resultSet.getString(i);
				        if (s == null)
	                      stringVector.setVal(row, "".getBytes());
				        else
                          stringVector.setVal(row, s.getBytes());
                        break;
                      case Types.FLOAT:
                        DoubleColumnVector floatColumnVector = (DoubleColumnVector) batch.cols[i - 1];
                        floatColumnVector.vector[row] = resultSet.getFloat(i);
                        break;
                   }
		   }
               if (batch.size == batch.getMaxSize()) {
                   writer.addRowBatch(batch);
                   batch.reset();
               }
            }
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
            writer.close();
        }
    }

}
