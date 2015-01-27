import java.io.StringReader
import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import au.com.bytecode.opencsv.CSVParser

val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

def ArrondisDate(t:String,y:SimpleDateFormat):String ={
val tmp = y.parse(t);
val minutes= tmp.getMinutes();
tmp.setMinutes(minutes+10-minutes%10);
tmp.setSeconds(0);
return y.format(tmp);
}


val JapanData = sc.textFile("data_1MB.csv").cache()

def dropHeader(data: RDD[String]): RDD[String] = {
         data.mapPartitionsWithIndex((idx, lines) => {
           if (idx == 0) {
             lines.drop(1)
           }
           lines
         })
       }

val withoutHeader: RDD[String] = dropHeader(JapanData)

val result = withoutHeader.mapPartitions(lines => {
         val parser = new CSVParser(',')
         lines.map(line => {
           val columns = parser.parseLine(line)
           Array(ArrondisDate(columns(0),simpleDateFormat),columns(2).substring(0,3),columns(5),columns(3),columns(4)).mkString(",")
         })
       })


result.collect();

/*
val input = sc.textFile("data_1MB.csv")
val result = input.map{ line => val reader = new CSVReader(new StringReader(line));reader.readNext();}

val transform = result.map{x=> (ArrondisDate(x(0),simpleDateFormat),x(2).substring(0,3),x(5),x(4),x(5))}

*/
