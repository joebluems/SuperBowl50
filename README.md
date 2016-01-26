# SuperBowl50
Read NFL game data into Spark and try to predict the over/under for the big game

### Processing the data yourself (optional)...


### Launching Spark shell & loading the data into an RDD
Note: I used spark version 1.5.2 for this analysis. Download Spark here: http://spark.apache.org/downloads.html<br>
First - start Spark shell, load imports and read the raw data into an RDD<br>
linux> /Users/jblue/spark-1.5.2-bin-hadoop2.4<br>
linux> ./bin/spark-shell<br>
<b>import scala.math<br>
import scala.util.Try<br>
val rawData = sc.textFile("seasons1990_2015")<br>
val data = rawData.map(line => line.split('|').map(elem => elem.trim))<br>
data.first()<br>
</b><br>
Next - create a function to parse the raw game data into a new RDD and cache:<br>
<b>def parseData(parts:Array[String]):(String, Double, Double, Double, Double, Double, Double, Double, Double, Double,String) = {<br>
  var time = 0.0<br>
  var regex = ":.\*$".r <br>
  if (regex.replaceAllIn(parts(1),"").toDouble>2 && parts(1).endsWith("pm")) { time = 1.0; }<br>
  var roof = 0.0<br>
  if (parts(2).matches(".\*dome.\*") || parts(2).matches(".\*closed.\*")) { roof = 1.0; }<br>
  var field = 0.0<br>
  if (parts(3).matches("grass.*")) { field = 1.0; }<br>
  var temperature = -99.0<br>
  var humidity = -99.0<br>
  var wind = -99.0<br>
  val weather = ",".r.replaceAllIn(parts(4),"").split(" ")<br>
  for(i <- 0 until weather.length) {<br>
        if (weather(i).matches("degrees")) { temperature = weather(i-1).toDouble;}<br>
        if (weather(i).matches("humidity")) { humidity = "%".r.replaceAllIn(weather(i+1),"").toDouble;}<br>
        if (weather(i).matches("mph")) { wind = weather(i-1).toDouble;}        <br>
        if (weather(i).matches("chill")) { temperature = weather(i+1).toDouble;}<br>
  }<br>
  val spread = {try{scala.math.abs(parts(5).toDouble)} catch {case e: Exception => -99.0 }}<br>
  val overunder = parts(6).toDouble<br>
  val points = parts(7).toDouble<br>
  (parts(0),time,roof,field,temperature,humidity,wind,spread,overunder,points,parts(8))<br>
}<br>
val rawFeatures = data.map{ parts=><br>
  parseData(parts)<br>
}<br>
rawFeatures.cache<br>
</b><br>
### Exploring the data
Run these shell commands to explore the data:<br>
Find lowest-scoring games:<br>
<b>rawFeatures.map{game=> (game.\_1,game.\_10) }.sortBy(_._2).take(5).foreach(println)<br></b>
Find highest-scoring games:<br>
<b>rawFeatures.map{game=> (game.\_1,game.\_10) }.sortBy(\_.\_2,false).take(5).foreach(println)<br></b>
Find games with the largest spread:<br>
<b>rawFeatures.map{game=> (game.\_1,game.\_8) }.sortBy(\_.\_2,false).take(5).foreach(println)<br></b>
Find the number of games where the score was over,under or the same:<br>
<b>rawFeatures.map{game=> (game.\_1,game.\_10-game.\_9)}.filter(m=> m.\_2 > 0).count<br>
rawFeatures.map{game=> (game.\_1,game.\_10-game.\_9)}.filter(m=> m.\_2 < 0).count<br>
rawFeatures.map{game=> (game.\_1,game.\_10-game.\_9)}.filter(m=> m.\_2 == 0).count<br>
</b><br>
### Running the KNN algorithm
Note: you need these statistics to standardize some of the features:<br>
<b>val tempStats = rawFeatures.filter(m=> m.\_5 > -99.0).map{ p => (p._5) }.stats<br>
val windStats = rawFeatures.filter(m=> m.\_7 > -99.0).map{ p => (p._7) }.stats<br>
val spreadStats = rawFeatures.filter(m=> m.\_8 > -99.0).map{ p => (p._8) }.stats<br>
val overStats = rawFeatures.map{ p => (p._9) }.stats<br>
</b><br>
Note: edit this string to change the forecast for the Superbowl<br>
<b>val superString = "Super Bowl 50 - February 7th, 2016 |6:30pm|outdoors|grass |60 degrees relative humidity 72%, wind 10 mph|4.5|45.0 |00|2015"<br>
var parsedSuper = parseData(superString.split('|').map(elem => elem.trim))<br>
</b><br>
Use these shell commands to calculate the 50 most-similar games:<br>
<b>def standardize(measure:Double,stats:org.apache.spark.util.StatCounter):Double = {<br>
   ((measure - stats.min) / (stats.max - stats.min))<br>
}<br>
<br>
def calcDistance(game:(String, Double, Double, Double, Double, Double, Double, Double, Double, Double, String),
        test:(String, Double, Double, Double, Double, Double, Double, Double, Double, Double,String)):(Double) = {<br>
   var distance = 0.0<br>
   //(parts(0),time,roof,field,temperature,humidity,wind,spread,overunder,points)<br>
   distance += math.pow(game.\_2-test.\_2,2)<br>
   distance += math.pow(game.\_3-test.\_3,2)<br>
   distance += math.pow(game.\_4-test.\_4,2)<br>
   distance += math.pow(standardize(game.\_5,tempStats)-standardize(test.\_5,tempStats),2)<br>
   distance += math.pow(game.\_6/100-test._6/100,2)<br>
   distance += math.pow(standardize(game.\_7,windStats)-standardize(test.\_7,windStats),2)<br>
   distance += math.pow(standardize(game.\_8,spreadStats)-standardize(test.\_8,spreadStats),2)<br>
   distance += math.pow(standardize(game.\_9,overStats)-standardize(test.\_9,overStats),2)<br>
   (distance)<br>
}<br>
val distances = rawFeatures.filter(m=> m.\_5 > -99.0 && m.\_6> -99.0 && m.\_7> -99.0 && m.\_8> -99.0 && m.\_11.toDouble >2001).map{ game =><br>
   (game.\_1,calcDistance(game,parsedSuper),game._10)<br>
}<br>
distances.sortBy(\_.\_2).take(100).foreach(println)<br>
</b><br>
### Creating the graphs in R Studio
Note: These are the R commands I used to create the plot of points by season. It assumes your season data is located in a file called <b>stats</b><br>
<b>library("colorspace")<br>
seasons <- read.csv("stats",stringsAsFactors=FALSE,header=FALSE)<br>
colnames(seasons) <- c("season","overunder","points")<br>
attach(seasons)<br>

pcol <- c("red","blue","forestgreen")<br>
yl=c(30,50)<br>
plot(season,overunder,ylim=yl,main="NFL Scoring by Season", xlab="Seasons", ylab="Points per Game",
  col=pcol[1],pch=20,cex=1.5,type='b')<br>
  points(season,points,col=pcol[2],pch=20,cex=1.5,type='b')<br>
  legend(2005,35,c("avg. over/under","avg. actual"),pch=c(20,20),col=c(pcol[1],pcol[2]))<br>
</b><br>

Note: These are the R commands I used to create the histogram of points in similar games. It assumes your nearest neighbor output is located in a file called <b>knn</b><br>
<b>neighbors <- read.csv("knn",stringsAsFactors=FALSE,header=FALSE)<br>
colnames(neighbors) <- c("game","year","distance","points")<br>
attach(neighbors)<br>
medianPoints <- median(points)<br>
hist(points, main="Distribution of Points in Similar Games since 2002",xlab="Points Scored",ylab="Number of Games", 
   ylim=c(0,20),breaks=20)<br>
abline(v=c(45),lty=2,lwd=8,col="red")<br>
abline(v=c(medianPoints),lty=2,lwd=8,col="green")<br>
legend(10,17,c("SuperBowl Over/Under","Median Points, Similar Games"),pch=c(20,20),col=c("red","green"))<br>
