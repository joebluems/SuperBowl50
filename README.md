# SuperBowl50
Read NFL game data into Spark and try to predict the over/under for the big game

### Processing the data yourself (optional)...



### Launching Spark shell 

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
