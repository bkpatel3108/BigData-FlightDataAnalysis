# .bashrc

#unzip ~/projects/FlightDataAnalysis.zip -d ~/projects
#$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main ~/projects/FlightDataAnalysis/src/main/java/FlightDataAnalysisMain.java
#jar cf FlightDataAnalysis.jar FlightDataAnalysisMain.class

#sh ~/projects/get_flight_data.sh
#$HADOOP_HOME/bin/hdfs dfs -mkdir /user
#$HADOOP_HOME/bin/hdfs dfs -mkdir /user/ec2-user
#$HADOOP_HOME/bin/hdfs dfs -mkdir /user/ec2-user/tmp

cd ~/projects/data
year=(1989 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008)  
year=(1990)

for y in ${year[@]}; do  

  start=$(date +%s)
  
  $HADOOP_HOME/bin/hdfs dfs -rm -R /user/ec2-user/out*
  $HADOOP_HOME/bin/hdfs dfs -rm -R /tmp
  
  $HADOOP_HOME/bin/hdfs dfs -put ~/projects/data/$y.csv input
  
  $HADOOP_HOME/bin/hadoop jar ~/projects/FlightDataAnalysis.jar FlightDataAnalysisMain input out

  duration=$(expr $(date +%s) - $start)
  echo "Time elapsed for benchmarking #1 : $(($duration / 60)) minutes and $(($duration % 60)) seconds." >> ~/projects/benchmarking

  answer=(AirlinesBeingOnSchedule AirportTaxiInTime_Max AirportTaxiInTime_Min AirportTaxiOutTime_Max AirportTaxiOutTime_Min FlightCancellationMostCommonReason)
  for i in ${answer[@]}; do
#  echo y$y.$i 
  $HADOOP_HOME/bin/hdfs dfs -get out_$i/part-r-00000 ~/projects/results/y$y.$i 
  done
    
done

exit 0