/* reading the csv file using '|' as delimiter */
val caseDF = spark.read.option("header","true").option("delimiter", "|").csv("C:/Users/Giray/Desktop/case.csv")

/* checking dataframe schema with column names and types, checking top 5 rows*/
caseDF.printSchema
caseDF.show(5)

/* changing dataframe as view to run queries */
caseDF.createTempView("UserActivities")

/* answer of 1st question */
val output1 = spark.sql( "SELECT productId||'|'||count(*) as ProductViewCounts from UserActivities where eventName = 'view' group by productId" )
output1.show()

/* answer of 2nd question */
val output2 = spark.sql( "SELECT eventName||'|'||count(*) as EventCounts from UserActivities group by eventName")
output2.show()

/* answer of 3rd question */
val output3 = spark.sql( "SELECT distinct userId FROM (SELECT userId, eventName, count(*) from UserActivities group by userId, eventName having count(*) > 3) LIMIT 5")
output3.show()

/* answer of 4th question */
val output4 = spark.sql("SELECT eventName||'|'||count(*) as user47Events from UserActivities where userId = '47' group by eventName")
output4.show()

/* answer of 5th question */
val output5 = spark.sql("SELECT distinct productId from UserActivities where userId = '47' and eventName = 'view'")
output5.show()

/* writing output dataframes as text files */
val out1= output1.write.format("text").option("header","false").save("C:/Users/Giray/Desktop/output1.txt")
val out2= output2.write.format("text").option("header","false").save("C:/Users/Giray/Desktop/output2.txt")
val out3= output3.write.format("text").option("header","false").save("C:/Users/Giray/Desktop/output3.txt")
val out4= output4.write.format("text").option("header","false").save("C:/Users/Giray/Desktop/output4.txt")
val out5= output5.write.format("text").option("header","false").save("C:/Users/Giray/Desktop/output5.txt")
