#!/usr/bin/env python
from dbconnection import DBConnection
import datetime
import time
import cv2
import numpy as np
import time
from classify import Classify
import os
from shutil import copyfile

class PredictAccident:
    def predict_accident(self):
        insert_into_DB = 1
        db = DBConnection()
        conn = db.get_connection()
        mycursor = conn.cursor()
        mycursor.execute("SELECT path FROM buffer")
        buffer_items = mycursor.fetchall()
        for path_row in buffer_items:
            path = path_row[0]
            clf = Classify(path)
            class_name, percentage = clf.classify_image()
            if (class_name[0] is 'a' or class_name[0] is 'A') and (insert_into_DB is 1):
                insert_into_DB = 0
                print('accident detected')
                Camera_id = 'CAM001'
                db1 = DBConnection()
                conn1 = db1.get_connection()
                mycursor1 = conn1.cursor()
                mycursor1.execute("SELECT count(path) FROM Accident")
                count_row = mycursor1.fetchone()
                new_path = '../accident/Accident'+str(count_row[0])+'.jpg'
                copyfile(path, new_path)
                date_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                timestamp = time.time()
                sql1 = "insert into Accident(Camera_id,path,date_time,timestampAcc) values(%s,%s,%s,%s);"
                mycursor1.execute(sql1,[Camera_id,new_path,date_time,int(timestamp)])
                conn1.commit()
                mycursor1.execute("UPDATE flag set flag_var = 1 where flag_key = 1;")
                conn1.commit()
                mycursor1.execute("UPDATE smbool set continue_buffer = 0 where flag_var = 0")
                conn1.commit()
            if(insert_into_DB is 0):
                print('skipping database entry')
            sql = "DELETE FROM buffer WHERE path = %s"
            mycursor.execute(sql,[path])
            conn.commit()
            os.remove(path)
            

SampleSuperstoreData.csv’

storeData = LOAD 'SampleSuperstoreData.csv' USING PigStorage(',') AS (RowID:int, OrderID:chararray, OrderDate:chararray, ShipDate:chararray, ShipMode:chararray, CustomerID:chararray, CustomerName:chararray, Segment:chararray, Country:chararray, City:chararray, State:chararray, PostalCode:chararray, Region:chararray, ProductID:chararray, Category:chararray, SubCategory:chararray, ProductName:chararray, Sales:float, Quantity:int, Discount:float, Profit:double);

storeData = LOAD 'SampleSuperstoreDataNew3.csv' USING PigStorage(',') AS (RowID:int, OrderID:chararray, OrderDate:chararray, ShipDate:chararray, ShipMode:chararray, CustomerID:chararray, CustomerName:chararray, Segment:chararray, Country:chararray, City:chararray, State:chararray, PostalCode:chararray, Region:chararray, ProductID:chararray, Category:chararray, SubCategory:chararray, ProductName:chararray, Sales:float, Quantity:int, Discount:float, Profit:double);

SampleSuperstoreDataNew3.csv

superstore = LOAD 'SampleSuperstoreDataNew3.csv' USING PigStorage(',') AS (RowID:int, OrderID:chararray, OrderDate:chararray, ShipDate:chararray, ShipMode:chararray, CustomerID:chararray, CustomerName:chararray, Segment:chararray, Country:chararray, City:chararray, State:chararray, PostalCode:chararray, Region:chararray, ProductID:chararray, Category:chararray, SubCategory:chararray, ProductName:chararray, Sales:float, Quantity:int, Discount:float, Profit:double);


Q1:
filteredData = FILTER storeData BY Quantity>7 AND SubCategory=='Chairs' AND Category=='Furniture' AND (State=='California' OR State=='Texas') AND (ShipMode=='First Class');
selectedData = FOREACH filteredData GENERATE CustomerName, Profit;
orderData = ORDER selectedData by Profit Desc;
dump orderData;

Q1:
filtered = FILTER superstore BY Quantity>7 AND SubCategory=='Chairs' AND Category=='Furniture' AND (State=='California' OR State=='Texas') AND (ShipMode=='First Class');
selected = FOREACH filtered GENERATE CustomerName, Profit;
ordereddata = ORDER selected by Profit Desc;
dump ordereddata;


Q2:
filteredData = FILTER storeData BY (Category=='Furniture' OR Category=='Technology') AND ShipMode=='First Class';
data = RANK filteredData;
groupedData = GROUP filteredData by (Region,Segment);
maxProfit = FOREACH groupedData GENERATE FLATTEN(group) as (Region,Segment), MAX(filteredData.Profit);
dump maxProfit;


minProfit = FOREACH groupedData GENERATE FLATTEN(group) as (Region,Segment), MIN(filteredData.Profit);
dump minProfit;



Q2:
filtered = FILTER superstore BY (Category=='Furniture' OR Category=='Technology') AND ShipMode=='First Class';
new = RANK filtered;
grouped = GROUP filtered by (Region,Segment);
maxProfit = FOREACH grouped GENERATE FLATTEN(group) as (Region,Segment), MAX(filtered.Profit);
dump maxProfit;

minProfit = FOREACH grouped GENERATE FLATTEN(group) as (Region,Segment), MIN(filtered.Profit);
dump minProfit;




Q3:

filteredData = FILTER storeData BY Region=='Central' AND Discount < 0.5 AND (STARTSWITH(ProductName, 'H') OR STARTSWITH(ProductName, 'W'));
data = RANK filteredData;
groupedData = GROUP filteredData by SubCategory;
totalSales = FOREACH groupedData GENERATE group as SubCategory, SUM(filteredData.Sales) AS TotalSales;
rankedData = RANK totalSales BY TotalSales DESC;
dump rankedData;

Q3:

filtered = FILTER superstore BY Region=='Central' AND Discount < 0.5 AND (STARTSWITH(ProductName, 'H') OR STARTSWITH(ProductName, 'W'));
new = RANK filtered;
grouped = GROUP filtered by SubCategory;
totalSales = FOREACH grouped GENERATE group as SubCategory, SUM(filtered.Sales) AS TotalSales;
ranked = RANK totalSales BY TotalSales DESC;
dump ranked;


Q4:

orders = FOREACH storeData GENERATE OrderID, SUBSTRING(OrderDate, 7, 2) AS year, SUBSTRING(OrderDate, 4, 2) AS day;
ordersYearDay = GROUP orders BY (year, day);
count = FOREACH ordersYearDay GENERATE group.year AS year, group.day AS day, COUNT(orders) AS orderCount;
dump count;

cleanedData = FOREACH storeData GENERATE SUBSTRING(OrderDate, (int)SIZE(OrderDate)-2, (int)SIZE(OrderDate)+1) AS year, REGEX_EXTRACT(OrderDate, '([0-9]+)/([0-9]+)/([0-9]+)', 2) AS day;
groupData = GROUP cleanedData BY (year, day);
resultData = FOREACH groupData GENERATE FLATTEN(group) AS (year, day), COUNT(cleanedData) AS orderCount;
result = FOREACH resultData GENERATE CONCAT('(', '20', (chararray)year , ', ' ,  (chararray)day, ')') AS formattedtime, orderCount;
dump result;


cleandata = FOREACH superstore GENERATE SUBSTRING(OrderDate, (int)SIZE(OrderDate)-2, (int)SIZE(OrderDate)+1) AS year, REGEX_EXTRACT(OrderDate, '([0-9]+)/([0-9]+)/([0-9]+)', 2) AS day;
groupingdata = GROUP cleandata BY (year, day);
resultsdata = FOREACH groupingdata GENERATE FLATTEN(group) AS (year, day), COUNT(cleandata) AS ordercount;
results = FOREACH resultsdata GENERATE CONCAT('(', '20', (chararray)year , ', ' ,  (chararray)day, ')') AS formatted_time, ordercount;
dump results;


processed_data = FOREACH storeData GENERATE ToDate(OrderDate, 'MM/dd/yy') AS Order_Date;
day_year = FOREACH processed_data GENERATE GetDay(Order_Date) AS Day, GetYear(Order_Date) AS Year;
result = FOREACH (GROUP day_year BY (Year, Day)) { GENERATE FLATTEN(group) AS (Year, Day), COUNT(day_year) AS OrdersCount; }


Section B:
hadoop fs -put City_master.csv
hadoop fs -put Company_master.csv
hadoop fs -put Bus_master.csv
hadoop fs -put Bus_details.csv

busData = LOAD 'Bus_master.csv' USING PigStorage(',') AS (Bus_code: chararray, Airconditioned: chararray, Capacity: int, Bus_type: chararray);

cityData = LOAD 'City_master.csv' USING PigStorage(',') AS (City_code: chararray, City_name: chararray);

busDetails = LOAD 'Bus_details.csv' USING PigStorage(',') AS (Bus_code: chararray, City_code_source: chararray, City_code_destination: chararray, Company_code: chararray, Fare: int);

companyData = LOAD 'Company_master.csv' USING PigStorage(',') AS (Company_code: chararray, Company_name: chararray);
------

bus = LOAD 'Bus_master.csv' USING PigStorage(',') AS (Bus_code: chararray, Airconditioned: chararray, Capacity: int, Bus_type: chararray);

city = LOAD 'City_master.csv' USING PigStorage(',') AS (City_code: chararray, City_name: chararray);

busInfo = LOAD 'Bus_details.csv' USING PigStorage(',') AS (Bus_code: chararray, City_code_source: chararray, City_code_destination: chararray, Company_code: chararray, Fare: int);

companyInfo = LOAD 'Company_master.csv' USING PigStorage(',') AS (Company_code: chararray, Company_name: chararray);


Q1.

busDetailsData = JOIN busData BY Bus_code, busDetails BY Bus_code;
calcData = FOREACH busDetailsData GENERATE busDetails::Bus_code AS Bus_code, busDetails::Company_code AS Company_code, (busData::Capacity * busDetails::Fare) AS Revenue;
companyRev = JOIN calcData BY Company_code, companyData BY Company_code;
companyRevDetails = FOREACH companyRev GENERATE companyData::Company_name AS Company_name, calcData::Revenue AS Revenue;
companyGroup = GROUP companyRevDetails BY Company_name;
totalRev = FOREACH companyGroup GENERATE group AS Company_name, SUM(companyRevDetails.Revenue) AS Total_Revenue;
orderedTotalRev = ORDER totalRev BY Company_name;
DUMP orderedTotalRev;

busData = JOIN bus BY Bus_code, busInfo BY Bus_code;
calculator = FOREACH busData GENERATE busInfo::Bus_code AS Bus_code, busInfo::Company_code AS Company_code, (bus::Capacity * busInfo::Fare) AS Revenue;
company = JOIN calculator BY Company_code, companyInfo BY Company_code;
companyData = FOREACH company GENERATE companyInfo::Company_name AS Company_name, calculator::Revenue AS Revenue;
groupcompany = GROUP companyData BY Company_name;
totalcompany = FOREACH groupcompany GENERATE group AS Company_name, SUM(companyData.Revenue) AS Total_Revenue;
ordertotalrev = ORDER totalcompany BY Company_name;
dump ordertotalrev;

busrdetails = JOIN bus BY Bus_code, busdetails BY Bus_code;
calculater = FOREACH busrdetails GENERATE busdetails::Bus_code AS Bus_code, busdetails::Company_code AS Company_code, (busdata::Capacity * busdetails::Fare) AS Revenue;
companyr = JOIN calculater BY Company_code, companydata BY Company_code;
companyrdetails = FOREACH companyr GENERATE companydata::Company_name AS Company_name, calculater::Revenue AS Revenue;
groupedbycompany = GROUP companyrdetails BY Company_name;
totalrbycompany = FOREACH groupedbycompany GENERATE group AS Company_name, SUM(companyrdetails.Revenue) AS Total_Revenue;
orderedtotalrevenue = ORDER totalrbycompany BY Company_name;
DUMP orderedtotalrevenue;









Q2.
acBuses = FILTER busData BY Airconditioned == 'Yes';
newYorkData = FILTER cityData BY City_name == 'New York City';
dcData = FILTER cityData BY City_name == 'Washington DC';
routes = FILTER busDetails BY (City_code_source == 's001' AND City_code_destination == 's002') OR (City_code_source == 's002' AND City_code_destination == 's001');
acNycDcBuses = JOIN acBuses BY Bus_code, routes BY Bus_code;
capacityTotal = FOREACH (GROUP acNycDcBuses ALL) GENERATE SUM(acNycDcBuses.acBuses::Capacity) AS totalBuses;
DUMP capacityTotal;

acBus = FILTER bus BY Airconditioned == 'Yes';
NY = FILTER city BY City_name == 'New York City';
WDC = FILTER city BY City_name == 'Washington DC';
routes = FILTER busInfo BY (City_code_source == 's001' AND City_code_destination == 's002') OR (City_code_source == 's002' AND City_code_destination == 's001');
ACbusInfo = JOIN acBus BY Bus_code, routes BY Bus_code;
capacity = FOREACH (GROUP ACbusInfo ALL) GENERATE SUM(ACbusInfo.acBus::Capacity) AS totalcapacityacbuses;
DUMP capacity;


Section C;

Section C Abhi

*Use this loading for Question 2*

instrumentData = LOAD 'Musical_Instruments_5.json' USING JsonLoader('reviewerID:chararray, asin:chararray, reviewerName:chararray, helpful:bag{t:tuple(v:int)}, reviewText:chararray, overall:float, summary:chararray, unixReviewTime:chararray, reviewTime:chararray');

*Use this loading for Question 1*

instrumentData = LOAD 'Musical_Instruments_5.json' USING JsonLoader('reviewerID:chararray, asin:chararray, reviewerName:chararray, helpful:{(cnt:int)}, reviewText:chararray, overall:double, summary:chararray, unixReviewTime:long, reviewTime:chararray');

Q2 Determine the average score and number of reviews generated in different years 

years = FOREACH instrumentData GENERATE reviewerID, overall, (int)SUBSTRING(reviewTime, (int)SIZE(reviewTime)-4, (int)SIZE(reviewTime))AS year;
yearGroup = GROUP years BY year;
agg = FOREACH yearGroup GENERATE group AS year, AVG(years.overall) AS Average, COUNT(years) AS count;
dump agg;

Q.1 Determine the minimum and maximum overall score for different reviewerID ending with ‘N’


instrumentDataFiltered = FILTER instrumentData BY (ENDSWITH(reviewerID, 'N'));
reviewIdGroup = GROUP instrumentDataFiltered BY reviewerID;
result = FOREACH reviewIdGroup GENERATE group AS reviewerID, MIN(instrumentDataFiltered.overall) AS min_overall, MAX(instrumentDataFiltered.overall) AS max_overall;
dump result;

Section C Rachi

Use this loading for Question 2

instruments = LOAD 'Musical_Instruments_5.json' USING JsonLoader('reviewerID:chararray, asin:chararray, reviewerName:chararray, helpful:bag{t:tuple(v:int)}, reviewText:chararray, overall:float, summary:chararray, unixReviewTime:chararray, reviewTime:chararray');

Use this loading for Question 1

instruments = LOAD 'Musical_Instruments_5.json' USING JsonLoader('reviewerID:chararray, asin:chararray, reviewerName:chararray, helpful:{(cnt:int)}, reviewText:chararray, overall:double, summary:chararray, unixReviewTime:long, reviewTime:chararray');

Q2 Determine the average score and number of reviews generated in different years 

data_years = FOREACH instruments GENERATE reviewerID, overall, (int)SUBSTRING(reviewTime, (int)SIZE(reviewTime)-4, (int)SIZE(reviewTime))AS year;
group_by_year = GROUP data_years BY year;
aggregate = FOREACH group_by_year GENERATE group AS year, AVG(data_years.overall) AS Average, COUNT(data_years) AS count;
dump aggregate;

Q.1 Determine the minimum and maximum overall score for different reviewerID ending with ‘N’

filterinstruments = FILTER instruments BY (ENDSWITH(reviewerID, 'N'));
groupreviewid = GROUP filterinstruments BY reviewerID;
minmax = FOREACH groupreviewid GENERATE group AS reviewerID, MIN(filterinstruments.overall) AS min_overall, MAX(filterinstruments.overall) AS max_overall;
dump minmax;


Section D


hadoop fs -put ItemMaster.json
hadoop fs -put CustomerMaster.json
hadoop fs -put OrderDetails.json

Loads:

itemData = LOAD 'ItemMaster.json' USING JsonLoader('itemID:chararray, itemname:chararray, price:int');
custData = LOAD 'CustomerMaster.json' USING JsonLoader('customerID:chararray, customername:chararray, state:chararray');
orderDetailsData = LOAD 'OrderDetails.json' USING JsonLoader('orderID:chararray, items:{(itemID:chararray, quantity:int)}, customerID:chararray');


flatOrder =  FOREACH orderDetailsData GENERATE orderID, customerID, FLATTEN(items) AS (itemID:chararray, quantity:int);
itemOrder = JOIN flatOrder BY (itemID), itemData BY (itemID);
amountPerItem = FOREACH itemOrder GENERATE flatOrder::orderID AS orderID, flatOrder::customerID AS customerID, flatOrder::itemID AS itemID, flatOrder::quantity AS quantity, itemData::price AS price, (flatOrder::quantity * itemData::price) AS totalAmount;
result = FOREACH (GROUP amountPerItem BY customerID) GENERATE group AS customerID, SUM(amountPerItem. totalAmount) AS totalAmountPaid;
dump result;


Rachi

customermaster = LOAD 'CustomerMaster.json' USING JsonLoader('customerID:chararray, customername:chararray, state:chararray');
orderdetails = LOAD 'OrderDetails.json' USING JsonLoader('orderID:chararray, items:bag{item:tuple(itemID:chararray, quantity:int)}, customerID:chararray'); 
itemmaster = LOAD 'ItemMaster.json' USING JsonLoader('itemID:chararray, itemname:chararray, price:int');

flattened_result =  FOREACH orderdetails GENERATE orderID, customerID, FLATTEN(items) AS (itemID:chararray, quantity:int);
item_order = JOIN flattened_result BY (itemID), itemmaster BY (itemID);
item_amount = FOREACH item_order GENERATE flattened_result::orderID AS orderID, flattened_result::customerID AS customerID, flattened_result::itemID AS itemID, flattened_result::quantity AS quantity, itemmaster::price AS price, (flattened_result::quantity * itemmaster::price) AS totalamount;
totalamount = FOREACH (GROUP item_amount BY customerID) GENERATE group AS customerID, SUM(item_amount.totalamount) AS total_amount_paid;
dump totalamount;

order_flattened =  FOREACH order_details GENERATE orderID, customerID, FLATTEN(items) AS (itemID:chararray, quantity:int);
order_item = JOIN order_flattened BY (itemID), item_master BY (itemID);
amt_per_item = FOREACH order_item GENERATE order_flattened::orderID AS orderID, order_flattened::customerID AS customerID, order_flattened::itemID AS itemID, order_flattened::quantity AS quantity, item_master::price AS price, (order_flattened::quantity * item_master::price) AS total_amount;
total_amount = FOREACH (GROUP amt_per_item BY customerID) GENERATE group AS customerID, SUM(amt_per_item.total_amount) AS total_amount_paid;
dump total_amount;




Section E:
hadoop fs -mkdir /user/abhishettycc2427/jsondir/
for i in {1..50}; do     cp Customerdata.json Customerdata_$i.json; done
for i in {1..50}; do hadoop fs -put Customerdata_$i.json /user/abhishettycc2427/jsondir/; done

custData = LOAD 'jsondir' USING JsonLoader('id:int, name:chararray, email:chararray, address:map[], phone:chararray');
addGroup = GROUP custData BY address#'state';
count = FOREACH addGroup GENERATE group AS state, COUNT(custData) AS NoRecord;
dump count;


Rachi

hadoop fs -mkdir /user/rachitaf/jsondir/
for i in {1..50}; do     cp Customerdata.json Customerdata_$i.json; done
for i in {1..50}; do hadoop fs -put Customerdata_$i.json /user/rachitaf/jsondir/; done

custdata = LOAD 'jsondir' USING JsonLoader('id:int, name:chararray, email:chararray, address:map[], phone:chararray');
groupaddress = GROUP custdata BY address#'state';
countofstates = FOREACH groupaddress GENERATE group AS state, COUNT(custdata) AS norecords;
dump countofstates



