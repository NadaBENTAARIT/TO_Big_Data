-- Load the data
data = LOAD 'datashop.txt' USING PigStorage('\t') AS (date:chararray, time:chararray, location:chararray, category:chararray, amount:float, payment:chararray);

-- Filter the data for the year 2012
filtered_data = FILTER data BY SUBSTRING(date, 0, 4) == '2012';

-- Group the data by location
grouped_data = GROUP filtered_data BY location;

-- Calculate the total amount for each location
total_amount = FOREACH grouped_data GENERATE group AS location, SUM(filtered_data.amount) AS total_amount;

-- Store the result
STORE total_amount INTO 'output_pig_1';