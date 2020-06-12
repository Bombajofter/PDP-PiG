--Define the CSV loader
define CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

--Load the file into a variable
orderCSV= LOAD '/user/maria_dev/Diplomacy/orders.csv'
   USING CSVLoader(',') AS
           (game_id : int, 
           unit_id: int, 
           unit_order: chararray,
           location: chararray,
           target: chararray,
           target_dest: chararray,
           success: int,
           reason: chararray,
           turn_num: int);

--Sort the list of the location alphabetically
ordened_orders = Order orderCSV by location DESC;

--Filter the data based on the target "Holland"
filtered_data = FILTER ordened_orders BY target == 'Holland';

--Afterwards count the amount of times the target was also "Holland"
data = FOREACH(GROUP filtered_data BY location)
			GENERATE group as location, MAX(filtered_data.(target)) as target, COUNT($1) as c;

--Put the results into the variable endResult
endResult = ORDER data BY location ASC;

--Show the endResult
DUMP endResult;