--Define the CSV loader
define CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

--Load the file into a variable
orderCSV= LOAD '/user/maria_dev/Diplomacy/players.csv'
   USING CSVLoader(',') AS
           (game_id : int, 
           country : chararray,
           won : int, 
           num_supply_centers : int, 
           eliminated : int, 
           start_turn : chararray, 
           end_turn : int);

--Group the data by country and then;
--Since a loss is a 0 and a win is a 1 in this table we can just take the sum
--to find out how many times a country has won.
data = FOREACH(GROUP orderCSV by country)
				GENERATE group as country, SUM(orderCSV.won) as c;

dump data;