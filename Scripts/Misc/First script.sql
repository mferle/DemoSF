CREATE OR REPLACE TABLE buses_sample AS
SELECT 7 AS bus, 0 AS off FROM DUAL UNION ALL
SELECT 13 AS bus, 1 AS off FROM DUAL UNION ALL
SELECT 59 AS bus, 4 AS off FROM DUAL UNION ALL
SELECT 31 AS bus, 6 AS off FROM DUAL UNION ALL
SELECT 19 AS bus, 7 AS off FROM DUAL;

CREATE OR REPLACE TABLE buses AS 
SELECT 41 AS bus, 0 AS off FROM DUAL UNION ALL
SELECT 37 AS bus, 35 AS off FROM DUAL UNION ALL
SELECT 431 AS bus, 41 AS off FROM DUAL UNION ALL
SELECT 23 AS bus, 49 AS off FROM DUAL UNION ALL
SELECT 13 AS bus, 54 AS off FROM DUAL UNION ALL
SELECT 17 AS bus, 58 AS off FROM DUAL UNION ALL
SELECT 19 AS bus, 60 AS off FROM DUAL UNION ALL
SELECT 863 AS bus, 72 AS off FROM DUAL UNION ALL
SELECT 29 AS bus, 101 AS off FROM DUAL;

SELECT * FROM buses;

create or replace procedure bus_schedule()
  returns array
  language javascript
  as     
  $$  
    var return_array = [];
    var all_buses = [];
	var offsets = [];
	var flags = [];
	var cnt = 0;
    var my_sql_command = "select bus, off from buses";
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();
    // Loop through the results, processing one row at a time... 
    while (result_set1.next())  {
       all_buses.push(parseInt(result_set1.getColumnValue(1)));
       offsets.push(parseInt(result_set1.getColumnValue(2)));
	   flags.push(0);
	   cnt = cnt + 1;
       }

  var  ssum = 100000000000000;
  var	factor = 1
  var	next_factor = 1

	for (ii = 0; ii < 100000000000000; ii++) {
    if (((ssum + offsets[0]) % all_buses[0]) === 0) {
        if (flags[0] == 0) {
            flags[0] = 1;
            next_factor = next_factor * all_buses[0];
        }
        if (((ssum + offsets[1]) % all_buses[1]) === 0) {

            if (flags[1] == 0) {
                flags[1] = 1
                next_factor = next_factor * all_buses[1]
            }
            if (((ssum + offsets[2]) % all_buses[2]) == 0) {
                if (flags[2] == 0) {
                    flags[2] = 1
                    next_factor = next_factor * all_buses[2]
                }
                if (((ssum + offsets[3]) % all_buses[3]) == 0) {
                    if (flags[3] == 0) {
                        flags[3] = 1
                        next_factor = next_factor * all_buses[3]
                    }
                    if (((ssum + offsets[4]) % all_buses[4]) == 0) {
                        if (flags[4] == 0) {
                            flags[4] = 1
                            next_factor = next_factor * all_buses[4]
                        }
                        if (((ssum + offsets[5]) % all_buses[5]) == 0) {
                            if (flags[5] == 0) {
                                flags[5] = 1
                                next_factor = next_factor * all_buses[5]
                            }
                            if (((ssum + offsets[6]) % all_buses[6]) == 0) {
                                if (flags[6] == 0) {
                                    flags[6] = 1
                                    next_factor = next_factor * all_buses[6]
                                }
                                if (((ssum + offsets[7]) % all_buses[7]) == 0) {
                                    if (flags[7] == 0) {
                                        flags[7] = 1
                                        next_factor = next_factor * all_buses[7]
                                    }
                                    if (((ssum + offsets[8]) % all_buses[8]) == 0) {
                                        if (flags[8] == 0) {
                                            flags[8] = 1
                                            next_factor = next_factor * all_buses[8]
                                        }
return_array.push(ssum)

                                        return return_array; 

             }
            }
           }
          }
         }
        }
       }
      }
	 }
	factor = factor * next_factor;
    ssum = ssum + factor;
    next_factor = 1;

	}

  $$
  ;
  
 call bus_schedule();

 