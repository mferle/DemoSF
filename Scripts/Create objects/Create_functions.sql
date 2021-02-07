create or replace FUNCTION ret_date_min() RETURNS TIMESTAMP AS 
$$
    to_timestamp('01.01.1900 00:00:00', 'DD.MM.YYYY HH24:MI:SS')
$$
;

create or replace FUNCTION ret_date_max() RETURNS TIMESTAMP AS 
$$
    to_timestamp('31.12.3000 00:00:00', 'DD.MM.YYYY HH24:MI:SS')
$$
;