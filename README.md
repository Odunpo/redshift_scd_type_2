# redshift_scd_type_2

## Example of SQL transformations for slowly changing dimension type 2 for Redshift

This simple example just shows how to properly transform your staging table into existing table with Type 2 of SCD.  

The initial customers table already has some changes history:  
  
![image](https://github.com/Odunpo/redshift_scd_type_2/assets/55160762/81b743d3-b154-431b-bb56-fb2400876f12)

Current staging table also has two changes to transform:  
  
![image](https://github.com/Odunpo/redshift_scd_type_2/assets/55160762/17313f3b-0591-463a-abdf-7b70e2dcf5a1)

And after tranformations you have the next customers table:  
  
![image](https://github.com/Odunpo/redshift_scd_type_2/assets/55160762/3270b4ab-565c-49af-8208-53685be67e4b)
