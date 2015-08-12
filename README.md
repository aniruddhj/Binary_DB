# Binary_DB
Converting CSV file to binary file and index files containing the records and the location of the records in the binary file and perform query on it.

/*
* @author: Aniruddh Jhavar
*/
Programming project#2 (files and indexing):

The program is for indexing the files and creating a binary file and querying on the binary file.
The program also had the feature of giving an error if the fie data.db does not exist om the directory. 


Language: Java
Version: java7

==========================================================================
How to compile?
In the command prompt type:

javac MyDatabase.java
java MyDatabase

==========================================================================
Command Line after the code is executed:
/* The program will give an error if the data.db file is not present. */

1:To query the database
2:exit

Select the field to query on(please write the entire field name):
Id
Company Name
Drug_Id
Trials
Patients
Dosage
Reading
Double_Blind
Controlled_Study
Govt_Funded
FDA_Approved


Select one of the following operators to work on i.e. ==, !=, >, >=, <=, <

Provide the data for the operator to work on:

==========================================================================
Example:
1:To query the database
2:exit
1

Select the field to query on(please write the entire field name):
Id
Company Name
Drug_Id
Trials
Patients
Dosage
Reading
Double_Blind
Controlled_Study
Govt_Funded
FDA_Approved

Id

Select one of the following operators to work on i.e. ==, !=, >, >=, <=, <
<=

Provide the data for the operator to work on:
10
1,"Stephen L. LaFrance Pharmacy, Inc.",IZ-002,40,1206,316,39.5,false,true,true,true
10,Seton Pharmaceuticals,CG-024,20,1690,378,42.7,false,true,false,false

--------------------------------------------------------------------------
NOTE: If theres no data for the input given by the user it will print nothing in the OUTPUT section.
