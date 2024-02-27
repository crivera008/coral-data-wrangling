File format is as follows:

|- data.xls -> Excel spreadsheet with one tab per survey type, one tab listing all Records, one tab listing all Projects and one tab listing all Sites.
|- README.txt -> this file
|- shapefiles
|- - <project name>
|- - - projectExtent.zip -> Shape file for the project extent
|- - - sites.zip -> Shape file containing all Sites associated with the project
|- images
|- - <project name>
|- - - <image files> -> Images associated with the project itself (e.g. logo)
|- - - activities -> directory structure containing images for the activities and their outputs
|- - - - <activity name>
|- - - - - <image files> -> Images associated with the activity itself
|- - - - - <output name>
|- - - - - - <image files> -> images associated with an individual Output entity
|- - - records -> directory structure containing images for individual records not already organised by activty/output
|- - - - <occurrenceId>
|- - - - - <image files> -> Images associated with the record
|- - - records.csv -> Map of record id onto image locations


This download was produced on 25/02/2024 23:13:07 +0100 CET.
