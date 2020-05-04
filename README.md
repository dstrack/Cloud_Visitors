# README for Cloud_Visitors
Application to monitor visitors of remote APEX sites

## Demo
https://apex.oracle.com/pls/apex/f?p=143496:LOGIN_DESKTOP

## Compatibility:
at least Application Express 19.1
at least Oracle Database Express Edition 18c

## Installation:
1. Encoding setting for sqlplus under Windows / DOS

	set NLS_LANG=GERMAN_GERMANY.AL32UTF8 
	chcp 65001

2. SQLDEVELOPER Settings

	Environment / Encoding = UTF-8

3. At the demo sites:

Execute the script file cloud_visitors_utl.sql

The packages procedure cloud_visitors_utl.Define_RESTful_Service will be executed to install the REST endpoint.
In the view CLOUD_VISITORS_V, the visitor activity is selected from APEX_WORKSPACE_ACTIVITY_LOG, it is then grouped by session login_date, ip_address, application, and page. The performed requests and time spend is aggregated.

4. In your localhost:

Install the application in your local APEX development environment. Then go the Shared Components\Web Source Modules to edit the modules. The module IP_Geolocation is used to lookup the geolocations of your visitor's IP addresses and should not be removed. Remove the example modules and add modules for your demo sites.
The app enables you to choose the name of a web source module for a visitor's report.

## Usage
Launch the app and enter your apex credentials.

The page 'Cloud Visitors'

Here you can display recent visitor's activities in an interactive Report from a local table.
After choosing a web source, you can control the scheduler job for the web source.
Click the 'Update now' button to load the recent information from the data source into the local table could_visitors.
Click the 'Launch refresh job' button to enable the frequent refresh of the report data.
A scheduler job will load the recent information with an SQL REST function from the data source joins it with the IP geolocation data and merges the result into the table cloud_visitors. Your own public IP address is added to the table cloud_visitors_ip_black_list and will be excluded from the reports.

Now you can choose a web source to be displayed and you don't have to wait for the display of the result. The data is updated every 4 hours.

And another problem is solved: 

The regular call of a web service at xyz.oraclecloudapps.com will also prevent the shutdown of an always free ATP site caused by developer inactivity!

The page 'Visitors - Web Source':

shows an Interactive Report from location 'Web Source'. Only one web source (that you have to choose in the program editor) can be displayed and you have to wait up to 25 seconds for the result.

The page 'Visitors - SQL REST function':

shows an Interactive Report from an SQL REST function. You can choose a web source to be displayed but you have to wait up to 25 seconds for the result.

The page 'IP Location - SQL REST function' 

allows you to Lookup the geo-location for an IP-Adress. Your own current public IP address and an IP Black List is shows. Here you can add IP addresses of robots that are excluded from the Cloud Visitors reports.

You may have to increase the Limit of 1000 web service requests that are permitted per day for your workspace, to avoid errors in the web service calls. Log in instance administration and go to page Manage Instance \ Security. In the region 'Workspace Isolation' you can change the 'Maximum Web Service Requests' to a higher value.
