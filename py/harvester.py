#import the required modules
import exceptions
import urllib2, json, time
from cartodb import CartoDBAPIKey, CartoDBException

#cartodb variables
api_key = 'cdce2379e95923bfdd7715fbf09a0ababdc1c3b5'
cartodb_domain = 'fis-ocha'
cl = CartoDBAPIKey(api_key, cartodb_domain)

#timestamp log for last run
epochtime = int(time.time())

#source json
urlData = "output.json"

#gets the last_timestamp from the log file
def get_timestamp_log():
    try:
        open_lastrun = open("timestamp_log.txt","r")
        if open_lastrun.mode == 'r':
            # check to make sure that the file was opened
            global last_timestamp
            last_timestamp = open_lastrun.read()
    except:
        last_timestamp = 0
        print last_timestamp
        return last_timestamp

#updates or creates the timestamp log and stores the last run timestamp
def update_timestamp_log():
    lastrun_log = open("timestamp_log.txt","w+")
    for i in range(1):
        lastrun_log.write(str(epochtime))
    lastrun_log.close()
    print str(epochtime)

#executes the cartodb modifications    
def modify_cartodb(sql_query):
    try:
        # your CartoDB account:
        print cl.sql(sql_query)
    except CartoDBException as e:
        print ("some error ocurred", e)
        
# deletes rows from cartodb       
def delete_cartodb_sql():        
    try:
        sql_query = "DELETE FROM hid_checkins"
        print str(sql_query)
    except ValueError,e:
        print ("an error ocurred", e)
    modify_cartodb(sql_query)

#inserts rows into cartodb                
def insert_cartodb_sql(lat, lon, id, org, updated, origin, role, location, operation_id):
    try:
        sql_query = "INSERT INTO hid_checkins (the_geom, hid_id, org_name, last_updated, origin_location, role, location_country, operation_id) VALUES ("
        sql_query = sql_query + "'SRID=4326; POINT (%f %f)', '%s', '%s', '%s', '%s', '%s', '%s', '%s'" % (float(str(lon)), float(str(lat)), id, org, updated, origin, role, location, operation_id)
        sql_query = sql_query + ")"
        print str(sql_query)
    except ValueError,e:
        print ("some error ocurred", e)
    modify_cartodb(sql_query)

#main harvest
def parse_json(urlData):
    #open the json
    with open(urlData) as data:
        # Use the json module to load the string data into a dictionary
        json_data = json.load(data)
        
        # create placeholder list for parsed items
        insertable_items = []
        
        #defining the main level variables that will be pulled into CartoDB
        for i in json_data:
            id = i["id"]
            org = i["organization"]
            if (org == None):
                org = "null"
            org = org.replace("'", "''")
            updated = i["last_updated"]
            if (updated == None):
                updated = "null"
            origin = i["address"]
            role = i["role"]
            if (role == []):
                role = "null"
            else:
                for r in role:
                    role = r
            location = i["checkin"]["country"]
            lat = i["checkin"]["lat"]
            lon = i["checkin"]["lon"]
            operation_id = i["checkin"]["operation_id"]
            
            insertable_items += [(lat, lon, id, org, updated, origin, role, location, operation_id)]
            print "Processing..." + str(id) + ", Location: " + str(location)
        
        #checks the timestamp to determine if items need to be updates or added
        print str(insertable_items)
        print last_timestamp
        if (int(last_timestamp) == 0):
            pass #this is used on first run, you can insert all items by deleting "lastrun.txt" which will reset the timestamp to 0.
        else:
            delete_cartodb_sql()
        for i in insertable_items:
            insert_cartodb_sql(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8])
      

#the main function
if __name__ == "__main__":
    get_timestamp_log()
    parse_json(urlData)
    update_timestamp_log()