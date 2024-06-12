"""
functions.py 
- Daphne
All the functions you need for this SQM project (and locations dictionary)

"""


# imports 
import datetime
from datetime import date, datetime, timedelta, time
import numpy as np
import ephem
import pandas as pd
import platform
from tqdm import tqdm
import pytz
import os
import multiprocessing
import functools

# making the plots
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# Location dictionary
# location, VIIRS, airport, alt name??, lattitude, longitude, 25 mile population est (maps.ie), 10 pop est, dist to airport km, roof or ground
loc_dict =  {'Rolla':[19.56,'vichy_rolla_national_airport_mo_us','Physics building Rolla',
                      '37.9549','-91.7722', '112871', '41925', '20', '--']
              ,'MAC':[20.4,'farmington_regional_airport_mo_us','Mineral Area College',
                      '37.845291','-90.482322', '156041', '65512', '11', 'rooftop']
              ,'SEMO':[18.96,'cape_girardeau_municipal_airport_mo_us','Cape Girardeau, MO Rhodes Hall',
                       '37.316192','-89.529771', '129606', '71591', '10', '--']
              ,'Timberlane': [20.95,'midwest_national_air_center_airport_mo_us','Timberlane Obervatory',
                              '39.4128','-94.2417', '421127', '43213', '11', '--']
              ,'Ozark': [20.06,'springfield_weather_service_office_airport_mo_us','Ozark City Hall',
                         '37.021196','-93.205458', '474329', '129921', '30', '--']
              ,'Ozark_II': [20.06,'springfield_weather_service_office_airport_mo_us','Ozark Community Center',
                            '37.022','-93.221', '473062', '143671', '29', 'rooftop']
              ,'Perryville': [20.4,'cape_girardeau_municipal_airport_mo_us','Perryville',
                              '37.720184','-89.852254', '70757', '16930', '60', 'ground']
              ,'Overland': [18.49,'lees_summit_municipal_airport_mo_us','dyoung home',
                            '38.935','-94.589', '1936589', '781331', '19', '--']
              ,'Hillsboro': [20.57,'st_louis_spirit_of_st_louis_airport_mo_us','Jefferson College',
                             '38.265129','-90.558487', '724932', '92316', '46', 'ground']
              ,'Science Center': [17.69,'st_louis_lambert_international_airport_mo_us','McDonnell Planetarium SLSC',
                                  '38.630804','-90.270782', '2240649', '913656', '16', '--']
              ,'Magruder Hall': [19.89,'kirksville_regional_airport_mo_us','Magruder Hall',
                                 '40.187416', '-92.581552', '39215', '24104', '11', 'rooftop']
              ,'MG Hall': [19.89,'kirksville_regional_airport_mo_us','MG Hall',
                           '40.187416', '-92.581552', '39215', '24104', '11', 'rooftop']
              ,'TSO': [20.71,'kirksville_regional_airport_mo_us','TSO',
                       '40.177772', '-92.600972', '39009', '23844', '11', '--']
              ,'Thousand_Hills': [21.25,'kirksville_regional_airport_mo_us','Thousand_Hills',
                                  '40.191832', '-92.6492959', '37965', '23815', '13', 'ground']
              ,'Grand Gulf': [21.89, 'west_plains_municipal_airport_mo_us','Grand Gulf State Park',
                              '36.545278','-91.646131', '61167', '8100', '44', 'ground']
              ,'echo bluff': [21.96, 'west_plains_municipal_airport_mo_us', 'echo bluff',
                              '37.307542','-91.409945', '25193', '646', '65', 'ground']
              ,'Jefferson Arch': [17.38, 'st_louis_lambert_international_airport_mo_us','Gateway Arch National Park',
                                  '38.625732','-90.189310', '2147374', '732211', '21', 'rooftop']
              ,'Broemmelsiek Park': [20.29, 'st_louis_lambert_international_airport_mo_us', 'Broemmelsiek Park',
                                     '38.7197483','-90.8199591', '1045411', '207222', '38', 'rooftop']}


# Populate SQM Values
def populate_sqm_vals(night_date, df, sqm_data):
    """ 
    Update and return the dataframe with sqm values for one night
    NOTE we only reuse an SQM measurement once (so there aren't long periods of old data used)
    """
    # Iterate through filtered DataFrame rows
    for df_index, row in df[df['Night Date'] == night_date].iterrows():
        # df_window_start = datetime.combine(row['True Date'], row['Time'])
        df_window_start = datetime.combine(datetime.strptime(row['True Date'], '%Y-%m-%d').date(), datetime.strptime(row['Time'], '%H:%M:%S').time())


        df_window_end = df_window_start + timedelta(minutes=5)


        # initialize sqm_value as None (only 'remember' old data when it is proximate to the row with missing data)
        # a None sqm value will cause the No SQM Flag to be used instead of Old SQM Flag
        sqm_value = None



        """
        Iterate through SQM data to find matching entry for each DataFrame row 
        """
    
        for sqm_entry in sqm_data:
            sqm_date, sqm_time = sqm_entry[:2]
            sqm_date_time = datetime.strptime(f'{sqm_date} {sqm_time}', '%Y-%m-%d %H:%M')

            # Check if SQM entry falls within the time window of the DataFrame row
            if df_window_start <= sqm_date_time <= df_window_end:
                sqm_value = sqm_entry[2]
                df.loc[df_index, 'SQM'] = sqm_value 
                # print(f'sqm_updated for {sqm_date_time.strftime('%Y-%m-%d %H:%M')}') # Update SQM value in DataFrame
                break  # Exit the loop once matching SQM entry is found

            # Handle the case where no matching SQM entry is found (optional)
            elif sqm_date_time > df_window_end:
                
                if sqm_value == None: # there's no old measurement to use, so mark this row as missing sqm data
                    df.loc[df_index, 'No SQM Flag'] = True
                    break

                else:
                    print(f"OLD sqm_updated for sqm date:  {sqm_date_time.strftime('%Y-%m-%d %H:%M')} window: {df_window_end.strftime('%Y-%m-%d %H:%M')}")
                    # SQM value not found for this time: use old data and flag
                    # the variable sqm_value remembers the last sqm measurement, 
                    # so reuse it here, and flag for old SQM data
                    df.loc[df_index, 'SQM'] = sqm_value  # Update SQM value in DataFrame
                    df.loc[df_index, 'Old SQM Flag'] = True

                # now, you've 'remembered' an old value once. After this, don't reuse the same measurement anymore (set sqm_value to None)
                sqm_value = None
                break
            

    return df


# Populate Weather Data
def populate_weather_vals(night_date, df, weather_data):

    """ 
    Update and return the dataframe with sqm values for one night
    """
        
    for df_index, row in df[df['Night Date'] == night_date].iterrows():



        # df_window_start = datetime.combine(row['True Date'], row['Time'])
        df_window_start = datetime.combine(datetime.strptime(row['True Date'], '%Y-%m-%d').date(), datetime.strptime(row['Time'], '%H:%M:%S').time())
        df_window_end = df_window_start + timedelta(minutes=5)

        # classify as night_start if df_window_start is between 12:00 and 12:30
        df_window_start_time = df_window_start.time()
        if time(12,0) <= df_window_start_time <= time(12,30):
            night_start = True
        else:
            night_start = False

        # initialize weather_value as None (only 'remember' old data when it is proximate to the row with missing data)
        # a None weather value will cause the No weather Flag to be used instead of Old weather Flag
        weather_value = None

        """
        Iterate through weather data to find matching entry for each DataFrame row 
        """

        for weather_entry in weather_data:
            weather_date, weather_time = weather_entry[:2]

            # sometimes, the row is not well-formatted
            # turn this to a try except block
            try:
                weather_date_time = datetime.strptime(f'{weather_date} {weather_time}', '%Y-%m-%d %H:%M')
            
            except:
                print(f"Skipping weather entry: {weather_entry}. Unable to parse date and time.")
                print(f"Night Date: {night_date}")
                print(f'df first row: {df.iloc[0]}')
                continue

            """ 
            weather data is generally collected every hour.

            An SQM measurement should be assigned the nearest possible weather data,
            so the default will be for the weather data to be from between +/- 30 min
            """

            # Check if weather entry is within 30 min of SQM measurement on either side
            # or, for the start of night, check if it's within 1 hr
            # (this reduces the need to access the weather data from the previous night
            # and no harm done bc we don't really care about the weather during noon time)

            if night_start == True:
                weather_window_start = df_window_start - timedelta(minutes=60)
                weather_window_end = df_window_start + timedelta(minutes=60)
                
            
            else: 
                weather_window_start = df_window_start - timedelta(minutes=30)
                weather_window_end = df_window_start + timedelta(minutes=30)

           

            if weather_window_start <= weather_date_time <= weather_window_end:
                weather_values = weather_entry[2]
                
                
                # handle weird errors where there's an s ????
                try:
                    weather_values_list = [int(part.split(':')[1]) for part in weather_values.split() if ':' in part]
                    weather_value = max(weather_values_list) # choose the maximum okta value if there are multiple to choose from

                except Exception as e:
                    print(Exception)
                    continue

                
                # if there are multiple values, flag it 
                if len(weather_values_list) > 1:
                    df.loc[df_index, 'Mult Weather Data Flag'] = True

                df.loc[df_index, 'Cloud Cover'] = weather_value 
                # print(f'weather_updated for {weather_date_time.strftime('%Y-%m-%d %H:%M')}') # Update weather value in DataFrame
                break  # Exit the loop once matching weather entry is found

            

            # Handle the case where no matching weather entry is found (optional)
            elif weather_date_time > df_window_end:

                if weather_value == None: # there's no old measurement to use, so mark this row as missing weather data
                    df.loc[df_index, 'No Weather Flag'] = True
                    break

                else:
                    print(f"OLD weather updated for weather date:  {weather_date_time.strftime('%Y-%m-%d %H:%M')} window: {df_window_end.strftime('%Y-%m-%d %H:%M')}")
                    # weather value not found for this time: use old data and flag
                    # the variable weather_value remembers the last weather measurement, 
                    # so reuse it here, and flag for old weather data
                    df.loc[df_index, 'Weather'] = weather_value  # Update weather value in DataFrame
                    df.loc[df_index, 'Old Weather Flag'] = True

                
                break

    return df


# Populate All Sun Data
def populate_all_sun_data(df, sun_data):
    """ 
    Assuming input is in UTC time and we want to convert to Central 
    """

    # Preprocess DataFrame start and end dates
    df_start_datetime = datetime.combine(datetime.strptime(df.iloc[0]['True Date'], '%Y-%m-%d').date(),
                                         datetime.strptime(df.iloc[0]['Time'], '%H:%M:%S').time())
    df_end_datetime = datetime.combine(datetime.strptime(df.iloc[-1]['True Date'], '%Y-%m-%d').date(),
                                       datetime.strptime(df.iloc[-1]['Time'], '%H:%M:%S').time())

    # Preprocess sun data
    processed_sun_data = []
    for data in sun_data:
        sunrise_str, sunset_str, _ = data.split('\t')
        sunset_datetime_utc = datetime.strptime(sunset_str, '%Y/%m/%d %H:%M:%S')
        sunrise_datetime_utc = datetime.strptime(sunrise_str, '%Y/%m/%d %H:%M:%S')

        # define timezones
        utc_timezone = pytz.timezone('UTC')
        central_timezone = pytz.timezone('America/Chicago')  # Central Time Zone

        # read in utc sunset datetime to UTC
        sunset_datetime_utc = utc_timezone.localize(sunset_datetime_utc)

        # Convert UTC sunset datetime to Central Time
        sunset_datetime = sunset_datetime_utc.astimezone(central_timezone)

        # Convert sunrise datetime to UTC
        sunrise_datetime_utc = utc_timezone.localize(sunrise_datetime_utc)

        # Convert UTC sunrise datetime to Central Time
        sunrise_datetime = sunrise_datetime_utc.astimezone(central_timezone)

        # Now you have sunset_datetime_central and sunrise_datetime_central in Central Time
        # let's remove the timezone info though, because it confuses the program
        sunset_datetime = sunset_datetime.replace(tzinfo=None) 
        sunrise_datetime = sunrise_datetime.replace(tzinfo=None)

        # print(f'checkpoint: {sunrise_datetime}')

        if sunrise_datetime < df_start_datetime:
            continue
        if sunset_datetime > df_end_datetime:
            break

        processed_sun_data.append((sunset_datetime, sunrise_datetime)) #WHATS HAPPENING HEREEEEEEEEEE

    # Initialize tqdm with the total number of iterations
    progress_bar = tqdm(total=len(processed_sun_data), desc="Processing Sun Data")

    # Update sunrise and sunset in df
    for sunset_datetime, sunrise_datetime in processed_sun_data:
        df.loc[((df['True Date'] + ' ' + df['Time']).apply(lambda x: sunset_datetime <= datetime.strptime(x, '%Y-%m-%d %H:%M:%S') <= sunset_datetime + timedelta(minutes=5))), 'Astro Dusk'] = True
        df.loc[((df['True Date'] + ' ' + df['Time']).apply(lambda x: sunrise_datetime <= datetime.strptime(x, '%Y-%m-%d %H:%M:%S') <= sunrise_datetime + timedelta(minutes=5))), 'Astro Dawn'] = True

        progress_bar.update(1)

    # Close the progress bar
    progress_bar.close()

    return df



# update the sun up column
def populate_sun_up(df):

    # This mitigates a deprecated functionality of pandas library...
    # it doesn't like adding strings withouut this casting first
    df['Sun Up'] = df['Sun Up'].astype('object')


    # now figure out if the sun starts up or down
    
    # compare the first sun rise and sun set index...
    # if sun rise is earliest, that means the sun starts set
    # if the sun set is earliest, that means the sun starts up

    sunrise_df = df.dropna(subset=['Astro Dawn'])
    sunset_df = df.dropna(subset=['Astro Dusk'])

    # Find the index of the first occurrence of True in 'Sun Rise' column
    sunrise_index = sunrise_df[sunrise_df['Astro Dawn']].index.min()
    sunset_index = sunset_df[sunset_df['Astro Dusk']].index.min()

    if sunrise_index < sunset_index: # sun rises first -> starts set
        sun_up = False # initialize sun up
    
    elif sunset_index > sunrise_index:
        sun_up = True # initialize sun down


    # Iterate over rows
    for index, row in df.iterrows():
        
        # first check, did the sun rise or set this interval
        if row['Astro Dawn'] == True:
            sun_up = True

        elif row['Astro Dusk'] == True:
            sun_up = False
        

        # now, update sun up based on sun position
        df.at[index, 'Sun Up'] = sun_up
        
    return df



# Populate All Moon Data
def populate_all_moon_data(df, moon_data):

    # in the future: search for the start and end date of df, 
    # then only pick out those data from sun data... probably doesn't impact speed too much tho

    
    for data in moon_data:
        moon_rise_str, moon_set_str, peak_data_str = data.split('\t')
        

        # Convert moon rise and moon set strings to datetime objects
        moon_rise_datetime = datetime.strptime(moon_rise_str, '%Y/%m/%d %H:%M:%S')
        moon_set_datetime = datetime.strptime(moon_set_str, '%Y/%m/%d %H:%M:%S')

        
        
        
        # Extract peak moon position data
        peak_altitude, moon_peak_str, peak_azimuth = map(str, peak_data_str.split(';'))

        moon_peak_datetime = datetime.strptime(moon_peak_str, '%Y/%m/%d %H:%M:%S')

    """ 
    Assuming input is in UTC time and we want to convert to Central 
    """

    # Preprocess DataFrame start and end dates
    df_start_datetime = datetime.combine(datetime.strptime(df.iloc[0]['True Date'], '%Y-%m-%d').date(),
                                         datetime.strptime(df.iloc[0]['Time'], '%H:%M:%S').time())
    df_end_datetime = datetime.combine(datetime.strptime(df.iloc[-1]['True Date'], '%Y-%m-%d').date(),
                                       datetime.strptime(df.iloc[-1]['Time'], '%H:%M:%S').time())

    # Preprocess sun data
    processed_moon_data = []

    for data in moon_data:

        moon_rise_str, moon_set_str, peak_data_str = data.split('\t')
        

        # Convert moon rise and moon set strings to datetime objects
        moon_rise_datetime = datetime.strptime(moon_rise_str, '%Y/%m/%d %H:%M:%S')
        moon_set_datetime = datetime.strptime(moon_set_str, '%Y/%m/%d %H:%M:%S')

        
        # Extract peak moon position data
        peak_altitude, moon_peak_str, peak_azimuth = map(str, peak_data_str.split(';'))

        moon_peak_datetime = datetime.strptime(moon_peak_str, '%Y/%m/%d %H:%M:%S')


        # print(f'checkpoint: {sunrise_datetime}')

        if moon_rise_datetime < df_start_datetime:
            continue
        if moon_rise_datetime > df_end_datetime:
            break

        processed_moon_data.append((moon_set_datetime, moon_rise_datetime, moon_peak_datetime, peak_altitude)) 

    # Initialize tqdm with the total number of iterations
    progress_bar = tqdm(total=len(processed_moon_data), desc="Processing Moon Data")

    # Update moon rise and moon set in df
    for moon_set_datetime, moon_rise_datetime, moon_peak_datetime, peak_altitude in processed_moon_data:
        df.loc[((df['True Date'] + ' ' + df['Time']).apply(lambda x: moon_set_datetime <= datetime.strptime(x, '%Y-%m-%d %H:%M:%S') <= moon_set_datetime + timedelta(minutes=5))), 'Moon Set'] = True
        df.loc[((df['True Date'] + ' ' + df['Time']).apply(lambda x: moon_rise_datetime <= datetime.strptime(x, '%Y-%m-%d %H:%M:%S') <= moon_rise_datetime + timedelta(minutes=5))), 'Moon Rise'] = True
        df.loc[((df['True Date'] + ' ' + df['Time']).apply(lambda x: moon_peak_datetime <= datetime.strptime(x, '%Y-%m-%d %H:%M:%S') <= moon_peak_datetime + timedelta(minutes=5))), 'Moon Peak'] = True
        df.loc[((df['True Date'] + ' ' + df['Time']).apply(lambda x: moon_peak_datetime <= datetime.strptime(x, '%Y-%m-%d %H:%M:%S') <= moon_peak_datetime + timedelta(minutes=5))), 'Moon Peak Altitude'] = peak_altitude

        progress_bar.update(1)

    # Close the progress bar
    progress_bar.close()

    return df


# update the moon up column
def populate_moon_up(df):

    # This mitigates a deprecated functionality of pandas library...
    # it doesn't like adding strings withouut this casting first
    df['Moon Up'] = df['Moon Up'].astype('object')


    # now figure out if the moon starts up or down
    
    # compare the first moon rise and moon set index...
    # if moon rise is earliest, that means the moon starts set
    # if the moon set is earliest, that means the moon starts up

    moon_rise_df = df.dropna(subset=['Moon Rise'])
    moon_set_df = df.dropna(subset=['Moon Set'])

    # Find the index of the first occurrence of True in 'Moon Rise' column
    moon_rise_index = moon_rise_df[moon_rise_df['Moon Rise']].index.min()
    moon_set_index = moon_set_df[moon_set_df['Moon Set']].index.min()

    if moon_rise_index < moon_set_index: # moon rises first -> starts set
        moon_up = False # initialize moon up
    
    elif moon_set_index > moon_rise_index:
        moon_up = True # initialize moon down


    # Iterate over rows
    for index, row in df.iterrows():
        
        # first check, did the moon rise or set this interval
        if row['Moon Rise'] == True:
            moon_up = True

        elif row['Moon Set'] == True:
            moon_up = False
        

        # now, update moon up based on moon position
        df.at[index, 'Moon Up'] = moon_up
        
    return df



# Combine Data
def combine_data(location, start_date=None, end_date=None, row_time_int=5):

    """ 
    ARGS
    location: str - location that corresponds to the dictionary entry for the site
    start_date: str - form "YYYY-MM-DD" 
                if NONE, then search for all sqm data available for a given object
    end_date: str - form "YYYY-MM-DD" 
                if NONE, then search for all sqm data available for a given object
    row_time_int: length of dataframe row time interval in minutes 
                    (default is 5 min because that is how often SQM take measurements)

    RETURNS dataframe with SQM, Weather, Sun, and Moon Data

    GOAL
    Read in SQM data, weather data, sun data, and moon data for an object.
    Use this data to populate a dataframe in 5 min increments


    This code requires the directory to be formatted in a specific way
    """

    # this makes the code flexible for use on Linux and Windows machines 
    # (which require path in a different format)

    # in the future you can likely use os.path.join instead of this clever trick :)
    system = platform.system()
    if system == "Linux":
        path_slash = '/'
    elif system == "Windows":
        path_slash = '\\'    

    # Find the available date range for the SQM Data

    
    sqm_data_dir = loc_dict.get(location)[2] 
    dir_start_date = None
    dir_end_date = None

    # CHECKPOINT CHECK THIS  COME BACK 

    # Set the range for valid dates
    min_valid_date = datetime(2015, 1, 1)
    max_valid_date = datetime(2050, 1, 1)

    dir_start_date = None
    dir_end_date = None

    for filename in os.listdir(sqm_data_dir):
        # Check if the filename ends with ".txt"
        if filename.endswith(".txt"):
            # Extract the date part from the filename
            date_str = filename[-14:-4]  # Extract the substring containing the date
            date_str = date_str.replace("-", "")  # Remove hyphens
            
            # Convert the date string to a datetime object
            date = datetime.strptime(date_str, "%Y%m%d")
            
            # Check if the date is within the valid range
            if min_valid_date <= date <= max_valid_date:
                if dir_start_date is None or date < dir_start_date:
                    dir_start_date = date
                
                if dir_end_date is None or date > dir_end_date:
                    dir_end_date = date

    if dir_start_date is not None:
        print("Earliest date:", dir_start_date.strftime("%Y-%m-%d"))
    else:
        print("No files found in the directory")

    if dir_end_date is not None:
        print("Latest date:", dir_end_date.strftime("%Y-%m-%d"))
    else:
        print("No files found in the directory")


    if start_date == None:
        # retrive start date from the files
        # read it as a datetime object
        start_datetime = dir_start_date
        pass # remove once you complete conditional statement
    else:
        start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
        # now, if the requested start date is EARLIER than the directory's oldest file, then use the directory start date
        if start_datetime < dir_start_date:
            start_datetime = dir_start_date

    if end_date == None:
         # retrive end date from the files
         # read it as a datetime object
         end_datetime = dir_end_date
         pass # remove once you complete conditional statement
    
    else: 
        end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
        # now, if the requested end date is LATER than the directory's oldest file, then use the directory end date
        if end_datetime > dir_end_date:
            end_datetime = dir_end_date
    

    """ 
    MAKE THE DATAFRAME ------------------------------------------------------------------------------------------------------
    """
    
    # columns = ['True Date', 'Time', 'Julian Date', 'Night Date', 'SQM', 'Cloud Cover', 'Astro Dawn', 'Astro Dusk', 'Sun Up',
    #         'Moon Rise', 'Moon Set', 'Moon Up', 'Moon Peak', 'Moon Peak Altitude', 
    #         'Row Classification', 'Row Classification Confidence', 'Night Classification', 'Night Classification Confidence',
    #         'No SQM Flag', 'Old SQM Flag', 'No Weather Flag', 'Old Weather Flag', 'Mult Weather Data Flag' ]
    # df = pd.DataFrame(columns=columns)

    # # Generate list of datetime objects at 5-minute intervals
    # # start_date_time = datetime(2022, 1, 1, 0, 0)  # Replace with your start date and time
    # # end_date_time = datetime(2022, 1, 2, 0, 0)    # Replace with your end date and time
    # time_intervals = [start_datetime + timedelta(minutes=x) for x in range(0, int((end_datetime - start_datetime).total_seconds() / 60), row_time_int)]

    # # Create a list of dictionaries for row data
    # rows_list = []
    # for time_interval in time_intervals:
    #     if  time_interval.hour < 12:
    #         night_date = time_interval.date() - timedelta(days=1)
    #     else:
    #         night_date = time_interval.date()
    #     row_data = {'True Date': str(time_interval.date()), 'Time': str(time_interval.time()), 'Night Date': str(night_date)}
    #     for col in columns[3:]:
    #         row_data[col] = None
    #     rows_list.append(row_data)

    # # Concatenate the list of dictionaries into the DataFrame
    # df = pd.concat([df, pd.DataFrame(rows_list)], ignore_index=True)


   # MAKE THE DATAFRAME ------------------------------------------------------------------------------------------------------

    columns = ['True Date', 'Time', 'Julian Date', 'Night Date', 'SQM', 'Cloud Cover', 'Astro Dawn', 'Astro Dusk', 'Sun Up',
                    'Moon Rise', 'Moon Set', 'Moon Up', 'Moon Peak', 'Moon Peak Altitude', 
                    'Row Classification', 'Row Classification Confidence', 'Night Classification', 'Night Classification Confidence',
                    'No SQM Flag', 'Old SQM Flag', 'No Weather Flag', 'Old Weather Flag', 'Mult Weather Data Flag' ]
    df = pd.DataFrame(columns=columns)

    row_time_int = 5  # Time interval in minutes

    # Generate list of datetime objects at 5-minute intervals
    time_intervals = [start_datetime + timedelta(minutes=x) for x in range(0, int((end_datetime - start_datetime).total_seconds() / 60), row_time_int)]

    # Create a list of dictionaries for row data
    rows_list = []
    for time_interval in time_intervals:
        if  time_interval.hour < 12:
            night_date = (time_interval - timedelta(days=1)).date()
        else:
            night_date = time_interval.date()
        
        # Calculate Julian date
        julian_date = ephem.julian_date(time_interval)
        
        row_data = {'True Date': str(time_interval.date()), 'Time': str(time_interval.time()), 'Night Date': str(night_date)}
        for col in columns[4:]:
            row_data[col] = None
            
        row_data['Julian Date'] = julian_date
        rows_list.append(row_data)

    # Concatenate the rows into DataFrame
    df = pd.concat([df, pd.DataFrame(rows_list)], ignore_index=True)









    """
    UPDATE DATAFRAME WITH SQM, WEATHER, SUN, MOON DATA AND APPROPRIATE FLAGS --------------------------------------
    """

    """ 
    PROCESS:
    - No SQM Data
        - just flag and move on

    - No Weather Data 
        - just update SQM, flag for no weather
        - if there's intermittent missing SQM data, 
        use old data and flag accordingly

    - Weather Data and SQM data both available 
        - Update SQM data (handle missing rows as above)
        - Update weather data 
            - read in oktas covered
            - if there are multiple weather measurements in the window, 
            use the highest cloud measurement and flag accordingly
            - if weather data is more than 90 minutes old, 
            flag as old weather data

    - Sun/Moon
        - don't forget to convert Sun data from UTC to Central
    
    """
    # SUN AND MOON DATA ----------------------------------------------------------------------------------------------------------------------------
    # Read in moon and sun data for this location

    moon_data_path = f"Moon_Data{path_slash}" + location + "_Moon_Phase.dat"
    with open(moon_data_path, 'r') as moon_file:
        moon_data = moon_file.readlines()

    sun_data_path = f"Sun_Data{path_slash}" + location + "_Twilight.dat"
    with open(sun_data_path, 'r') as sun_file:
        sun_data = sun_file.readlines()


    # Populate all of the sun and moon data
    # this should display progress bars  
    df = populate_all_sun_data(df, sun_data)
    df = populate_sun_up(df)
    df = populate_all_moon_data(df, moon_data)
    df = populate_moon_up(df)


    # SQM AND WEATHER DATA ---------------------------------------------------------------------------------------------------------------------------
    # Get a list of all unique nights
    nights_list = df['Night Date'].unique()



    # Loop through each night for the location
    for night_date in tqdm(nights_list, desc="Processing nights"):
        # Construct paths for SQM and weather data
        sqm_path = loc_dict.get(location)[2] + path_slash + loc_dict.get(location)[2] + night_date + ".txt"
        weather_path = f"Daily_Weather_Data{path_slash}" + loc_dict.get(location)[1] + path_slash + loc_dict.get(location)[1] + night_date + ".txt"

        # # checkpoint
        # print(weather_path)
        # # checkpoint
        # print(sqm_path)

        # Check if SQM data exists for this night ----------------------------------------
        try:
            with open(sqm_path, 'r') as sqm_file: # if this works, then the SQM data exists
                sqm_data_raw = sqm_file.readlines()
                # Convert each line into a tuple and create a list of tuples
                sqm_data = [tuple(line.strip().split(';')) for line in sqm_data_raw]

        except FileNotFoundError: # no SQM data 
            print(f'NO SQM DATA for {night_date}. Moving On')
            # Flag for no SQM data
            df.loc[df['Night Date'] == night_date, 'No SQM Flag'] = True
            continue  # Go to next night

        # Check if weather data exists for this night --------------------------------------
        
        try: # if this works, weather data exists
            with open(weather_path, 'r') as weather_file:


                weather_data_raw = weather_file.readlines()
                weather_data = [tuple(line.strip().split(';')) for line in weather_data_raw]
                no_weather_data = False # there is weather data

        except FileNotFoundError: # weather data doesn't exist
            # print(f'NO WEATHER DATA for {night_date}.')
            no_weather_data = True # there isn't weather data




        # NO WEATHER DATA ---------------------------------------------------------------------
        # If there's no weather data, flag the row accordingly and update the SQM measurement
        # If there's intermittent SQM data, reuse an old measurement one time (and flag accordingly),
        # then don't reuse that measurement anymore (and flag as missing SQM data)

        # Filter the DataFrame based on the presence of weather data
        if no_weather_data:
            # Update DataFrame rows with no weather data
            df.loc[df['Night Date'] == night_date, 'No Weather Flag'] = True

            df = populate_sqm_vals(night_date, df, sqm_data)


        # WE HAVE WEATHER AND SQM DATA ---------------------------------------------------------------  
        # Populate the SQM and weather data
        # If there's intermittent SQM data, reuse an old measurement one time (and flag accordingly),
        # then don't reuse that measurement anymore (and flag as missing SQM data)

        # Also, flag the weather data if the weather data is old, 
        # if there are multiple weather measurements for one interval etc...
        else: # NOW let's look at the rows where weather data is available

            # Update DataFrame for rows with SQM data and Weather Data
            df = populate_sqm_vals(night_date, df, sqm_data) 
            df = populate_weather_vals(night_date, df, weather_data)
    

    # save dataframe to the desired path 
    save_dir = 'Combined Data Tables'
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    save_path = os.path.join(save_dir, f'{location.replace(" ", "_")}_data_{start_datetime.strftime("%Y-%m-%d")}_{end_datetime.strftime("%Y-%m-%d")}.csv')
    df.to_csv(save_path, index=False, mode='w')



    return df



# Classify Rows
def classify_rows(df):

    # This mitigates a deprecated functionality of pandas library...
    # it doesn't like adding strings withouut this casting first
    df['Row Classification'] = df['Row Classification'].astype('object')
    df['Row Classification Confidence'] = df['Row Classification Confidence'].astype('object')

        
    # Check cloud cover and set Row Classification and Confidence accordingly
    df.loc[(df['Cloud Cover'] == 0) & (df['Sun Up'] == False) & (df['Moon Up'] == False), 'Row Classification'] = 'Clear'
    df.loc[(df['Cloud Cover'] == 0) & (df['Sun Up'] == False) & (df['Moon Up'] == False), 'Row Classification Confidence'] = True

    df.loc[(df['Cloud Cover'] == 8) & (df['Sun Up'] == False) & (df['Moon Up'] == False), 'Row Classification'] = 'Overcast'
    df.loc[(df['Cloud Cover'] == 8) & (df['Sun Up'] == False) & (df['Moon Up'] == False), 'Row Classification Confidence'] = True

    df.loc[(df['Cloud Cover'].isin([1, 2])) & (df['Sun Up'] == False) & (df['Moon Up'] == False), 'Row Classification'] = 'Clear'
    df.loc[(df['Cloud Cover'].isin([1, 2])) & (df['Sun Up'] == False) & (df['Moon Up'] == False), 'Row Classification Confidence'] = False

    df.loc[(df['Cloud Cover'].isin([6, 7])) & (df['Sun Up'] == False) & (df['Moon Up'] == False), 'Row Classification'] = 'Overcast'
    df.loc[(df['Cloud Cover'].isin([6, 7])) & (df['Sun Up'] == False) & (df['Moon Up'] == False), 'Row Classification Confidence'] = False

    return df



# Classify Nights
def classify_nights(df):

    # This mitigates a deprecated functionality of pandas library...
    # it doesn't like adding strings withouut this casting first
    df['Night Classification'] = df['Night Classification'].astype('object')
    df['Night Classification Confidence'] = df['Night Classification Confidence'].astype('object')
    
    # make a list of unique nights in the df
    nights_list = df['Night Date'].unique()

    # Loop through each night for the location
    for night_date in nights_list:

        # filter the df to only look at night time (when sun up = false)
        night_df = df[(df["Sun Up"] == False) & (df["Night Date"] == night_date)]

        
        # Look at all the row classifications and confidences
        row_classifications = night_df['Row Classification'].tolist()
        row_classifications_confs = night_df['Row Classification Confidence'].tolist()

        # if all row classifications are clear, the night is clear

        # Check if all entries are 'Clear'
        clear_night = all(row_classification == 'Clear' for row_classification in row_classifications) # True if all entries in the list are clear
        overcast_night = all(row_classification == 'Overcast' for row_classification in row_classifications) # True if all entries in the list are overcast

        classification_confidence_night = all(row_classification_conf == True for row_classification_conf in row_classifications_confs) # True if all entries in the list are clear

        
        if clear_night: 
            
            # if night is clear, update night classification for entire night df to clear
            df.loc[(df['Night Date'] == night_date) & (df['Sun Up'] == False), "Night Classification"] = "Clear"

            # Update night classification confidence based on confidence values
            df.loc[(df['Night Date'] == night_date) & (df['Sun Up'] == False), "Night Classification Confidence"] = classification_confidence_night

        elif overcast_night:
            # if night is overcast, update night classification for entire night df to overcast
            df.loc[(df['Night Date'] == night_date) & (df['Sun Up'] == False), "Night Classification"] = "Overcast"

            # Update night classification confidence based on confidence values
            df.loc[(df['Night Date'] == night_date) & (df['Sun Up'] == False), "Night Classification Confidence"] = classification_confidence_night


    return df



""" 
Functions for the plots
"""


# Make Histogram
def make_hist(df, location, classification, high_confidence, row_or_night):

    """ 
    ARGS:
        - df: pandas dataframe
        - classification: str - "Clear" or "Overcast" 
        - high_confidence: bool - True for strict classifications, False for less strict classifications
    """

    # read in the the dates
    start_date_str = df.at[0, "Night Date"]
    last_row_index = df.index[-1]
    end_date_str = df.at[last_row_index, "Night Date"]

    # row or night
    if row_or_night == "Row":
        row_or_night_str = "Measurements"
    elif row_or_night == "Night":
        row_or_night_str = "Nights"
    
    # filter the df based on the classification and confidence conditions

    # if the row_or_night is night, then only look at the unique nights
    if row_or_night == "Night":
        
        df = df[df["Sun Up"]==False] # make sure we only look at the night (classifications are stored only for night)
        df = df.drop_duplicates(subset='Night Date', keep='first')

        bin_width = 0.5 # bigger bin width because there's less data

    else: 
        bin_width = 0.1 # smaller bin width for lots of measurements 

    # if high_confidence is True, only pick out rows where Row Classification Confidence is True
    # otherwise, we don't care what the confidence is
    if high_confidence == True:
        filtered_df = df[(df["Row Classification"] == classification) & df[f"{row_or_night} Classification Confidence"] == high_confidence]
    elif high_confidence == False:
        filtered_df = df[(df["Row Classification"] == classification)]

    sqms = filtered_df["SQM"].dropna().to_list()
    num_sqms = len(sqms)

    if high_confidence == True:
        conf_str = "Strict Classifications"

    elif high_confidence != True:
        conf_str = "Lenient Classifications"


    # choose color based on classification
    if classification == "Clear":
        color = "black"
    elif classification == "Overcast":
        color = "green"


    # now, find averages
    sqm_avg = np.mean(sqms)


    # now, plot the histogram
    bins = np.arange(11.95, 22.05, bin_width)
    plt.figure()
    plt.hist(sqms, bins, color=color, alpha=0.6)

    # Plot the mean sky brightness as a grey dotted line
    plt.axvline(x=sqm_avg, color=color, linestyle='dotted', linewidth=1.5, label=f'Mean Sky Brightness: {sqm_avg:.2f} mag/arcsec$^2$')

    # Add legend
    plt.legend()

    # Title
    title = f"{location.replace('_', ' ')}: {start_date_str.replace('-', '/')} - {end_date_str.replace('-', '/')}"
    plt.title(title, fontsize=16, loc='center', pad=20)

    # Subtitle
    subtitle = f"{num_sqms} {classification} {row_or_night_str} -- {conf_str}"
    plt.text(0.5, 1.03, subtitle, ha='center', va='center', fontsize=12, transform=plt.gca().transAxes)

    plt.xlabel(r'Sky Brightness [mag/arcsec$^2$]', fontsize=12, ha='center', labelpad=0)
    plt.minorticks_on()
    plt.grid()
    
    plt_save_path = os.path.join("histograms", "single histograms", f"{location.replace(' ', '_')}_hist_{start_date_str}_{end_date_str}_{classification}_{row_or_night_str}_{conf_str.replace(' ', '_')}")
    # Ensure the directory exists
    os.makedirs(os.path.dirname(plt_save_path), exist_ok=True)
    plt.savefig(plt_save_path)
    plt.close()
    # plt.clf()



# # Make Stacked Histogram
# def make_stacked_hist(df, location, high_confidence, row_or_night, save_fig=True):

#     """ 
#     ARGS:
#         - df: pandas dataframe
#         - classification: str - "Clear" or "Overcast" 
#         - high_confidence: bool - True for strict classifications, False for less strict classifications
#     """

#     # read in the the dates
#     start_date_str = df.at[0, "Night Date"]
#     last_row_index = df.index[-1]
#     end_date_str = df.at[last_row_index, "Night Date"]

#     # row or night
#     if row_or_night == "Row":
#         row_or_night_str = "Measurements"
#     elif row_or_night == "Night":
#         row_or_night_str = "Nights"
    
#     # filter the df based on the classification and confidence conditions

#     # if the row_or_night is night, then only look at the unique nights
#     if row_or_night == "Night":
        
#         df = df[df["Sun Up"]==False] # make sure we only look at the night (classifications are stored only for night)
#         df = df.drop_duplicates(subset='Night Date', keep='first')

#         bin_width = 0.5 # bigger bin width because there's less data

#     else: 
#         bin_width = 0.1 # smaller bin width for lots of measurements 

#     # if high_confidence is True, only pick out rows where Row Classification Confidence is True
#     # otherwise, we don't care what the confidence is

#     if high_confidence == True:
#         clear_df = df[(df["Row Classification"] == "Clear") & df[f"{row_or_night} Classification Confidence"] == high_confidence]
#         overcast_df = df[(df["Row Classification"] == "Overcast") & df[f"{row_or_night} Classification Confidence"] == high_confidence]
    
#     elif high_confidence == False:
#         clear_df = df[(df["Row Classification"] == "Clear")]
#         overcast_df = df[(df["Row Classification"] == "Overcast")]

#     clear_sqms = clear_df["SQM"].dropna().to_list()
#     num_clear_sqms = len(clear_sqms)

#     overcast_sqms = overcast_df["SQM"].dropna().to_list()
#     num_overcast_sqms = len(overcast_sqms)

#     if high_confidence == True:
#         conf_str = "Strict Classifications"

#     elif high_confidence != True:
#         conf_str = "Lenient Classifications"

        
#     # now, find averages
#     clear_sqm_avg = np.mean(clear_sqms)
#     overcast_sqm_avg = np.mean(overcast_sqms)

#     # Calculate the difference
#     difference = clear_sqm_avg - overcast_sqm_avg


#     # now, plot the histogram
#     bins = np.arange(11.95, 22.05, bin_width)
#     plt.figure()
#     _, _, patches = plt.hist([overcast_sqms, clear_sqms],bins, histtype = 'stepfilled', alpha = 0.6, color=["green", "black"], label=["Overcast", "Clear"])


#     # Plot the mean sky brightness as a grey dotted line
#     plt.axvline(x=clear_sqm_avg, color='black', linestyle='dotted', linewidth=1.5, label=f'Mean (Clear): {clear_sqm_avg:.2f} mag/arcsec$^2$')
#     plt.axvline(x=overcast_sqm_avg, color='green', linestyle='dotted', linewidth=1.5, label=f'Mean (Overcast): {overcast_sqm_avg:.2f} mag/arcsec$^2$')


#      # Add legend

#      # Extract the legend handles for Clear and Overcast histograms
#     clear_patch = plt.Rectangle((0, 0), 1, 1, fc="black", edgecolor = 'black', alpha=0.6, label="Clear")
#     overcast_patch = plt.Rectangle((0, 0), 1, 1, fc="green", edgecolor = 'green', alpha=0.6, label="Overcast")

    
#     legend_entry_diff = f'Difference: {difference:.2f}'
#     plt.legend(handles=[clear_patch,
#                         overcast_patch,
#                         plt.Line2D([0], [0], color='black', linestyle='dotted', label=f'Clear Average: {clear_sqm_avg:.2f}'),
#                         plt.Line2D([0], [0], color='green', linestyle='dotted', label=f'Overcast Average: {overcast_sqm_avg:.2f}'),
#                         plt.Line2D([0], [0], alpha=0, label=legend_entry_diff)
#                         ])

#     # Title
#     title = f"{location.replace('_', ' ')}: {start_date_str.replace('-', '/')} - {end_date_str.replace('-', '/')}"
#     plt.title(title, fontsize=16, loc='center', pad=20)

#     # Subtitle
#     subtitle = f"{num_clear_sqms} Clear & {num_overcast_sqms} Overcast {row_or_night_str} -- {conf_str}"
#     plt.text(0.5, 1.03, subtitle, ha='center', va='center', fontsize=10, transform=plt.gca().transAxes)


#     plt.xlabel(r'Sky Brightness [mag/arcsec$^2$]', fontsize=12, ha='center', labelpad=0)
#     plt.minorticks_on()
#     plt.grid()
    
#     if save_fig:
#         plt_save_path = os.path.join("histograms", "stacked histograms", f"{location.replace(' ', '_')}_stacked-hist_{start_date_str}_{end_date_str}_{row_or_night_str}_{conf_str.replace(' ', '_')}")
#         plt.savefig(plt_save_path)
#         # plt.clf()



# 


def make_stacked_hist(df, location, high_confidence, row_or_night, ax=None, save_fig=True):
    """ 
    ARGS:
        - df: pandas dataframe
        - location: str - location name
        - high_confidence: bool - True for strict classifications, False for less strict classifications
        - row_or_night: str - "Row" or "Night"
        - ax: matplotlib axis (optional) - subplot axis
        - save_fig: bool (optional) - whether to save the figure
    """

    # read in the the dates
    start_date_str = df.at[0, "Night Date"]
    last_row_index = df.index[-1]
    end_date_str = df.at[last_row_index, "Night Date"]

    # row or night
    if row_or_night == "Row":
        row_or_night_str = "Measurements"
    elif row_or_night == "Night":
        row_or_night_str = "Nights"
    
    # filter the df based on the classification and confidence conditions

    # if the row_or_night is night, then only look at the unique nights
    if row_or_night == "Night":
        df = df[df["Sun Up"]==False] # make sure we only look at the night (classifications are stored only for night)
        df = df.drop_duplicates(subset='Night Date', keep='first')

        bin_width = 0.5 # bigger bin width because there's less data
    else: 
        bin_width = 0.1 # smaller bin width for lots of measurements 

    # if high_confidence is True, only pick out rows where Row Classification Confidence is True
    # otherwise, we don't care what the confidence is
    if high_confidence == True:
        clear_df = df[(df["Row Classification"] == "Clear") & df[f"{row_or_night} Classification Confidence"] == high_confidence]
        overcast_df = df[(df["Row Classification"] == "Overcast") & df[f"{row_or_night} Classification Confidence"] == high_confidence]
    elif high_confidence == False:
        clear_df = df[(df["Row Classification"] == "Clear")]
        overcast_df = df[(df["Row Classification"] == "Overcast")]

    clear_sqms = clear_df["SQM"].dropna().to_list()
    num_clear_sqms = len(clear_sqms)

    overcast_sqms = overcast_df["SQM"].dropna().to_list()
    num_overcast_sqms = len(overcast_sqms)

    if high_confidence == True:
        conf_str = "Strict Classifications"
    elif high_confidence != True:
        conf_str = "Lenient Classifications"

    # now, find averages
    clear_sqm_avg = np.mean(clear_sqms)
    overcast_sqm_avg = np.mean(overcast_sqms)

    # Calculate the difference
    difference = clear_sqm_avg - overcast_sqm_avg

    # Initialize a new figure or use the provided axis
    if ax is None:
        plt.figure()
        ax = plt.gca()

    # now, plot the histogram
    bins = np.arange(11.95, 22.05, bin_width)
    _, _, patches = ax.hist([overcast_sqms, clear_sqms], bins, histtype='stepfilled', alpha=0.6, color=["green", "black"], label=["Overcast", "Clear"])

    # Plot the mean sky brightness as a grey dotted line
    ax.axvline(x=clear_sqm_avg, color='black', linestyle='dotted', linewidth=1.5, label=f'Mean (Clear): {clear_sqm_avg:.2f} mag/arcsec$^2$')
    ax.axvline(x=overcast_sqm_avg, color='green', linestyle='dotted', linewidth=1.5, label=f'Mean (Overcast): {overcast_sqm_avg:.2f} mag/arcsec$^2$')

    # Add legend
    clear_patch = plt.Rectangle((0, 0), 1, 1, fc="black", edgecolor='black', alpha=0.6, label="Clear")
    overcast_patch = plt.Rectangle((0, 0), 1, 1, fc="green", edgecolor='green', alpha=0.6, label="Overcast")
    
    legend_entry_diff = f'Difference: {difference:.2f}'
    ax.legend(handles=[clear_patch,
                        overcast_patch,
                        plt.Line2D([0], [0], color='black', linestyle='dotted', label=f'Clear Average: {clear_sqm_avg:.2f}'),
                        plt.Line2D([0], [0], color='green', linestyle='dotted', label=f'Overcast Average: {overcast_sqm_avg:.2f}'),
                        plt.Line2D([0], [0], alpha=0, label=legend_entry_diff)
                        ])

    # Title
    title = f"{location.replace('_', ' ')}: {start_date_str.replace('-', '/')} - {end_date_str.replace('-', '/')}"
    ax.set_title(title, fontsize=16, loc='center', pad=20)

    # Subtitle
    subtitle = f"{num_clear_sqms} Clear & {num_overcast_sqms} Overcast {row_or_night_str} -- {conf_str}"
    ax.text(0.5, 1.03, subtitle, ha='center', va='center', fontsize=10, transform=ax.transAxes)

    ax.set_xlabel(r'Sky Brightness [mag/arcsec$^2$]', fontsize=12, ha='center', labelpad=0)
    ax.minorticks_on()
    ax.grid()
    
    if save_fig:
        plt_save_path = os.path.join("histograms", "stacked histograms", f"{location.replace(' ', '_')}_stacked-hist_{start_date_str}_{end_date_str}_{row_or_night_str}_{conf_str.replace(' ', '_')}")
        os.makedirs(os.path.dirname(plt_save_path), exist_ok=True)
        plt.savefig(plt_save_path)
        plt.close()
        # plt.clf()  # Uncomment if you want to clear the figure after saving


