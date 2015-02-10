
import pandas as pd
#import LatLon
import math

def distance_on_unit_sphere(lat1, long1, lat2, long2):

    rayon_terre = 6373
    # Convert latitude and longitude to
    # spherical coordinates in radians.
    degrees_to_radians = math.pi/180.0

    # phi = 90 - latitude
    phi1 = (90.0 - lat1)*degrees_to_radians
    phi2 = (90.0 - lat2)*degrees_to_radians

    # theta = longitude
    theta1 = long1*degrees_to_radians
    theta2 = long2*degrees_to_radians

    # Compute spherical distance from spherical coordinates.

    # For two locations in spherical coordinates
    # (1, theta, phi) and (1, theta, phi)
    # cosine( arc length ) =
    #    sin phi sin phi' cos(theta-theta') + cos phi cos phi'
    # distance = rho * arc length

    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) +
           math.cos(phi1)*math.cos(phi2))
    arc = math.acos( cos )

    # Remember to multiply arc by the radius of the earth
    # in your favorite set of units to get length.
    return rayon_terre*arc
    
def findListVilles(SeismeLatitude,SeismeLongitude):
    Villes_List = []
    rayon=500
    '''rows = session.execute('SELECT name, age, email FROM users')
    for user_row in rows:
    print user_row.name, user_row.age, user_row.email
    ===> data.values : rows'''
    #Creation de la dataFrame Ville    
    columns=['Ville','Lat','Long']
    data=[['Tok',35.732727,139.722404],
    ['Yok', 35.462635 , 139.774854],
    ['Osa',34.705359,135.500729],
    ['Nag',35.193866,136.907394],
    ['Sap',43.179025,141.388028],
    ['Kob',34.699714,135.187619],
    ['Fuk',33.643127,130.355035],
    ['Kyo',35.043493,135.771593],
    ['Kaw',35.557485,139.698357],
    ['Sai',35.867481,139.642576]]
    Villes=pd.DataFrame(data=data, columns=columns)

    for line in Villes.values:
        distance = distance_on_unit_sphere(SeismeLatitude,SeismeLongitude,line[1],line[2])
        if distance<rayon:
            Villes_List.append(line[0])
    return Villes_List

#Result = findListVilles(35.0,135.0)

def getClosest(SeismeLatitude,SeismeLongitude):
    #Creation de la dataFrame Ville    
    columns=['Ville','Lat','Long']
    data=[['Tok',35.732727,139.722404],
    ['Yok', 35.462635 , 139.774854],
    ['Osa',34.705359,135.500729],
    ['Nag',35.193866,136.907394],
    ['Sap',43.179025,141.388028],
    ['Kob',34.699714,135.187619],
    ['Fuk',33.643127,130.355035],
    ['Kyo',35.043493,135.771593],
    ['Kaw',35.557485,139.698357],
    ['Sai',35.867481,139.642576]]
    Villes=pd.DataFrame(data=data, columns=columns)

    mindist=float("inf")
    closest=''
    i=1
    for line in Villes.values:
        distance = distance_on_unit_sphere(SeismeLatitude,SeismeLongitude,line[1],line[2])
        if distance<mindist:
            mindist=distance
            closest=line[0]
            node=i
        i+=1
    print str(closest) +": shutdown the nodes =>"+ str(node%5)
    return closest, node%5



#Result, nodes = getClosest(35.0,135.0)
