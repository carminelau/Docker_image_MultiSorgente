from geopy import distance
import numpy as np
from turfpy.measurement import boolean_point_in_polygon, bbox
from turfpy.meta import feature_each
from geojson import Point
from databases import world, region, province, municipality, squares

class Geobap:

    def __init__(self, geojson=None, geomap=None):
        self.geojson=geojson
        if geojson!=None:
            if(geomap!=None):
                self.geomap=geomap
            else:
                self.geomap= self.getGeomap(geojson)
        else:
            self.geomap=geomap

    # Esegue il geodecode su più livello (in base al livello di zoom richiesto)
    @staticmethod
    def geoinfo_base(lat, lon, zoom):
        if zoom < 0 or zoom > 4:
            return {"error": "Unable to geodecode"}
        result = {}
        geo = Geobap()
        for i in range(0, zoom+1):
            geo.setGeojson(Geobap.getGeojson(i, result))
            result[Geobap.getZoomName(i)] = geo.geodecode(lat, lon)
        for key in result.keys():
            if result[key]=="Unable to geodecode":
                myjson = {"error": "Unable to geodecode"}
                return myjson
        return result

    @staticmethod
    def geoinfo(lat, lon, zoom):
        places = {}
        squareID = None
        geo = Geobap.geoinfo_base(lat, lon, zoom)
        if "error" in geo.keys():
            square_trovati = {}
            for i in np.arange(-0.0105, 0.012, 0.0105):
                for j in np.arange(-0.0105, 0.012, 0.0105):
                    geo = Geobap.geoinfo_base(lat + i, lon + j, 4)
                    if "squareID" in geo:
                        places[geo["squareID"]] = geo
                        if geo["provincia"] in square_trovati:
                            if geo["comune"] in square_trovati[geo["provincia"]]:
                                square_trovati[geo["provincia"]][geo["comune"]].append(geo["squareID"])
                            else:
                                square_trovati[geo["provincia"]][geo["comune"]] = [geo["squareID"]]
                        else:
                            square_trovati[geo["provincia"]] = {geo["comune"]: [geo["squareID"]]}
            dis = 99999

            for prov in square_trovati:
                sqs = squares.find_one({"prov": prov})
                for com in square_trovati[prov]:
                    for s in sqs["comuni"][com]["features"]:
                        if s["properties"]["name"] in square_trovati[prov][com]:
                            coordinates = s["bbox"]
                            lat_square = (coordinates[1] + coordinates[3])/2
                            lon_square = (coordinates[0] + coordinates[2])/2
                            new_dis = distance.distance((lat, lon), (lat_square, lon_square), ellipsoid='WGS-84').m
                            if new_dis < dis:
                                squareID = s["properties"]["name"]
                                dis = new_dis

        else:
            squareID = geo["squareID"]
            places[squareID] = geo

        if squareID != None:
            result = places[squareID]
        else:
            result = {"error": "Unable to geodecode"}
        return result
    

    # Esegue il geodecode su un solo livello
    def geodecode(self, lat, lon):
        try :
            if(self.geojson==None or self.geomap==None):
                raise ("Must define a geojson/geomap")
            if(isinstance(lat, float)==False or isinstance(lon, float)==False):
                raise ("lat/lon is not a number")
            match = self.match_square(lat, lon)
            for element in match:
                geo = self.geojson["features"][element["pos"]]
                if geo["geometry"]["type"] == "MultiPolygon":
                    for coords in geo["geometry"]["coordinates"]:
                        feature = {'type':'Polygon','coordinates':coords}
                        if boolean_point_in_polygon(Point([lon, lat]), feature) == True:
                            return element["name"]
                else:
                    if boolean_point_in_polygon(Point([lon, lat]), geo) == True:
                        return element["name"]
            return "Unable to geodecode"
        except:
            return "Unable to geodecode"

    # Restituisce le info sulle bbox nelle quali è presente il punto 
    def match_square(self, lat, lon):
        match = []
        for element in self.geomap:
            if lat > element["bottom_left"]["lat"] and lat < element["top_right"]["lat"]:
                if lon > element["bottom_left"]["lon"] and lon < element["top_right"]["lon"]:
                    match.append({
                        "name": element["name"],
                        "pos": element["pos"]
                    })
        return match

    # Genera il file geomap relativo al Geojson
    def getGeomap(self, geojson):
        geomap=[]
        def funcname(currentFeature, featureIndex):
            nonlocal geomap
            square= bbox(currentFeature)
            geomap.append({
                'name':currentFeature["properties"]["name"],
                'bottom_left': {
                    'lon': square[0],
                    'lat': square[1]
                },
                'top_right': {
                    'lon':square[2],
                    'lat':square[3]
                },
                'pos': featureIndex
            })
            return True
        if geojson!=None:
            feature_each(geojson, funcname)
            return geomap
        else:
            return None

    # Restituisce il nome relativo al livello di zoom
    @staticmethod
    def getZoomName(zoom=0):
        names = [
            "nazione",
            "regione",
            "provincia",
            "comune",
            "squareID"
        ]
        return names[zoom]

    # Aggiorna il file geojson (e geomap) usato
    def setGeojson(self, geojson, geomap=None):
        self.geojson = geojson
        if geomap != None:
            self.geomap = geomap
        else:
            self.geomap = self.getGeomap(geojson)

    # Restituisce il geojson da usare in base al livello di zoom
    @staticmethod
    def getGeojson(zoom, params=None):
        collections = [world, region, province, municipality, squares]
        query = ["None", "naz_name", "reg_name", "prov_name", "prov"]
        req_form = {}
        if(params != None):
            for key in params:
                req_form[key] = params[key]
        if zoom == 0:
            geojson = collections[zoom].find_one()
        elif zoom == 4:
            temp = collections[zoom].find_one({query[zoom]: req_form[Geobap.getZoomName(zoom-2)]}) 
            if temp!=None:
                geojson = temp["comuni"][req_form[Geobap.getZoomName(zoom-1)]]
            else:
                geojson = None
        else:
            geojson = collections[zoom].find_one({query[zoom]: req_form[Geobap.getZoomName(zoom-1)]})
        if zoom != 4 and geojson!=None:
            del geojson["_id"]
        return geojson

# Main di test della libreria
if __name__ == "__main__":
    print(Geobap.geoinfo(40.6755282563532, 14.768886566162111,4))
    #40.946115837367785, 14.37039205328215