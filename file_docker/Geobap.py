from turfpy.measurement import boolean_point_in_polygon, bbox
from turfpy.meta import feature_each
from geojson import Point
from databases import world_hd, region_hd, province_hd, municipality_hd, square, world, region, province, municipality, square_hd

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
    def geoinfo_old(lat, lon, zoom):
        if zoom < 0 or zoom > 4:
            return None
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
        if zoom < 0 or zoom > 4:
            result = {"error": "Unable to geodecode"}
        result = {}
        query = {"geometry": {"$geoIntersects": {"$geometry": {"type": "Point", "coordinates": [lon, lat]}}}}
        if zoom >= 0:
            naz = world_hd.find_one(query,{"naz_name":1, "_id":0})
            if naz != None:
                result["nazione"] = naz["naz_name"]
            else:
                result = {"error": "Unable to geodecode"}
            if zoom >= 1:
                reg = region_hd.find_one(query,{"properties":1, "_id":0})
                if reg != None:
                    result["regione"] = reg["properties"]["name"]
                else:
                    result = {"error": "Unable to geodecode"}
                if zoom >= 2:
                    prov = province_hd.find_one(query,{"properties":1, "_id":0})
                    if prov != None:
                        result["provincia"] = prov["properties"]["name"]
                    else:
                        result = {"error": "Unable to geodecode"}
                    if zoom >= 3:
                        com = municipality_hd.find_one(query,{"properties":1, "_id":0})
                        if com != None:
                            result["comune"] = com["properties"]["name"]
                        else:
                            result = {"error": "Unable to geodecode"}
                        if zoom == 4:
                            square = square_hd.find_one(query,{"properties":1, "_id":0})
                            if square != None:
                                result["squareID"] = square["properties"]["name"]
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
        collections = [world, region, province, municipality, square]
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
    print(Geobap.geoinfo(39.251333, 11.547637, 4))