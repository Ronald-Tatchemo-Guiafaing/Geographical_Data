import geopandas as gpd
import pymongo
import sys
import json
import numpy as np
import pandas as pd  

def load_shapefile_to_mongodb(shapefile_path):
    
    if not shapefile_path.endswith('.shp'):
        print("Please provide a valid shapefile path.")
        sys.exit(1)
    
    
    try:
        
        gdf = gpd.read_file(shapefile_path)
        
        
        date_columns = ['creation_d', 'last_updat']
        for col in date_columns:
            if col in gdf.columns:
                gdf[col] = gdf[col].astype(str).replace('nan', '')
        
        
        float_columns = ['district', 'county_fip', 'countynum', 'shape_Leng', 'shape_Area']
        for col in float_columns:
            if col in gdf.columns:
                gdf[col] = gdf[col].apply(lambda x: str(x) if pd.notnull(x) else '')
        
        
        print("Shapefile Information:")
        print(f"Total Features: {len(gdf)}")
        print(f"Columns: {list(gdf.columns)}")
        print(f"Coordinate Reference System (CRS): {gdf.crs}")
        print("\nFirst few rows:")
        print(gdf.head())
    except Exception as e:
        print(f"Error reading shapefile: {e}")
        sys.exit(1)

    
    try:
        
        features = []
        
        
        for idx, row in gdf.iterrows():
            
            feature = {
                "type": "Feature",
                "geometry": row['geometry'].__geo_interface__,
                "properties": {}
            }
            
            
            for col in gdf.columns:
                if col != 'geometry':
                    
                    value = row[col]
                    
                    if isinstance(value, (np.integer, np.floating)):
                        value = float(value)
                    
                    feature["properties"][col] = str(value) if pd.notnull(value) else None
            
            features.append(feature)
        
        # Create the complete GeoJSON structure
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
    except Exception as e:
        print(f"Error converting to GeoJSON: {e}")
        sys.exit(1)

    
    try:
        
        client = pymongo.MongoClient('mongodb://localhost:27017/')
        
        
        if 'InvasiveSpeciesExtremeConcernAreas_Watersheds' in shapefile_path:
            db = client['dc_watersheds']
            collection = db['invasive_species']
        else:  
            db = client['maryland_counties']
            collection = db['county_boundaries']
        
        
        collection.delete_many({})
        
        
        if features:
            collection.insert_many(geojson['features'])
            print(f"\nSuccessfully inserted {len(features)} documents into {db.name}.{collection.name}")
        else:
            print("No features found in the shapefile")
    except Exception as e:
        print(f"Error connecting to MongoDB or inserting data: {e}")
        sys.exit(1)

# Main execution
if __name__ == "__main__":
    
    if len(sys.argv) < 2:
        print("Please provide the shapefile path as an argument.")
        sys.exit(1)

    shapefile_path = sys.argv[1]
    load_shapefile_to_mongodb(shapefile_path)
